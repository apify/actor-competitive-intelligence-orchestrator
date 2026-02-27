import { setTimeout } from 'node:timers/promises';

import { createClient, type SupabaseClient } from '@supabase/supabase-js';
import { Actor, log } from 'apify';
import { config as loadEnv } from 'dotenv';

loadEnv({ override: true });

interface Configuration {
    id: string;
    actor_id: string;
    input: Record<string, unknown>;
}

const MAX_RETRIES = 2;
const BATCH_SIZE = 500;

const CREATE_TABLES_SQL = `
CREATE TABLE IF NOT EXISTS jobs (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    configuration_id uuid NOT NULL REFERENCES actor_configs(id),
    run_id text,
    status text NOT NULL,
    started_at timestamptz,
    finished_at timestamptz,
    error text
);

CREATE TABLE IF NOT EXISTS results (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id uuid NOT NULL REFERENCES jobs(id),
    data jsonb NOT NULL
);
`;

async function ensureTables(projectRef: string, managementToken: string) {
    const response = await fetch(`https://api.supabase.com/v1/projects/${projectRef}/database/query`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${managementToken}`,
        },
        body: JSON.stringify({ query: CREATE_TABLES_SQL }),
    });

    if (!response.ok) {
        const body = await response.text();
        log.error('Failed to create tables via Management API', { status: response.status, body });
        throw await Actor.fail('Failed to ensure tables exist. Check SUPABASE_MANAGEMENT_ACCESS_TOKEN and project ref.');
    }

    // Notify PostgREST to reload its schema cache so new tables are visible
    await fetch(`https://api.supabase.com/v1/projects/${projectRef}/database/query`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${managementToken}`,
        },
        body: JSON.stringify({ query: "NOTIFY pgrst, 'reload schema';" }),
    });

    // Brief wait for PostgREST to pick up the schema change
    await setTimeout(2000);

    log.info('Tables "jobs" and "results" are ready');
}

async function insertDatasetItems(supabase: SupabaseClient, jobId: string, datasetId: string) {
    const client = Actor.apifyClient;
    const dataset = client.dataset(datasetId);
    let offset = 0;
    const limit = 1000;

    while (true) {
        const { items } = await dataset.listItems({ offset, limit });
        if (items.length === 0) break;

        // Insert in batches
        for (let i = 0; i < items.length; i += BATCH_SIZE) {
            const batch = items.slice(i, i + BATCH_SIZE);
            const rows = batch.map((item) => ({ job_id: jobId, data: item }));
            const { error } = await supabase.from('results').insert(rows);
            if (error) {
                log.warning(`Failed to insert results batch for job ${jobId}`, { error: error.message });
            }
        }

        offset += items.length;
        if (items.length < limit) break;
    }
}

async function runWithRetries(
    supabase: SupabaseClient,
    config: Configuration,
): Promise<void> {
    const client = Actor.apifyClient;

    for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
        if (attempt > 0) {
            log.info(`Retrying configuration ${config.id}, attempt ${attempt + 1}/${MAX_RETRIES + 1}`);
        }

        // Start the run
        let run;
        try {
            run = await client.actor(config.actor_id).start(config.input);
        } catch (err) {
            const errorMessage = (err as Error).message;
            log.error(`Failed to start actor ${config.actor_id} for configuration ${config.id}`, {
                error: errorMessage,
            });

            const isMemErr = errorMessage.toLowerCase().includes('exceed the memory limit');

            if (isMemErr && attempt < MAX_RETRIES) {
                log.warning(`Start failed with memory error for configuration ${config.id}, will retry in 10s`);
                await setTimeout(10_000);
                continue;
            }

            // Insert a failed job record for non-retryable errors or final attempt
            await supabase.from('jobs').insert({
                configuration_id: config.id,
                status: 'FAILED',
                started_at: new Date().toISOString(),
                finished_at: new Date().toISOString(),
                error: errorMessage,
            });
            return;
        }

        log.info(`Started run ${run.id} for configuration ${config.id} (actor: ${config.actor_id})`);

        // Insert job row
        const { data: job, error: jobError } = await supabase
            .from('jobs')
            .insert({
                configuration_id: config.id,
                run_id: run.id,
                status: 'RUNNING',
                started_at: run.startedAt ?? new Date().toISOString(),
            })
            .select('id')
            .single();

        if (jobError || !job) {
            log.error(`Failed to insert job for run ${run.id}`, { error: jobError?.message });
            return;
        }

        // Wait for the run to finish
        let finishedRun;
        try {
            finishedRun = await client.run(run.id).waitForFinish();
        } catch (err) {
            log.error(`Error waiting for run ${run.id}`, { error: (err as Error).message });
            await supabase
                .from('jobs')
                .update({
                    status: 'FAILED',
                    finished_at: new Date().toISOString(),
                    error: (err as Error).message,
                })
                .eq('id', job.id);
            return;
        }

        // Update job with final status
        await supabase
            .from('jobs')
            .update({
                status: finishedRun.status,
                finished_at: finishedRun.finishedAt ?? new Date().toISOString(),
                error: finishedRun.statusMessage ?? null,
            })
            .eq('id', job.id);

        // If succeeded, fetch and store dataset items
        if (finishedRun.status === 'SUCCEEDED' && finishedRun.defaultDatasetId) {
            log.info(`Fetching dataset items for run ${run.id}`);
            await insertDatasetItems(supabase, job.id, finishedRun.defaultDatasetId);
        }

        // Done with this configuration (succeeded or non-retryable failure)
        return;
    }
}

// ---- Main ----

await Actor.init();

Actor.on('aborting', async () => {
    log.warning('Actor is aborting, exiting gracefully...');
    await setTimeout(1000);
    await Actor.exit();
});

// Read Supabase credentials from environment variables
const supabaseUrl = process.env.PUBLIC_SUPABASE_URL;
const supabaseKey = process.env.PUBLIC_SUPABASE_PUBLISHABLE_DEFAULT_KEY;
if (!supabaseUrl || !supabaseKey) {
    throw await Actor.fail('Missing required env vars: PUBLIC_SUPABASE_URL and PUBLIC_SUPABASE_PUBLISHABLE_DEFAULT_KEY');
}

const supabase = createClient(supabaseUrl, supabaseKey);

// Ensure jobs and results tables exist
const managementToken = process.env.SUPABASE_MANAGEMENT_ACCESS_TOKEN;
if (!managementToken) {
    throw await Actor.fail('Missing required env var: SUPABASE_MANAGEMENT_ACCESS_TOKEN');
}
const projectRef = new URL(supabaseUrl).hostname.split('.')[0];
await ensureTables(projectRef, managementToken);

// Fetch configurations
const { data: configurations, error: configError } = await supabase
    .from('actor_configs')
    .select('*');

if (configError) {
    throw await Actor.fail(`Failed to fetch configurations: ${configError.message}`);
}

if (!configurations || configurations.length === 0) {
    log.warning('No configurations found in Supabase, nothing to do.');
    await Actor.exit();
}

log.info(`Found ${configurations.length} configurations to process`);

// Run all configurations in parallel
await Promise.allSettled(
    configurations.map(async (config: Configuration) => runWithRetries(supabase, config)),
);

log.info('All configurations processed');
await Actor.exit();
