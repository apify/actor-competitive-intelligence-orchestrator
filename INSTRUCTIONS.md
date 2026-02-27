# Instructions

Create an Actor that will start runs based on configuration stored in the database, wait until they finish, and then store the datasets to the database.

The database being connected to is Supabase.

Input:
 - secret database connection string

The database will have three tables:
 - configurations = actor id + input
 - results = every item in every dataset
 - jobs = every run that has been started and it's final state

The runs should be started in parallel, if a total memory limit is encountered, retry the failed jobs.
