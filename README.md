# User Behaviours Analytics


## Requirements:
1. Given raw OLTP data of user behaviour, perform transformation to make it easy to do analytics on it.
2. You can use any type of infra to setup to perform the task
3. Goal is to learn data pipeline.


## Technical Steps

1. Use cron job to perform a task at schedule time
2. Write a python script to read and transform the data.
3. Use Metabase to let user analyse the data.
4. Use DuckDb as Data Warehouse which is a embedded olap database.

## Non-Functional Requirements

1. Maintainable transformation
2. Easy to schedule a cron job, multiple

## High Level Design

![Alt text](High_level_design.png)

## Tasks:

1. [ ] Local directory will acts as data lake, where data given by customer will dumped to transform
2. [ ] DuckDb to store after transform.
3. [ ] Make Cron Job configurable, rather than a subprocess.
4. [ ] Deploy metabase on docker.
5. [ ] Connect DuckDb to metabase.