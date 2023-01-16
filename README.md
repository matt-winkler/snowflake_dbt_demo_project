Welcome to the dbt Labs demo dbt project! We use the [TPCH dataset](https://docs.snowflake.com/en/user-guide/sample-data-tpch.html) to create a sample project to emulate what a production project might look like!

                        _              __                   
       ____ ___  ____ _(_)___     ____/ /__  ____ ___  ____ 
      / __ `__ \/ __ `/ / __ \   / __  / _ \/ __ `__ \/ __ \
     / / / / / / /_/ / / / / /  / /_/ /  __/ / / / / / /_/ /
    /_/ /_/ /_/\__,_/_/_/ /_/   \__,_/\___/_/ /_/ /_/\____/ 

## Special demos

- **dbt-external-tables:** Manage database objects that read data external to the warehouse within dbt. See `models/demo_examples/external_sources.yml`.
- **snapshots:** Create versioned datasets to track Type II changes. See `snapshots/source_data__snapshot.sql` and `models/demo_examples/snapshots/source_data.sql` for working code.
- **Airflow with dbt Cloud:** Trigger dbt Cloud jobs to run remotely via Apache Airflow. See instructions [here](airflow/README.md) for how to get started.
