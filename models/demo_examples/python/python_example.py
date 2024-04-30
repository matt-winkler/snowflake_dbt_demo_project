import holidays, s3fs

def model(dbt, session):
    dbt.config(
        materialized='table',
        packages = ['holidays','s3fs'] # how to import python libraries in dbt's context
    )
    df = dbt.ref("fct_orders")
    upstream = dbt.builtins.ref('fct_orders').identifier
    df_describe = df.describe() # basic statistics profiling

    return df_describe