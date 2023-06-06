import holidays, s3fs

def model( dbt,_):
    dbt.config(
        materialized='table',
        packages = ['holidays','s3fs'] # how to import python libraries in dbt's context
    )
    df = dbt.ref("fct_orders")
    df_describe = df.describe() # basic statistics profiling
    return df_describe