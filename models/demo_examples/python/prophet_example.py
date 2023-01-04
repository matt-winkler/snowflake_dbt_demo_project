import pandas as pd
from prophet import Prophet

def model( dbt,_):
    dbt.config(
        materialized='table',
        packages = ['Prophet'] # how to import python libraries in dbt's context
    )

    # Create a dataframe with a column ds (the time column) and a column y (the metric to forecast)
    df = dbt.ref("fct_orders").to_pandas()
    df_subset = df[['ORDER_DATE','GROSS_ITEM_SALES_AMOUNT']] # capitalization matters
    df_subset.rename(columns = {'ORDER_DATE':'ds', 'GROSS_ITEM_SALES_AMOUNT':'y'}, inplace=True)
    #df_subset['ds'] = pd.to_datetime(df_subset['ds'])
    #df_subset['ds'] = df_subset['ds'].dt.tz_localize(None)

    # use historical data to fit model
    m = Prophet()
    m.fit(df_subset)

    # forecast returns and output dataframe
    future = m.make_future_dataframe(periods=365)
    df_future = m.predict(future)

    return df_future