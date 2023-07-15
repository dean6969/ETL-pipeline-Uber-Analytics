import pandas as pd 
from io import BytesIO
from io import StringIO
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook


def transform_data(**kwargs):
    hook_data_lake = WasbHook('lake_gen_ingest_cases_death')
    blob_service_client = hook_data_lake.get_conn()

    container_name = kwargs["container_name"]   #"raw"
    blob_name = kwargs["path_file"]#"uber/uber_data.csv"

    # Get the blob client
    blob_client = blob_service_client.get_blob_client(container_name, blob_name)

    # Get the blob data in BytesIO object
    blob_data = BytesIO(blob_client.download_blob().readall())

    # Read the BytesIO object into a pandas DataFrame
    df = pd.read_csv(blob_data)

    #datetime dim
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    df = df.drop_duplicates().reset_index(drop=True)
    df['trip_id'] = df.index

    datetime_dim = df[['tpep_pickup_datetime','tpep_dropoff_datetime']].reset_index(drop=True)
    datetime_dim['tpep_pickup_datetime'] = datetime_dim['tpep_pickup_datetime']
    datetime_dim['pick_hour'] = datetime_dim['tpep_pickup_datetime'].dt.hour
    datetime_dim['pick_day'] = datetime_dim['tpep_pickup_datetime'].dt.day
    datetime_dim['pick_month'] = datetime_dim['tpep_pickup_datetime'].dt.month
    datetime_dim['pick_year'] = datetime_dim['tpep_pickup_datetime'].dt.year
    datetime_dim['pick_weekday'] = datetime_dim['tpep_pickup_datetime'].dt.weekday

    datetime_dim['tpep_dropoff_datetime'] = datetime_dim['tpep_dropoff_datetime']
    datetime_dim['drop_hour'] = datetime_dim['tpep_dropoff_datetime'].dt.hour
    datetime_dim['drop_day'] = datetime_dim['tpep_dropoff_datetime'].dt.day
    datetime_dim['drop_month'] = datetime_dim['tpep_dropoff_datetime'].dt.month
    datetime_dim['drop_year'] = datetime_dim['tpep_dropoff_datetime'].dt.year
    datetime_dim['drop_weekday'] = datetime_dim['tpep_dropoff_datetime'].dt.weekday


    datetime_dim['datetime_id'] = datetime_dim.index

    # datetime_dim = datetime_dim.rename(columns={'tpep_pickup_datetime': 'datetime_id'}).reset_index(drop=True)
    datetime_dim = datetime_dim[['datetime_id', 'tpep_pickup_datetime', 'pick_hour', 'pick_day', 'pick_month', 'pick_year', 'pick_weekday',
                                'tpep_dropoff_datetime', 'drop_hour', 'drop_day', 'drop_month', 'drop_year', 'drop_weekday']]

    print("done process datetime dimension")
    # passenger_dim
    passenger_count_dim = df[['passenger_count']].reset_index(drop=True)
    passenger_count_dim['passenger_count_id'] = passenger_count_dim.index
    passenger_count_dim = passenger_count_dim[['passenger_count_id','passenger_count']]

    print("done process passenger dimension")

    #trip distance dim
    trip_distance_dim = df[['trip_distance']].reset_index(drop=True)
    trip_distance_dim['trip_distance_id'] = trip_distance_dim.index
    trip_distance_dim = trip_distance_dim[['trip_distance_id','trip_distance']]

    print("done process trip dimension")

    #rate_code_dim
    rate_code_type = {
        1:"Standard rate",
        2:"JFK",
        3:"Newark",
        4:"Nassau or Westchester",
        5:"Negotiated fare",
        6:"Group ride"
    }


    rate_code_dim = df[['RatecodeID']].reset_index(drop=True)
    rate_code_dim['rate_code_id'] = rate_code_dim.index
    rate_code_dim['rate_code_name'] = rate_code_dim['RatecodeID'].map(rate_code_type)
    rate_code_dim = rate_code_dim[['rate_code_id','RatecodeID','rate_code_name']]

    print("done process rate code dimension")

    #pickup_location_dim
    pickup_location_dim = df[['pickup_longitude', 'pickup_latitude']].reset_index(drop=True)
    pickup_location_dim['pickup_location_id'] = pickup_location_dim.index
    pickup_location_dim = pickup_location_dim[['pickup_location_id','pickup_latitude','pickup_longitude']] 

    print("done process pickup location dimension")

    # dropoff_dim
    dropoff_location_dim = df[['dropoff_longitude', 'dropoff_latitude']].reset_index(drop=True)
    dropoff_location_dim['dropoff_location_id'] = dropoff_location_dim.index
    dropoff_location_dim = dropoff_location_dim[['dropoff_location_id','dropoff_latitude','dropoff_longitude']]

    print("done process dropoff location dimension")

    # payment_type_dime
    payment_type_name = {
        1:"Credit card",
        2:"Cash",
        3:"No charge",
        4:"Dispute",
        5:"Unknown",
        6:"Voided trip"
    }
    payment_type_dim = df[['payment_type']].reset_index(drop=True)
    payment_type_dim['payment_type_id'] = payment_type_dim.index
    payment_type_dim['payment_type_name'] = payment_type_dim['payment_type'].map(payment_type_name)
    payment_type_dim = payment_type_dim[['payment_type_id','payment_type','payment_type_name']]

    print("done process payment_type dimension")

    # fact_table
    fact_table = df.merge(passenger_count_dim, left_on='trip_id', right_on='passenger_count_id') \
                .merge(trip_distance_dim, left_on='trip_id', right_on='trip_distance_id') \
                .merge(rate_code_dim, left_on='trip_id', right_on='rate_code_id') \
                .merge(pickup_location_dim, left_on='trip_id', right_on='pickup_location_id') \
                .merge(dropoff_location_dim, left_on='trip_id', right_on='dropoff_location_id')\
                .merge(datetime_dim, left_on='trip_id', right_on='datetime_id') \
                .merge(payment_type_dim, left_on='trip_id', right_on='payment_type_id') \
                [['trip_id','VendorID', 'datetime_id', 'passenger_count_id',
                'trip_distance_id', 'rate_code_id', 'store_and_fwd_flag', 'pickup_location_id', 'dropoff_location_id',
                'payment_type_id', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
                'improvement_surcharge', 'total_amount']]

    print("done process fact Table")

    container_name_lake = "processed"

    try:
        container_client_lake = blob_service_client.create_container(container_name_lake)   
    except:
        container_client_lake = blob_service_client.get_container_client(container_name_lake) 


    def upload_df_to_azure(df, blob_service_client, blob_name):
        folder_lake = kwargs["path_folder"]#"uber"
        
        blob_name_in_lake = f"{folder_lake}/{blob_name}"
        blob_client = blob_service_client.get_blob_client(blob_name_in_lake)
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        try:
            blob_client.upload_blob(csv_buffer.getvalue(), overwrite=True)
            print(f'DataFrame {blob_name} uploaded successfully.')
        except Exception as e:
            print(f'Failed to upload DataFrame: {e}')

            

    # Upload the three dataframes
    upload_df_to_azure(fact_table, container_client_lake, "fact_table.csv")
    upload_df_to_azure(datetime_dim, container_client_lake, "datetime_dim.csv")
    upload_df_to_azure(dropoff_location_dim, container_client_lake, "dropoff_location_dim.csv")
    upload_df_to_azure(payment_type_dim, container_client_lake, "payment_type_dim.csv")
    upload_df_to_azure(pickup_location_dim, container_client_lake, "pickup_location_dim.csv")
    upload_df_to_azure(rate_code_dim, container_client_lake, "rate_code_dim.csv")
    upload_df_to_azure(trip_distance_dim, container_client_lake, "trip_distance_dim.csv")
    upload_df_to_azure(passenger_count_dim, container_client_lake, "passenger_count_dim.csv")