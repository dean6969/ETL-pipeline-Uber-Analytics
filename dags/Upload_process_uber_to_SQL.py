
from azure.storage.blob import BlobServiceClient

import pandas as pd
import os
from io import BytesIO
import pandas as pd
from sqlalchemy import create_engine
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import re
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook


# The function write_pandas automatically creates a table with the same structure as the DataFrame and uploads the data.

def Upload_process_uber_to_SQL():
    try:
        project_name = "uber"

        hook_data_lake = WasbHook('lake_gen_ingest_cases_death')
        blob_service_client = hook_data_lake.get_conn()

        # connect_str = "DefaultEndpointsProtocol=https;AccountName=covidreportingmankaydl;AccountKey=bhZR8pvPm4tTzmlmPkzwl11ZJnody+ZdXTlswDkV77iiucI8DVsRGFXK2+IacQEkTxu/uRSwnAZ5+AStT8Vr2g==;EndpointSuffix=core.windows.net"


        # #Create the BlobServiceClient object
        # blob_service_client = BlobServiceClient.from_connection_string(connect_str)

        container_name = "processed"   #"raw"
        #"uber/uber_data.csv"

        cnn = snowflake.connector.connect(
                    user="mankay1805",
                    password="CHungpro$$12",
                    account="VFXEKYV-WR76288",
                    database="uber_analytics",
                    schema="uber"
                )

        container_client_blob = blob_service_client.get_container_client(container_name)

        source_blobs = container_client_blob.list_blobs()

        regex = f"(?<={project_name}/)(.*)(?=.csv)"
        for blob in source_blobs:
            
            if project_name != blob.name: 
                schema_name = re.findall(regex, blob.name)[0]

                blob_name = blob.name


                # Get the blob client
                blob_client = blob_service_client.get_blob_client(container_name, blob_name)

                # Get the blob data in BytesIO object
                blob_data = BytesIO(blob_client.download_blob().readall())

                # Read the BytesIO object into a pandas DataFrame
                df = pd.read_csv(blob_data)

                success, nchunks, nrows, _ = write_pandas(cnn, df, schema_name, quote_identifiers=False, auto_create_table=True)

                print("Upload table schema_name")

                # Close the connection
        cnn.close()
        print("Successful")
    except Exception as e:
        print(str(e))
    