import os
import requests
from io import StringIO
import pandas as pd
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

def blob_connection(container_name, json_file):
    try:
        print("Azure Blob Storage Python quickstart sample")

        # Retrieve the connection string for use with the application. 
        # connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')


        # Create the BlobServiceClient object
        # blob_service_client = BlobServiceClient.from_connection_string(connect_str)

        hook = WasbHook('covidreportingsamankay')
        blob_service_client = hook.get_conn()

        # Create a unique name for the container
        container_name = container_name

        print(container_name)

        try:
        # Create the container
            container_client = blob_service_client.create_container(container_name)
        except:
            container_client = blob_service_client.get_container_client(container_name)

        for element in json_file:

            # Download the file from GitHub
            url = element["base_url"] + element["file_name"]
        
            try:
                response = requests.get(url)
                response.raise_for_status()  # Raises a HTTPError if the status is 4xx, 5xx
            except requests.exceptions.RequestException as err:
                print ("OOps: Something Else",err)
            except requests.exceptions.HTTPError as errh:
                print ("Http Error:",errh)
            except requests.exceptions.ConnectionError as errc:
                print ("Error Connecting:",errc)
            except requests.exceptions.Timeout as errt:
                print ("Timeout Error:",errt)

            # Get the content of the response
            data = response.text

            # Convert the CSV data into a DataFrame
            df = pd.read_csv(StringIO(data))

            # Save the downloaded file to a local file
            local_file_name = element["file_name"]

            df.to_csv(local_file_name)
            # with open(local_file_name, 'wb') as f:
            #     f.write(response.content)

            # Create a blob client using the local file name as the name for the blob
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_name)

            print("\nUploading to Azure Storage as blob:\n\t" + local_file_name)

            # Upload the created file
            with open(local_file_name, mode="rb") as data:
                blob_client.upload_blob(data)

            print("\nListing blobs...")

            # List the blobs in the container
            blob_list = container_client.list_blobs()
            for blob in blob_list:
                print("\t" + blob.name)

            # Delete the local source file
            print("Deleting the local source file...")
            print(local_file_name)
            os.remove(local_file_name)

            print("Done")

    except Exception as ex:
        print('Exception:')
        print(ex)
