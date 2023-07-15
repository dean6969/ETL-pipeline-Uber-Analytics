from airflow.providers.microsoft.azure.hooks.wasb import WasbHook


def copy_blob_to_lake(**kwargs):
    print("Azure Blob Storage Python quickstart sample")


    hook_blob = WasbHook('covidreportingsamankay')
    blob_service_client = hook_blob.get_conn()

    hook_data_lake = WasbHook('lake_gen_ingest_cases_death')
    datalake_service_client = hook_data_lake.get_conn()

    container_name_blob = kwargs["container_name"]

    container_name_lake ="raw"

    folder_lake = kwargs["container_name"]

    container_client_blob = blob_service_client.get_container_client(container_name_blob)

    try:
        container_client_lake = datalake_service_client.create_container(container_name_lake)   
    except:
        container_client_lake = datalake_service_client.get_container_client(container_name_lake) 






    source_blobs = container_client_blob.list_blobs()
    # source_lakes = container_client_lake.list_blobs()

    for blob in source_blobs:
        
                

        # Get the blob client for the source blob
        source_blob_client = container_client_blob.get_blob_client(blob.name)

        # Download the blob to a stream
        data = source_blob_client.download_blob().readall()

        # Create a new blob in Data Lake Storage (in the subfolder) and upload the data into it
        blob_name_in_lake = f"{folder_lake}/{blob.name}"
        blob_client_lake = container_client_lake.get_blob_client(blob_name_in_lake)
        blob_client_lake.upload_blob(data, overwrite=True)

        print(f"{blob.name} is updated in data lakes")

    print("All blobs copied successfully.")





