from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

def delete_source_file(**kwargs):
    print("Azure Blob Storage Python quickstart sample")


    hook_blob = WasbHook('covidreportingsamankay')
    blob_service_client = hook_blob.get_conn()

    container_name_blob = kwargs["container_name"]
    container_client_blob = blob_service_client.get_container_client(container_name_blob)

    container_client_blob.delete_container()

    print("source file is deleted")