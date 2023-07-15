from helper.read_json_github import json_load_file
from helper.blob_connection import blob_connection


def create_ecdcraw_data(**kwargs):
    try:
        blob_connection(kwargs["container_name"], json_load_file(kwargs["json_file"]))

    except Exception as e:
        print(str(e))

create_ecdcraw_data