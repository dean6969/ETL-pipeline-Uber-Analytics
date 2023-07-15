
import json

def json_load_file(file):
    try:
        with open(f"dags/helper/{file}") as json_file:
            file_json = json.load(json_file)
        return file_json
    except Exception as e:
        print(str(e))

