from src.utils.mongo_db import upload_attributes_to_mongo
from src.utils.s3 import upload_file_to_s3
from src.constants import (
    cache_path
)
import os
from src.utils.file_size import get_file_size_readable

def upload_dataset(dataset_description):
    print("uploading set")
    spaces_path = dataset_description["spaces_path"]
    dataset_description["spaces_url"] = f"https://the-stack-report.ams3.cdn.digitaloceanspaces.com/{spaces_path}"

    file_path = str(cache_path / dataset_description["cache_file_path"])

    dataset_description["file_size"] = get_file_size_readable(file_path)

    if os.path.isfile(file_path):
        upload_attributes_to_mongo(
            key=dataset_description["key"],
            attributes=dataset_description
        )



        upload_file_to_s3(
            file_path = file_path,
            object_name=spaces_path,
            make_public=True
        )
    else:
        print(f"File: {file_path} is missing!!")



