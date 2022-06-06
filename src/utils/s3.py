import os
import boto3
from botocore.client import Config
from dotenv import load_dotenv
from botocore.exceptions import ClientError

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("S3_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = os.getenv("S3_SECRET_KEY")
DATASETS_BUCKET = os.getenv("DATASETS_BUCKET")
S3_REGION = os.getenv("S3_REGION")
S3_ENDPOINT = os.getenv("S3_ENDPOINT")

session = boto3.session.Session()

# Client, used to upload file
s3_client = session.client("s3",
    region_name=S3_REGION,
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

# Resource, used to set file Access Control List.
s3_resource = boto3.resource(
    "s3",
    region_name=S3_REGION,
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )


def upload_file_to_s3(file_path, object_name, make_public=False):
    print("uploading: ", file_path)
    try:
        response = s3_client.upload_file(
            file_path,
            DATASETS_BUCKET,
            object_name,
        )
        if make_public:
            object_acl = s3_resource.ObjectAcl(DATASETS_BUCKET, object_name)
            resp = object_acl.put(ACL="public-read")
            return True
        else:
            return True
    except ClientError as e:
        print(e)
        return False
    return True


def bucket_creation_date():
    return s3_resource.Bucket(DATASETS_BUCKET).creation_date

def list_bucket():
    bucket = s3_resource.Bucket(DATASETS_BUCKET)
    items = bucket.objects.all()
    return items
