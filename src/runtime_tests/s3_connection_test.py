from src.utils.s3 import bucket_creation_date

def run_test():
    established_connection = False
    print("Testing S3 connection by accessing bucket creation date.")
    try:
        creation_date = bucket_creation_date()
        print(creation_date)
        established_connection = True
    except Exception as e:
        print("Error with S3 connection:")
        print(e)
        established_connection = False
    return established_connection