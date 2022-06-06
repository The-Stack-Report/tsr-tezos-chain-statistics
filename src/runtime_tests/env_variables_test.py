from os import environ

expected_env_keys = [
    "LOOP",
    "MACHINE",
    "CACHE_PATH",
    "RPC_ADDRESS",
    "TZKT_ADDRESS",
    "TZKT_ADDRESS_PUBLIC",

    "PG_ADDRESS",
    "PG_DB",
    "PG_USER",
    "PG_PW",

    "S3_ACCESS_KEY",
    "S3_SECRET_KEY",
    "S3_REGION",
    "S3_ENDPOINT",
    "DATASETS_BUCKET",
    "MONGODB_CONNECT_URL"
]



def run_test():
    all_variables_available = True

    for key in expected_env_keys:
        if key in environ:
            print(f"{key} - present")
        else:
            print(f"Missing env variable: {key} !")
            all_variables_available = False


    return all_variables_available