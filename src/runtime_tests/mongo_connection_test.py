from src.utils.mongo_db import server_info

def run_test():

    mongo_connected = False

    try:
        info = server_info()
        print("Mongodb server info successfully queried.")
        print("Mongodb version: ", info["version"])
        mongo_connected = True
    except Exception as e:
        print("Error connecting to Mongodb:")
        print(e)
        mongo_connected = True

    return mongo_connected