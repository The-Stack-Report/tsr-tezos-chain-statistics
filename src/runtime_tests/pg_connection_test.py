import pandas as pd
from src.utils.postgress import pg_connection
from src.queries import test_query

def run_test():
    print("testing postgres connection")
    dbConnection = pg_connection()
    if dbConnection:
        q = test_query()

        test_resp = pd.read_sql(q, dbConnection)
        print("testing a pg query:")
        print(test_resp)

        if len(test_resp) > 0:
            return True
        else:
            return False
    else:
        print("Error in establishing postgress connection.")
        return False