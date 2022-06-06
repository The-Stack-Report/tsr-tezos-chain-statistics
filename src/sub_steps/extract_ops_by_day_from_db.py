import pandas as pd
from tqdm import tqdm
import datetime
from src.queries import ops_for_date_flat_query
import os
from src.utils.postgress import pg_connection

def extract_ops_by_day_from_db(dates):
    dbConnection = pg_connection()
    for dt64 in tqdm(dates):
        dt = pd.Timestamp(dt64)
        dt = datetime.datetime.strptime(dt.strftime("%Y-%m-%d"), '%Y-%m-%d')
        print(dt)
        dt_formatted = dt.strftime("%Y-%m-%d")
        file_path = f"cache/by_date/ops_{dt_formatted}.csv"
        if not os.path.exists(file_path):
            q = ops_for_date_flat_query(dt)
            resp = pd.read_sql(q, dbConnection)
            print(resp)
            resp.to_csv(file_path, header=True, index=False)
        else:
            print(f"file exists: {file_path}, skipping.")