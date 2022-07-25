import pandas as pd
import datetime
from datetime import timezone
import pandas as pd
from tqdm import tqdm
from src.queries import get_accounts
from src.utils.postgress import pg_connection
import os
import asyncio
from src.sub_steps.extract_ops_by_day_from_db import extract_ops_by_day_from_db
from src.sub_steps.enrich_ops_with_account_data import enrich_ops_with_account_data
from src.sub_steps.stats_by_day import stats_by_day



chain_start_date = "2018-06-30"

async def get_chain_stats():
    dbConnection = pg_connection()
    print('get chain stats')
    yesterday = datetime.datetime.now(timezone.utc) - datetime.timedelta(days=1)
    yesterday = yesterday.strftime("%Y-%m-%d")

    # test_date = datetime.datetime.strptime("2018-06-30", "%Y-%m-%d")
    # q = ops_for_date_query(test_date)
    # test_resp = pd.read_sql(q, dbConnection)
    # print(test_resp)
    # test_date_formatted = test_date.strftime("%Y-%m-%d")
    # test_resp.to_csv(f"cache/by_date/ops_{test_date_formatted}.csv", header=True, index=False)

    # Pandas generate date range until yesterday

    date_range = pd.date_range(start=chain_start_date, end=yesterday, freq="D")

    date_range_values = date_range.values
    print(date_range_values)
    extract_ops_by_day_from_db(dates=date_range_values)

        
    # Get most recent accounts
    print("getting accounts df for merging")

    accounts_file_path = "cache/accounts.csv"

    today = datetime.datetime.now(timezone.utc)
    today_formatted = today.strftime("%Y-%m-%d")
    accounts_update_time = False
    accounts_update_time_formatted = False

    if os.path.exists(accounts_file_path):
        accounts_update_time = os.path.getmtime(accounts_file_path)

        accounts_update_time = datetime.datetime.fromtimestamp(accounts_update_time)

        accounts_update_time_formatted = accounts_update_time.strftime("%Y-%m-%d")

        print(accounts_update_time)
    else:
        print("No file available yet")

    if not accounts_update_time_formatted == today_formatted:
        print(f"Accounts file not synced yet for day {today_formatted}, redownloading.")
        accounts_q = get_accounts()
        resp = pd.read_sql(accounts_q, dbConnection)
        resp.to_csv(accounts_file_path, header=True, index=False)
        print(resp)
    else:
        print(f"Accounts df file already synced for {today_formatted}, skipping querying")


    # Merging accounts df with days

    enrich_ops_with_account_data(dates=date_range_values)


    # Iterate over days
    # Check if ops file exists
    # If not exists, query ops from db & store to file

    # Loop over files to calculate daily stats

    # Loop over weeks as date range, merge files temporarily into df to calculate stats

    # 

    stats_by_day(dates=date_range_values)

    return True

