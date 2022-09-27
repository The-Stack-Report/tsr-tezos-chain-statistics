import pandas as pd
from tqdm import tqdm
import datetime
import os
from src.calculate_stats_for_df import calculate_stats_for_df
import json

def stats_by_day(dates):
    print("calculating stats by day")

    for dt64 in tqdm(dates):
        dt = pd.Timestamp(dt64)
        dt = datetime.datetime.strptime(dt.strftime("%Y-%m-%d"), '%Y-%m-%d')
        dt_formatted = dt.strftime("%Y-%m-%d")

        ops_path = f"cache/by_date_with_account/{dt_formatted}.csv"

        stats_by_day_path = f"cache/stats_by_day/stats-{dt_formatted}.json"

        if not os.path.exists(stats_by_day_path) or True:
            print(f"calculating stats for {dt_formatted}")

            ops_df = pd.read_csv(ops_path)
            
            stats = calculate_stats_for_df(ops_df)

            stats["date"] = dt_formatted

            with open(stats_by_day_path, "w") as outfile:
                json.dump(stats, outfile, indent=4)
        
        else:
            print(f"Stats exist already at: {stats_by_day_path}")
    

    print("merging to final df")

    full_data = []

    for dt64 in tqdm(dates):
        dt = pd.Timestamp(dt64)
        dt = datetime.datetime.strptime(dt.strftime("%Y-%m-%d"), '%Y-%m-%d')
        dt_formatted = dt.strftime("%Y-%m-%d")

        stats_by_day_path = f"cache/stats_by_day/stats-{dt_formatted}.json"

        day_data = open(stats_by_day_path)
        day_data = json.load(day_data)
        full_data.append(day_data)
    

    all_days_df = pd.DataFrame(full_data)
    print(all_days_df)
    all_days_df.to_csv("cache/output/tezos_stats_by_day.csv",header=True, index=False)


