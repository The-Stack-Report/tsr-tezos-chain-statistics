import pandas as pd
from tqdm import tqdm
import datetime
import os


def enrich_ops_with_account_data(dates):
    accounts_df = pd.read_csv("cache/accounts.csv")
    print(accounts_df)

    addresses_by_id = dict(zip(accounts_df["Id"], accounts_df["Address"]))
    # Run through all days again
    for dt64 in tqdm(dates):
        dt = pd.Timestamp(dt64)
        dt = datetime.datetime.strptime(dt.strftime("%Y-%m-%d"), '%Y-%m-%d')
        print(dt)
        dt_formatted = dt.strftime("%Y-%m-%d")
        file_path = f"cache/by_date/ops_{dt_formatted}.csv"
        file_w_acc_path = f"cache/by_date_with_account/{dt_formatted}.csv"
        if not os.path.exists(file_w_acc_path):
            ops_for_day_df = pd.read_csv(file_path)


            ids_for_day = pd.unique(ops_for_day_df[["TargetId", "SenderId", "InitiatorId"]].values.ravel("K"))

            print(f"Unique addresses involved for day {len(ids_for_day)}")

            accounts_for_day_df = accounts_df[accounts_df["Id"].isin(ids_for_day)]
            addresses_by_id_for_day = dict(zip(accounts_for_day_df["Id"], accounts_for_day_df["Address"]))


            # Sender address
            ops_for_day_df["target_address"] = ops_for_day_df["TargetId"]


            # Initiator address
            ops_for_day_df["sender_address"] = ops_for_day_df["SenderId"]

            # Target address
            ops_for_day_df["initiator_address"] = ops_for_day_df["InitiatorId"]

            ops_for_day_df.replace({
                "target_address": addresses_by_id_for_day,
                "sender_address": addresses_by_id_for_day,
                "initiator_address": addresses_by_id_for_day
            }, inplace=True)


            

            ops_for_day_df.sort_values(by="Id", ascending=True, inplace=True)

            print(ops_for_day_df)

            ops_for_day_df.to_csv(file_w_acc_path, header=True, index=False)
            
        else:
            print(f"enriched file exists already: {file_w_acc_path}")

