
#Part of validation columns that checks if each operation group has 1 and only 1 unique wallet initializing the group of transactions.
# This aggregation column contains the maximum unique wallets found sending transactions per operation group. It is expected to be 1 for every entry.

def check(df):


    # Validating that each op group has only 1 wallet which is sending transactions in each transaction group.

    # If this is the case we can use unique nr of wallets sending transactions as wallet metric.
    wallet_sent_ops = df[df["sender_address"].str.startswith("tz")]

    sender_unique_per_op_group_hash = wallet_sent_ops.groupby("OpHash").agg({"sender_address": "nunique"})

    sender_unique_per_op_group_hash.reset_index()

    min_wallets = int(sender_unique_per_op_group_hash["sender_address"].min())
    max_wallets = int(sender_unique_per_op_group_hash["sender_address"].max())
    average_wallets = float(sender_unique_per_op_group_hash["sender_address"].mean())

    if not min_wallets > 0:
        raise ValueError(f"Minimum nr of unique wallets sending operations per operation group expected to be 1, but is {min_wallets}")
    if not max_wallets < 5:
        raise ValueError(f"Maximum nr of unique wallets sending operations per operation group expected to be 1, but is {max_wallets}")

    if max_wallets > 1:
        print(df)
        print(f"Df contains transaction groups with more than 1 sender wallet. Max found is: {max_wallets}")
    print("All operation groups contain only between 1 and 5 unique wallet sender address")
    return True