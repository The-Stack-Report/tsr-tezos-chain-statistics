
#Part of validation columns that checks if each operation group has 1 and only 1 unique wallet initializing the group of transactions.
# This aggregation column contains the maximum unique wallets found sending transactions per operation group. It is expected to be 1 for every entry.

def transaction_groups(df, stats):
    return stats