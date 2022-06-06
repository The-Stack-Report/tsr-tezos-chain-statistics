def unique_wallets(df, stats):
    unique_sender_wallets = df[["sender_address"]]

    unique_sender_wallets = unique_sender_wallets[unique_sender_wallets["sender_address"].str.startswith("tz")]
    unique_sender_wallets = unique_sender_wallets["sender_address"].unique()

    stats["unique_sender_wallets"] = len(unique_sender_wallets)


    unique_initiator_wallets = df[["initiator_address"]]
    unique_initiator_wallets.fillna("", inplace=True)
    unique_initiator_wallets = unique_initiator_wallets[unique_initiator_wallets["initiator_address"].str.startswith("tz")]
    unique_initiator_wallets = unique_initiator_wallets["initiator_address"].unique()

    stats["unique_initiator_wallets"] = len(unique_initiator_wallets)

    return stats