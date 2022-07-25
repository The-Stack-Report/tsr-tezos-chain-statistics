from sqlalchemy import null
from src.stat_funcs.baker_fee import baker_fee
from src.stat_funcs.unique_wallets import unique_wallets
from src.stat_funcs.unique_contracts import unique_contracts
from src.stat_funcs.transaction_groups import transaction_groups
import src.data_validation_checks.operation_groups_check as operation_groups_check

checks = [
    operation_groups_check
]

def calculate_stats_for_df(df):
    check_results = []
    for check in checks:
        check_results.append(
            check.check(df)
        )
    
    if all(check_results) == True:
        print("df passed all checks, proceeding with calculating statistics.") 
    else:
        raise ValueError("Data frame did not pass all checks")

    # Wallet sender group
    wallet_sender_df = df[df["sender_address"].str.startswith("tz")]
    wallet_to_wallet_df = wallet_sender_df[wallet_sender_df["target_address"].str.startswith("tz")]
    wallet_to_contract_df = wallet_sender_df[wallet_sender_df["target_address"].str.startswith("KT")]

    unique_wallet_senders = [addr for addr in df["sender_address"].values if addr.startswith("tz")]
    unique_wallets_targetted = [addr for addr in df["target_address"].values if addr.startswith("tz")]

    total_unique_wallets = set([*unique_wallet_senders, *unique_wallets_targetted])

    # Contract sender group
    contract_sender_df = df[df["sender_address"].str.startswith("KT")]
    contract_to_contract_df = contract_sender_df[contract_sender_df["target_address"].str.startswith("KT")]
    contract_to_wallet_df = contract_sender_df[contract_sender_df["target_address"].str.startswith("tz")]

    # Wallet targetted
    wallet_targeted_df = df[df["target_address"].str.startswith("tz")]

    # Contract targetted
    contract_targeted_df = df[df["target_address"].str.startswith("KT")]

    # Contract entrypoint calls
    entrypoint_calls_df = df[df["Entrypoint"].notnull()]

    # Transaction groups with contract calls
    ophashes_with_entrypoint = entrypoint_calls_df["OpHash"].unique()

    entrypoint_call_transactions_count = len(entrypoint_calls_df)

    transaction_groups_with_entrypoint_calls = len(ophashes_with_entrypoint)

    transactions_count = len(df)
    transactions_groups_count = len(df["OpHash"].unique())
    transactions_in_groups_with_entrypoint_calls_df = df[df["OpHash"].isin(ophashes_with_entrypoint)]
    transactions_in_groups_with_entrypoint_calls = len(transactions_in_groups_with_entrypoint_calls_df)

    smart_contract_transactions_to_transaction_groups_ratio = False
    if(transaction_groups_with_entrypoint_calls > 0):
        smart_contract_transactions_to_transaction_groups_ratio = transactions_in_groups_with_entrypoint_calls / transaction_groups_with_entrypoint_calls
    
    stats = {
        "transactions": transactions_count,
        "wallet_sender_transactions": len(wallet_sender_df),
        "contract_sender_transactions": len(contract_sender_df),
        "wallet_targeted_transactions": len(wallet_targeted_df),
        "wallet_targeted_transactions": len(wallet_targeted_df),
        "contract_targeted_transactions": len(contract_targeted_df),
        "wallet_to_wallet_transactions": len(wallet_to_wallet_df),
        "wallet_to_contract_transactions": len(wallet_to_contract_df),
        "contract_to_contract_transactions": len(contract_to_contract_df),
        "contract_to_wallet_transactions": len(contract_to_wallet_df),
        "transaction_groups": transactions_groups_count,
        "transactions_to_groups_ratio": transactions_count / transactions_groups_count, 
        "smart_contract_transactions_to_groups_ratio": smart_contract_transactions_to_transaction_groups_ratio,

        "wallets_sending_transactions": len(wallet_sender_df["sender_address"].unique()),
        "wallets_calling_contracts": len(wallet_to_contract_df["sender_address"].unique()),
        "contracts_sending_transactions": len(contract_sender_df["sender_address"].unique()),
        "wallets_involved_in_transactions": len(total_unique_wallets),

        "entrypoint_call_transactions": entrypoint_call_transactions_count,
        "transactions_in_groups_with_entrypoint_calls": transactions_in_groups_with_entrypoint_calls,
        "transaction_groups_with_entrypoint": transaction_groups_with_entrypoint_calls
    }

    stats = transaction_groups(df, stats)

    return stats