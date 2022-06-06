from datetime import datetime
from src.utils.upload_dataset import upload_dataset
from src.load_column_descriptions import load_column_descriptions



def upload_datasets():

    col_descriptions = load_column_descriptions()
    chain_stats_md_description = open("content_config/dataset_descriptions/tezos_daily_chain_stats_description.md", "r").read()


    daily_chain_stats_dataset = {
        "name": "Tezos daily chain stats",
        "description_md": chain_stats_md_description,
        "key": "tezos-daily-chain-stats",
        "cache_file_path":"output/tezos_stats_by_day.csv",
        "spaces_path":"datasets/tezos/chain/tezos-daily-chain-stats.csv",
        "upload_date": datetime.now(),
        "columns": col_descriptions,
        "tags": ["daily", "tezos"]
    }

    upload_dataset(daily_chain_stats_dataset)