import os
from src.constants import cache_path
from pathlib import Path

def load_column_descriptions():
    print("loading column descriptions")

    col_descriptions_dir =  Path("content_config/column_descriptions")

    col_description_files = os.listdir(col_descriptions_dir)

    col_description_files = [f for f in col_description_files if f.endswith(".md")]

    print(col_description_files)

    col_descriptions = []

    for f in col_description_files:
        col_name = f[:-3]
        print(col_name)

        col_description_path = col_descriptions_dir / f

        col_description = open(col_description_path, "r").read()

        col_descriptions.append({
            "column": col_name,
            "description_md": col_description
        })

    return col_descriptions


