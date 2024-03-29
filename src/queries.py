import sqlalchemy
import datetime
import pandas as pd
from src.utils.postgress import pg_connection, disconnect

def all_ops_query():
    return sqlalchemy.text("""SELECT
ops."Id",
ops."TargetId",
ops."Entrypoint",
ops."Amount",
ops."Timestamp",
ops."Status",
ops."OpHash",
ops."Errors",
ops."SenderId",
ops."InitiatorId",
ops."BakerFee",
ops."StorageFee",
ops."AllocationFee",
ops."GasUsed",
ops."GasLimit",
ops."Level"
FROM "TransactionOps" as ops
WHERE ops."Status" = 1
ORDER BY ops."Timestamp" ASC
""")

def ops_for_date_query(dt):
    next_dt = dt + datetime.timedelta(days=1)
    dt_formatted = dt.strftime("%Y-%m-%d")
    next_dt_formatted = next_dt.strftime("%Y-%m-%d")

    print(f"Generating query from date: {dt_formatted} to date {next_dt_formatted}")
    return sqlalchemy.text(f"""SELECT
ops."Id",
ops."TargetId",
ops."Entrypoint",
ops."Timestamp",
ops."Status",
ops."OpHash",
ops."Errors",
ops."InitiatorId",
acc."Id",
acc."Address",
acc2."Address" as "initiator_address"
FROM "TransactionOps" as ops
LEFT JOIN "Accounts" as acc
ON acc."Id" = ops."TargetId"
JOIN "Accounts" acc2 ON acc2."Id" = ops."InitiatorId"
WHERE ops."Status" = 1
AND ops."Timestamp" BETWEEN '{dt_formatted}' AND '{next_dt_formatted}'
ORDER BY ops."Timestamp" ASC
""")


def ops_for_date_flat_query(dt):
    next_dt = dt + datetime.timedelta(days=1)
    dt_formatted = dt.strftime("%Y-%m-%d")
    next_dt_formatted = next_dt.strftime("%Y-%m-%d")

    print(f"Generating query from date: {dt_formatted} to date {next_dt_formatted}")
    return sqlalchemy.text(f"""SELECT
ops."Id",
ops."TargetId",
ops."Entrypoint",
ops."Amount",
ops."Timestamp",
ops."Status",
ops."OpHash",
ops."Errors",
ops."SenderId",
ops."InitiatorId",
ops."BakerFee",
ops."StorageFee",
ops."AllocationFee",
ops."GasUsed",
ops."GasLimit",
ops."Level"
FROM "TransactionOps" as ops
WHERE ops."Status" = 1
AND ops."Timestamp" BETWEEN '{dt_formatted}' AND '{next_dt_formatted}'
ORDER BY ops."Timestamp" ASC
""")

def test_query():
    return """
SELECT * FROM public."Blocks"
ORDER BY "Id" ASC LIMIT 100
"""



def get_accounts():
    return """
SELECT * FROM public."Accounts"
ORDER BY "Id" ASC
"""


def get_max_block_level():
    return """
SELECT MAX("Level") as max_block_level FROM public."Blocks"
"""

if __name__ == "__main__":
    print("testing queries")
    dbConnection = pg_connection()
    max_level_query = get_max_block_level()
    max_level = pd.read_sql(max_level_query, dbConnection)
    print(max_level)
    print(max_level["max_block_level"].max())

    print("disconnecting engine")
    disconnect()

