# Tezos chain stats

This repository contains scripts to produce statistics datasets on the Tezos blockchain at various time intervals.
The starting dataset contains various chain-level statistics such as active wallets, nr of transactions etc.

It is set up to run o at regular intervals. Statistics at various intervals are cached locally on disk.

## Run instructions

This script depends on access to RPC endpoints, tzkt API and the underlying postgres db populated by tzkt

The output is uploaded to a specified s3 bucket with dataset metadata uploaded to mongodb.

Script status messages are sent through telegram to be notified on successful and failed runs.

*Step 1 - initialize TZKT*

Start the TZKT indexer on the same machine as this script with the postgres database locally accessible.
For instructions to run tzkt, see [https://github.com/baking-bad/tzkt](https://github.com/baking-bad/tzkt).

*Step 2*

Set environment variables. (see .env.sample file for variables to be set)

RPC_ADDRESS - Used to validate if tzkt indexer is in sync by checking head levels
TZKT_ADDRESS - Used to check current head level of the tzkt indexer
TZKT_ADDRESS_PUBLIC - Used to check if the local tzkt instance is in sync with public network

PG_ADDRESS - 

Data pull
TZKT indexer

Data push
S3

The script will perform a number of startup tests to check if everything is set up correctly.

When running in loop mode the script will re-test some connections & indexer sync state on every new run.

**Dependencies**

Status updates for running the script are sent to a Telegram bot using the telegram-send package for easy tracking of uptime.



Run the main script  to perform the calculations.


## Microservice

The main script sets up and maintains a task queue. The task queue handles the task of calculating statistics. When running the script, the task queue is initialized with the task of calculating statistics, once it finishes this task it calculates the time until midnight +1H the following day, and initializes a task for that time. Through this recursive initialisation it enters an infinite loop to calculate new statistics every night (until the script is interupted).

Statistics are calculated in three steps.

First step extracts the opps data from the TZKT database.

The second step extracts account data.

THe third step joins the account data on the opps data using account ids so that account names etc can be dirctly used in opps statistics.

Then stats for the whole and various subsets can be calculated.






# Validation checks
- Minimum nr of unique wallet sender addresses per operation group hash is supposed to be exactly 1
- Maximum nr of unique wallet sender addresses per operation group hash is supposed to be exactly 1

Part of validation columns that checks if each operation group has 1 and only 1 unique wallet initializing the group of transactions.
This aggregation column contains the maximum unique wallets found sending transactions per operation group. It is expected to be 1 for every entry.

## Statistics calculated

Statistics are calculated by the calculate_stats_for_df function on multiple time deltas.
Providing this function a dataframe returns a dict with the following counts:

- Transactions
- Wallets sending transactions
- Contracts sending transactions
- Wallet targetted transactions
- Contract targetted transactions
- Wallet to wallet transactions
- Wallet to contract transactions
- Contract to contract transactions
- Contract to wallet transactions
- Transaction groups

- Wallets sending transactions
- Wallets calling contracts
- Wallets targetted in transactions
- Contracts targetted in transactions
- Wallets involved in transactions
- Contracts involved in transactions

(transaction group: transactions grouped in one operation)

- Average transactions per wallet
- Average transaction groups per wallet
- Median transactions per wallet
- Median transaction groups per wallet
- Max transactions per wallet
- Max transaction groups per wallet

- Baker fee
- Gas used

- Blocks
- 
