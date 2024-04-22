from datetime import timezone
import datetime
import pandas as pd
import asyncio
import aiohttp

import json
from src.time_utils import delta_until_utc_post_midnight
from src.get_chain_stats import get_chain_stats
from src.verbose_timedelta import verbose_timedelta
from pathlib import Path
from src.runtime_tests.tests import perform_startup_tests, perform_pre_sync_tests
from src.upload_datasets import upload_datasets
import telegram_send
import src.utils.postgress
from src.utils.status_messages import stat_completed_message
from src.utils.postgress import disconnect


from tqdm import tqdm
import os
from dotenv import load_dotenv

load_dotenv()

machine = os.getenv("MACHINE")


cache = Path("cache")

content_config = Path("content_config")

by_date_cache = cache / "by_date"

by_date_with_account_cache = cache / "by_date_with_account"

stats_by_day_cache = cache / "stats_by_day"

stats_by_week_cache = cache / "stats_by_week"

stats_by_month_cache = cache / "stats_by_month"

output_cache = cache / "output"


dirs = [
    content_config,
    cache,
    by_date_cache,
    by_date_with_account_cache,
    stats_by_day_cache,
    stats_by_week_cache,
    stats_by_month_cache,
    output_cache
]

task_queue = asyncio.Queue()

yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
loop = os.getenv("LOOP")
if loop == "TRUE":
    loop = True
elif loop == "FALSE":
    loop = False
else:
    print("Loop is not [TRUE, FALSE], setting: False, default.")
    loop = False

# loop = True

# If not all tests pass, send notification message & try again 1h.


def retry_interval(retries):
    if retries == 0:
        return 60 # 1min
    elif retries == 1:
        return 60 * 5 # 5min
    elif retries == 2:
        return 60 * 30 # 30min
    else:
        return 60 * 60 # 1h

async def process_tasks():
    retries_on_day = 0
    while not task_queue.empty():
        task = await task_queue.get()
        task_start_ts = datetime.datetime.now()

        # Check if head of local tzkt node and public tzkt are in sync
        pre_sync_tests_passed = {"passed": False}

        try:
            pre_sync_tests_passed = await perform_pre_sync_tests()
        except Exception:
            print("error in running presync tests")
            print(Exception)
            telegram_send.send(messages=["error in running presync tests"], parse_mode="markdown")

        pre_sync_tests_summary = pre_sync_tests_passed["summary"]
        print(pre_sync_tests_summary)

        telegram_send.send(
            messages=[f"Presync test results", pre_sync_tests_summary],
            parse_mode="markdown")

        if pre_sync_tests_passed["passed"]:
            # Main calculation function
            retries_on_day = 0
            machine = os.getenv("MACHINE")
            start_message = f"{machine} Initiating *Tezos chain stats* daily script at: \n {task_start_ts}"
            print(start_message)
            telegram_send.send(messages=[start_message], parse_mode="markdown")


            ########################################
            #
            # Processing chain stats here
            #
            stats_successful = False
            try:
                stats_successful = await get_chain_stats()
            except Exception:
                print("Error in calculating chain stats:")
                print(stats_successful)

            #
            ########################################


            if stats_successful:
                print("Stats successfully calculated, uploading datasets.")
                upload_datasets()
            else:
                print("Error in calculating statistics")
            
            

            task_end_ts = datetime.datetime.now()

            task_runtime = task_end_ts - task_start_ts

            task_runtime_verbose = verbose_timedelta(task_runtime)

            sleep_time_delta = delta_until_utc_post_midnight()

            sleep_time_seconds = sleep_time_delta.total_seconds()


            sleep_time_verbose = verbose_timedelta(sleep_time_delta)

            completed_msg = stat_completed_message(
                stats_successful,
                task_start_ts,
                task_end_ts,
                task_runtime_verbose,
                loop,
                sleep_time_verbose
            )
            print(completed_msg)
            telegram_send.send(messages=[completed_msg], parse_mode="markdown")


            # Disconnecting from postgres to free up connection

            print("Finished processing, disconnecting db before sleeping until next run or exiting.")
            disconnect()

            if loop:
                print(f"sleeping for {sleep_time_verbose} ({sleep_time_seconds} seconds)")
                await asyncio.sleep(sleep_time_seconds)
                await task_queue.put("sync!")
        else:
            retries_on_day += 1
            retry_time = retry_interval(retries_on_day)
            retry_time_verbose = verbose_timedelta(datetime.timedelta(seconds=retry_time))
            disconnect()
            print(f"Some pre-sync tests have failed.")
            print(f"Trying again in {retry_time_verbose}")

            
            telegram_send.send(
                messages=[f"Some pre-sync tests have failed. \nRetrying in {retry_time_verbose}"],
                parse_mode="markdown"
            )
            await asyncio.sleep(retry_time)
            await task_queue.put("sync!")


chain_start_date = "2018-06-30"

async def main():
    print("Initializing directories:")
    startup_tests_passed = False
    for d in dirs:
        print(d)
        d.mkdir(exist_ok=True, parents=True)

    while startup_tests_passed == False:

        print("Initializing startup tests")
        startup_tests = await perform_startup_tests()

        await asyncio.sleep(1)

        print("Startup tests finished")
        print(startup_tests["summary"])

        if startup_tests["passed"] == True:
            print("Passed startup tests, initializing first sync task in task queue.")
            startup_tests_passed = True
        else:
            disconnect()
            print("Failed startup tests, retrying in 1h.")
            telegram_send.send(
                messages=[startup_tests["summary"]],
                parse_mode="markdown"
            )
            telegram_send.send(
                messages=[f"Some pre-sync tests have failed. \nRetrying in 1h"],
                parse_mode="markdown"
            )
            await asyncio.sleep(60 * 60)
        
    print("running")

    await task_queue.put("sync!")

    await asyncio.gather(
        asyncio.create_task(process_tasks())
    )


if __name__ == "__main__":
    print("Initializing Tezos Chain stats script.")

    print(f"running looping: {loop}")
    if loop == True:
        print("Running in looping mode")
    if loop == False:
        print("Running in single mode")
    
    asyncio.run(main())
