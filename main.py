from datetime import timezone
import datetime
import pandas as pd
import asyncio
import aiohttp
import os
from dotenv import load_dotenv
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

from tqdm import tqdm



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

        pre_sync_tests_passed = await perform_pre_sync_tests()

        pre_sync_tests_summary = pre_sync_tests_passed["summary"]
        print(pre_sync_tests_summary)

        telegram_send.send(
            messages=[f"Presync test results", pre_sync_tests_summary],
            parse_mode="markdown")

        if pre_sync_tests_passed["passed"]:
            # Main calculation function
            retries_on_day = 0
            telegram_send.send(messages=[f"Initiating *Tezos chain stats* daily script at: \n {task_start_ts}"], parse_mode="markdown")

            stats_successful = await get_chain_stats()
            stats_successful = True

            if stats_successful:
                print("Stats successfully calculated, uploading datasets.")
                upload_datasets()
            
            

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
            if loop:
                print(f"sleeping for {sleep_time_verbose} ({sleep_time_seconds} seconds)")
                await asyncio.sleep(sleep_time_seconds)
                await task_queue.put("sync!")
        else:
            retries_on_day += 1
            retry_time = retry_interval(retries_on_day)
            retry_time_verbose = verbose_timedelta(datetime.timedelta(seconds=retry_time))
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
    for d in dirs:
        print(d)
        d.mkdir(exist_ok=True, parents=True)

    print("Initializing startup tests")
    startup_tests = await perform_startup_tests()

    print("Startup tests finished")
    print(startup_tests["summary"])

    if startup_tests["passed"] == True:
        print("Passed startup tests, initializing first sync task in task queue.")
    else:
        print("Failed startup tests, aborting script.")
        
        return False
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
