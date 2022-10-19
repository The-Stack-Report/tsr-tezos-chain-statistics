import src.runtime_tests.env_variables_test as env_variables_test
import src.runtime_tests.pg_connection_test as pg_connection_test
import src.runtime_tests.tzkt_connections_test as tzkt_connections_test
import src.runtime_tests.head_levels_test as head_levels_test
import src.runtime_tests.s3_connection_test as s3_connection_test
import src.runtime_tests.mongo_connection_test as mongo_connection_test
import src.runtime_tests.telegram_bot_test as telegram_bot_test
import inspect

startup_tests = [
    env_variables_test,
    pg_connection_test,
    # tzkt_connections_test,
    s3_connection_test,
    # mongo_connection_test,
    # telegram_bot_test,
    head_levels_test
]

pre_sync_tests = [
    head_levels_test,
    pg_connection_test,
]


async def run_tests(tests, batch_name=""):
    all_tests_passed = True
    tests_summary = []
    for test in tests:
        print(f"Running test: {test.__name__}")
        test_result_msg = ""
        if hasattr(test, 'run_test') and callable(test.run_test):
            test_passed = False
            if inspect.iscoroutinefunction(test.run_test):
                test_passed = await test.run_test()
            else:
                test_passed = test.run_test()
            
            print(test_passed)
            if test_passed == False:
                all_tests_passed = False
                test_result_msg = f"[ ] - {test.__name__} has failed."
            else:
                if type(test_passed) is dict:
                    print("test sent back dict")
                    
                    result_msg = test_passed["msg"]
                    if test_passed["passed"] == False:
                        all_tests_passed = False
                        test_result_msg = f"[ ] - {test.__name__} has failed. {result_msg}"
                    else:
                        test_result_msg = f"[x] - {test.__name__} has passed.  {result_msg}"
                else:
                    test_result_msg = f"[x] - {test.__name__} has passed."
        else:
            test_result_msg = f"[ ] - {test.__name__} is missing callable run_test attribute."
            all_tests_passed = False
        tests_summary.append(test_result_msg)
        print(test_result_msg)
    
    testresults_summary = f"---- test results: {batch_name} ----"
    for t in tests_summary:
        testresults_summary += f"\n{t}"
    return {"summary": testresults_summary, "passed": all_tests_passed}

async def perform_startup_tests():
    print('performing runtime startup tests')
    return await run_tests(startup_tests, "tezos chain stats - startup tests")


# Run before syncing dataset
async def perform_pre_sync_tests():
    print("pre sync tests")

    return await run_tests(pre_sync_tests, "pre-sync")

