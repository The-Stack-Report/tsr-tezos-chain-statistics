from src.runtime_tests.tests import perform_startup_tests, perform_pre_sync_tests
import asyncio

async def run_tests():
    pre_sync_tests_passed = await perform_pre_sync_tests()


async def main():
    await run_tests()


if __name__ == "__main__":
    asyncio.run(main())