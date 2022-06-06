
def stat_completed_message(
    stats_successful,
    task_start_ts,
    task_end_ts,
    task_runtime_verbose,
    loop,
    sleep_time
    ):
    success_line = ""
    if stats_successful:
        success_line = "✅ stats successfully processed."
    else:
        success_line = "❌ Error in processing stats."
    return f"""--------
*Tezos chain stats daily script*
{success_line}
Task start: {task_start_ts}
Task end: {task_end_ts}
Task runtime duration:
{task_runtime_verbose}
-
Looping: {loop}
Sleep time until midnight: {sleep_time}
--------
"""
