from datetime import timezone
import datetime


def delta_until_utc_post_midnight():
    dt = datetime.datetime.now(timezone.utc)

    tomorrow = dt + datetime.timedelta(days=1)

    dt_combined = datetime.datetime.combine(tomorrow, datetime.time.min)
    dt_combined = dt_combined.replace(tzinfo=timezone.utc)
    dt_combined = dt_combined

    dt_combined = dt_combined + datetime.timedelta(hours=1)


    return dt_combined - dt

