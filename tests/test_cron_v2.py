
import pytz
import random
import datetime
import time
import croniter
from aiokeydb.v2.utils.cron import validate_cron_schedule


tz = pytz.timezone('US/Central')
local_date = tz.localize(datetime.datetime.now())


test_patterns = [
    'every {n} minutes',
    'every {n} minutes and 10 seconds',
    'every {n} minutes, 10 seconds',
    '{n} minutes and 10 seconds',
    '{n} minutes, 10 seconds',
    'every {n} hours and 10 minutes and 10 seconds',
    'every {n} hours, 10 minutes, 10 seconds',
    'every {n} hours, 10 minutes and 10 seconds',
    'every {n} hours and 10 minutes',
    'every {n} hours',
    'every {n} days and 10 hours and 10 minutes and 10 seconds',
    'every {n} seconds',
    '{n} seconds',
    'every 30 seconds',
    'every 2 minutes',
    '5 s',
    '10 min',
    '1 hr',
]


for pattern in test_patterns:
    pattern = pattern.format(n=random.randint(1, 15))
    try:
        cron_expression = validate_cron_schedule(pattern)
        now = time.time()
        is_valid = croniter.croniter.is_valid(cron_expression)
        next_time = croniter.croniter(cron_expression, now).get_next()
        next_date = croniter.croniter(cron_expression, local_date).get_next(datetime.datetime)
        print(f"Pattern: {pattern}\nCron Expression: {cron_expression}\nValid: {is_valid}\nNext Time: {next_time} ({next_time - now:2f} s)\nNext Date: {next_date}")
    except ValueError as e:
        print(f"Pattern: {pattern}\nError: {str(e)}\n")
    print()

