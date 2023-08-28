"""
Cron Utils
"""

import re
import croniter

# '*/5 * * * *' # every 5 minutes
# '*/10 * * * *' # every 10 minutes

# _schedule_fmt = {
#     'minutes': '*/{value} * * * *',
#     'hours': '* * */{value} * * *',
#     'days': '* * * */{value} * *',
#     'weeks': '* * * * */{value} *',
# }

## v3 

_time_aliases_groups = {
    'seconds': ['s', 'sec', 'secs'],
    'minutes': ['m', 'min', 'mins'],
    'hours': ['h', 'hr', 'hrs'],
    'days': ['d', 'day'],
    'weeks': ['w', 'wk', 'wks'],
    'months': ['mo', 'mon', 'mons'],
}
_time_aliases = {alias: unit for unit, aliases in _time_aliases_groups.items() for alias in aliases}
_time_pattern = re.compile(r'(?:(?:every )?(\d+) (\w+))(?:, | and )?')

def validate_cron_schedule(cron_schedule: str) -> str:
    """
    Convert natural language to cron format using regex patterns
    """
    if croniter.croniter.is_valid(cron_schedule): return cron_schedule
    time_units = {
        'seconds': None,
        'minutes': '*',
        'hours': '*',
        'days': '*',
        'weeks': '*',
        'months': '*'
    }
    match = _time_pattern.findall(cron_schedule)
    if not match: raise ValueError(f"Invalid cron expression: {cron_schedule}")

    for num, unit in match:
        if unit in _time_aliases: unit = _time_aliases[unit]
        if not unit.endswith('s'): unit += 's'
        if unit not in time_units:
            raise ValueError(f"Invalid time unit in cron expression: unit: {unit}, num: {num}")
        time_units[unit] = f'*/{num}'
    
    if time_units['hours'] != "*" and time_units['minutes'] == '*':
        time_units['minutes'] = 0
    if time_units['days'] != "*" and time_units['hours'] == '*':
        time_units['hours'] = 0
    if time_units['weeks'] != "*" and time_units['days'] == '*':
        time_units['days'] = 0
    if time_units['months'] != "*" and time_units['weeks'] == '*':
        time_units['weeks'] = 0
    
    cron_expression = f"{time_units['minutes']} {time_units['hours']} {time_units['days']} {time_units['months']} {time_units['weeks']}"
    if time_units['seconds']:
        cron_expression += f" {time_units['seconds']}"
    return cron_expression.strip()


