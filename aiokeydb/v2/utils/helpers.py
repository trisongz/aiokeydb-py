from __future__ import annotations

import inspect
import logging
from tenacity import retry, wait_exponential, stop_after_delay, before_sleep_log

from typing import Optional, Union, Type, TYPE_CHECKING

if TYPE_CHECKING:
    from aiokeydb.v2.core import KeyDB, AsyncKeyDB


logger = logging.getLogger(__name__)

def create_retryable_client(
    client: Type[Union[KeyDB, AsyncKeyDB]],

    max_attempts: int = 15,
    max_delay: int = 60,
    logging_level: int = logging.WARNING,

    verbose: Optional[bool] = False,
    **kwargs
) -> Type[Union[KeyDB, AsyncKeyDB]]:
    """
    Creates a retryable client
    """
    if hasattr(client, '_is_retryable_wrapped'): return client
    decorator = retry(
        wait = wait_exponential(multiplier = 0.5, min = 1, max = max_attempts),
        stop = stop_after_delay(max_delay),
        before_sleep = before_sleep_log(logger, logging_level)
    )

    for attr in dir(client):
        if attr.startswith('_'): continue
        attr_val = getattr(client, attr)
        if inspect.isfunction(attr_val) or inspect.iscoroutinefunction(attr_val):
            if verbose: logger.info(f'Wrapping {attr} with retryable decorator')
            setattr(client, attr, decorator(attr_val))
        
    setattr(client, '_is_retryable_wrapped', True)
    return client



    

# def wrap_retryable(
#     sess: 'KeyDBSession'
# ):
#     """
#     Wraps the keydb-session with a retryable decorator
#     """
#     if hasattr(sess, '_is_retryable_wrapped'): return sess
#     decorator = retry(
#         wait = wait_exponential(multiplier=0.5, min=1, max=15),
#         stop = stop_after_delay(60),
#         before_sleep = before_sleep_log(logger, logging.INFO)
#     )
#     for attr in dir(sess):
#         attr_val = getattr(sess, attr)
#         if inspect.isfunction(attr_val) or inspect.iscoroutinefunction(attr_val):
#             logger.info(f'Wrapping {attr} with retryable decorator')
#             setattr(sess, attr, decorator(attr_val))
        
#     setattr(sess, '_is_retryable_wrapped', True)
#     return sess
