from __future__ import annotations

import inspect
import logging
from tenacity import retry, wait_exponential, stop_after_delay, before_sleep_log, retry_unless_exception_type, retry_if_exception_type, retry_if_exception
from typing import Optional, Union, Tuple, Type, TYPE_CHECKING

if TYPE_CHECKING:
    from aiokeydb.v2.core import KeyDB, AsyncKeyDB
    from tenacity import WrappedFn


logger = logging.getLogger(__name__)

_excluded_funcs = ['parse_response']

class retry_if_type(retry_if_exception):
    """
    Retries if the exception is of the given type
    """
    def __init__(
        self, 
        exception_types: Union[
            Type[BaseException],
            Tuple[Type[BaseException], ...],
        ] = Exception,
        excluded_types: Union[
            Type[BaseException],
            Tuple[Type[BaseException], ...],
        ] = None,
    ):
        self.exception_types = exception_types
        self.excluded_types = excluded_types
        super().__init__(
            lambda e: isinstance(e, exception_types) and not isinstance(e, excluded_types)
        )

def get_retryable_wrapper(  
    max_attempts: int = 15,
    max_delay: int = 60,
    logging_level: int = logging.DEBUG,
    **kwargs,
) -> 'WrappedFn':
    
    """
    Creates a retryable decorator
    """
    from aiokeydb import exceptions as aiokeydb_exceptions
    from redis import exceptions as redis_exceptions
    return retry(
        wait = wait_exponential(multiplier = 0.5, min = 1, max = max_attempts),
        stop = stop_after_delay(max_delay),
        before_sleep = before_sleep_log(logger, logging_level),
        retry = retry_if_type(
            exception_types = (
                aiokeydb_exceptions.ConnectionError,
                aiokeydb_exceptions.TimeoutError,
                aiokeydb_exceptions.BusyLoadingError,
                redis_exceptions.ConnectionError,
                redis_exceptions.TimeoutError,
                redis_exceptions.BusyLoadingError,
            ),
            excluded_types = (
                aiokeydb_exceptions.AuthenticationError,
                aiokeydb_exceptions.AuthorizationError,
                aiokeydb_exceptions.InvalidResponse,
                aiokeydb_exceptions.ResponseError,
                aiokeydb_exceptions.NoScriptError,
                redis_exceptions.AuthenticationError,
                redis_exceptions.AuthorizationError,
                redis_exceptions.InvalidResponse,
                redis_exceptions.ResponseError,
                redis_exceptions.NoScriptError,
            )
        )
    )


def create_retryable_client(
    client: Type[Union[KeyDB, AsyncKeyDB]],

    max_attempts: int = 15,
    max_delay: int = 60,
    logging_level: int = logging.DEBUG,

    verbose: Optional[bool] = False,
    **kwargs
) -> Type[Union[KeyDB, AsyncKeyDB]]:
    """
    Creates a retryable client
    """
    if hasattr(client, '_is_retryable_wrapped'): return client
    decorator = get_retryable_wrapper(
        max_attempts = max_attempts,
        max_delay = max_delay,
        logging_level = logging_level,
        **kwargs
    )
    for attr in dir(client):
        if attr.startswith('_'): continue
        if attr in _excluded_funcs: continue
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
