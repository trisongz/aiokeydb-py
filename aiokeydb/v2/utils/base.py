
import hashlib
import inspect
import logging
from contextlib import contextmanager, asynccontextmanager
from typing import Union, Optional, List, Callable, Generator, AsyncGenerator, TYPE_CHECKING
from aiokeydb.v2.types import ENOVAL

try:
    import hiredis  # noqa

    # Only support Hiredis >= 1.0:
    HIREDIS_AVAILABLE = not hiredis.__version__.startswith("0.")
    HIREDIS_PACK_AVAILABLE = hasattr(hiredis, "pack_command")
except ImportError:
    HIREDIS_AVAILABLE = False
    HIREDIS_PACK_AVAILABLE = False

try:
    import cryptography  # noqa

    CRYPTOGRAPHY_AVAILABLE = True
except ImportError:
    CRYPTOGRAPHY_AVAILABLE = False

from aiokeydb.client.types import ENOVAL
from redis.utils import (
    str_if_bytes,
    safe_str,
    dict_merge,
    list_keys_to_dict,
    merge_result,
)

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from _aiokeydb.client import KeyDB, AsyncKeyDB, Pipeline, AsyncPipeline

def from_url(url, asyncio: bool = False, _is_async: Optional[bool] = None, **kwargs) -> Union["KeyDB", "AsyncKeyDB"]:
    """
    Returns an active Redis client generated from the given database URL.

    Will attempt to extract the database id from the path url fragment, if
    none is provided.
    """
    _is_async = asyncio if _is_async is None else _is_async

    if _is_async:
        from _aiokeydb.client import AsyncKeyDB
        return AsyncKeyDB.from_url(url, **kwargs)
    
    from _aiokeydb.client import KeyDB
    return KeyDB.from_url(url, **kwargs)



@contextmanager
def pipeline(keydb_obj: "KeyDB") -> Generator["Pipeline", None, None]:
    p = keydb_obj.pipeline()
    yield p
    p.execute()
    del p

@asynccontextmanager
async def async_pipeline(keydb_obj: "AsyncKeyDB") -> AsyncGenerator["AsyncPipeline", None]:
    p = keydb_obj.pipeline()
    yield p
    await p.execute()
    del p

def get_ulimits():
    import resource
    soft_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    return soft_limit

def set_ulimits(
    max_connections: int = 500,
    verbose: bool = False,
):
    """
    Sets the system ulimits
    to allow for the maximum number of open connections

    - if the current ulimit > max_connections, then it is ignored
    - if it is less, then we set it.
    """
    import resource

    soft_limit, hard_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft_limit > max_connections: return
    if hard_limit < max_connections and verbose:
        logger.warning(f"The current hard limit ({hard_limit}) is less than max_connections ({max_connections}).")
    new_hard_limit = max(hard_limit, max_connections)
    if verbose: logger.info(f"Setting new ulimits to ({soft_limit}, {hard_limit}) -> ({max_connections}, {new_hard_limit})")
    resource.setrlimit(resource.RLIMIT_NOFILE, (max_connections + 10, new_hard_limit))
    new_soft, new_hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if verbose: logger.info(f"New Limits: ({new_soft}, {new_hard})")


def full_name(func, follow_wrapper_chains=True):
    """
    Return full name of `func` by adding the module and function name.

    If this function is decorated, attempt to unwrap it till the original function to use that
    function name by setting `follow_wrapper_chains` to True.
    """
    if follow_wrapper_chains: func = inspect.unwrap(func)
    return f'{func.__module__}.{func.__qualname__}'


def args_to_key(
    base, 
    args: Optional[tuple] = None, 
    kwargs: Optional[dict] = None, 
    typed: bool = False,
    exclude: Optional[List[str]] = None
):
    """Create cache key out of function arguments.
    :param tuple base: base of key
    :param tuple args: function arguments
    :param dict kwargs: function keyword arguments
    :param bool typed: include types in cache key
    :return: cache key tuple
    """
    key = base + args

    if kwargs:
        if exclude: kwargs = {k: v for k, v in kwargs.items() if k not in exclude}
        key += (ENOVAL,)
        sorted_items = sorted(kwargs.items())

        for item in sorted_items:
            key += item

    if typed:
        key += tuple(type(arg) for arg in args)
        if kwargs: key += tuple(type(value) for _, value in sorted_items)

    cache_key = ':'.join(str(k) for k in key)
    return hashlib.md5(cache_key.encode()).hexdigest()


def import_string(func: str) -> Callable:
    """Import a function from a string."""
    module, func = func.rsplit('.', 1)
    return getattr(__import__(module, fromlist=[func]), func)

