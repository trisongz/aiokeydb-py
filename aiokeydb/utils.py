import logging
from contextlib import contextmanager
from typing import Any, Dict, Mapping, Union

try:
    import hiredis  # noqa

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

logger = logging.getLogger(__name__)

def from_url(url, asyncio: bool = False, **kwargs):
    """
    Returns an active Redis client generated from the given database URL.

    Will attempt to extract the database id from the path url fragment, if
    none is provided.
    """
    if asyncio:
        from aiokeydb.asyncio.core import AsyncKeyDB
        return AsyncKeyDB.from_url(url, **kwargs)
    
    from aiokeydb.core import KeyDB
    return KeyDB.from_url(url, **kwargs)


@contextmanager
def pipeline(keydb_obj):
    p = keydb_obj.pipeline()
    yield p
    p.execute()


def str_if_bytes(value: Union[str, bytes]) -> str:
    return (
        value.decode("utf-8", errors="replace") if isinstance(value, bytes) else value
    )


def safe_str(value):
    return str(str_if_bytes(value))


def dict_merge(*dicts: Mapping[str, Any]) -> Dict[str, Any]:
    """
    Merge all provided dicts into 1 dict.
    *dicts : `dict`
        dictionaries to merge
    """
    merged = {}

    for d in dicts:
        merged.update(d)

    return merged


def list_keys_to_dict(key_list, callback):
    return dict.fromkeys(key_list, callback)


def merge_result(command, res):
    """
    Merge all items in `res` into a list.

    This command is used when sending a command to multiple nodes
    and the result from each node should be merged into a single list.

    res : 'dict'
    """
    result = set()

    for v in res.values():
        for value in v:
            result.add(value)

    return list(result)


def get_ulimits():
    import resource
    soft_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    return soft_limit

def set_ulimits(max_connections: int = 500):
    """
    Sets the system ulimits
    to allow for the maximum number of open connections

    - if the current ulimit > max_connections, then it is ignored
    - if it is less, then we set it.
    """
    import resource

    soft_limit, hard_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft_limit > max_connections: return
    if hard_limit < max_connections:
        logger.warning(f"The current hard limit ({hard_limit}) is less than max_connections ({max_connections}).")
    new_hard_limit = max(hard_limit, max_connections)
    logger.info(f"Setting new ulimits to ({soft_limit}, {hard_limit}) -> ({max_connections}, {new_hard_limit})")
    resource.setrlimit(resource.RLIMIT_NOFILE, (max_connections + 10, new_hard_limit))
    new_soft, new_hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    logger.info(f"New Limits: ({new_soft}, {new_hard})")

