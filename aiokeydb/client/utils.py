import hashlib, inspect
from typing import Optional, List
from aiokeydb.client.types import ENOVAL

__all__ = [
    'full_name',
    'args_to_key'
]

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
