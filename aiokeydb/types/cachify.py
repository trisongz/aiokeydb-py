from __future__ import annotations

"""
Custom KeyDB Caching
"""
import time
import anyio
import inspect
import contextlib 
import functools
import hashlib

from lazyops.types.common import UpperStrEnum
from lazyops.utils import timed_cache
from lazyops.utils.helpers import create_background_task, fail_after
from lazyops.utils.lazy import lazy_import
from lazyops.utils.pooler import ThreadPooler
from lazyops.utils.lazy import get_function_name

from .compat import BaseModel, root_validator, get_pyd_dict
from .base import ENOVAL
from typing import Optional, Dict, Any, Callable, List, Union, TypeVar, Type, overload, TYPE_CHECKING
from aiokeydb.utils.logs import logger

if TYPE_CHECKING:
    from .session import KeyDBSession


def get_cachify_session(name: Optional[str] = None, **kwargs) -> 'KeyDBSession':
    """
    Returns the cachify session
    """
    from aiokeydb import KeyDBClient
    return KeyDBClient.get_session(name = name, **kwargs)


async def run_as_coro(
    func: Callable,
    *args,
    **kwargs
) -> Any:
    """
    Runs a function as a coroutine
    """
    return await ThreadPooler.asyncish(func, *args, **kwargs)

def isclassmethod(method):
    bound_to = getattr(method, '__self__', None)
    if not isinstance(bound_to, type):
        # must be bound to a class
        return False
    name = method.__name__
    for cls in bound_to.__mro__:
        descriptor = vars(cls).get(name)
        if descriptor is not None:
            return isinstance(descriptor, classmethod)
    return False

@contextlib.contextmanager
def safely(
    timeout: Optional[float] = 1.0,
    verbose: Optional[bool] = False,
):
    """
    Safely runs a coroutine
    """
    try:
        with fail_after(timeout):
            yield
    except Exception as e:
        if verbose:  logger.trace('Exception', e)


@contextlib.asynccontextmanager
async def asafely(
    timeout: Optional[float] = 1.0,
    verbose: Optional[bool] = False,
):
    """
    Safely runs a coroutine
    """
    try:
        async with anyio.fail_after(timeout):
            yield
    except Exception as e:
        if verbose: 
            logger.trace('Exception', e)

    

def hash_key(
    args: Optional[tuple] = None, 
    kwargs: Optional[dict] = None, 
    typed: Optional[bool] = True,
    exclude_keys: Optional[List[str]] = None,
    exclude_null_values: Optional[bool] = None,
    exclude_default_values: Optional[bool] = None,

    # Private
    is_class_method: Optional[bool] = None,
):
    """Create cache key out of function arguments.
    :param tuple base: base of key
    :param tuple args: function arguments
    :param dict kwargs: function keyword arguments
    :param bool typed: include types in cache key
    :param list exclude_keys: exclude these keys from cache key
    :param bool exclude_null_values: exclude null values from cache key
    :param bool exclude_default_values: exclude default values from cache key
    :return: cache key tuple
    """
    if is_class_method and args: 
        # logger.warning(f'Class Method: {args}')
        args = args[1:]

        
    key = args or ()
    if kwargs:
        if exclude_keys: kwargs = {k: v for k, v in kwargs.items() if k not in exclude_keys}
        if exclude_null_values: kwargs = {k: v for k, v in kwargs.items() if v is not None}
        # if exclude_default_values: kwargs = {k: v for k, v in kwargs.items() if v != kwargs.get(k)}
        if exclude_default_values: kwargs = {k: v for k, v in kwargs.items() if not v}
        key += (ENOVAL,)
        sorted_items = sorted(kwargs.items())
        for item in sorted_items:
            key += item

    if typed:
        key += tuple(type(arg) for arg in args)
        if kwargs: key += tuple(type(value) for _, value in sorted_items)

    cache_key = ':'.join(str(k) for k in key)
    # logger.warning(f'Cache Key:\nargs: {args}\nkwargs: {kwargs}\n{cache_key}')
    return hashlib.md5(cache_key.encode()).hexdigest()


class CachePolicy(UpperStrEnum):
    """
    The cache policy for cachify

    LRU: Least Recently Used
        Discards the least recently used items first by timestamp
    LFU: Least Frequently Used
        Discards the least frequently used items first by hits
    FIFO: First In First Out
        Discards the oldest items first by timestamp
    LIFO: Last In First Out
        Discards the newest items first by timestamp
    """
    LRU = 'LRU'
    LFU = 'LFU'
    FIFO = 'FIFO'
    LIFO = 'LIFO'


class CachifyKwargs(BaseModel):
    """
    Cachify Config
    """

    ttl: Optional[int] = 60 * 10 # 10 minutes
    keybuilder: Optional[Callable] = None
    name: Optional[Union[str, Callable]] = None
    typed: Optional[bool] = True
    exclude_keys: Optional[List[str]] = None
    exclude_null: Optional[bool] = True
    exclude_exceptions: Optional[Union[bool, List[Exception]]] = True

    exclude_null_values_in_hash: Optional[bool] = None
    exclude_default_values_in_hash: Optional[bool] = None

    disabled: Optional[Union[bool, Callable]] = None
    invalidate_after: Optional[Union[int, Callable]] = None
    invalidate_if: Optional[Callable] = None
    overwrite_if: Optional[Callable] = None
    bypass_if: Optional[Callable] = None

    timeout: Optional[float] = 5.0
    verbose: Optional[bool] = False
    super_verbose: Optional[bool] = False
    raise_exceptions: Optional[bool] = True

    encoder: Optional[Union[str, Callable]] = None
    decoder: Optional[Union[str, Callable]] = None

    # Allow for custom hit setters and getters
    hit_setter: Optional[Callable] = None
    hit_getter: Optional[Callable] = None

    # Allow for max cache size
    cache_max_size: Optional[int] = None
    cache_max_size_policy: Optional[Union[str, CachePolicy]] = CachePolicy.LFU # 'LRU' | 'LFU' | 'FIFO' | 'LIFO'

    # Allow for post-init hooks
    post_init_hook: Optional[Union[str, Callable]] = None
    
    # Allow for post-call hooks
    post_call_hook: Optional[Union[str, Callable]] = None

    # Private
    cache_field: Optional[str] = None
    # session: Optional['KeyDBSession'] = None
    is_class_method: Optional[bool] = None
    has_ran_post_init_hook: Optional[bool] = None
    is_async: Optional[bool] = None

    if TYPE_CHECKING:
        session: Optional['KeyDBSession'] = None
    else:
        session: Optional[Any] = None

    # is_object_method: Optional[bool] = None

    @classmethod
    def validate_callable(cls, v: Optional[Union[str, int, Callable]]) -> Optional[Union[Callable, Any]]:
        """
        Validates the callable
        """
        return lazy_import(v) if isinstance(v, str) else v

    @classmethod
    def validate_decoder(cls, v) -> Optional[Callable]:
        """
        Returns the decoder
        """
        if v is None: 
            from aiokeydb.serializers import DillSerializer
            return DillSerializer.loads
        v = cls.validate_callable(v)
        if not inspect.isfunction(v):
            if hasattr(v, 'loads') and inspect.isfunction(v.loads):
                return v.loads
            raise ValueError('Encoder must be callable or have a callable "dumps" method')
        return v
        

    @classmethod
    def validate_encoder(cls, v) -> Optional[Callable]:
        """
        Returns the encoder
        """
        if v is None: 
            from aiokeydb.serializers import DillSerializer
            return DillSerializer.dumps
        v = cls.validate_callable(v)
        if not inspect.isfunction(v):
            if hasattr(v, 'dumps') and inspect.isfunction(v.dumps):
                return v.dumps
            raise ValueError('Encoder must be callable or have a callable "dumps" method')
        return v
    
    @classmethod
    def validate_kws(cls, values: Dict[str, Any], is_update: Optional[bool] = False) -> Dict[str, Any]:
        """
        Validates the attributes
        """
        if 'name' in values:
            values['name'] = cls.validate_callable(values.get('name'))
        if 'keybuilder' in values:
            values['keybuilder'] = cls.validate_callable(values.get('keybuilder'))
        if 'encoder' in values:
            values['encoder'] = cls.validate_encoder(values.get('encoder'))
        if 'decoder' in values:
            values['decoder'] = cls.validate_decoder(values.get('decoder'))
        if 'hit_setter' in values:
            values['hit_setter'] = cls.validate_callable(values.get('hit_setter'))
        if 'hit_getter' in values:
            values['hit_getter'] = cls.validate_callable(values.get('hit_getter'))
        if 'disabled' in values:
            values['disabled'] = cls.validate_callable(values.get('disabled'))
        if 'invalidate_if' in values:
            values['invalidate_if'] = cls.validate_callable(values.get('invalidate_if'))
        if 'invalidate_after' in values:
            values['invalidate_after'] = cls.validate_callable(values.get('invalidate_after'))
        if 'overwrite_if' in values:
            values['overwrite_if'] = cls.validate_callable(values.get('overwrite_if'))
        if 'bypass_if' in values:
            values['bypass_if'] = cls.validate_callable(values.get('bypass_if'))
        if 'post_init_hook' in values:
            values['post_init_hook'] = cls.validate_callable(values.get('post_init_hook'))
        if 'post_call_hook' in values:
            values['post_call_hook'] = cls.validate_callable(values.get('post_call_hook'))

        if 'cache_max_size' in values:
            values['cache_max_size'] = int(values['cache_max_size']) if values['cache_max_size'] else None
            if 'cache_max_size_policy' in values:
                values['cache_max_size_policy'] = CachePolicy(values['cache_max_size_policy'])
            elif not is_update:
                values['cache_max_size_policy'] = CachePolicy.LFU
        elif 'cache_max_size_policy' in values:
            values['cache_max_size_policy'] = CachePolicy(values['cache_max_size_policy'])
        return values
        

    class Config:
        """
        Config for CachifyKwargs
        """
        extra = 'ignore'
        arbitrary_types_allowed = True

    @root_validator(mode = 'after')
    def validate_attrs(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validates the attributes
        """
        return cls.validate_kws(values)

    def update(self, **kwargs):
        """
        Validates and updates the kwargs
        """
        kwargs = self.validate_kws(kwargs, is_update = True)
        for k, v in kwargs.items():
            if not hasattr(self, k): continue
            setattr(self, k, v)


    def build_hash_name(self, func: Callable, *args, **kwargs) -> str:
        """
        Builds the name for the function
        """
        if self.cache_field is not None: return self.cache_field
        if self.name: 
            self.cache_field = self.name(func, *args, **kwargs) if callable(self.name) else self.name
        else:
            func = inspect.unwrap(func)
            self.cache_field = f'{func.__module__}.{func.__qualname__}'
        return self.cache_field
    
    async def abuild_hash_name(self, func: Callable, *args, **kwargs) -> str:
        """
        Builds the name for the function
        """
        if self.cache_field is not None: return self.cache_field
        if self.name: 
            self.cache_field = await run_as_coro(self.name, func, *args, **kwargs) if callable(self.name) else self.name
        else:
            func = inspect.unwrap(func)
            self.cache_field = f'{func.__module__}.{func.__qualname__}'
        return self.cache_field
    
    def build_hash_key(self, *args, **kwargs) -> str:
        """
        Builds the key for the function
        """
        hash_func = self.keybuilder or hash_key
        return hash_func(
            args = args, 
            kwargs = kwargs, 
            typed = self.typed, 
            exclude_keys = self.exclude_keys,
            exclude_null_values = self.exclude_null_values_in_hash,
            exclude_default_values = self.exclude_default_values_in_hash,
            is_class_method = self.is_class_method,
        )
    
    async def abuild_hash_key(self, *args, **kwargs) -> str:
        """
        Builds the key for the function
        """

        hash_func = self.keybuilder or hash_key
        return await run_as_coro(
            hash_func, 
            args = args, 
            kwargs = kwargs, 
            typed = self.typed, 
            exclude_keys = self.exclude_keys,
            exclude_null_values = self.exclude_null_values_in_hash,
            exclude_default_values = self.exclude_default_values_in_hash,
            is_class_method = self.is_class_method,
        )
        
    
    def should_cache(self, *args, **kwargs) -> bool:
        """
        Returns whether or not the function should be cached
        """
        if self.disabled is not None: return not self.disabled
        return not self.disabled(*args, **kwargs) if callable(self.disabled) else True
    
    async def ashould_cache(self, *args, **kwargs) -> bool:
        """
        Returns whether or not the function should be cached
        """
        if self.disabled is not None: return not self.disabled
        return not await run_as_coro(self.disabled, *args, **kwargs) if callable(self.disabled) else True
    

    def should_cache_value(self, val: Any) -> bool:
        """
        Returns whether or not the value should be cached
        """
        if self.exclude_null and val is None: return False
        if self.exclude_exceptions:
            if isinstance(self.exclude_exceptions, list): 
                return not isinstance(val, tuple(self.exclude_exceptions))
            if isinstance(val, Exception): return False
        return True
    
    async def ashould_cache_value(self, val: Any) -> bool:
        """
        Returns whether or not the value should be cached
        """
        if self.exclude_null and val is None: return False
        if self.exclude_exceptions:
            if isinstance(self.exclude_exceptions, list): 
                return not isinstance(val, tuple(self.exclude_exceptions))
            if isinstance(val, Exception): return False
        return True

    def should_invalidate(self, *args, _hits: Optional[int] = None, **kwargs) -> bool:
        """
        Returns whether or not the function should be invalidated
        """
        if self.invalidate_if is not None: return self.invalidate_if(*args, **kwargs)
        if self.invalidate_after is not None: 
            if _hits and isinstance(self.invalidate_after, int):
                return _hits >= self.invalidate_after
            return self.invalidate_after(*args, _hits = _hits, **kwargs)
        return False
    
    async def ashould_invalidate(self, *args, **kwargs) -> bool:
        """
        Returns whether or not the function should be invalidated
        """
        if self.invalidate_if is not None: return await run_as_coro(self.invalidate_if, *args, **kwargs)
        if self.invalidate_after is not None: 
            _hits = await self.anum_hits
            if _hits and isinstance(self.invalidate_after, int):
                return _hits >= self.invalidate_after
            return await run_as_coro(self.invalidate_after, *args, _hits = _hits, **kwargs)
        return False
    

    def should_bypass(self, *args, **kwargs) -> bool:
        """
        Returns whether or not the cache should be bypassed, returning 
        a fresh value from the function call
        """
        return self.bypass_if(*args, **kwargs) if \
            self.bypass_if is not None else False
    
    async def ashould_bypass(self, *args, **kwargs) -> bool:
        """
        Returns whether or not the cache should be bypassed, returning 
        a fresh value from the function call
        """
        if self.bypass_if is not None: 
            return await run_as_coro(self.bypass_if, *args, **kwargs)
        return False

    """
    Props
    """
    @property
    def has_post_init_hook(self) -> bool:
        """
        Returns whether or not there is a post init hook
        """
        return self.post_init_hook is not None
    
    @property
    def has_post_call_hook(self) -> bool:
        """
        Returns whether or not there is a post call hook
        """
        return self.post_call_hook is not None

    @property
    def num_default_keys(self) -> int:
        """
        Returns the number of default keys
        """
        n = 1
        if self.cache_max_size is not None: n += 2
        return n


    @property
    async def anum_hits(self) -> int:
        """
        Returns the number of hits
        """
        async with asafely(timeout = self.timeout):
            val = await self.session.async_client.hget(self.cache_field, 'hits')
            return int(val) if val else 0
        
    @property
    async def anum_keys(self) -> int:
        """
        Returns the number of keys
        """
        async with asafely(timeout = self.timeout):
            val = await self.session.async_client.hlen(self.cache_field)
            return max(int(val) - self.num_default_keys, 0) if val else 0
        
    @property
    async def acache_keys(self) -> List[str]:
        """
        Returns the keys
        """
        async with asafely(timeout = self.timeout):
            val = await self.session.async_client.hkeys(self.cache_field)
            return [v.decode() for v in val] if val else []
        
    @property
    async def acache_values(self) -> List[Any]:
        """
        Returns the values
        """
        async with asafely(timeout = self.timeout):
            val = await self.session.async_client.hvals(self.cache_field)
            return [v.decode() for v in val] if val else []
    
    @property
    async def acache_items(self) -> Dict[str, Any]:
        """
        Returns the items
        """
        async with asafely(timeout = self.timeout):
            val = await self.session.async_client.hgetall(self.cache_field)
            return {k.decode(): self.decode(v) for k, v in val.items()} if val else {}

    @property
    async def acache_keyhits(self) -> Dict[str, int]:
        """
        Returns the size of the cache
        """
        async with asafely(timeout = self.timeout):
            val = await self.session.async_client.hget(self.cache_field, 'keyhits')
            return {k.decode(): int(v) for k, v in val.items()} if val else {}
        
    @property
    async def acache_timestamps(self) -> Dict[str, float]:
        """
        Returns the size of the cache
        """
        async with asafely(timeout = self.timeout):
            val = await self.session.async_client.hget(self.cache_field, 'timestamps')
            return {k.decode(): float(v) for k, v in val.items()} if val else {}
    

    @property
    async def acache_info(self) -> Dict[str, Any]:
        """
        Returns the info for the cache
        """
        return {
            'name': self.cache_field,
            'hits': await self.anum_hits,
            'keys': await self.anum_keys,
            'keyhits': await self.acache_keyhits,
            'timestamps': await self.acache_timestamps,
            'max_size': self.cache_max_size,
            'max_size_policy': self.cache_max_size_policy,
        }


    @property
    def num_hits(self) -> int:
        """
        Returns the number of hits
        """
        with safely(timeout = self.timeout):
            val = self.session.client.hget(self.cache_field, 'hits')
            return int(val) if val else 0
        
    @property
    def num_keys(self) -> int:
        """
        Returns the number of keys
        """
        with safely(timeout = self.timeout):
            val = self.session.client.hlen(self.cache_field)
            return max(int(val) - self.num_default_keys, 0) if val else 0
        
    @property
    def cache_keys(self) -> List[str]:
        """
        Returns the keys
        """
        with safely(timeout = self.timeout):
            val = self.session.client.hkeys(self.cache_field)
            return [v.decode() for v in val] if val else []
        
    @property
    def cache_values(self) -> List[Any]:
        """
        Returns the values
        """
        with safely(timeout = self.timeout):
            val = self.session.client.hvals(self.cache_field)
            return [v.decode() for v in val] if val else []
    
    @property
    def cache_items(self) -> Dict[str, Any]:
        """
        Returns the items
        """
        with safely(timeout = self.timeout):
            val = self.session.client.hgetall(self.cache_field)
            return {k.decode(): self.decode(v) for k, v in val.items()} if val else {}

    @property
    def cache_keyhits(self) -> Dict[str, int]:
        """
        Returns the size of the cache
        """
        with safely(timeout = self.timeout):
            val = self.session.client.hget(self.cache_field, 'keyhits')
            return {k.decode(): int(v) for k, v in val.items()} if val else {}
        
    @property
    def cache_timestamps(self) -> Dict[str, float]:
        """
        Returns the size of the cache
        """
        with safely(timeout = self.timeout):
            val = self.session.client.hget(self.cache_field, 'timestamps')
            return {k.decode(): float(v) for k, v in val.items()} if val else {}
    

    @property
    def cache_info(self) -> Dict[str, Any]:
        """
        Returns the info for the cache
        """
        return {
            'name': self.cache_field,
            'hits': self.num_hits,
            'keys': self.num_keys,
            'keyhits': self.cache_keyhits,
            'timestamps': self.cache_timestamps,
            'max_size': self.cache_max_size,
            'max_size_policy': self.cache_max_size_policy,
        }


    """
    Methods
    """

    def encode(self, value: Any) -> bytes:
        """
        Encodes the value
        """
        return self.encoder(value)
    
    def decode(self, value: bytes) -> Any:
        """
        Decodes the value
        """
        return self.decoder(value)


    def invalidate_cache(self, key: str) -> int:
        """
        Invalidates the cache
        """
        with safely(timeout = self.timeout):
            return self.session.client.hdel(self.cache_field, key, 'hits', 'timestamps', 'keyhits')

    async def ainvalidate_cache(self, key: str) -> int:
        """
        Invalidates the cache
        """
        async with asafely(timeout = self.timeout):
            return await self.session.async_client.hdel(self.cache_field, key, 'hits', 'timestamps', 'keyhits')

    async def aadd_key_hit(self, key: str):
        """
        Adds a hit to the cache key
        """
        async with asafely(timeout = self.timeout):
            key_hits = await self.session.async_client.hget(self.cache_field, 'keyhits') or {}
            if key not in key_hits: key_hits[key] = 0
            key_hits[key] += 1
            await self.session.async_client.hset(self.cache_field, 'keyhits', key_hits)

    async def aadd_key_timestamp(self, key: str):
        """
        Adds a timestamp to the cache key
        """
        async with asafely(timeout = self.timeout):
            timestamps = await self.session.async_client.hget(self.cache_field, 'timestamps') or {}
            timestamps[key] = time.perf_counter()
            await self.session.async_client.hset(self.cache_field, 'timestamps', timestamps)

    async def aadd_hit(self):
        """
        Adds a hit to the cache
        """
        async with asafely(timeout = self.timeout):
            await self.session.async_client.hincrby(self.cache_field, 'hits', 1)


    async def aencode_hit(self, value: Any, *args, **kwargs) -> bytes:
        """
        Encodes the hit
        """
        if self.hit_setter is not None: 
            value = await run_as_coro(self.hit_setter, value, *args, **kwargs)
        return self.encode(value)
    
    async def adecode_hit(self, value: bytes, *args, **kwargs) -> Any:
        """
        Decodes the hit
        """
        value = self.decode(value)
        if self.hit_getter is not None: 
            value = await run_as_coro(self.hit_getter, value, *args, **kwargs)
        return value
    
    async def acheck_cache_policies(self, key: str, *args, **kwargs) -> None:
        # sourcery skip: low-code-quality
        """
        Runs the cache policies
        """
        if await self.anum_keys <= self.cache_max_size: return
        num_keys = await self.anum_keys
        if self.verbose: logger.info(f'[{self.cache_field}] Cache Max Size Reached: {num_keys}/{self.cache_max_size}. Running Cache Policy: {self.cache_max_size_policy}')
        if self.cache_max_size_policy == CachePolicy.LRU:
            # Least Recently Used
            timestamps = await self.session.async_client.hget(self.cache_field, 'timestamps') or {}
            keys_to_delete = sorted(timestamps, key = timestamps.get)[:num_keys - self.cache_max_size]
            if key in keys_to_delete: keys_to_delete.remove(key)
            if self.verbose: logger.info(f'[{self.cache_field}] Deleting {len(keys_to_delete)} Keys: {keys_to_delete}')
            await self.aclear(keys_to_delete)
            return
        
        if self.cache_max_size_policy == CachePolicy.LFU:
            # Least Frequently Used
            key_hits = await self.session.async_client.hget(self.cache_field, 'keyhits') or {}
            keys_to_delete = sorted(key_hits, key = key_hits.get)[:num_keys - self.cache_max_size]
            if key in keys_to_delete: keys_to_delete.remove(key)
            if self.verbose: logger.info(f'[{self.cache_field}] Deleting {len(keys_to_delete)} Keys: {keys_to_delete}')
            await self.aclear(keys_to_delete)
            return
        
        if self.cache_max_size_policy == CachePolicy.FIFO:
            # First In First Out
            timestamps = await self.session.async_client.hget(self.cache_field, 'timestamps') or {}
            keys_to_delete = sorted(timestamps, key = timestamps.get, reverse = True)[:num_keys - self.cache_max_size]
            if key in keys_to_delete: keys_to_delete.remove(key)
            if self.verbose: logger.info(f'[{self.cache_field}] Deleting {len(keys_to_delete)} Keys: {keys_to_delete}')
            await self.aclear(keys_to_delete)
            return
        
        if self.cache_max_size_policy == CachePolicy.LIFO:
            # Last In First Out
            timestamps = await self.session.async_client.hget(self.cache_field, 'timestamps') or {}
            keys_to_delete = sorted(timestamps, key = timestamps.get)[:num_keys - self.cache_max_size]
            if key in keys_to_delete: keys_to_delete.remove(key)
            if self.verbose: logger.info(f'[{self.cache_field}] Deleting {len(keys_to_delete)} Keys: {keys_to_delete}')
            await self.aclear(keys_to_delete)
            return

    async def avalidate_cache_policies(self, key: str, *args, **kwargs) -> None:
        """
        Runs the cache policies
        """
        await self.aadd_hit()
        if self.cache_max_size is None: return
        await self.aadd_key_timestamp(key)
        await self.aadd_key_hit(key)
        await self.acheck_cache_policies(key, *args, **kwargs)

    async def ashould_not_retrieve(self, *args, **kwargs) -> bool:
        """
        Returns whether or not the value should be retrieved
        which is based on the overwrite_if function
        """
        if self.overwrite_if is not None: 
            return await run_as_coro(self.overwrite_if, *args, **kwargs)
        return False

    async def aretrieve(self, key: str, *args, **kwargs) -> Any:
        """
        Retrieves the value from the cache
        """
        if await self.ashould_not_retrieve(*args, **kwargs): 
            if self.super_verbose: logger.info(f'[{self.cache_field}:{key}] Not Retrieving')
            return ENOVAL
        try:
            async with anyio.fail_after(self.timeout):
                if not await self.session.async_client.hexists(self.cache_field, key):
                    if self.super_verbose: logger.info(f'[{self.cache_field}:{key}] Not Found')
                    return ENOVAL
                value = await self.session.async_client.hget(self.cache_field, key)
            
        except TimeoutError:
            if self.super_verbose: logger.error(f'[{self.cache_field}:{key}] Retrieve Timeout')
            return ENOVAL
        
        except Exception as e:
            if self.verbose: logger.trace(f'[{self.cache_field}:{key}] Retrieve Exception', error = e)
            return ENOVAL
        
        create_background_task(self.avalidate_cache_policies, key, *args, **kwargs)
        return await self.adecode_hit(value)
        
    async def aset(self, key: str, value: Any, *args, **kwargs) -> None:
        """
        Sets the value in the cache
        """
        # if not await self.ashould_cache_value(value): return
        try:
            async with anyio.fail_after(self.timeout):
                await self.session.async_client.hset(
                    self.cache_field, key, 
                    await self.aencode_hit(value, *args, **kwargs)
                )
                if self.ttl:
                    await self.session.async_client.expire(self.cache_field, self.ttl)
        except TimeoutError:
            if self.super_verbose: logger.error(f'[{self.cache_field}:{key}] Set Timeout')
        except Exception as e:
            if self.verbose: logger.trace(f'[{self.cache_field}:{key}] Set Exception: {value}', error = e)
    
    async def aclear(self, keys: Union[str, List[str]] = None) -> Optional[int]:
        """
        Clears the cache
        """
        async with asafely(timeout = self.timeout):
            if keys:
                return await self.session.async_client.hdel(self.cache_field, keys)
            else:
                return await self.session.async_client.delete(self.cache_field)

    def add_key_hit(self, key: str):
        """
        Adds a hit to the cache key
        """
        with safely(timeout = self.timeout):
            key_hits = self.session.client.hget(self.cache_field, 'keyhits') or {}
            if key not in key_hits: key_hits[key] = 0
            key_hits[key] += 1
            self.session.client.hset(self.cache_field, 'keyhits', key_hits)

    def add_key_timestamp(self, key: str):
        """
        Adds a timestamp to the cache key
        """
        with safely(timeout = self.timeout):
            timestamps = self.session.client.hget(self.cache_field, 'timestamps') or {}
            timestamps[key] = time.perf_counter()
            self.session.client.hset(self.cache_field, 'timestamps', timestamps)

    def add_hit(self):
        """
        Adds a hit to the cache
        """
        with safely(timeout = self.timeout):
            self.session.client.hincrby(self.cache_field, 'hits', 1)


    def encode_hit(self, value: Any, *args, **kwargs) -> bytes:
        """
        Encodes the hit
        """
        if self.hit_setter is not None: 
            value = self.hit_setter(value, *args, **kwargs)
        return self.encode(value)
    
    def decode_hit(self, value: bytes, *args, **kwargs) -> Any:
        """
        Decodes the hit
        """
        value = self.decode(value)
        if self.hit_getter is not None: 
            value = self.hit_getter(value, *args, **kwargs)
        return value
    
    def check_cache_policies(self, key: str, *args, **kwargs) -> None:
        # sourcery skip: low-code-quality
        """
        Runs the cache policies
        """
        if self.num_keys <= self.cache_max_size: return
        num_keys = self.num_keys
        if self.verbose: logger.info(f'[{self.cache_field}] Cache Max Size Reached: {num_keys}/{self.cache_max_size}. Running Cache Policy: {self.cache_max_size_policy}')
        if self.cache_max_size_policy == CachePolicy.LRU:
            # Least Recently Used
            timestamps = self.session.client.hget(self.cache_field, 'timestamps') or {}
            keys_to_delete = sorted(timestamps, key = timestamps.get)[:num_keys - self.cache_max_size]
            if key in keys_to_delete: keys_to_delete.remove(key)
            if self.verbose: logger.info(f'[{self.cache_field}] Deleting {len(keys_to_delete)} Keys: {keys_to_delete}')
            self.clear(keys_to_delete)
            return
        
        if self.cache_max_size_policy == CachePolicy.LFU:
            # Least Frequently Used
            key_hits = self.session.client.hget(self.cache_field, 'keyhits') or {}
            keys_to_delete = sorted(key_hits, key = key_hits.get)[:num_keys - self.cache_max_size]
            if key in keys_to_delete: keys_to_delete.remove(key)
            if self.verbose: logger.info(f'[{self.cache_field}] Deleting {len(keys_to_delete)} Keys: {keys_to_delete}')
            self.clear(keys_to_delete)
            return
        
        if self.cache_max_size_policy == CachePolicy.FIFO:
            # First In First Out
            timestamps = self.session.client.hget(self.cache_field, 'timestamps') or {}
            keys_to_delete = sorted(timestamps, key = timestamps.get, reverse = True)[:num_keys - self.cache_max_size]
            if key in keys_to_delete: keys_to_delete.remove(key)
            if self.verbose: logger.info(f'[{self.cache_field}] Deleting {len(keys_to_delete)} Keys: {keys_to_delete}')
            self.clear(keys_to_delete)
            return
        
        if self.cache_max_size_policy == CachePolicy.LIFO:
            # Last In First Out
            timestamps = self.session.client.hget(self.cache_field, 'timestamps') or {}
            keys_to_delete = sorted(timestamps, key = timestamps.get)[:num_keys - self.cache_max_size]
            if key in keys_to_delete: keys_to_delete.remove(key)
            if self.verbose: logger.info(f'[{self.cache_field}] Deleting {len(keys_to_delete)} Keys: {keys_to_delete}')
            self.clear(keys_to_delete)
            return

    def validate_cache_policies(self, key: str, *args, **kwargs) -> None:
        """
        Runs the cache policies
        """
        self.add_hit()
        if self.cache_max_size is None: return
        self.add_key_timestamp(key)
        self.add_key_hit(key)
        self.check_cache_policies(key, *args, **kwargs)

    def should_not_retrieve(self, *args, **kwargs) -> bool:
        """
        Returns whether or not the value should be retrieved
        which is based on the overwrite_if function
        """
        if self.overwrite_if is not None: 
            return self.overwrite_if(*args, **kwargs)
        return False

    def retrieve(self, key: str, *args, **kwargs) -> Any:
        """
        Retrieves the value from the cache
        """
        if self.should_not_retrieve(*args, **kwargs): 
            if self.super_verbose: logger.info(f'[{self.cache_field}:{key}] Not Retrieving')
            return ENOVAL
        try:
            with anyio.fail_after(self.timeout):
                if not self.session.client.hexists(self.cache_field, key):
                    if self.super_verbose: logger.info(f'[{self.cache_field}:{key}] Not Found')
                    return ENOVAL
                value = self.session.client.hget(self.cache_field, key)
            
        except TimeoutError:
            if self.super_verbose: logger.error(f'[{self.cache_field}:{key}] Retrieve Timeout')
            return ENOVAL
        
        except Exception as e:
            if self.verbose: logger.trace(f'[{self.cache_field}:{key}] Retrieve Exception', error = e)
            return ENOVAL
        
        create_background_task(self.validate_cache_policies, key, *args, **kwargs)
        return self.decode_hit(value)
        
    def set(self, key: str, value: Any, *args, **kwargs) -> None:
        """
        Sets the value in the cache
        """
        # if not self.ashould_cache_value(value): return
        try:
            with fail_after(self.timeout):
                self.session.client.hset(
                    self.cache_field, key, 
                    self.encode_hit(value, *args, **kwargs)
                )
                if self.ttl:
                    self.session.client.expire(self.cache_field, self.ttl)
        except TimeoutError:
            if self.super_verbose: logger.error(f'[{self.cache_field}:{key}] Set Timeout')
        except Exception as e:
            if self.verbose: logger.trace(f'[{self.cache_field}:{key}] Set Exception: {value}', error = e)
    
    def clear(self, keys: Union[str, List[str]] = None) -> Optional[int]:
        """
        Clears the cache
        """
        with safely(timeout = self.timeout):
            if keys:
                return self.session.client.hdel(self.cache_field, keys)
            else:
                return self.session.client.delete(self.cache_field)


    def validate_is_class_method(self, func: Callable):
        """
        Validates if the function is a class method
        """
        if self.is_class_method is not None: return
        self.is_class_method = hasattr(func, '__class__') and inspect.isclass(func.__class__) and isclassmethod(func)
    
    async def arun_post_init_hook(self, func: Callable, *args, **kwargs) -> None:
        """
        Runs the post init hook which fires once after the function is initialized
        """
        if not self.has_post_init_hook: return
        if self.has_ran_post_init_hook: return
        if self.verbose: logger.info(f'[{self.cache_field}] Running Post Init Hook')
        create_background_task(self.post_init_hook, func, *args, **kwargs)
        self.has_ran_post_init_hook = True

    async def arun_post_call_hook(self, result: Any, *args, is_hit: Optional[bool] = None, **kwargs) -> None:
        """
        Runs the post call hook which fires after the function is called
        """
        if not self.has_post_call_hook: return
        if self.super_verbose: logger.info(f'[{self.cache_field}] Running Post Call Hook')
        create_background_task(self.post_call_hook, result, *args, is_hit = is_hit, **kwargs)

    
    def run_post_init_hook(self, func: Callable, *args, **kwargs) -> None:
        """
        Runs the post init hook which fires once after the function is initialized
        """
        if not self.has_post_init_hook: return
        if self.has_ran_post_init_hook: return
        if self.verbose: logger.info(f'[{self.cache_field}] Running Post Init Hook')
        create_background_task(self.post_init_hook, func, *args, **kwargs)
        self.has_ran_post_init_hook = True

    def run_post_call_hook(self, result: Any, *args, is_hit: Optional[bool] = None, **kwargs) -> None:
        """
        Runs the post call hook which fires after the function is called
        """
        if not self.has_post_call_hook: return
        if self.super_verbose: logger.info(f'[{self.cache_field}] Running Post Call Hook')
        create_background_task(self.post_call_hook, result, *args, is_hit = is_hit, **kwargs)


FT = TypeVar('FT', bound = Callable)

def cachify_async(
    sess: 'KeyDBSession',
    _kwargs: CachifyKwargs,
    
) -> FT:
    """
    Handles the async caching
    """

    _kwargs.session = sess
    _kwargs.is_async = True
    
    def decorator(func):

        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            # Set the cache field
            await _kwargs.abuild_hash_name(func, *args, **kwargs)
            _kwargs.validate_is_class_method(func)
            await _kwargs.arun_post_init_hook(func, *args, **kwargs)
            
            # Check if we should cache
            if not await _kwargs.ashould_cache(*args, **kwargs):
                if _kwargs.super_verbose: logger.info('Not Caching', prefix = _kwargs.cache_field, colored = True)
                return await func(*args, **kwargs)
            
            # Check if we should bypass
            if await _kwargs.ashould_bypass(*args, **kwargs):
                if _kwargs.super_verbose: logger.info('Bypassing', prefix = _kwargs.cache_field, colored = True)
                return await func(*args, **kwargs)
        
            # Get the cache key
            cache_key = await wrapper.__cache_key__(*args, **kwargs)
            
            # Check if we should invalidate
            if await _kwargs.ashould_invalidate(*args, **kwargs):
                if _kwargs.verbose: logger.info('Invalidating', prefix = f'{_kwargs.cache_field}:{cache_key}', colored = True)
                await _kwargs.ainvalidate_cache(cache_key)
            
            # Check if we have a cache hit
            value = await _kwargs.aretrieve(cache_key, *args, **kwargs)
            if value == ENOVAL:
                try:
                    value = await func(*args, **kwargs)
                    if await _kwargs.ashould_cache_value(value):
                        if _kwargs.super_verbose: logger.info('Caching Value', prefix = f'{_kwargs.cache_field}:{cache_key}', colored = True)
                        await _kwargs.aset(cache_key, value, *args, **kwargs)
                    if _kwargs.super_verbose: logger.info('Cache Miss', prefix = f'{_kwargs.cache_field}:{cache_key}', colored = True)
                    await _kwargs.arun_post_call_hook(value, *args, is_hit = False, **kwargs)
                    return value
                
                except Exception as e:
                    if _kwargs.verbose: logger.trace(f'[{_kwargs.cache_field}:{cache_key}] Exception', error = e)
                    if _kwargs.raise_exceptions: raise e
                    return None
            
            
            if _kwargs.super_verbose: logger.info('Cache Hit', prefix = f'{_kwargs.cache_field}:{cache_key}', colored = True)
            await _kwargs.arun_post_call_hook(value, *args, is_hit = True, **kwargs)
            return value

        async def __cache_key__(*args, **kwargs) -> str:
            """
            Returns the cache key
            """
            return await _kwargs.abuild_hash_key(*args, **kwargs)
        
        wrapper.__cache_key__ = __cache_key__
        return wrapper
    
    return decorator

        


def cachify_sync(
    sess: 'KeyDBSession',
    _kwargs: CachifyKwargs,
) -> FT:
    """
    Handles the sync caching
    """
    _kwargs.session = sess
    _kwargs.is_async = False


    def decorator(func):

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Set the cache field
            _kwargs.build_hash_name(func, *args, **kwargs)
            _kwargs.validate_is_class_method(func)
            _kwargs.run_post_init_hook(func, *args, **kwargs)
            
            # Check if we should cache
            if not _kwargs.should_cache(*args, **kwargs):
                if _kwargs.super_verbose: logger.info('Not Caching', prefix = _kwargs.cache_field, colored = True)
                return func(*args, **kwargs)
            
            # Check if we should bypass
            if _kwargs.should_bypass(*args, **kwargs):
                if _kwargs.super_verbose: logger.info('Bypassing', prefix = _kwargs.cache_field, colored = True)
                return func(*args, **kwargs)
        
            # Get the cache key
            cache_key = wrapper.__cache_key__(*args, **kwargs)
            
            # Check if we should invalidate
            if _kwargs.should_invalidate(*args, **kwargs):
                if _kwargs.verbose: logger.info('Invalidating', prefix = f'{_kwargs.cache_field}:{cache_key}', colored = True)
                _kwargs.invalidate_cache(cache_key)
            
            # Check if we have a cache hit
            value = _kwargs.retrieve(cache_key, *args, **kwargs)
            if value == ENOVAL:
                try:
                    value = func(*args, **kwargs)
                    if _kwargs.should_cache_value(value):
                        if _kwargs.super_verbose: logger.info('Caching Value', prefix = f'{_kwargs.cache_field}:{cache_key}', colored = True)
                        _kwargs.set(cache_key, value, *args, **kwargs)
                    if _kwargs.super_verbose: logger.info('Cache Miss', prefix = f'{_kwargs.cache_field}:{cache_key}', colored = True)
                    _kwargs.run_post_call_hook(value, *args, is_hit = False, **kwargs)
                    return value
                
                except Exception as e:
                    if _kwargs.verbose: logger.trace(f'[{_kwargs.cache_field}:{cache_key}] Exception', error = e)
                    if _kwargs.raise_exceptions: raise e
                    return None
            
            
            if _kwargs.super_verbose: logger.info('Cache Hit', prefix = f'{_kwargs.cache_field}:{cache_key}', colored = True)
            _kwargs.run_post_call_hook(value, *args, is_hit = True, **kwargs)
            return value

        def __cache_key__(*args, **kwargs) -> str:
            """
            Returns the cache key
            """
            return _kwargs.build_hash_key(*args, **kwargs)
        
        wrapper.__cache_key__ = __cache_key__
        return wrapper
    
    return decorator
    


"""
The Key Schema for Cachify is as follows:

    name: {module_name}.{func_name}
    key: {key[Callable]} | {keybuilder(*args, **kwargs)}

"""


def fallback_sync_wrapper(func: FT, session: 'KeyDBSession', _kwargs: CachifyKwargs) -> FT:
    """
    [Sync] Handles the fallback wrapper
    """

    _sess_ctx: Optional['KeyDBSession'] = None

    def _get_sess():
        nonlocal _sess_ctx
        if _sess_ctx is None:
            with contextlib.suppress(Exception):
                if session.client.ping(): _sess_ctx = session
        return _sess_ctx
    
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        """
        The wrapper for cachify
        """
        _sess = _get_sess()
        if _sess is None:
            with contextlib.suppress(Exception):
                return timed_cache(secs = _kwargs.ttl)
            return func(*args, **kwargs)
        return cachify_sync(_sess, _kwargs)(func)(*args, **kwargs)

    def clear(keys: Optional[Union[str, List[str]]] = None, **kwargs) -> Optional[int]:
        """
        Clears the cache
        """
        return _kwargs.clear(keys = keys)
    
    def num_hits(*args, **kwargs) -> int:
        """
        Returns the number of hits
        """
        return _kwargs.num_hits
    
    def num_keys(**kwargs) -> int:
        """
        Returns the number of keys
        """
        return _kwargs.num_keys
    
    def cache_keys(**kwargs) -> List[str]:
        """
        Returns the keys
        """
        return _kwargs.cache_keys
    
    def cache_values(**kwargs) -> List[Any]:
        """
        Returns the values
        """
        return _kwargs.cache_values
    
    def cache_items(**kwargs) -> Dict[str, Any]:
        """
        Returns the items
        """
        return _kwargs.cache_items
    
    def invalidate_key(key: str, **kwargs) -> int:
        """
        Invalidates the cache
        """
        return _kwargs.invalidate_cache(key)
    
    def cache_timestamps(**kwargs) -> Dict[str, float]:
        """
        Returns the timestamps
        """
        return _kwargs.cache_timestamps
    
    def cache_keyhits(**kwargs) -> Dict[str, int]:
        """
        Returns the keyhits
        """
        return _kwargs.cache_keyhits
    
    def cache_policy(**kwargs) -> Dict[str, Union[int, CachePolicy]]:
        """
        Returns the cache policy
        """
        return {
            'max_size': _kwargs.cache_max_size,
            'max_size_policy': _kwargs.cache_max_size_policy,
        }

    def cache_config(**kwargs) -> Dict[str, Any]:
        """
        Returns the cache config
        """
        values = get_pyd_dict(
            _kwargs, exclude = {'session'}
        )
        for k, v in values.items():
            if callable(v): values[k] = get_function_name(v)
        return values

    def cache_info(**kwargs) -> Dict[str, Any]:
        """
        Returns the info for the cache
        """
        return _kwargs.cache_info
    
    def cache_update(**kwargs) -> Dict[str, Any]:
        """
        Updates the cache config
        """
        _kwargs.update(**kwargs)
        return cache_config(**kwargs)

    wrapper.clear = clear
    wrapper.num_hits = num_hits
    wrapper.num_keys = num_keys
    wrapper.cache_keys = cache_keys
    wrapper.cache_values = cache_values
    wrapper.cache_items = cache_items
    wrapper.invalidate_key = invalidate_key
    wrapper.cache_timestamps = cache_timestamps
    wrapper.cache_keyhits = cache_keyhits
    wrapper.cache_policy = cache_policy
    wrapper.cache_config = cache_config
    wrapper.cache_info = cache_info
    wrapper.cache_update = cache_update


    return wrapper



def fallback_async_wrapper(func: FT, session: 'KeyDBSession', _kwargs: CachifyKwargs) -> FT:
    """
    [Async] Handles the fallback wrapper
    """
    
    _sess_ctx: Optional['KeyDBSession'] = None

    async def _get_sess():
        nonlocal _sess_ctx
        if _sess_ctx is None:
            with contextlib.suppress(Exception):
                async with anyio.fail_after(1.0):
                    if await session.async_client.ping(): _sess_ctx = session
            if _kwargs.verbose and _sess_ctx is None: logger.error('Could not connect to KeyDB')
        return _sess_ctx

    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        """
        The wrapper for cachify
        """
        _sess = await _get_sess()
        if _sess is None:
            with contextlib.suppress(Exception):
                return await timed_cache(secs = _kwargs.ttl)
            return await func(*args, **kwargs)
        return await cachify_async(_sess, _kwargs)(func)(*args, **kwargs)
    

    async def clear(keys: Optional[Union[str, List[str]]] = None, **kwargs) -> Optional[int]:
        """
        Clears the cache
        """
        return await _kwargs.aclear(keys = keys)
    
    async def num_hits(*args, **kwargs) -> int:
        """
        Returns the number of hits
        """
        return await _kwargs.anum_hits
    
    async def num_keys(**kwargs) -> int:
        """
        Returns the number of keys
        """
        return await _kwargs.anum_keys
    
    async def cache_keys(**kwargs) -> List[str]:
        """
        Returns the keys
        """
        return await _kwargs.acache_keys
    
    async def cache_values(**kwargs) -> List[Any]:
        """
        Returns the values
        """
        return await _kwargs.acache_values
    
    async def cache_items(**kwargs) -> Dict[str, Any]:
        """
        Returns the items
        """
        return await _kwargs.acache_items
    
    async def invalidate_key(key: str, **kwargs) -> int:
        """
        Invalidates the cache
        """
        return await _kwargs.ainvalidate_cache(key)
    
    async def cache_timestamps(**kwargs) -> Dict[str, float]:
        """
        Returns the timestamps
        """
        return await _kwargs.acache_timestamps
    
    async def cache_keyhits(**kwargs) -> Dict[str, int]:
        """
        Returns the keyhits
        """
        return await _kwargs.acache_keyhits
    
    async def cache_policy(**kwargs) -> Dict[str, Union[int, CachePolicy]]:
        """
        Returns the cache policy
        """
        return {
            'max_size': _kwargs.cache_max_size,
            'max_size_policy': _kwargs.cache_max_size_policy,
        }

    async def cache_config(**kwargs) -> Dict[str, Any]:
        """
        Returns the cache config
        """
        values = get_pyd_dict(
            _kwargs, exclude = {'session'}
        )
        for k, v in values.items():
            if callable(v): values[k] = get_function_name(v)
        return values

    async def cache_info(**kwargs) -> Dict[str, Any]:
        """
        Returns the info for the cache
        """
        return await _kwargs.acache_info
    
    async def cache_update(**kwargs) -> Dict[str, Any]:
        """
        Updates the cache config
        """
        _kwargs.update(**kwargs)
        return await cache_config(**kwargs)

    wrapper.clear = clear
    wrapper.num_hits = num_hits
    wrapper.num_keys = num_keys
    wrapper.cache_keys = cache_keys
    wrapper.cache_values = cache_values
    wrapper.cache_items = cache_items
    wrapper.invalidate_key = invalidate_key
    wrapper.cache_timestamps = cache_timestamps
    wrapper.cache_keyhits = cache_keyhits
    wrapper.cache_policy = cache_policy
    wrapper.cache_config = cache_config
    wrapper.cache_info = cache_info
    wrapper.cache_update = cache_update

    return wrapper



def cachify(
    session: Optional['KeyDBSession'] = None,
    **kwargs
):
    """
    This version implements a custom KeyDB caching decorator
    that utilizes hset/hget/hdel/hmset/hmget/hmgetall
    instead of the default set/get/del
    """
    _kwargs = CachifyKwargs(**kwargs)
    def decorator(func: FT) -> FT:
        """
        The decorator for cachify
        """
        nonlocal session
        if session is None:  session = get_cachify_session(name = kwargs.get('session_name'), **kwargs.get('session_kwargs', {}))
        if inspect.iscoroutinefunction(func):
            return fallback_async_wrapper(func, session, _kwargs)
        else:
            return fallback_sync_wrapper(func, session, _kwargs)
    return decorator

def create_cachify(
    **kwargs,
):
    """
    Creates a new `cachify` partial decorator with the given kwargs
    """
    # import makefun
    return functools.partial(cachify, **kwargs)

if TYPE_CHECKING:
    
    @overload
    def cachify(
        ttl: Optional[int] = None,
        keybuilder: Optional[Callable] = None,
        name: Optional[Union[str, Callable]] = None,
        typed: Optional[bool] = True,
        
        exclude_keys: Optional[List[str]] = None,
        exclude_null: Optional[bool] = True,
        exclude_exceptions: Optional[Union[bool, List[Exception]]] = True,
        exclude_null_values_in_hash: Optional[bool] = None,
        exclude_default_values_in_hash: Optional[bool] = None,

        disabled: Optional[Union[bool, Callable]] = None,
        invalidate_after: Optional[Union[int, Callable]] = None,
        invalidate_if: Optional[Callable] = None,
        overwrite_if: Optional[Callable] = None,
        bypass_if: Optional[Callable] = None,

        timeout: Optional[float] = 1.0,
        verbose: Optional[bool] = False,
        super_verbose: Optional[bool] = False,
        raise_exceptions: Optional[bool] = True,

        encoder: Optional[Union[str, Callable]] = None,
        decoder: Optional[Union[str, Callable]] = None,

        hit_setter: Optional[Callable] = None,
        hit_getter: Optional[Callable] = None,

        # Private
        cache_field: Optional[str] = None,
        session: Optional['KeyDBSession'] = None,
        **kwargs,
    ) -> Callable[[FT], FT]:
        """
        Enhanced Cachify

        Args:
            ttl (Optional[int], optional): The TTL for the cache. Defaults to None.
            keybuilder (Optional[Callable], optional): The keybuilder for the cache. Defaults to None.
            name (Optional[Union[str, Callable]], optional): The name for the cache. Defaults to None.
            typed (Optional[bool], optional): Whether or not to include types in the cache key. Defaults to True.
            exclude_keys (Optional[List[str]], optional): The keys to exclude from the cache key. Defaults to None.
            exclude_null (Optional[bool], optional): Whether or not to exclude null values from the cache. Defaults to True.
            exclude_exceptions (Optional[Union[bool, List[Exception]]], optional): Whether or not to exclude exceptions from the cache. Defaults to True.
            exclude_null_values_in_hash (Optional[bool], optional): Whether or not to exclude null values from the cache hash. Defaults to None.
            exclude_default_values_in_hash (Optional[bool], optional): Whether or not to exclude default values from the cache hash. Defaults to None.
            disabled (Optional[Union[bool, Callable]], optional): Whether or not the cache is disabled. Defaults to None.
            invalidate_after (Optional[Union[int, Callable]], optional): The number of hits after which the cache should be invalidated. Defaults to None.
            invalidate_if (Optional[Callable], optional): The function to determine whether or not the cache should be invalidated. Defaults to None.
            overwrite_if (Optional[Callable], optional): The function to determine whether or not the cache should be overwritten. Defaults to None.
            bypass_if (Optional[Callable], optional): The function to determine whether or not the cache should be bypassed. Defaults to None.
            timeout (Optional[float], optional): The timeout for the cache. Defaults to 1.0.
            verbose (Optional[bool], optional): Whether or not to log verbose messages. Defaults to False.
            super_verbose (Optional[bool], optional): Whether or not to log super verbose messages. Defaults to False.
            raise_exceptions (Optional[bool], optional): Whether or not to raise exceptions. Defaults to True.
            encoder (Optional[Union[str, Callable]], optional): The encoder for the cache. Defaults to None.
            decoder (Optional[Union[str, Callable]], optional): The decoder for the cache. Defaults to None.
            hit_setter (Optional[Callable], optional): The hit setter for the cache. Defaults to None.
            hit_getter (Optional[Callable], optional): The hit getter for the cache. Defaults to None.
            
        """
        ...

    
    @overload
    def create_cachify(
        ttl: Optional[int] = None,
        keybuilder: Optional[Callable] = None,
        name: Optional[Union[str, Callable]] = None,
        typed: Optional[bool] = True,
        
        exclude_keys: Optional[List[str]] = None,
        exclude_null: Optional[bool] = True,
        exclude_exceptions: Optional[Union[bool, List[Exception]]] = True,
        exclude_null_values_in_hash: Optional[bool] = None,
        exclude_default_values_in_hash: Optional[bool] = None,

        disabled: Optional[Union[bool, Callable]] = None,
        invalidate_after: Optional[Union[int, Callable]] = None,
        invalidate_if: Optional[Callable] = None,
        overwrite_if: Optional[Callable] = None,
        bypass_if: Optional[Callable] = None,

        timeout: Optional[float] = 1.0,
        verbose: Optional[bool] = False,
        super_verbose: Optional[bool] = False,
        raise_exceptions: Optional[bool] = True,

        encoder: Optional[Union[str, Callable]] = None,
        decoder: Optional[Union[str, Callable]] = None,

        hit_setter: Optional[Callable] = None,
        hit_getter: Optional[Callable] = None,

        # Private
        cache_field: Optional[str] = None,
        session: Optional['KeyDBSession'] = None,
        **kwargs,
    ) -> Callable[[FT], FT]:
        """
        Creates a new `cachify` partial decorator with the given kwargs

        Args:
            ttl (Optional[int], optional): The TTL for the cache. Defaults to None.
            keybuilder (Optional[Callable], optional): The keybuilder for the cache. Defaults to None.
            name (Optional[Union[str, Callable]], optional): The name for the cache. Defaults to None.
            typed (Optional[bool], optional): Whether or not to include types in the cache key. Defaults to True.
            exclude_keys (Optional[List[str]], optional): The keys to exclude from the cache key. Defaults to None.
            exclude_null (Optional[bool], optional): Whether or not to exclude null values from the cache. Defaults to True.
            exclude_exceptions (Optional[Union[bool, List[Exception]]], optional): Whether or not to exclude exceptions from the cache. Defaults to True.
            exclude_null_values_in_hash (Optional[bool], optional): Whether or not to exclude null values from the cache hash. Defaults to None.
            exclude_default_values_in_hash (Optional[bool], optional): Whether or not to exclude default values from the cache hash. Defaults to None.
            disabled (Optional[Union[bool, Callable]], optional): Whether or not the cache is disabled. Defaults to None.
            invalidate_after (Optional[Union[int, Callable]], optional): The number of hits after which the cache should be invalidated. Defaults to None.
            invalidate_if (Optional[Callable], optional): The function to determine whether or not the cache should be invalidated. Defaults to None.
            overwrite_if (Optional[Callable], optional): The function to determine whether or not the cache should be overwritten. Defaults to None.
            bypass_if (Optional[Callable], optional): The function to determine whether or not the cache should be bypassed. Defaults to None.
            timeout (Optional[float], optional): The timeout for the cache. Defaults to 1.0.
            verbose (Optional[bool], optional): Whether or not to log verbose messages. Defaults to False.
            super_verbose (Optional[bool], optional): Whether or not to log super verbose messages. Defaults to False.
            raise_exceptions (Optional[bool], optional): Whether or not to raise exceptions. Defaults to True.
            encoder (Optional[Union[str, Callable]], optional): The encoder for the cache. Defaults to None.
            decoder (Optional[Union[str, Callable]], optional): The decoder for the cache. Defaults to None.
            hit_setter (Optional[Callable], optional): The hit setter for the cache. Defaults to None.
            hit_getter (Optional[Callable], optional): The hit getter for the cache. Defaults to None.
            
        """
        ...
