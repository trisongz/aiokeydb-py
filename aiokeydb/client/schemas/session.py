import time
import anyio
import typing
import logging
import asyncio

import functools

from aiokeydb.typing import Number, KeyT
from aiokeydb.lock import Lock
from aiokeydb.connection import Encoder
from aiokeydb.core import KeyDB, PubSub, Pipeline

from aiokeydb.asyncio.lock import AsyncLock
from aiokeydb.asyncio.core import AsyncKeyDB, AsyncPubSub, AsyncPipeline
from aiokeydb.exceptions import (
    ConnectionError,
    TimeoutError,
)

from aiokeydb.client.config import KeyDBSettings
from aiokeydb.client.utils import ENOVAL, full_name, args_to_key
from aiokeydb.client.serializers import BaseSerializer

from inspect import iscoroutinefunction

logger = logging.getLogger(__name__)

class KeyDBSession:
    """
    Class to hold both the sync and async clients
    """

    def __init__(
        self,
        uri: str,
        name: str,
        db_id: int,
        encoder: typing.Optional[Encoder] = None,
        serializer: typing.Optional[typing.Type[BaseSerializer]] = None,
        settings: typing.Optional[KeyDBSettings] = None,
        **config,
    ):
        self.uri = uri
        self.name = name
        self.db_id = db_id
        self.config = config
        self.settings = settings or KeyDBSettings()
        self.encoder = encoder or Encoder(
            encoding = config.get('encoding', 'utf-8'),
            encoding_errors = config.get('encoding_errors', 'strict'),
            decode_responses = True,
        )
        self.serializer = serializer

        self._client: KeyDB = None
        self._async_client: AsyncKeyDB = None
        self._lock: Lock = None
        self._async_lock: AsyncLock = None
        self._pipeline: Pipeline = None
        self._async_pipeline: AsyncPipeline = None

        self._locks: typing.Dict[str, Lock] = {}
        self._async_locks: typing.Dict[str, AsyncLock] = {}

        self._pubsub: PubSub = None
        self._async_pubsub: AsyncPubSub = None

        self._pipeline: Pipeline = None
        self._async_pipeline: AsyncPipeline = None

    @property
    def decode_responses(self):
        return self.serializer is None

    @property
    def client(self) -> KeyDB:
        if self._client is None:
            self._client = KeyDB.from_url(self.uri, decode_responses = self.decode_responses, **self.config)
        return self._client
    
    @property
    def async_client(self) -> AsyncKeyDB:
        if self._async_client is None:
            self._async_client = AsyncKeyDB.from_url(self.uri, decode_responses = self.decode_responses, **self.config)
        return self._async_client
    
    @property
    def pubsub(self) -> PubSub:
        """
        Initializes a `KeyDB` Client and returns a `PubSub`.

        Requires reinitialzing a new client because `decode_responses` should be set to `True`.
        """
        if self._pubsub is None:
            self._pubsub = KeyDB.from_url(self.uri, decode_responses = True, **self.config).pubsub()
        return self._pubsub
    
    @property
    def async_pubsub(self) -> AsyncPubSub:
        """
        Initializes a `AsyncKeyDB` Client and returns a `AsyncPubSub`.

        Requires reinitialzing a new client because `decode_responses` should be set to `True`.
        """
        if self._async_pubsub is None:
            self._async_pubsub = AsyncKeyDB.from_url(self.uri, decode_responses = True, **self.config).pubsub()
        return self._async_pubsub
    
    @property
    def pipeline(self) -> Pipeline:
        """
        Initializes a `KeyDB` Client and returns a `Pipeline`.
        """
        if self._pipeline is None:
            self._pipeline = self.client.pipeline(
                transaction = self.config.get('transaction', True),
                shard_hint = self.config.get('shard_hint', None),
            )
        return self._pipeline
    
    @property
    def async_pipeline(self) -> AsyncPipeline:
        """
        Initializes a `AsyncKeyDB` Client and returns a `AsyncPipeline`.
        """
        if self._async_pipeline is None:
            self._async_pipeline = self.async_client.pipeline(
                transaction = self.config.get('transaction', True),
                shard_hint = self.config.get('shard_hint', None),
            )
        return self._async_pipeline
    
    @property
    def lock(self) -> Lock:
        if self._lock is None:
            self._lock = Lock(self.client)
        return self._lock
    
    @property
    def async_lock(self) -> AsyncLock:
        if self._async_lock is None:
            self._async_lock = AsyncLock(self.async_client)
        return self._async_lock
    
    def get_lock(
        self, 
        name: str, 
        timeout: typing.Optional[Number] = None,
        sleep: Number = 0.1,
        blocking: bool = True,
        blocking_timeout: typing.Optional[Number] = None,
        thread_local: bool = True,
    ) -> Lock:
        """
        Create a new Lock instance named ``name`` using the Redis client
        supplied by ``keydb``.

        ``timeout`` indicates a maximum life for the lock in seconds.
        By default, it will remain locked until release() is called.
        ``timeout`` can be specified as a float or integer, both representing
        the number of seconds to wait.

        ``sleep`` indicates the amount of time to sleep in seconds per loop
        iteration when the lock is in blocking mode and another client is
        currently holding the lock.

        ``blocking`` indicates whether calling ``acquire`` should block until
        the lock has been acquired or to fail immediately, causing ``acquire``
        to return False and the lock not being acquired. Defaults to True.
        Note this value can be overridden by passing a ``blocking``
        argument to ``acquire``.

        ``blocking_timeout`` indicates the maximum amount of time in seconds to
        spend trying to acquire the lock. A value of ``None`` indicates
        continue trying forever. ``blocking_timeout`` can be specified as a
        float or integer, both representing the number of seconds to wait.

        ``thread_local`` indicates whether the lock token is placed in
        thread-local storage. By default, the token is placed in thread local
        storage so that a thread only sees its token, not a token set by
        another thread. 
        """
        if name not in self._locks:
            self._locks[name] = Lock(
                self.client, 
                name = name, 
                timeout = timeout, 
                sleep = sleep, 
                blocking = blocking, 
                blocking_timeout = blocking_timeout, 
                thread_local = thread_local
            )
        if not self._lock:  self._lock = self._locks[name]
        return self._locks[name]
    
    def get_async_lock(
        self, 
        name: str, 
        timeout: typing.Optional[Number] = None,
        sleep: Number = 0.1,
        blocking: bool = True,
        blocking_timeout: typing.Optional[Number] = None,
        thread_local: bool = True,
    ) -> AsyncLock:
        """
        Create a new Lock instance named ``name`` using the Redis client
        supplied by ``keydb``.

        ``timeout`` indicates a maximum life for the lock in seconds.
        By default, it will remain locked until release() is called.
        ``timeout`` can be specified as a float or integer, both representing
        the number of seconds to wait.

        ``sleep`` indicates the amount of time to sleep in seconds per loop
        iteration when the lock is in blocking mode and another client is
        currently holding the lock.

        ``blocking`` indicates whether calling ``acquire`` should block until
        the lock has been acquired or to fail immediately, causing ``acquire``
        to return False and the lock not being acquired. Defaults to True.
        Note this value can be overridden by passing a ``blocking``
        argument to ``acquire``.

        ``blocking_timeout`` indicates the maximum amount of time in seconds to
        spend trying to acquire the lock. A value of ``None`` indicates
        continue trying forever. ``blocking_timeout`` can be specified as a
        float or integer, both representing the number of seconds to wait.

        ``thread_local`` indicates whether the lock token is placed in
        thread-local storage. By default, the token is placed in thread local
        storage so that a thread only sees its token, not a token set by
        another thread. 
        """
        if name not in self._async_locks:
            self._async_locks[name] = AsyncLock(
                self.async_client, 
                name = name, 
                timeout = timeout, 
                sleep = sleep, 
                blocking = blocking, 
                blocking_timeout = blocking_timeout, 
                thread_local = thread_local
            )
        if not self._async_lock:  self._async_lock = self._async_locks[name]
        return self._async_locks[name]

    def close_locks(
        self, 
        names: typing.Optional[typing.Union[typing.List[str], str]] = None
    ):
        if names is None: names = list(self._locks.keys())
        if isinstance(names, str): names = [names]

        for name in names:
            if name in self._locks:
                self._locks[name].release()
                del self._locks[name]
    
    async def async_close_locks(
        self, 
        names: typing.Optional[typing.Union[typing.List[str], str]] = None
    ):
        if names is None: names = list(self._async_locks.keys())
        if isinstance(names, str): names = [names]
        for name in names:
            if name in self._async_locks:
                await self._async_locks[name].release()
                del self._async_locks[name]
    

    def close(self):
        self.close_locks()
        if self._pubsub is not None:
            self._pubsub.close()
            self._pubsub = None
        
        if self._client is not None:
            self._client.close()
            self._client = None

    async def aclose(self):
        await self.async_close_locks()
        if self._async_pubsub is not None:
            await self._async_pubsub.close()
            self._async_pubsub = None
        
        if self._async_client is not None:
            await self._async_client.close()
            self._async_client = None

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.aclose()
    
    """
    Primary Functions
    """
    def set(
        self, 
        name: str, 
        value: typing.Any,
        **kwargs
    ) -> typing.Any:
        """
        Serializes the Value using `serializer` and executes a set command
        """
        if self.serializer:
            value = self.serializer.dumps(value)
        return self.client.set(
            name = name,
            value = value,
            **kwargs
        )

    async def async_set(
        self, 
        name: str, 
        value: typing.Any,
        **kwargs
    ) -> typing.Any:
        """
        Serializes the Value using `serializer` and executes a set command
        """
        if self.serializer:
            value = self.serializer.dumps(value)
        return await self.async_client.set(
            name = name,
            value = value,
            **kwargs
        )
    
    def get(
        self, 
        name: str, 
        default: typing.Any = None, 
        **kwargs
    ) -> typing.Any:
        """
        Serializes the Value using `serializer` and executes a set command

        - `default` is the value to return if the key is not found
        """
        val = self.client.get(name)
        if not val: return default
        if self.serializer:
            val = self.serializer.loads(val, **kwargs)
        return val
    
    async def async_get(
        self, 
        name: str, 
        default: typing.Any = None, 
        **kwargs
    ) -> typing.Any:
        """
        Serializes the Value using `serializer` and executes a set command

        - `default` is the value to return if the key is not found
        """
        val = await self.async_client.get(name)
        if not val: return default
        if self.serializer:
            val = self.serializer.loads(val, **kwargs)
        return val
    
    def delete(
        self, 
        names: typing.Union[typing.List[str], str],
        **kwargs,
    ) -> typing.Any:
        """
        Delete one or more keys specified by names
        """
        if isinstance(names, str): names = [names]
        return self.client.delete(*names)

    async def async_delete(
        self, 
        names: typing.Union[typing.List[str], str],
        **kwargs,
    ) -> typing.Any:
        """
        Delete one or more keys specified by names
        """
        if isinstance(names, str): names = [names]
        return await self.async_client.delete(*names)

    def update(
        self, 
        name: str, 
        data: typing.Dict[typing.Any, typing.Any], 
        overwrite: typing.Optional[bool] = False, 
        **kwargs
    ) -> typing.Any:
        """
        Update the key: `name` with data

        equivilent to:
            key = 'mykey'; new_data = {'key2': 'value2'}
            x = await KeyDBClient.get(key); -> x = {'key': 'value'}
            x.update(new_data); -> x.update({'key2': 'value2'})
            await KeyDBClient.set(key, x); -> {'key': 'value', 'key2': 'value2'}
        """
        if overwrite: return self.set(name = name, value = data, **kwargs)
        src_data: typing.Dict[typing.Any, typing.Any] = self.get(name, {})
        src_data.update(data)
        return self.set(
            name = name,
            value = src_data,
            **kwargs
        )

    async def async_update(
        self, 
        name: str, 
        data: typing.Dict[typing.Any, typing.Any], 
        overwrite: typing.Optional[bool] = False, 
        **kwargs
    ) -> typing.Any:
        """
        Update the key: `name` with data

        equivilent to:
            key = 'mykey'; new_data = {'key2': 'value2'}
            x = await KeyDBClient.get(key); -> x = {'key': 'value'}
            x.update(new_data); -> x.update({'key2': 'value2'})
            await KeyDBClient.set(key, x); -> {'key': 'value', 'key2': 'value2'}
        """
        if overwrite: return await self.async_set(name = name, value = data, **kwargs)
        src_data: typing.Dict[typing.Any, typing.Any] = await self.async_get(name, {})
        src_data.update(data)
        return await self.async_set(
            name = name,
            value = src_data,
            **kwargs
        )

    def exists(
        self, 
        keys: typing.Union[typing.List[KeyT], KeyT], 
        **kwargs
    ) -> bool:
        """
        Check if a key exists
        """
        if not isinstance(keys, list): keys = [keys]
        return self.client.exists(*keys)

    async def async_exists(
        self, 
        keys: typing.Union[typing.List[KeyT], KeyT], 
        **kwargs
    ) -> bool:
        """
        Check if a key exists
        """
        if not isinstance(keys, list): keys = [keys]
        return await self.async_client.exists(*keys)

    """
    Other utilities
    """

    def ping(self, **kwargs):
        """
        Ping the keydb client
        """
        return self.client.ping(**kwargs)

    async def async_ping(self, **kwargs):
        """
        Ping the keydb client
        """
        return await self.async_client.ping(**kwargs)

    def wait_for_ready(
        self, 
        interval: int = 1.0,
        max_attempts: typing.Optional[int] = None,
        timeout: typing.Optional[float] = 60.0,
        verbose: bool = False, 
        **kwargs
    ):
        attempts = 0
        start_time = time.time()
        while True:
            if max_attempts and attempts >= max_attempts:
                raise ConnectionError(f'Max {max_attempts} attempts reached')
            if timeout and time.time() - start_time >= timeout:
                raise TimeoutError(f'Timeout of {timeout} seconds reached')
            try:
                self.ping()
                if verbose: logger.info(f'KeyDB is Ready after {attempts} attempts')
                break
            except (InterruptedError, KeyboardInterrupt) as e:
                logger.error(e)
                break            
            except Exception as e:
                if verbose: logger.info(f'KeyDB is not ready, retrying in {interval} seconds')
                time.sleep(interval)
                attempts += 1

    async def async_wait_for_ready(
        self, 
        interval: int = 1.0,
        max_attempts: typing.Optional[int] = None,
        timeout: typing.Optional[float] = 60.0,
        verbose: bool = False, 
        **kwargs
    ):
        attempts = 0
        start_time = time.time()
        while True:
            if max_attempts and attempts >= max_attempts:
                raise ConnectionError(f'Max {max_attempts} attempts reached')
            if timeout and time.time() - start_time >= timeout:
                raise TimeoutError(f'Timeout of {timeout} seconds reached')

            try:
                await self.async_ping()
                if verbose: logger.info(f'KeyDB is Ready after {attempts} attempts')
                break
            except (InterruptedError, KeyboardInterrupt, asyncio.CancelledError) as e:
                logger.error(e)
                break            
            except Exception as e:
                if verbose: logger.info(f'KeyDB is not ready, retrying in {interval} seconds')
                await asyncio.sleep(interval)
                attempts += 1
    

    """
    Cachify
    """

    def cachify(
        self,
        cache_ttl: int = None, 
        typed: bool = False, 
        cache_prefix: str = None, 
        exclude: typing.List[str] = None,
        exclude_null: typing.Optional[bool] = False,
        exclude_return_types: typing.Optional[typing.List[type]] = None,
        exclude_return_objs: typing.Optional[typing.List[typing.Any]] = None,
        include_cache_hit: typing.Optional[bool] = False,
        _no_cache: typing.Optional[bool] = False,
        _no_cache_kwargs: typing.Optional[typing.List[str]] = None,
        _no_cache_validator: typing.Optional[typing.Callable] = None,
        _func_name: typing.Optional[str] = None,
        _validate_requests: typing.Optional[bool] = True,
        **kwargs
    ):
        """Memoizing cache decorator. Repeated calls with the same arguments
        will look up the result in cache and avoid function evaluation.

        If `_func_name` is set to None (default), the callable name will be determined
        automatically.

        When expire is set to zero, function results will not be set in the
        cache. Store lookups still occur, however. Read
        :doc:`case-study-landing-page-caching` for example usage.

        If typed is set to True, function arguments of different types will be
        cached separately. For example, f(3) and f(3.0) will be treated as
        distinct calls with distinct results.

        WARNING: You can pass param `no_cache=True` to the function being wrapped
        (not to the decorator) to avoid cache. This allows you to control cache usage
        from where you call the function. However, if your function accepts
        `**kwargs` and passes them to another function, it is your responsibility
        to remove this param from `kwargs` if you don't want to pass it further. Otherwise,
        you'll get the "unexpected keyword argument" exception.

        The original underlying function is accessible through the __wrapped__
        attribute. This is useful for introspection or for rewrapping the
        function with a different cache.

        Example:

        >>> from kops.clients.keydb import KeyDBClient
        >>> @KeyDBClient.cachify(expire=1)
        ... async def fibonacci(number):
        ...     if number == 0:
        ...         return 0
        ...     elif number == 1:
        ...         return 1
        ...     else:
        ...         return fibonacci(number - 1) + fibonacci(number - 2)
        >>> print(fibonacci(100))
        ... # 354224848179261915075

        An additional `__cache_key__` attribute can be used to generate the
        cache key used for the given arguments.

        >>> key = fibonacci.__cache_key__(100)
        >>> print(cache[key])
        ... # 54224848179261915075

        Remember to call memoize when decorating a callable. If you forget,
        then a TypeError will occur. Note the lack of parenthenses after
        memoize below:

        >>> @KeyDBClient.cachify
        ... async def test():
        ...     pass
        ... # Traceback (most recent call last):
        ... # <...>

        :param str _func_name: name given for callable (default None, automatic)
        :param bool typed: cache different types separately (default False)
        :param int expire: seconds until arguments expire
            (default None, no expiry)
        :param cache_prefix: prefix to add to key
            (default KeyDBClient.cache_prefix | `cache_`)
        :return: callable decorator
        """
        cache_prefix = cache_prefix if cache_prefix is not None else self.settings.cache_prefix
        cache_ttl = cache_ttl if cache_ttl is not None else self.settings.cache_ttl

        def decorator(func):
            "Decorator created by memoize() for callable `func`."
            # Support _func_name for usage in wrapped up decorators
            base = (_func_name or full_name(func),)
            if iscoroutinefunction(func):
                @functools.wraps(func)
                async def wrapper(*args, **kwargs):
                    "Wrapper for callable to cache arguments and return values."

                    # Validate exclusions for no cache
                    if _no_cache:
                        if include_cache_hit:
                            return await func(*args, **kwargs), False
                        return await func(*args, **kwargs)

                    if _no_cache_kwargs and any(kwargs.get(k) for k in _no_cache_kwargs):
                        if include_cache_hit:
                            return await func(*args, **kwargs), False
                        return await func(*args, **kwargs)

                    if _no_cache_validator:
                        if iscoroutinefunction(_no_cache_validator):
                            if await _no_cache_validator(*args, **kwargs):
                                if include_cache_hit:
                                    return await func(*args, **kwargs), False
                                return await func(*args, **kwargs)
                        else:
                            if _no_cache_validator(*args, **kwargs):
                                if include_cache_hit:
                                    return await func(*args, **kwargs), False
                                return await func(*args, **kwargs)

                    # Handle validation for requests
                    if _validate_requests:
                        copy_kwargs = kwargs.copy()
                        request = copy_kwargs.pop("request", None)
                        headers: typing.Dict[str, str] = kwargs.pop("headers", getattr(request, "headers", kwargs.pop("headers", None)))
                        # Handle No-Cache
                        if headers is not None:
                            for key in {"Cache-Control", "X-Cache-Control"}:
                                if headers.get(key, headers.get(key.lower(), "")) in {"no-store", "no-cache"}:
                                    if include_cache_hit:
                                        return await func(*args, **kwargs), False
                                    return await func(*args, **kwargs)
                        

                    # Do the actual caching
                    key = wrapper.__cache_key__(*args, **kwargs)
                    try:
                        with anyio.fail_after(1):
                            result = await self.async_get(key, default = ENOVAL)

                    except TimeoutError:
                        result = ENOVAL
                        logger.error(f'Calling GET on async KeyDB timed out. Cached function: {base}')

                    is_cache_hit = result is not ENOVAL
                    if not is_cache_hit:
                        result = await func(*args, **kwargs)
                        # Return right away since it's a null value
                        if exclude_null and result is None:
                            return (result, False) if include_cache_hit else result
                        if exclude_return_types and isinstance(result, tuple(exclude_return_types)):
                            return (result, False) if include_cache_hit else result
                        if exclude_return_objs and issubclass(type(result), tuple(exclude_return_objs)):
                            return (result, False) if include_cache_hit else result
                        if cache_ttl is None or cache_ttl > 0:
                            try:
                                with anyio.fail_after(1):
                                    await self.async_set(key, result, ex=cache_ttl)
                            except TimeoutError:
                                logger.error(f'Calling SET on async KeyDB timed out. Cached function: {base}')
                    
                    return (result, is_cache_hit) if include_cache_hit else result

            else:
                @functools.wraps(func)
                def wrapper(*args, **kwargs):

                    # Validate exclusions for no cache
                    if _no_cache:
                        if include_cache_hit:
                            return func(*args, **kwargs), False
                        return func(*args, **kwargs)

                    if _no_cache_kwargs and any(kwargs.get(k) for k in _no_cache_kwargs):
                        if include_cache_hit:
                            return func(*args, **kwargs), False
                        return func(*args, **kwargs)
                    # We assume the func that is not a coro
                    if _no_cache_validator and _no_cache_validator(*args, **kwargs):
                        if include_cache_hit:
                            return func(*args, **kwargs), False
                        return func(*args, **kwargs)

                    # Handle validation for requests
                    if _validate_requests:
                        copy_kwargs = kwargs.copy()
                        request = copy_kwargs.pop("request", None)
                        headers = kwargs.pop("headers", getattr(request, "headers", kwargs.pop("headers", None)))
                        # Handle No-Cache
                        headers: typing.Dict[str, str] = kwargs.pop("headers", getattr(request, "headers", kwargs.pop("headers", None)))
                        if headers is not None:
                            for key in {"Cache-Control", "X-Cache-Control"}:
                                if headers.get(key, headers.get(key.lower(), "")) in {"no-store", "no-cache"}:
                                    if include_cache_hit:
                                        return func(*args, **kwargs), False
                                    return func(*args, **kwargs)


                    # Do the actual caching
                    key = wrapper.__cache_key__(*args, **kwargs)
                    try:
                        result = self.get(key, default = ENOVAL)
                    except TimeoutError:
                        result = ENOVAL
                        logger.error(f'Calling GET on KeyDB timed out. Cached function: {base}')
                    is_cache_hit = result is not ENOVAL
                    if not is_cache_hit:
                        result = func(*args, **kwargs)
                        if exclude_null and result is None:
                            return (result, False) if include_cache_hit else result
                        if exclude_return_types and isinstance(result, tuple(exclude_return_types)):
                            return (result, False) if include_cache_hit else result
                        if exclude_return_objs and issubclass(type(result), tuple(exclude_return_objs)):
                            return (result, False) if include_cache_hit else result
                        if cache_ttl is None or cache_ttl > 0:
                            try:
                                self.set(key, result, ex=cache_ttl)
                            except TimeoutError:
                                logger.error(f'Calling SET on KeyDB timed out. Cached function: {base}')
                    
                    return (result, is_cache_hit) if include_cache_hit else result


            def __cache_key__(*args, **kwargs):
                "Make key for cache given function arguments."
                return f'{cache_prefix}_{args_to_key(base, args, kwargs, typed, exclude)}'

            wrapper.__cache_key__ = __cache_key__
            return wrapper

        return decorator
    
    # def __getattribute__(self, name: str) -> typing.Any:
    #     if "async_" in name:
    #         return getattr(self.async_client, name.replace("async_", ""))
    #     if hasattr(self.client, name):
    #         return getattr(self.client, name)
    #     return super().__getattribute__(name)
    
    def __call__(self, method: str, *args, **kwargs) -> typing.Any:
        return getattr(self.client, method)(*args, **kwargs)
    
    async def __acall__(self, method: str, *args, **kwargs) -> typing.Any:
        return await getattr(self.async_client, method)(*args, **kwargs)

    def __getitem__(self, key: KeyT, default: typing.Any = None) -> typing.Any:
        return self.get(key, default)
    
    def __setitem__(self, key: KeyT, value: typing.Any) -> None:
        self.set(key, value)
    
    def __delitem__(self, key: KeyT) -> None:
        self.delete(key)
    
    def __contains__(self, key: KeyT) -> bool:
        return self.exists(key)
    
    def __repr__(self) -> str:
        return f'<{self.__class__.__name__} {self.name}> {self.uri} @ {self.db_id}'
    
    def __str__(self) -> str:
        return self.uri