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
from aiokeydb.client.types import KeyDBUri
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
        #uri: str,
        uri: typing.Union[str, KeyDBUri],
        name: str,
        db_id: typing.Optional[int] = None,
        encoder: typing.Optional[Encoder] = None,
        serializer: typing.Optional[typing.Type[BaseSerializer]] = None,
        settings: typing.Optional[KeyDBSettings] = None,
        cache_ttl: typing.Optional[Number] = None,
        cache_prefix: typing.Optional[str] = None,
        cache_enabled: typing.Optional[bool] = None,
        _decode_responses: typing.Optional[bool] = None,
        **config,
    ):
        if isinstance(uri, str): uri = KeyDBUri(dsn = uri)
        self.uri: KeyDBUri = uri
        self.name = name
        self.db_id = db_id or uri.db_id
        self.config = config
        self.settings = settings or KeyDBSettings()
        self.encoder = encoder or Encoder(
            encoding = config.get('encoding', self.settings.encoding),
            encoding_errors = config.get('encoding_errors', self.settings.encoding_errors),
            decode_responses = True,
        )
        self.serializer = serializer

        self.cache_prefix = cache_prefix or self.settings.cache_prefix
        self.cache_ttl = cache_ttl if cache_ttl is not None else self.settings.cache_ttl
        self.cache_enabled = cache_enabled if cache_enabled is not None else self.settings.cache_enabled


        self._decode_responses = self.config.pop('decode_responses', _decode_responses)
        self._active = False
        # We'll use this to keep track of the number of times we've tried to
        # connect to the database. This is used to determine if we should
        # attempt to reconnect to the database.
        # if the max_attempts have been reached, we'll stop trying to reconnect
        self._cache_max_attempts = config.get('cache_max_attempts', 20)
        self._cache_failed_attempts = 0

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
        return self._decode_responses if self._decode_responses is not None else (self.serializer is None)

    @property
    def client(self) -> KeyDB:
        if self._client is None:
            self._client = KeyDB.from_url(self.uri.connection, decode_responses = self.decode_responses, **self.config)
        return self._client
    
    @property
    def async_client(self) -> AsyncKeyDB:
        if self._async_client is None:
            self._async_client = AsyncKeyDB.from_url(self.uri.connection, decode_responses = self.decode_responses, **self.config)
        return self._async_client
    
    @property
    def pubsub(self) -> PubSub:
        """
        Initializes a `KeyDB` Client and returns a `PubSub`.

        Requires reinitialzing a new client because `decode_responses` should be set to `True`.
        """
        if self._pubsub is None:
            self._pubsub = KeyDB.from_url(self.uri.connection, decode_responses = True, **self.config).pubsub()
        return self._pubsub
    
    @property
    def async_pubsub(self) -> AsyncPubSub:
        """
        Initializes a `AsyncKeyDB` Client and returns a `AsyncPubSub`.

        Requires reinitialzing a new client because `decode_responses` should be set to `True`.
        """
        if self._async_pubsub is None:
            self._async_pubsub = AsyncKeyDB.from_url(self.uri.connection, decode_responses = True, **self.config).pubsub()
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
        **kwargs,
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
        **kwargs,
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
    
    
    def decr(
        self,
        name: str,
        amount: typing.Optional[int] = 1,
        **kwargs
    ) -> int:
        """
        Decrement the value of key `name` by `amount`
        """
        return self.client.decr(name, amount = amount, **kwargs)
    
    async def async_decr(
        self,
        name: str,
        amount: typing.Optional[int] = 1,
        **kwargs
    ) -> int:
        """
        Decrement the value of key `name` by `amount`
        """
        return await self.async_client.decr(name, amount = amount, **kwargs)
    
    def decrby(
        self,
        name: str,
        amount: typing.Optional[int] = 1,
        **kwargs
    ) -> int:
        """
        Decrement the value of key `name` by `amount`
        """
        return self.client.decrby(name, amount = amount, **kwargs)
    
    async def async_decrby(
        self,
        name: str,
        amount: typing.Optional[int] = 1,
        **kwargs
    ) -> int:
        """
        Decrement the value of key `name` by `amount`
        """
        return await self.async_client.decrby(name, amount = amount, **kwargs)
    
    def incr(
        self,
        name: str,
        amount: typing.Optional[int] = 1,
        **kwargs
    ) -> int:
        """
        Increment the value of key `name` by `amount`
        """
        return self.client.incr(name, amount = amount, **kwargs)
    
    async def async_incr(
        self,
        name: str,
        amount: typing.Optional[int] = 1,
        **kwargs
    ) -> int:
        """
        Increment the value of key `name` by `amount`
        """
        return await self.async_client.incr(name, amount = amount, **kwargs)
    
    def incrby(
        self,
        name: str,
        amount: typing.Optional[int] = 1,
        **kwargs
    ) -> int:
        """
        Increment the value of key `name` by `amount`
        """
        return self.client.incrby(name, amount = amount, **kwargs)
    
    async def async_incrby(
        self,
        name: str,
        amount: typing.Optional[int] = 1,
        **kwargs
    ) -> int:
        """
        Increment the value of key `name` by `amount`
        """
        return await self.async_client.incrby(name, amount = amount, **kwargs)
    
    def incrbyfloat(
        self,
        name: str,
        amount: typing.Optional[float] = 1.0,
        **kwargs
    ) -> float:
        """
        Increment the value of key `name` by `amount`
        """
        return self.client.incrbyfloat(name, amount = amount, **kwargs)
    
    async def async_incrbyfloat(
        self,
        name: str,
        amount: typing.Optional[float] = 1.0,
        **kwargs
    ) -> float:
        """
        Increment the value of key `name` by `amount`
        """
        return await self.async_client.incrbyfloat(name, amount = amount, **kwargs)
    
    def getbit(
        self,
        name: str,
        offset: int,
        **kwargs
    ) -> int:
        """
        Returns the bit value at offset in the string value stored at key
        """
        return self.client.getbit(name, offset, **kwargs)
    
    async def async_getbit(
        self,
        name: str,
        offset: int,
        **kwargs
    ) -> int:
        """
        Returns the bit value at offset in the string value stored at key
        """
        return await self.async_client.getbit(name, offset, **kwargs)
    
    def setbit(
        self,
        name: str,
        offset: int,
        value: int,
        **kwargs
    ) -> int:
        """
        Sets or clears the bit at offset in the string value stored at key
        """
        return self.client.setbit(name, offset, value, **kwargs)
    
    async def async_setbit(
        self,
        name: str,
        offset: int,
        value: int,
        **kwargs
    ) -> int:
        """
        Sets or clears the bit at offset in the string value stored at key
        """
        return await self.async_client.setbit(name, offset, value, **kwargs)
    
    def bitcount(
        self,
        name: str,
        start: typing.Optional[int] = None,
        end: typing.Optional[int] = None,
        **kwargs
    ) -> int:
        """
        Count the number of set bits (population counting) in a string
        """
        return self.client.bitcount(name, start = start, end = end, **kwargs)
    
    async def async_bitcount(
        self,
        name: str,
        start: typing.Optional[int] = None,
        end: typing.Optional[int] = None,
        **kwargs
    ) -> int:
        """
        Count the number of set bits (population counting) in a string
        """
        return await self.async_client.bitcount(name, start = start, end = end, **kwargs)
    
    def bitop(
        self,
        operation: str,
        dest: str,
        *keys: str,
        **kwargs
    ) -> int:
        """
        Perform bitwise operations between strings
        """
        return self.client.bitop(operation, dest, *keys, **kwargs)
    
    async def async_bitop(
        self,
        operation: str,
        dest: str,
        *keys: str,
        **kwargs
    ) -> int:
        """
        Perform bitwise operations between strings
        """
        return await self.async_client.bitop(operation, dest, *keys, **kwargs)
    
    def bitpos(
        self,
        name: str,
        bit: int,
        start: typing.Optional[int] = None,
        end: typing.Optional[int] = None,
        **kwargs
    ) -> int:
        """
        Find first bit set or clear in a string
        """
        return self.client.bitpos(name, bit, start = start, end = end, **kwargs)
    
    async def async_bitpos(
        self,
        name: str,
        bit: int,
        start: typing.Optional[int] = None,
        end: typing.Optional[int] = None,
        **kwargs
    ) -> int:
        """
        Find first bit set or clear in a string
        """
        return await self.async_client.bitpos(name, bit, start = start, end = end, **kwargs)
    
    def strlen(
        self,
        name: str,
        **kwargs
    ) -> int:
        """
        Get the length of the value stored in a key
        """
        return self.client.strlen(name, **kwargs)
    
    async def async_strlen(
        self,
        name: str,
        **kwargs
    ) -> int:
        """
        Get the length of the value stored in a key
        """
        return await self.async_client.strlen(name, **kwargs)
    
    def getrange(
        self,
        name: str,
        start: int,
        end: int,
        **kwargs
    ) -> str:
        """
        Get a substring of the string stored at a key
        """
        return self.client.getrange(name, start, end, **kwargs)
    
    async def async_getrange(
        self,
        name: str,
        start: int,
        end: int,
        **kwargs
    ) -> str:
        """
        Get a substring of the string stored at a key
        """
        return await self.async_client.getrange(name, start, end, **kwargs)
    
    def setrange(
        self,
        name: str,
        offset: int,
        value: str,
        **kwargs
    ) -> int:
        """
        Overwrite part of a string at key starting at the specified offset
        """
        return self.client.setrange(name, offset, value, **kwargs)
    
    async def async_setrange(
        self,
        name: str,
        offset: int,
        value: str,
        **kwargs
    ) -> int:
        """
        Overwrite part of a string at key starting at the specified offset
        """
        return await self.async_client.setrange(name, offset, value, **kwargs)
    
    def getset(
        self,
        name: str,
        value: str,
        **kwargs
    ) -> str:
        """
        Set the string value of a key and return its old value
        """
        return self.client.getset(name, value, **kwargs)

    async def async_getset(
        self,
        name: str,
        value: str,
        **kwargs
    ) -> str:
        """
        Set the string value of a key and return its old value
        """
        return await self.async_client.getset(name, value, **kwargs)
    
    def mget(
        self,
        *names: str,
        **kwargs
    ) -> typing.List[typing.Optional[str]]:
        """
        Get the values of all the given keys
        """
        return self.client.mget(*names, **kwargs)
    
    async def async_mget(
        self,
        *names: str,
        **kwargs
    ) -> typing.List[typing.Optional[str]]:
        """
        Get the values of all the given keys
        """
        return await self.async_client.mget(*names, **kwargs)
    
    def mset(
        self,
        mapping: typing.Mapping[str, str],
        **kwargs
    ) -> bool:
        """
        Set multiple keys to multiple values
        """
        return self.client.mset(mapping, **kwargs)
    
    async def async_mset(
        self,
        mapping: typing.Mapping[str, str],
        **kwargs
    ) -> bool:
        """
        Set multiple keys to multiple values
        """
        return await self.async_client.mset(mapping, **kwargs)
    
    def msetnx(
        self,
        mapping: typing.Mapping[str, str],
        **kwargs
    ) -> bool:
        """
        Set multiple keys to multiple values, only if none of the keys exist
        """
        return self.client.msetnx(mapping, **kwargs)
    
    async def async_msetnx(
        self,
        mapping: typing.Mapping[str, str],
        **kwargs
    ) -> bool:
        """
        Set multiple keys to multiple values, only if none of the keys exist
        """
        return await self.async_client.msetnx(mapping, **kwargs)
    
    def expire(
        self,
        name: str,
        time: int,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
        **kwargs
    ) -> bool:
        """
        Set a key's time to live in seconds
        """
        return self.client.expire(name, time, nx = nx, xx = xx, gt = gt, lt = lt, **kwargs)
    
    async def async_expire(
        self,
        name: str,
        time: int,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
        **kwargs
    ) -> bool:
        """
        Set a key's time to live in seconds
        """
        return await self.async_client.expire(name, time, nx = nx, xx = xx, gt = gt, lt = lt, **kwargs)
    
    def expireat(
        self,
        name: str,
        when: int,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
        **kwargs
    ) -> bool:
        """
        Set the expiration for a key as a UNIX timestamp
        """
        return self.client.expireat(name, when, nx = nx, xx = xx, gt = gt, lt = lt, **kwargs)
    
    async def async_expireat(
        self,
        name: str,
        when: int,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
        **kwargs
    ) -> bool:
        """
        Set the expiration for a key as a UNIX timestamp
        """
        return await self.async_client.expireat(name, when, nx = nx, xx = xx, gt = gt, lt = lt, **kwargs)
    
    def pexpire(
        self,
        name: str,
        time: int,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
        **kwargs
    ) -> bool:
        """
        Set a key's time to live in milliseconds
        """
        return self.client.pexpire(name, time, nx = nx, xx = xx, gt = gt, lt = lt, **kwargs)
    
    async def async_pexpire(
        self,
        name: str,
        time: int,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
        **kwargs
    ) -> bool:
        """
        Set a key's time to live in milliseconds
        """
        return await self.async_client.pexpire(name, time, nx = nx, xx = xx, gt = gt, lt = lt, **kwargs)
    
    def pexpireat(
        self,
        name: str,
        when: int,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
        **kwargs
    ) -> bool:
        """
        Set the expiration for a key as a UNIX timestamp specified in milliseconds
        """
        return self.client.pexpireat(name, when, nx = nx, xx = xx, gt = gt, lt = lt, **kwargs)
    
    async def async_pexpireat(
        self,
        name: str,
        when: int,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
        **kwargs
    ) -> bool:
        """
        Set the expiration for a key as a UNIX timestamp specified in milliseconds
        """
        return await self.async_client.pexpireat(name, when, nx = nx, xx = xx, gt = gt, lt = lt, **kwargs)
    
    def ttl(
        self,
        name: str,
        **kwargs
    ) -> int:
        """
        Get the time to live for a key
        """
        return self.client.ttl(name, **kwargs)
    
    async def async_ttl(
        self,
        name: str,
        **kwargs
    ) -> int:
        """
        Get the time to live for a key
        """
        return await self.async_client.ttl(name, **kwargs)
    
    def pttl(
        self,
        name: str,
        **kwargs
    ) -> int:
        """
        Get the time to live for a key in milliseconds
        """
        return self.client.pttl(name, **kwargs)
    
    async def async_pttl(
        self,
        name: str,
        **kwargs
    ) -> int:
        """
        Get the time to live for a key in milliseconds
        """
        return await self.async_client.pttl(name, **kwargs)
    
    def persist(
        self,
        name: str,
        **kwargs
    ) -> bool:
        """
        Remove the expiration from a key
        """
        return self.client.persist(name, **kwargs)
    
    async def async_persist(
        self,
        name: str,
        **kwargs
    ) -> bool:
        """
        Remove the expiration from a key
        """
        return await self.async_client.persist(name, **kwargs)
    
    def psetex(
        self,
        name: str,
        time_ms: int,
        value: typing.Any,
        **kwargs
    ) -> bool:
        """
        Set the value and expiration in milliseconds of a key
        """
        return self.client.psetex(name, time_ms, value, **kwargs)
    
    async def async_psetex(
        self,
        name: str,
        time_ms: int,
        value: typing.Any,
        **kwargs
    ) -> bool:
        """
        Set the value and expiration in milliseconds of a key
        """
        return await self.async_client.psetex(name, time_ms, value, **kwargs)
    
    def setex(
        self,
        name: str,
        time: int,
        value: typing.Any,
        **kwargs
    ) -> bool:
        """
        Set the value and expiration of a key
        """
        return self.client.setex(name, time, value, **kwargs)
    
    async def async_setex(
        self,
        name: str,
        time: int,
        value: typing.Any,
        **kwargs
    ) -> bool:
        """
        Set the value and expiration of a key
        """
        return await self.async_client.setex(name, time, value, **kwargs)
    
    def getdel(
        self,
        name: str,
        **kwargs
    ) -> typing.Any:
        """
        Get the value of a key and delete it
        """
        return self.client.getdel(name, **kwargs)
    
    async def async_getdel(
        self,
        name: str,
        **kwargs
    ) -> typing.Any:
        """
        Get the value of a key and delete it
        """
        return await self.async_client.getdel(name, **kwargs)
    
    def hdel(
        self,
        name: str,
        *keys: str,
        **kwargs
    ) -> int:
        """
        Delete one or more hash fields
        """
        return self.client.hdel(name, *keys, **kwargs)
    
    async def async_hdel(
        self,
        name: str,
        *keys: str,
        **kwargs
    ) -> int:
        """
        Delete one or more hash fields
        """
        return await self.async_client.hdel(name, *keys, **kwargs)
    
    def hexists(
        self,
        name: str,
        key: str,
        **kwargs
    ) -> bool:
        """
        Determine if a hash field exists
        """
        return self.client.hexists(name, key, **kwargs)
    
    async def async_hexists(
        self,
        name: str,
        key: str,
        **kwargs
    ) -> bool:
        """
        Determine if a hash field exists
        """
        return await self.async_client.hexists(name, key, **kwargs)
    
    def hget(
        self,
        name: str,
        key: str,
        **kwargs
    ) -> typing.Any:
        """
        Get the value of a hash field
        """
        return self.client.hget(name, key, **kwargs)
    
    async def async_hget(
        self,
        name: str,
        key: str,
        **kwargs
    ) -> typing.Any:
        """
        Get the value of a hash field
        """
        return await self.async_client.hget(name, key, **kwargs)
    
    def hgetall(
        self,
        name: str,
        **kwargs
    ) -> typing.Dict:
        """
        Get all the fields and values in a hash
        """
        return self.client.hgetall(name, **kwargs)
    
    async def async_hgetall(
        self,
        name: str,
        **kwargs
    ) -> typing.Dict:
        """
        Get all the fields and values in a hash
        """
        return await self.async_client.hgetall(name, **kwargs)
    
    def hincrby(
        self,
        name: str,
        key: str,
        amount: int = 1,
        **kwargs
    ) -> int:
        """
        Increment the integer value of a hash field by the given number
        """
        return self.client.hincrby(name, key, amount, **kwargs)
    
    async def async_hincrby(
        self,
        name: str,
        key: str,
        amount: int = 1,
        **kwargs
    ) -> int:
        """
        Increment the integer value of a hash field by the given number
        """
        return await self.async_client.hincrby(name, key, amount, **kwargs)
    
    def hincrbyfloat(
        self,
        name: str,
        key: str,
        amount: float = 1.0,
        **kwargs
    ) -> float:
        """
        Increment the float value of a hash field by the given amount
        """
        return self.client.hincrbyfloat(name, key, amount, **kwargs)
    
    async def async_hincrbyfloat(
        self,
        name: str,
        key: str,
        amount: float = 1.0,
        **kwargs
    ) -> float:
        """
        Increment the float value of a hash field by the given amount
        """
        return await self.async_client.hincrbyfloat(name, key, amount, **kwargs)
    
    def hkeys(
        self,
        name: str,
        **kwargs
    ) -> typing.List:
        """
        Get all the fields in a hash
        """
        return self.client.hkeys(name, **kwargs)
    
    async def async_hkeys(
        self,
        name: str,
        **kwargs
    ) -> typing.List:
        """
        Get all the fields in a hash
        """
        return await self.async_client.hkeys(name, **kwargs)
    
    def hlen(
        self,
        name: str,
        **kwargs
    ) -> int:
        """
        Get the number of fields in a hash
        """
        return self.client.hlen(name, **kwargs)
    
    async def async_hlen(
        self,
        name: str,
        **kwargs
    ) -> int:
        """
        Get the number of fields in a hash
        """
        return await self.async_client.hlen(name, **kwargs)
    
    def hmget(
        self,
        name: str,
        keys: typing.List,
        **kwargs
    ) -> typing.List:
        """
        Get the values of all the given hash fields
        """
        return self.client.hmget(name, keys, **kwargs)
    
    async def async_hmget(
        self,
        name: str,
        keys: typing.List,
        **kwargs
    ) -> typing.List:
        """
        Get the values of all the given hash fields
        """
        return await self.async_client.hmget(name, keys, **kwargs)
    
    def hmset(
        self,
        name: str,
        mapping: typing.Dict,
        **kwargs
    ) -> bool:
        """
        Set multiple hash fields to multiple values
        """
        return self.client.hmset(name, mapping, **kwargs)
    
    async def async_hmset(
        self,
        name: str,
        mapping: typing.Dict,
        **kwargs
    ) -> bool:
        """
        Set multiple hash fields to multiple values
        """
        return await self.async_client.hmset(name, mapping, **kwargs)
    
    def hset(
        self,
        name: str,
        key: str,
        value: typing.Any,
        **kwargs
    ) -> bool:
        """
        Set the string value of a hash field
        """
        return self.client.hset(name, key, value, **kwargs)
    
    async def async_hset(
        self,
        name: str,
        key: str,
        value: typing.Any,
        **kwargs
    ) -> bool:
        """
        Set the string value of a hash field
        """
        return await self.async_client.hset(name, key, value, **kwargs)
    
    def hsetnx(
        self,
        name: str,
        key: str,
        value: typing.Any,
        **kwargs
    ) -> bool:
        """
        Set the value of a hash field, only if the field does not exist
        """
        return self.client.hsetnx(name, key, value, **kwargs)
    
    async def async_hsetnx(
        self,
        name: str,
        key: str,
        value: typing.Any,
        **kwargs
    ) -> bool:
        """
        Set the value of a hash field, only if the field does not exist
        """
        return await self.async_client.hsetnx(name, key, value, **kwargs)
    
    def hstrlen(
        self,
        name: str,
        key: str,
        **kwargs
    ) -> int:
        """
        Get the length of the value of a hash field
        """
        return self.client.hstrlen(name, key, **kwargs)
    
    async def async_hstrlen(
        self,
        name: str,
        key: str,
        **kwargs
    ) -> int:
        """
        Get the length of the value of a hash field
        """
        return await self.async_client.hstrlen(name, key, **kwargs)
    
    def hvals(
        self,
        name: str,
        **kwargs
    ) -> typing.List:
        """
        Get all the values in a hash
        """
        return self.client.hvals(name, **kwargs)
    
    async def async_hvals(
        self,
        name: str,
        **kwargs
    ) -> typing.List:
        """
        Get all the values in a hash
        """
        return await self.async_client.hvals(name, **kwargs)
    
    def lindex(
        self,
        name: str,
        index: int,
        **kwargs
    ) -> typing.Any:
        """
        Get an element from a list by its index
        """
        return self.client.lindex(name, index, **kwargs)
    
    async def async_lindex(
        self,
        name: str,
        index: int,
        **kwargs
    ) -> typing.Any:
        """
        Get an element from a list by its index
        """
        return await self.async_client.lindex(name, index, **kwargs)
    
    def linsert(
        self,
        name: str,
        where: str,
        refvalue: typing.Any,
        value: typing.Any,
        **kwargs
    ) -> int:
        """
        Insert an element before or after another element in a list
        """
    
        return self.client.linsert(name, where, refvalue, value, **kwargs)
    
    async def async_linsert(
        self,
        name: str,
        where: str,
        refvalue: typing.Any,
        value: typing.Any,
        **kwargs
    ) -> int:
        """
        Insert an element before or after another element in a list
        """
        return await self.async_client.linsert(name, where, refvalue, value, **kwargs)

    def llen(
        self,
        name: str,
        **kwargs
    ) -> int:
        """
        Get the length of a list
        """
        return self.client.llen(name, **kwargs)

    async def async_llen(
        self,
        name: str,
        **kwargs
    ) -> int:
        """
        Get the length of a list
        """
        return await self.async_client.llen(name, **kwargs)

    def lpop(
        self,
        name: str,
        **kwargs
    ) -> typing.Any:
        """
        Remove and get the first element in a list
        """
        return self.client.lpop(name, **kwargs)
    
    async def async_lpop(
        self,
        name: str,
        **kwargs
    ) -> typing.Any:
        """
        Remove and get the first element in a list
        """
        return await self.async_client.lpop(name, **kwargs)
    
    def lpush(
        self,
        name: str,
        *values: typing.Any,
        **kwargs
    ) -> int:
        """
        Prepend one or multiple values to a list
        """
        return self.client.lpush(name, *values, **kwargs)
    
    async def async_lpush(
        self,
        name: str,
        *values: typing.Any,
        **kwargs
    ) -> int:
        """
        Prepend one or multiple values to a list
        """
        return await self.async_client.lpush(name, *values, **kwargs)
    
    def lpushx(
        self,
        name: str,
        value: typing.Any,
        **kwargs
    ) -> int:
        """
        Prepend a value to a list, only if the list exists
        """
        return self.client.lpushx(name, value, **kwargs)
    
    async def async_lpushx(
        self,
        name: str,
        value: typing.Any,
        **kwargs
    ) -> int:
        """
        Prepend a value to a list, only if the list exists
        """
        return await self.async_client.lpushx(name, value, **kwargs)
    
    def lrange(
        self,
        name: str,
        start: int,
        end: int,
        **kwargs
    ) -> typing.List:
        """
        Get a range of elements from a list
        """
        return self.client.lrange(name, start, end, **kwargs)
    
    async def async_lrange(
        self,
        name: str,
        start: int,
        end: int,
        **kwargs
    ) -> typing.List:
        """
        Get a range of elements from a list
        """
        return await self.async_client.lrange(name, start, end, **kwargs)
    
    def lrem(
        self,
        name: str,
        count: int,
        value: typing.Any,
        **kwargs
    ) -> int:
        """
        Remove elements from a list
        """
        return self.client.lrem(name, count, value, **kwargs)
    
    async def async_lrem(
        self,
        name: str,
        count: int,
        value: typing.Any,
        **kwargs
    ) -> int:
        """
        Remove elements from a list
        """
        return await self.async_client.lrem(name, count, value, **kwargs)
    
    def lset(
        self,
        name: str,
        index: int,
        value: typing.Any,
        **kwargs
    ) -> bool:
        """
        Set the value of an element in a list by its index
        """
        return self.client.lset(name, index, value, **kwargs)
    
    async def async_lset(
        self,
        name: str,
        index: int,
        value: typing.Any,
        **kwargs
    ) -> bool:
        """
        Set the value of an element in a list by its index
        """
        return await self.async_client.lset(name, index, value, **kwargs)
    
    def ltrim(
        self,
        name: str,
        start: int,
        end: int,
        **kwargs
    ) -> bool:
        """
        Trim a list to the specified range
        """
        return self.client.ltrim(name, start, end, **kwargs)
    
    async def async_ltrim(
        self,
        name: str,
        start: int,
        end: int,
        **kwargs
    ) -> bool:
        """
        Trim a list to the specified range
        """
        return await self.async_client.ltrim(name, start, end, **kwargs)
    
    def zadd(
        self,
        name: str,
        *args: typing.Any,
        **kwargs
    ) -> int:
        """
        Add one or more members to a sorted set, or update its score if it already exists
        """
        return self.client.zadd(name, *args, **kwargs)
    
    async def async_zadd(
        self,
        name: str,
        *args: typing.Any,
        **kwargs
    ) -> int:
        """
        Add one or more members to a sorted set, or update its score if it already exists
        """
    
    def zcard(
        self,
        name: str,
        **kwargs
    ) -> int:
        """
        Get the number of members in a sorted set
        """
        return self.client.zcard(name, **kwargs)
    
    async def async_zcard(
        self,
        name: str,
        **kwargs
    ) -> int:
        """
        Get the number of members in a sorted set
        """
        return await self.async_client.zcard(name, **kwargs)
    
    def zcount(
        self,
        name: str,
        min: typing.Any,
        max: typing.Any,
        **kwargs

    ) -> int:
        """
        Count the members in a sorted set with scores within the given values
        """
        return self.client.zcount(name, min, max, **kwargs)
    
    async def async_zcount(
        self,
        name: str,
        min: typing.Any,
        max: typing.Any,
        **kwargs
    ) -> int:
        """
        Count the members in a sorted set with scores within the given values
        """
        return await self.async_client.zcount(name, min, max, **kwargs)
    
    def zincrby(
        self,
        name: str,
        amount: float,
        value: typing.Any,
        **kwargs
    ) -> float:
        """
        Increment the score of a member in a sorted set
        """
        return self.client.zincrby(name, amount, value, **kwargs)
    
    async def async_zincrby(
        self,
        name: str,
        amount: float,
        value: typing.Any,
        **kwargs
    ) -> float:
        """
        Increment the score of a member in a sorted set
        """
        return await self.async_client.zincrby(name, amount, value, **kwargs)
    
    def zinterstore(
        self,
        dest: str,
        keys: typing.List[str],
        aggregate: str = None,
        **kwargs
    ) -> int:
        """
        Intersect multiple sorted sets and store the resulting sorted set in a new key
        """
        return self.client.zinterstore(dest, keys, aggregate, **kwargs)
    
    async def async_zinterstore(
        self,
        dest: str,
        keys: typing.List[str],
        aggregate: str = None,
        **kwargs
    ) -> int:
        """
        Intersect multiple sorted sets and store the resulting sorted set in a new key
        """
        return await self.async_client.zinterstore(dest, keys, aggregate, **kwargs)
    
    def zlexcount(
        self,
        name: str,
        min: typing.Any,
        max: typing.Any,
        **kwargs
    ) -> int:
        """
        Count the number of members in a sorted set between a given lexicographical range
        """
        return self.client.zlexcount(name, min, max, **kwargs)
    
    async def async_zlexcount(
        self,
        name: str,
        min: typing.Any,
        max: typing.Any,
        **kwargs
    ) -> int:
        """
        Count the number of members in a sorted set between a given lexicographical range
        """
        return await self.async_client.zlexcount(name, min, max, **kwargs)
    
    def zpopmax(
        self,
        name: str,
        count: int = None,
        **kwargs
    ) -> typing.List:
        """
        Remove and return members with the highest scores in a sorted set
        """
        return self.client.zpopmax(name, count, **kwargs)
    
    async def async_zpopmax(
        self,
        name: str,
        count: int = None,
        **kwargs
    ) -> typing.List:
        """
        Remove and return members with the highest scores in a sorted set
        """
        return await self.async_client.zpopmax(name, count, **kwargs)
    
    def zpopmin(
        self,
        name: str,
        count: int = None,
        **kwargs
    ) -> typing.List:
        """
        Remove and return members with the lowest scores in a sorted set
        """
        return self.client.zpopmin(name, count, **kwargs)
    
    async def async_zpopmin(
        self,
        name: str,
        count: int = None,
        **kwargs
    ) -> typing.List:
        """
        Remove and return members with the lowest scores in a sorted set
        """
        return await self.async_client.zpopmin(name, count, **kwargs)
    
    def zrange(
        self,
        name: str,
        start: int,
        stop: int,
        desc: bool = False,
        withscores: bool = False,
        score_cast_func: typing.Callable = float,
        **kwargs
    ) -> typing.List:
        """
        Return a range of members in a sorted set, by index
        """
        return self.client.zrange(name, start, stop, desc, withscores, score_cast_func, **kwargs)
    
    async def async_zrange(
        self,
        name: str,
        start: int,
        stop: int,
        desc: bool = False,
        withscores: bool = False,
        score_cast_func: typing.Callable = float,
        **kwargs
    ) -> typing.List:
        """
        Return a range of members in a sorted set, by index
        """
        return await self.async_client.zrange(name, start, stop, desc, withscores, score_cast_func, **kwargs)
    
    def zrangebylex(
        self,
        name: str,
        min: typing.Any,
        max: typing.Any,
        start: int = None,
        num: int = None,
        **kwargs
    ) -> typing.List:
        """
        Return a range of members in a sorted set, by lexicographical range
        """
        return self.client.zrangebylex(name, min, max, start, num, **kwargs)
    
    async def async_zrangebylex(
        self,
        name: str,
        min: typing.Any,
        max: typing.Any,
        start: int = None,
        num: int = None,
        **kwargs
    ) -> typing.List:
        """
        Return a range of members in a sorted set, by lexicographical range
        """
        return await self.async_client.zrangebylex(name, min, max, start, num, **kwargs)
    
    def zrangebyscore(
        self,
        name: str,
        min: typing.Any,
        max: typing.Any,
        start: int = None,
        num: int = None,
        withscores: bool = False,
        score_cast_func: typing.Callable = float,
        **kwargs
    ) -> typing.List:
        """
        Return a range of members in a sorted set, by score
        """
        return self.client.zrangebyscore(name, min, max, start, num, withscores, score_cast_func, **kwargs)
    
    async def async_zrangebyscore(
        self,
        name: str,
        min: typing.Any,
        max: typing.Any,
        start: int = None,
        num: int = None,
        withscores: bool = False,
        score_cast_func: typing.Callable = float,
        **kwargs
    ) -> typing.List:
        """
        Return a range of members in a sorted set, by score
        """
        return await self.async_client.zrangebyscore(name, min, max, start, num, withscores, score_cast_func, **kwargs)
    
    def zrank(
        self,
        name: str,
        value: typing.Any,
        **kwargs
    ) -> int:
        """
        Determine the index of a member in a sorted set
        """
        return self.client.zrank(name, value, **kwargs)
    
    async def async_zrank(
        self,
        name: str,
        value: typing.Any,
        **kwargs
    ) -> int:
        """
        Determine the index of a member in a sorted set
        """
        return await self.async_client.zrank(name, value, **kwargs)
    
    def zrem(
        self,
        name: str,
        *values: typing.Any,
        **kwargs
    ) -> int:
        """
        Remove one or more members from a sorted set
        """
        return self.client.zrem(name, *values, **kwargs)
    
    async def async_zrem(
        self,
        name: str,
        *values: typing.Any,
        **kwargs
    ) -> int:
        """
        Remove one or more members from a sorted set
        """
        return await self.async_client.zrem(name, *values, **kwargs)
    
    def zremrangebylex(
        self,
        name: str,
        min: typing.Any,
        max: typing.Any,
        **kwargs
    ) -> int:
        """
        Remove all members in a sorted set between the given lexicographical range
        """
        return self.client.zremrangebylex(name, min, max, **kwargs)
    
    async def async_zremrangebylex(
        self,
        name: str,
        min: typing.Any,
        max: typing.Any,
        **kwargs
    ) -> int:
        """
        Remove all members in a sorted set between the given lexicographical range
        """
        return await self.async_client.zremrangebylex(name, min, max, **kwargs)
    
    def zremrangebyrank(
        self,
        name: str,
        min: int,
        max: int,
        **kwargs
    ) -> int:
        """
        Remove all members in a sorted set within the given indexes
        """
        return self.client.zremrangebyrank(name, min, max, **kwargs)
    
    async def async_zremrangebyrank(
        self,
        name: str,
        min: int,
        max: int,
        **kwargs
    ) -> int:
        """
        Remove all members in a sorted set within the given indexes
        """
        return await self.async_client.zremrangebyrank(name, min, max, **kwargs)
    
    def zremrangebyscore(
        self,
        name: str,
        min: typing.Any,
        max: typing.Any,
        **kwargs
    ) -> int:
        """
        Remove all members in a sorted set within the given scores
        """
        return self.client.zremrangebyscore(name, min, max, **kwargs)

    async def async_zremrangebyscore(
        self,
        name: str,
        min: typing.Any,
        max: typing.Any,
        **kwargs
    ) -> int:
        """
        Remove all members in a sorted set within the given scores
        """
        return await self.async_client.zremrangebyscore(name, min, max, **kwargs)
    
    def zrevrange(
        self,
        name: str,
        start: int,
        num: int,
        withscores: bool = False,
        score_cast_func: typing.Callable = float,
        **kwargs
    ) -> typing.List:
        """
        Return a range of members in a sorted set, by index, with scores ordered from high to low
        """
        return self.client.zrevrange(name, start, num, withscores, score_cast_func, **kwargs)
    
    async def async_zrevrange(
        self,
        name: str,
        start: int,
        num: int,
        withscores: bool = False,
        score_cast_func: typing.Callable = float,
        **kwargs
    ) -> typing.List:
        """
        Return a range of members in a sorted set, by index, with scores ordered from high to low
        """
        return await self.async_client.zrevrange(name, start, num, withscores, score_cast_func, **kwargs)
    
    def zrevrangebylex(
        self,
        name: str,
        max: typing.Any,
        min: typing.Any,
        start: int = None,
        num: int = None,
        **kwargs
    ) -> typing.List:
        """
        Return a range of members in a sorted set, by lexicographical range, ordered from higher to lower strings.
        """
        return self.client.zrevrangebylex(name, max, min, start, num, **kwargs)
    
    async def async_zrevrangebylex(
        self,
        name: str,
        max: typing.Any,
        min: typing.Any,
        start: int = None,
        num: int = None,
        **kwargs
    ) -> typing.List:
        """
        Return a range of members in a sorted set, by lexicographical range, ordered from higher to lower strings.
        """
        return await self.async_client.zrevrangebylex(name, max, min, start, num, **kwargs)
    
    def zrevrangebyscore(
        self,
        name: str,
        max: typing.Any,
        min: typing.Any,
        start: int = None,
        num: int = None,
        withscores: bool = False,
        score_cast_func: typing.Callable = float,
        **kwargs
    ) -> typing.List:
        """
        Return a range of members in a sorted set, by score, with scores ordered from high to low
        """
        return self.client.zrevrangebyscore(name, max, min, start, num, withscores, score_cast_func, **kwargs)
    
    async def async_zrevrangebyscore(
        self,
        name: str,
        max: typing.Any,
        min: typing.Any,
        start: int = None,
        num: int = None,
        withscores: bool = False,
        score_cast_func: typing.Callable = float,
        **kwargs
    ) -> typing.List:
        """
        Return a range of members in a sorted set, by score, with scores ordered from high to low
        """
        return await self.async_client.zrevrangebyscore(name, max, min, start, num, withscores, score_cast_func, **kwargs)
    
    def zrevrank(
        self,
        name: str,
        value: typing.Any,
        **kwargs
    ) -> int:
        """
        Determine the index of a member in a sorted set, with scores ordered from high to low
        """
        return self.client.zrevrank(name, value, **kwargs)
    
    async def async_zrevrank(
        self,
        name: str,
        value: typing.Any,
        **kwargs
    ) -> int:
        """
        Determine the index of a member in a sorted set, with scores ordered from high to low
        """
        return await self.async_client.zrevrank(name, value, **kwargs)
    
    def zscore(
        self,
        name: str,
        value: typing.Any,
        **kwargs
    ) -> float:
        """
        Get the score associated with the given member in a sorted set
        """
        return self.client.zscore(name, value, **kwargs)
    
    async def async_zscore(
        self,
        name: str,
        value: typing.Any,
        **kwargs
    ) -> float:
        """
        Get the score associated with the given member in a sorted set
        """
        return await self.async_client.zscore(name, value, **kwargs)
    
    def zunionstore(
        self,
        dest: str,
        keys: typing.List[str],
        aggregate: str = None,
        **kwargs
    ) -> int:
        """
        Add multiple sorted sets and store the resulting sorted set in a new key
        """
        return self.client.zunionstore(dest, keys, aggregate, **kwargs)
    
    async def async_zunionstore(
        self,
        dest: str,
        keys: typing.List[str],
        aggregate: str = None,
        **kwargs
    ) -> int:
        """
        Add multiple sorted sets and store the resulting sorted set in a new key
        """
        return await self.async_client.zunionstore(dest, keys, aggregate, **kwargs)
    
    def scan(
        self,
        cursor: int = 0,
        match: str = None,
        count: int = None,
        **kwargs
    ) -> typing.Tuple[int, typing.List]:
        """
        Incrementally iterate the keys space
        """
        return self.client.scan(cursor, match, count, **kwargs)
    
    async def async_scan(
        self,
        cursor: int = 0,
        match: str = None,
        count: int = None,
        **kwargs
    ) -> typing.Tuple[int, typing.List]:
        """
        Incrementally iterate the keys space
        """
        return await self.async_client.scan(cursor, match, count, **kwargs)
    
    def sscan(
        self,
        name: str,
        cursor: int = 0,
        match: str = None,
        count: int = None,
        **kwargs
    ) -> typing.Tuple[int, typing.List]:
        """
        Incrementally iterate Set elements
        """
        return self.client.sscan(name, cursor, match, count, **kwargs)
    
    async def async_sscan(
        self,
        name: str,
        cursor: int = 0,
        match: str = None,
        count: int = None,
        **kwargs
    ) -> typing.Tuple[int, typing.List]:
        """
        Incrementally iterate Set elements
        """
        return await self.async_client.sscan(name, cursor, match, count, **kwargs)
    
    def hscan(
        self,
        name: str,
        cursor: int = 0,
        match: str = None,
        count: int = None,
        **kwargs
    ) -> typing.Tuple[int, typing.List]:
        """
        Incrementally iterate hash fields and associated values
        """
        return self.client.hscan(name, cursor, match, count, **kwargs)
    
    async def async_hscan(
        self,
        name: str,
        cursor: int = 0,
        match: str = None,
        count: int = None,
        **kwargs
    ) -> typing.Tuple[int, typing.List]:
        """
        Incrementally iterate hash fields and associated values
        """
        return await self.async_client.hscan(name, cursor, match, count, **kwargs)
    
    def zscan(
        self,
        name: str,
        cursor: int = 0,
        match: str = None,
        count: int = None,
        score_cast_func: typing.Callable = float,
        **kwargs
    ) -> typing.Tuple[int, typing.List]:
        """
        Incrementally iterate sorted sets elements and associated scores
        """
        return self.client.zscan(name, cursor, match, count, score_cast_func, **kwargs)
    
    async def async_zscan(
        self,
        name: str,
        cursor: int = 0,
        match: str = None,
        count: int = None,
        score_cast_func: typing.Callable = float,
        **kwargs
    ) -> typing.Tuple[int, typing.List]:
        """
        Incrementally iterate sorted sets elements and associated scores
        """
        return await self.async_client.zscan(name, cursor, match, count, score_cast_func, **kwargs)
    
    def zunion(
        self,
        name: str,
        keys: typing.List[str],
        aggregate: str = None,
        **kwargs
    ) -> typing.List:
        """
        Add multiple sorted sets and store the resulting sorted set in a new key
        """
        return self.client.zunion(name, keys, aggregate, **kwargs)
    
    async def async_zunion(
        self,
        name: str,
        keys: typing.List[str],
        aggregate: str = None,
        **kwargs
    ) -> typing.List:
        """
        Add multiple sorted sets and store the resulting sorted set in a new key
        """
        return await self.async_client.zunion(name, keys, aggregate, **kwargs)
    
    def zinter(
        self,
        name: str,
        keys: typing.List[str],
        aggregate: str = None,
        **kwargs
    ) -> typing.List:
        """
        Intersect multiple sorted sets and store the resulting sorted set in a new key
        """
        return self.client.zinter(name, keys, aggregate, **kwargs)
    
    async def async_zinter(
        self,
        name: str,
        keys: typing.List[str],
        aggregate: str = None,
        **kwargs
    ) -> typing.List:
        """
        Intersect multiple sorted sets and store the resulting sorted set in a new key
        """
        return await self.async_client.zinter(name, keys, aggregate, **kwargs)
    

    """
    Other utilities
    """

    def command(self, **kwargs):
        """
        Run a command
        """
        return self.client.command(**kwargs)
    
    async def async_command(self, **kwargs):
        """
        Run a command
        """
        return await self.async_client.command(**kwargs)
    
    def transaction(
        self,
        func: Pipeline,
        *watches: typing.Any,
        **kwargs
    ) -> typing.List:
        """
        Run a transaction
        """
        return self.client.transaction(func, *watches, **kwargs)
    
    async def async_transaction(
        self,
        func: AsyncPipeline,
        *watches: typing.Any,
        **kwargs
    ) -> typing.List:
        """
        Run a transaction
        """
        return await self.async_client.transaction(func, *watches, **kwargs)
    

    def config_get(
        self,
        pattern: str,
        **kwargs
    ) -> typing.Dict:
        """
        Get the value of a configuration parameter
        """
        return self.client.config_get(pattern, **kwargs)

    async def async_config_get(
        self,
        pattern: str,
        **kwargs
    ) -> typing.Dict:
        """
        Get the value of a configuration parameter
        """
        return await self.async_client.config_get(pattern, **kwargs)

    def config_set(
        self,
        name: str,
        value: typing.Any,
        **kwargs
    ) -> bool:
        """
        Set a configuration parameter to the given value
        """
        return self.client.config_set(name, value, **kwargs)

    async def async_config_set(
        self,
        name: str,
        value: typing.Any,
        **kwargs
    ) -> bool:
        """
        Set a configuration parameter to the given value
        """
        return await self.async_client.config_set(name, value, **kwargs)

    def config_resetstat(self, **kwargs):
        """
        Reset the stats returned by INFO
        """
        return self.client.config_resetstat(**kwargs)

    async def async_config_resetstat(self, **kwargs):
        """
        Reset the stats returned by INFO
        """
        return await self.async_client.config_resetstat(**kwargs)

    def config_rewrite(self, **kwargs):
        """
        Rewrite the configuration file with the in memory configuration
        """
        return self.client.config_rewrite(**kwargs)

    async def async_config_rewrite(self, **kwargs):
        """
        Rewrite the configuration file with the in memory configuration
        """
        return await self.async_client.config_rewrite(**kwargs)
    
    def keys(
        self,
        pattern: typing.Optional[str] = None,
        **kwargs
    ) -> typing.List[KeyT]:
        """
        Get a List of all keys
        """
        return self.client.keys(pattern = pattern, **kwargs)
    
    async def async_keys(
        self,
        pattern: typing.Optional[str] = None,
        **kwargs
    ) -> typing.List[KeyT]:
        """
        Get a List of all keys
        """
        return await self.async_client.keys(pattern = pattern, **kwargs)

    def flushall(self, **kwargs):
        """
        Delete all keys in the current database
        """
        return self.client.flushall(**kwargs)
    
    async def async_flushall(self, **kwargs):
        """
        Delete all keys in the current database
        """
        return await self.async_client.flushall(**kwargs)
    
    def flushdb(self, **kwargs):
        """
        Delete all keys in the current database
        """
        return self.client.flushdb(**kwargs)
    
    async def async_flushdb(self, **kwargs):
        """
        Delete all keys in the current database
        """
        return await self.async_client.flushdb(**kwargs)
    
    def dbsize(self, **kwargs) -> int:
        """
        Return the number of keys in the current database
        """
        return self.client.dbsize(**kwargs)
    
    async def async_dbsize(self, **kwargs) -> int:
        """
        Return the number of keys in the current database
        """
        return await self.async_client.dbsize(**kwargs)
    
    def randomkey(self, **kwargs):
        """
        Return a random key from the current database
        """
        return self.client.randomkey(**kwargs)
    
    async def async_randomkey(self, **kwargs):
        """
        Return a random key from the current database
        """
        return await self.async_client.randomkey(**kwargs)
    
    def info(
        self,  
        section: typing.Optional[str] = None,
        *args,
        **kwargs
    ):
        """
        Return information and statistics about the server
        """
        return self.client.info(*args, section = section, **kwargs)
    
    async def async_info(
        self,
        section: typing.Optional[str] = None,
        *args,
        **kwargs
    ):
        """
        Return information and statistics about the server
        """
        return await self.async_client.info(*args, section = section, **kwargs)
    
    def move(
        self,
        key: KeyT,
        db: int,
        **kwargs
    ) -> bool:
        """
        Move a key to another database
        """
        return self.client.move(key, db, **kwargs)
    
    async def async_move(
        self,
        key: KeyT,
        db: int,
        **kwargs
    ) -> bool:
        """
        Move a key to another database
        """
        return await self.async_client.move(key, db, **kwargs)
    
    def rename(
        self,
        key: KeyT,
        newkey: KeyT,
        **kwargs
    ) -> bool:
        """
        Rename a key
        """
        return self.client.rename(key, newkey, **kwargs)
    
    async def async_rename(
        self,
        key: KeyT,
        newkey: KeyT,
        **kwargs
    ) -> bool:
        """
        Rename a key
        """
        return await self.async_client.rename(key, newkey, **kwargs)
    
    def renamenx(
        self,
        key: KeyT,
        newkey: KeyT,
        **kwargs
    ) -> bool:
        """
        Rename a key, only if the new key does not exist
        """
        return self.client.renamenx(key, newkey, **kwargs)
    
    async def async_renamenx(
        self,
        key: KeyT,
        newkey: KeyT,
        **kwargs
    ) -> bool:
        """
        Rename a key, only if the new key does not exist
        """
        return await self.async_client.renamenx(key, newkey, **kwargs)
    
    def migrate(
        self,
        session: typing.Optional[typing.Type['KeyDBSession']] = None,
        host: typing.Optional[str] = None,
        port: typing.Optional[int] = None,
        keys: typing.Optional[typing.Union[KeyT, typing.List[KeyT]]] = None,
        destination_db: typing.Optional[int] = None,
        timeout: typing.Optional[int] = None,
        copy: typing.Optional[bool] = None,
        replace: typing.Optional[bool] = None,
        auth: typing.Optional[str] = None,
        **kwargs
    ):
        """
        Migrate a key to a different KeyDB instance
        """
        if session:
            host = host or session.uri.host
            port = port if port is not None else session.uri.port
            auth = auth if auth is not None else session.uri.auth_str
            destination_db = destination_db if destination_db is not None else session.uri.db_id
        
        return self.client.migrate(
            session = session,
            host = host,
            port = port,
            keys = keys,
            destination_db = destination_db,
            timeout = timeout,
            copy = copy,
            replace = replace,
            auth = auth,
            **kwargs
        )
    
    async def async_migrate(
        self,
        session: typing.Optional[typing.Type['KeyDBSession']] = None,
        host: typing.Optional[str] = None,
        port: typing.Optional[int] = None,
        keys: typing.Optional[typing.Union[KeyT, typing.List[KeyT]]] = None,
        destination_db: typing.Optional[int] = None,
        timeout: typing.Optional[int] = None,
        copy: typing.Optional[bool] = None,
        replace: typing.Optional[bool] = None,
        auth: typing.Optional[str] = None,
        **kwargs
    ):
        """
        Migrate a key to a different KeyDB instance
        """
        if session:
            host = host or session.uri.host
            port = port if port is not None else session.uri.port
            auth = auth if auth is not None else session.uri.auth_str
            destination_db = destination_db if destination_db is not None else session.uri.db_id
        
        return await self.async_client.migrate(
            session = session,
            host = host,
            port = port,
            keys = keys,
            destination_db = destination_db,
            timeout = timeout,
            copy = copy,
            replace = replace,
            auth = auth,
            **kwargs
        )

    def module_list(self, **kwargs):
        """
        List all modules
        """
        return self.client.module_list(**kwargs)
    
    async def async_module_list(self, **kwargs):
        """
        List all modules
        """
        return await self.async_client.module_list(**kwargs)

    def module_load(
        self,
        path: str,
        **kwargs
    ):
        """
        Load a module
        """
        return self.client.module_load(path, **kwargs)
    
    async def async_module_load(
        self,
        path: str,
        **kwargs
    ):
        """
        Load a module
        """
        return await self.async_client.module_load(path, **kwargs)
    
    def module_loadex(
        self,
        path: str,
        options: typing.Optional[typing.List[str]] = None,
        args: typing.Optional[typing.List[str]] = None,
        **kwargs
    ):
        """
        Load a module
        """
        return self.client.module_loadex(path, options = options, args = args, **kwargs)

    async def async_module_loadex(
        self,
        path: str,
        options: typing.Optional[typing.List[str]] = None,
        args: typing.Optional[typing.List[str]] = None,
        **kwargs
    ):
        """
        Load a module
        """
        return await self.async_client.module_loadex(path, options = options, args = args, **kwargs)
    
    def module_unload(
        self,
        name: str,
        **kwargs
    ):
        """
        Unload a module
        """
        return self.client.module_unload(name, **kwargs)
    
    async def async_module_unload(
        self,
        name: str,
        **kwargs
    ):
        """
        Unload a module
        """
        return await self.async_client.module_unload(name, **kwargs)
    

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
                raise ConnectionError(f'[{self.name}] Max {max_attempts} attempts reached')
            if timeout and time.time() - start_time >= timeout:
                raise TimeoutError(f'[{self.name}] Timeout of {timeout} seconds reached')
            try:
                self.ping()
                if verbose: logger.info(f'[{self.name}] KeyDB is Ready after {attempts} attempts')
                self._active = True
                break
            except (InterruptedError, KeyboardInterrupt) as e:
                logger.error(e)
                break            
            except Exception as e:
                if verbose: logger.info(f'[{self.name}] KeyDB is not ready, retrying in {interval} seconds')
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
                raise ConnectionError(f'[{self.name}] Max {max_attempts} attempts reached')
            if timeout and time.time() - start_time >= timeout:
                raise TimeoutError(f'[{self.name}] Timeout of {timeout} seconds reached')

            try:
                await self.async_ping()
                if verbose: logger.info(f'[{self.name}] KeyDB is Ready after {attempts} attempts')
                self._active = True
                break
            except (InterruptedError, KeyboardInterrupt, asyncio.CancelledError) as e:
                logger.error(e)
                break            
            except Exception as e:
                if verbose: logger.info(f'[{self.name}] KeyDB is not ready, retrying in {interval} seconds')
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
        exclude_kwargs: typing.Optional[typing.List[str]] = None,
        include_cache_hit: typing.Optional[bool] = False,
        _no_cache: typing.Optional[bool] = False,
        _no_cache_kwargs: typing.Optional[typing.List[str]] = None,
        _no_cache_validator: typing.Optional[typing.Callable] = None,
        _func_name: typing.Optional[str] = None,
        _validate_requests: typing.Optional[bool] = True,
        _exclude_request_headers: typing.Optional[typing.Union[typing.List[str], bool]] = True,
        _cache_invalidator: typing.Optional[typing.Union[bool, typing.Callable]] = None,
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
        cache_prefix = cache_prefix if cache_prefix is not None else self.cache_prefix
        cache_ttl = cache_ttl if cache_ttl is not None else self.cache_ttl

        def decorator(func):
            "Decorator created by memoize() for callable `func`."
            # Support _func_name for usage in wrapped up decorators
            base = (_func_name or full_name(func),)
            if iscoroutinefunction(func):
                @functools.wraps(func)
                async def wrapper(*args, **kwargs):
                    "Wrapper for callable to cache arguments and return values."

                    # If cache is disabled, return
                    if self.cache_enabled is False:
                        if include_cache_hit:
                            return await func(*args, **kwargs), False
                        return await func(*args, **kwargs)
                    
                    # if the max attempts for connection is reached, return the function
                    if (self._cache_max_attempts and self._cache_max_attempts > 0) \
                         and self._cache_failed_attempts >= self._cache_max_attempts:
                        if include_cache_hit:
                            return await func(*args, **kwargs), False
                        return await func(*args, **kwargs)

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

                    keybuilder_kwargs = kwargs.copy()
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
                        
                        # Remove headers from keybuilder
                        if _exclude_request_headers:
                            _nested_headers = None
                            if keybuilder_kwargs.get("request", None) is not None and keybuilder_kwargs['requests'].get('headers', None) is not None:
                                headers = keybuilder_kwargs["request"].pop("headers", None)
                                _nested_headers = True
                            elif keybuilder_kwargs.get("headers", None) is not None:
                                headers = keybuilder_kwargs.pop("headers", None)
                                _nested_headers = False
                            
                            if _nested_headers is not None and isinstance(_exclude_request_headers, (list, tuple)):
                                for key in _exclude_request_headers:
                                    _ = headers.pop(key, None)
                                    _ = headers.pop(key.lower(), None)
                            
                                if _nested_headers:
                                    keybuilder_kwargs["request"]["headers"] = headers
                                else:
                                    keybuilder_kwargs["headers"] = headers
                    
                    if exclude_kwargs:
                        for key in exclude_kwargs:
                            _ = keybuilder_kwargs.pop(key, None)

                    # Do the actual caching
                    key = wrapper.__cache_key__(*args, **keybuilder_kwargs)

                    # Handle invalidating the key
                    _invalidate_key = False
                    if _cache_invalidator:
                        if isinstance(_cache_invalidator, bool):
                            _invalidate_key = _cache_invalidator

                        elif iscoroutinefunction(_cache_invalidator):
                            _invalidate_key = await _cache_invalidator(*args, _cache_key = key, **kwargs)
                        
                        else:
                            _invalidate_key = _cache_invalidator(*args, _cache_key = key, **kwargs)

                    if _invalidate_key:
                        if self.settings.debug_enabled:
                            logger.info(f'[{self.name}] Invalidating cache key: {key}')
                        try:
                            with anyio.fail_after(1):
                                await self.async_delete(key)

                        except TimeoutError:
                            logger.error(f'[{self.name}] Calling DELETE on async KeyDB timed out. Cached function: {base}')
                            self._cache_failed_attempts += 1

                    try:
                        with anyio.fail_after(1):
                            result = await self.async_get(key, default = ENOVAL)

                    except TimeoutError:
                        result = ENOVAL
                        logger.error(f'[{self.name}] Calling GET on async KeyDB timed out. Cached function: {base}')
                        self._cache_failed_attempts += 1

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
                                logger.error(f'[{self.name}] Calling SET on async KeyDB timed out. Cached function: {base}')
                                self._cache_failed_attempts += 1
                    
                    return (result, is_cache_hit) if include_cache_hit else result

            else:
                @functools.wraps(func)
                def wrapper(*args, **kwargs):

                    # If cache is disabled, return
                    if self.cache_enabled is False:
                        if include_cache_hit:
                            return func(*args, **kwargs), False
                        return func(*args, **kwargs)
                    
                    # if the max attempts for connection is reached, return the function
                    if (self._cache_max_attempts and self._cache_max_attempts > 0) \
                         and self._cache_failed_attempts >= self._cache_max_attempts:
                        if include_cache_hit:
                            return func(*args, **kwargs), False
                        return func(*args, **kwargs)

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


                    keybuilder_kwargs = kwargs.copy()

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
                    
                        # Remove headers from keybuilder
                        if _exclude_request_headers:
                            _nested_headers = None
                            if keybuilder_kwargs.get("request", None) is not None and keybuilder_kwargs['requests'].get('headers', None) is not None:
                                headers = keybuilder_kwargs["request"].pop("headers", None)
                                _nested_headers = True
                            elif keybuilder_kwargs.get("headers", None) is not None:
                                headers = keybuilder_kwargs.pop("headers", None)
                                _nested_headers = False
                            
                            if _nested_headers is not None and isinstance(_exclude_request_headers, (list, tuple)):
                                for key in _exclude_request_headers:
                                    _ = headers.pop(key, None)
                                    _ = headers.pop(key.lower(), None)
                            
                                if _nested_headers:
                                    keybuilder_kwargs["request"]["headers"] = headers
                                else:
                                    keybuilder_kwargs["headers"] = headers
                    
                    if exclude_kwargs:
                        for key in exclude_kwargs:
                            _ = keybuilder_kwargs.pop(key, None)


                    # Do the actual caching
                    key = wrapper.__cache_key__(*args, **kwargs)
                    
                    # Handle invalidating the key
                    _invalidate_key = False
                    if _cache_invalidator:
                        if isinstance(_cache_invalidator, bool):
                            _invalidate_key = _cache_invalidator
                        else:
                            _invalidate_key = _cache_invalidator(*args, _cache_key = key, **kwargs)

                    if _invalidate_key:
                        if self.settings.debug_enabled:
                            logger.info(f'[{self.name}] Invalidating cache key: {key}')
                        try:
                            self.delete(key)
                        except TimeoutError:
                            logger.error(f'[{self.name}] Calling DELETE on KeyDB timed out. Cached function: {base}')
                            self._cache_failed_attempts += 1
                    
                    try:
                        result = self.get(key, default = ENOVAL)
                    except TimeoutError:
                        result = ENOVAL
                        logger.error(f'[{self.name}] Calling GET on KeyDB timed out. Cached function: {base}')
                        self._cache_failed_attempts += 1

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
                                logger.error(f'[{self.name}] Calling SET on KeyDB timed out. Cached function: {base}')
                                self._cache_failed_attempts += 1
                    
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
        return f'<{self.__class__.__name__} {self.name}> {self.uri} @ {self.uri.db_id}'
    
    def __str__(self) -> str:
        return self.uri
    
    def __len__(self) -> int:
        return self.dbsize()