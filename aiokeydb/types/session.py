import sys
import time
import anyio
import typing
import logging
import asyncio
import functools
import contextlib
from pydantic import BaseModel
from pydantic.types import ByteSize
from aiokeydb.typing import Number, KeyT, ExpiryT, AbsExpiryT, PatternT
from aiokeydb.lock import Lock, AsyncLock
from aiokeydb.core import KeyDB, PubSub, Pipeline, PipelineT, PubSubT
from aiokeydb.core import AsyncKeyDB, AsyncPubSub, AsyncPipeline, AsyncPipelineT, AsyncPubSubT

from aiokeydb.connection import Encoder, ConnectionPool, AsyncConnectionPool
from aiokeydb.exceptions import (
    ConnectionError,
    TimeoutError,
)
from aiokeydb.types import KeyDBUri, ENOVAL
# from aiokeydb.configs import KeyDBSettings, settings as default_settings
from aiokeydb.configs import KeyDBSettings
from aiokeydb.utils import full_name, args_to_key, get_keydb_settings
from aiokeydb.utils.helpers import create_retryable_client, afail_after
from aiokeydb.utils.logs import logger
from .cachify import cachify, create_cachify, FT

from aiokeydb.serializers import BaseSerializer

from inspect import iscoroutinefunction

if sys.version_info >= (3, 11, 3):
    from asyncio import timeout as async_timeout
else:
    from async_timeout import timeout as async_timeout



class ClientPools(BaseModel):
    """
    Holds the reference for connection pools
    """
    name: str # the key for the connectionpools
    pool: typing.Union[ConnectionPool, typing.Type[ConnectionPool]]
    apool: typing.Union[AsyncConnectionPool, typing.Type[AsyncConnectionPool]]

    class Config:
        arbitrary_types_allowed = True
    
    def with_db_id(
        self,
        db_id: int
    ) -> 'ClientPools':
        """
        Returns a new ClientPools with the given db_id
        """
        return ClientPools(
            name = self.name,
            pool = self.pool.with_db_id(db_id),
            apool = self.apool.with_db_id(db_id),
        )
    


    async def arecreate_pools(
        self,
        inuse_connections: bool = True,
        raise_exceptions: bool = False,
        with_lock: bool = False,
        **recreate_kwargs
    ):
        """
        Resets the connection pools
        """
        self.pool = self.pool.recreate(
            inuse_connections = inuse_connections,
            raise_exceptions = raise_exceptions,
            with_lock = with_lock,
            **recreate_kwargs
        )
        self.apool = await self.apool.recreate(
            inuse_connections = inuse_connections,
            raise_exceptions = raise_exceptions,
            with_lock = with_lock,
            **recreate_kwargs
        )


class SessionCtx(BaseModel):
    """
    Holds the reference for session ctx
    """

    active: bool = False
    # We'll use this to keep track of the number of times we've tried to
    # connect to the database. This is used to determine if we should
    # attempt to reconnect to the database.
    # if the max_attempts have been reached, we'll stop trying to reconnect
    cache_max_attempts: int = 20
    cache_failed_attempts: int = 0

    client: typing.Optional[KeyDB] = None
    async_client: typing.Optional[AsyncKeyDB] = None

    lock: typing.Optional[Lock] = None
    async_lock: typing.Optional[AsyncLock] = None
    
    pipeline: typing.Optional[Pipeline] = None
    async_pipeline: typing.Optional[AsyncPipeline] = None

    locks: typing.Dict[str, Lock] = {}
    async_locks: typing.Dict[str, AsyncLock] = {}

    pubsub: typing.Optional[PubSub] = None
    async_pubsub: typing.Optional[AsyncPubSub] = None

    pipeline: typing.Optional[Pipeline] = None
    async_pipeline: typing.Optional[AsyncPipeline] = None

    dict_hash_key: typing.Optional[str] = None
    dict_async_enabled: typing.Optional[bool] = None
    dict_method: typing.Optional[str] = None

    class Config:
        arbitrary_types_allowed = True


def _configure_dict_methods(
    session: 'KeyDBSession',
    method: typing.Optional[str] = None,
    async_enabled: typing.Optional[bool] = None,
):
    """
    Configures the Dict get/set methods
    """
    if method is None: method = session.state.dict_method
    if async_enabled is None: async_enabled = session.state.dict_async_enabled
    if method == session.state.dict_method and async_enabled == session.state.dict_async_enabled:
        return
    
    if async_enabled:
        async def getitem(self: 'KeyDBSession', key: KeyT) -> typing.Any:
            if method == 'hash':
                value = await self.async_client.hget(self.dict_hash_key, key)
            else:
                value = await self.async_client.get(key)
            if value is None: 
                key_value = f'{self.dict_hash_key}:{key}' if method == 'hash' else key
                raise KeyError(key_value)
            if self.dict_decoder is not False:
                value = self.dict_decoder(value)
            return value
    else:
        def getitem(self: 'KeyDBSession', key: KeyT) -> typing.Any:
            if method == 'hash':
                value = self.client.hget(self.dict_hash_key, key)
            else:
                value = self.client.get(key)
            if value is None: 
                key_value = f'{self.dict_hash_key}:{key}' if method == 'hash' else key
                raise KeyError(key_value)
            if self.dict_decoder is not False:
                value = self.dict_decoder(value)
            return value

    setattr(KeyDBSession, '__getitem__', getitem)
    session.__getitem__ = getitem
    session.state.dict_method = method
    session.state.dict_async_enabled = async_enabled
    # logger.info(f'Configured Dict Get Method: {method} | Async Enabled: {async_enabled}, {iscoroutinefunction(getitem)}')



class KeyDBSession:
    """
    Class to hold both the sync and async clients
    """

    def __init__(
        self,
        uri: typing.Union[str, KeyDBUri],
        name: str,
        client_pools: ClientPools,
        db_id: typing.Optional[int] = None,
        encoder: typing.Optional[Encoder] = None,
        serializer: typing.Optional[typing.Type[BaseSerializer]] = None,
        settings: typing.Optional[KeyDBSettings] = None,
        cache_ttl: typing.Optional[Number] = None,
        cache_prefix: typing.Optional[str] = None,
        cache_enabled: typing.Optional[bool] = None,
        **config,
    ):
        
        if isinstance(uri, str): uri = KeyDBUri(dsn = uri)
        self.uri: KeyDBUri = uri
        self.name = name
        self.db_id = db_id or uri.db_id
        config = config or {}
        self.config = config
        self.settings = settings or get_keydb_settings()
        self.encoder = encoder or Encoder(
            encoding = config.get('encoding', self.settings.encoding),
            encoding_errors = config.get('encoding_errors', self.settings.encoding_errors),
            decode_responses = True,
        )

        self.serializer = serializer or self.settings.get_serializer()

        self.cache_prefix = cache_prefix or self.settings.cache_prefix
        self.cache_ttl = cache_ttl if cache_ttl is not None else self.settings.cache_ttl
        self.cache_enabled = cache_enabled if cache_enabled is not None else self.settings.cache_enabled

        self.client_pools = client_pools
        self.state = SessionCtx(
            cache_max_attempts = config.get('cache_max_attempts', 20)
        )

        # Enhanced Dict
        
        self.dict_hash_key: typing.Optional[str] = config.get('dict_hash_key', f'data:{self.name}:dict')
        self.dict_encoder: typing.Callable = config.get('dict_encoder', self.serializer.dumps)
        self.dict_decoder: typing.Callable = config.get('dict_decoder', self.serializer.loads)
        self.state.dict_async_enabled = config.get('dict_async_enabled', False)
        self.state.dict_method = config.get('dict_method', 'default')
        self.configure_dict_methods()
        
        # logger.info(f'Encoder: {self.encoder} | Serializer: {self.serializer}')
    
    def configure_dict_methods(
        self, 
        method: typing.Optional[str] = None,
        async_enabled: typing.Optional[bool] = None,
    ):
        """
        Configures the Dict get/set methods
        """
        _configure_dict_methods(
            self,
            method = method,
            async_enabled = async_enabled,
        )


    @classmethod
    def _get_client_class(
        cls, 
        settings: KeyDBSettings,
        retry_client_enabled: typing.Optional[bool] = None,
        retry_client_max_attempts: typing.Optional[int] = None,
        retry_client_max_delay: typing.Optional[int] = None,
        retry_client_logging_level: typing.Optional[int] = None,
        **kwargs
    ) -> typing.Type[KeyDB]:
        """
        Returns the client class
        """
        retry_client_enabled = retry_client_enabled if retry_client_enabled is not None else settings.retry_client_enabled
        if not retry_client_enabled: return KeyDB
        retry_client_max_attempts = retry_client_max_attempts if retry_client_max_attempts is not None else settings.retry_client_max_attempts
        retry_client_max_delay = retry_client_max_delay if retry_client_max_delay is not None else settings.retry_client_max_delay
        retry_client_logging_level = retry_client_logging_level if retry_client_logging_level is not None else settings.retry_client_logging_level
        return create_retryable_client(
            KeyDB,
            max_attempts = retry_client_max_attempts,
            max_delay = retry_client_max_delay,
            logging_level = retry_client_logging_level,
            **kwargs
        )
    

    @classmethod
    def _get_async_client_class(
        cls, 
        settings: KeyDBSettings,
        retry_client_enabled: typing.Optional[bool] = None,
        retry_client_max_attempts: typing.Optional[int] = None,
        retry_client_max_delay: typing.Optional[int] = None,
        retry_client_logging_level: typing.Optional[int] = None,
        **kwargs
    ) -> typing.Type[AsyncKeyDB]:
        """
        Returns the client class
        """
        retry_client_enabled = retry_client_enabled if retry_client_enabled is not None else settings.retry_client_enabled
        if not retry_client_enabled: return AsyncKeyDB
        retry_client_max_attempts = retry_client_max_attempts if retry_client_max_attempts is not None else settings.retry_client_max_attempts
        retry_client_max_delay = retry_client_max_delay if retry_client_max_delay is not None else settings.retry_client_max_delay
        retry_client_logging_level = retry_client_logging_level if retry_client_logging_level is not None else settings.retry_client_logging_level
        return create_retryable_client(
            AsyncKeyDB,
            max_attempts = retry_client_max_attempts,
            max_delay = retry_client_max_delay,
            logging_level = retry_client_logging_level,
            **kwargs
        )
    

    @property
    def client(self) -> KeyDB:
        if self.state.client is None:
            self.state.client = self._get_client_class(
                self.settings,
                **self.config,
            )(
                connection_pool = self.client_pools.pool,
            )
            # self.state.client = KeyDB(
            #     connection_pool = self.client_pools.pool,
            # )
        return self.state.client
    
    @property
    def async_client(self) -> AsyncKeyDB:
        if self.state.async_client is None:
            self.state.async_client = self._get_async_client_class(
                self.settings,
                **self.config,
            )(
                connection_pool = self.client_pools.apool,
            )
            # self.state.async_client = AsyncKeyDB(
            #     connection_pool = self.client_pools.apool,
            # )
        return self.state.async_client
    
    @property
    def pubsub(self) -> PubSub:
        """
        Initializes a `KeyDB` Client and returns a `PubSub`.

        Requires reinitialzing a new client because `decode_responses` should be set to `True`.
        """
        if self.state.pubsub is None:
            self.state.pubsub = self.client.pubsub()
        return self.state.pubsub
    
    @property
    def async_pubsub(self) -> AsyncPubSub:
        """
        Initializes a `AsyncKeyDB` Client and returns a `AsyncPubSub`.

        Requires reinitialzing a new client because `decode_responses` should be set to `True`.
        """
        if self.state.async_pubsub is None:
            self.state.async_pubsub = self.async_client.pubsub()
        return self.state.async_pubsub
    

    def get_pubsub(
        self, 
        retryable: typing.Optional[bool] = None,
        **kwargs
    ) -> PubSubT:
        """
        Return a Publish/Subscribe object. With this object, you can
        subscribe to channels and listen for messages that get published to
        """
        if retryable is None: retryable = self.settings.retry_client_enabled
        return self.client.pubsub(
            retryable = retryable,
            **kwargs
        )

    def get_async_pubsub(
        self, 
        retryable: typing.Optional[bool] = None,
        **kwargs
    ) -> AsyncPubSubT:
        """
        Return a Publish/Subscribe object. With this object, you can
        subscribe to channels and listen for messages that get published to
        """
        if retryable is None: retryable = self.settings.retry_client_enabled
        return self.async_client.pubsub(
            retryable = retryable,
            **kwargs
        )
    
    @property
    def pipeline(self) -> Pipeline:
        """
        Initializes a `KeyDB` Client and returns a `Pipeline`.
        """
        if self.state.pipeline is None:
            self.state.pipeline = self.client.pipeline(
                transaction = self.config.get('transaction', True),
                shard_hint = self.config.get('shard_hint', None),
            )
        return self.state.pipeline
    
    @property
    def async_pipeline(self) -> AsyncPipeline:
        """
        Initializes a `AsyncKeyDB` Client and returns a `AsyncPipeline`.
        """
        if self.state.async_pipeline is None:
            self.state.async_pipeline = self.async_client.pipeline(
                transaction = self.config.get('transaction', True),
                shard_hint = self.config.get('shard_hint', None),
            )
        return self.state.async_pipeline
    
    def get_pipeline(
        self, 
        transaction: typing.Optional[bool] = True, 
        shard_hint: typing.Optional[str] = None, 
        retryable: typing.Optional[bool] = None,
        **kwargs
    ) -> PipelineT:
        """
        Return a new pipeline object that can queue multiple commands for
        later execution. ``transaction`` indicates whether all commands
        should be executed atomically. Apart from making a group of operations
        atomic, pipelines are useful for reducing the back-and-forth overhead
        between the client and server.
        """
        if retryable is None: retryable = self.settings.retry_client_enabled
        return self.client.pipeline(
            transaction = transaction,
            shard_hint = shard_hint,
            retryable = retryable,
        )

    def get_async_pipeline(
        self, 
        transaction: typing.Optional[bool] = True, 
        shard_hint: typing.Optional[str] = None, 
        retryable: typing.Optional[bool] = None,
        **kwargs
    ) -> AsyncPipelineT:
        """
        Return a new pipeline object that can queue multiple commands for
        later execution. ``transaction`` indicates whether all commands
        should be executed atomically. Apart from making a group of operations
        atomic, pipelines are useful for reducing the back-and-forth overhead
        between the client and server.
        """
        if retryable is None: retryable = self.settings.retry_client_enabled
        return self.async_client.pipeline(
            transaction = transaction,
            shard_hint = shard_hint,
            retryable = retryable,
        )


    @property
    def lock(self) -> Lock:
        if self.state.lock is None:
            self.state.lock = Lock(self.client)
        return self.state.lock
    
    @property
    def async_lock(self) -> AsyncLock:
        if self.state.async_lock is None:
            self.state.async_lock = AsyncLock(self.async_client)
        return self.state.async_lock

    def with_db_id(
        self,
        db_id: int,
    ) -> 'KeyDBSession':
        """
        Initialize a new session with the given db_id
        if the db_id is different from the current one.
        """
        if db_id != self.db_id:
            return self.__class__(
                uri = self.uri,
                name = self.name,
                client_pools = self.client_pools.with_db_id(db_id),
                db_id = db_id,
                encoder = self.encoder,
                serializer = self.serializer,
                settings = self.settings,
                cache_ttl = self.cache_ttl,
                cache_prefix = self.cache_prefix,
                cache_enabled = self.cache_enabled,
                **self.config,
            )
        return self

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
        if name not in self.state.locks:
            self.state.locks[name] = Lock(
                self.client, 
                name = name, 
                timeout = timeout, 
                sleep = sleep, 
                blocking = blocking, 
                blocking_timeout = blocking_timeout, 
                thread_local = thread_local
            )
        if not self.state.lock:  self.state.lock = self.state.locks[name]
        return self.state.locks[name]
    
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
        if name not in self.state.async_locks:
            self.state.async_locks[name] = AsyncLock(
                self.async_client, 
                name = name, 
                timeout = timeout, 
                sleep = sleep, 
                blocking = blocking, 
                blocking_timeout = blocking_timeout, 
                thread_local = thread_local
            )
        if not self.state.async_lock:  self.state.async_lock = self.state.async_locks[name]
        return self.state.async_locks[name]

    def close_locks(
        self, 
        names: typing.Optional[typing.Union[typing.List[str], str]] = None
    ):
        if names is None: names = list(self.state.locks.keys())
        if isinstance(names, str): names = [names]

        for name in names:
            if name in self.state.locks:
                self.state.locks[name].release()
                del self.state.locks[name]
    
    async def async_close_locks(
        self, 
        names: typing.Optional[typing.Union[typing.List[str], str]] = None
    ):
        if names is None: names = list(self.state.async_locks.keys())
        if isinstance(names, str): names = [names]
        for name in names:
            if name in self.state.async_locks:
                await self.state.async_locks[name].release()
                del self.state.async_locks[name]
    

    def close(self, close_pool: bool = False, raise_exceptions: bool = False):
        self.close_locks()
        if self.state.pubsub is not None:
            self.state.pubsub.close()
            self.state.pubsub = None
        
        if self.state.client is not None:
            self.state.client.close()
            if close_pool:
                self.state.client.connection_pool.disconnect(raise_exceptions = raise_exceptions)
            self.state.client = None

    async def aclose(self, close_pool: bool = False, raise_exceptions: bool = False):
        await self.async_close_locks()
        if self.state.async_pubsub is not None:
            await self.state.async_pubsub.close()
            self.state.async_pubsub = None
        
        if self.state.async_client is not None:
            await self.state.async_client.close()
            if close_pool:
                await self.state.async_client.connection_pool.disconnect(raise_exceptions = raise_exceptions)
            self.state.async_client = None
        
        if self.state.client is not None:
            self.state.client.close()
            if close_pool:
                self.state.client.connection_pool.disconnect(raise_exceptions = raise_exceptions)
            self.state.client = None


    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.aclose()
    
    def publish(self, channel: str, message: typing.Any, **kwargs):
        """
        [PubSub] Publishes a message to a channel
        """
        return self.client.publish(channel, message, **kwargs)

    async def async_publish(self, channel: str, message: typing.Any, **kwargs):
        """
        [PubSub] Publishes a message to a channel
        """
        return await self.async_client.publish(channel, message, **kwargs)

    def subscribe(self, *channels: str, **kwargs):
        """
        [PubSub] Subscribes to a channel
        """
        return self.pubsub.subscribe(*channels, **kwargs)
    
    async def async_subscribe(self, *channels: str, **kwargs):
        """
        [PubSub] Subscribes to a channel
        """
        return await self.async_pubsub.subscribe(*channels, **kwargs)

    def unsubscribe(self, *channels: str, **kwargs):
        """
        [PubSub] Unsubscribes from a channel
        """
        return self.pubsub.unsubscribe(*channels, **kwargs)

    async def async_unsubscribe(self, *channels: str, **kwargs):
        """
        [PubSub] Unsubscribes from a channel
        """
        return await self.async_pubsub.unsubscribe(*channels, **kwargs)

    def psubscribe(self, *patterns: str, **kwargs):
        """
        [PubSub] Subscribes to a pattern
        """
        return self.pubsub.psubscribe(*patterns, **kwargs)

    async def async_psubscribe(self, *patterns: str, **kwargs):
        """
        [PubSub] Subscribes to a pattern
        """
        return await self.async_pubsub.psubscribe(*patterns, **kwargs)

    def punsubscribe(self, *patterns: str, **kwargs):
        """
        [PubSub] Unsubscribes from a pattern
        """
        return self.pubsub.punsubscribe(*patterns, **kwargs)
    
    async def async_punsubscribe(self, *patterns: str, **kwargs):
        """
        [PubSub] Unsubscribes from a pattern
        """
        return await self.async_pubsub.punsubscribe(*patterns, **kwargs)
    
    def plisten(
        self, 
        *patterns: str,
        timeout: typing.Optional[float] = None,
        # decode_responses: typing.Optional[bool] = True,
        unsubscribe_after: typing.Optional[bool] = False,
        close_after: typing.Optional[bool] = False,
        listen_callback: typing.Optional[typing.Callable] = None,
        cancel_callback: typing.Optional[typing.Callable] = None,
        **kwargs
    ) -> typing.Iterator[typing.Any]:
        """
        [PubSub] Listens for messages
        """
        def _listen():
            if timeout:
                from lazyops.utils import fail_after
                with contextlib.suppress(TimeoutError):
                    with fail_after(timeout):
                        for message in self.pubsub.listen():
                            if listen_callback: listen_callback(message, **kwargs)
                            if cancel_callback and cancel_callback(message, **kwargs): 
                                break
                            yield message
            else:
                for message in self.pubsub.listen():
                    if listen_callback: listen_callback(message, **kwargs)
                    if cancel_callback and cancel_callback(message, **kwargs): 
                        break
                    yield message
            
        try:
            if patterns:
                self.pubsub.psubscribe(*patterns)
            yield from _listen()
        finally:
            if unsubscribe_after:
                self.pubsub.unsubscribe(*patterns)
            if close_after:
                self.pubsub.close()
    
    async def async_plisten(
        self,
        *patterns: str,
        timeout: typing.Optional[float] = None,
        # decode_responses: typing.Optional[bool] = True,
        unsubscribe_after: typing.Optional[bool] = False,
        close_after: typing.Optional[bool] = False,
        listen_callback: typing.Optional[typing.Callable] = None,
        cancel_callback: typing.Optional[typing.Callable] = None,
        **kwargs
    ) -> typing.AsyncIterator[typing.Any]:
        """
        [PubSub] Listens for messages
        """
        async def _listen():

            if timeout:
                try:
                    async with async_timeout(timeout):
                        async for message in self.async_pubsub.listen():
                            if listen_callback:
                                await listen_callback(message, **kwargs)
                            if cancel_callback and await cancel_callback(message, **kwargs):
                                break
                            yield message
                
                except (TimeoutError, asyncio.TimeoutError):
                    return


            else:
                async for message in self.async_pubsub.listen():
                    if listen_callback:
                        await listen_callback(message, **kwargs)
                    if cancel_callback and await cancel_callback(message, **kwargs):
                        break
                    yield message
            
            
        try:
            if patterns:
                await self.async_pubsub.psubscribe(*patterns)

            async for message in _listen():
                yield message
    
        finally:
            if unsubscribe_after:
                await self.async_pubsub.unsubscribe(*patterns)
            if close_after:
                await self.async_pubsub.close()


    def serialize(
        self, 
        value: typing.Any, 
        serializer: typing.Optional[typing.Union[typing.Callable, bool]] = None,
        **kwargs
    ):
        """
        Handles serialization
        """
        if serializer and callable(serializer): return serializer(value, **kwargs)
        if serializer is False: return value
        if serializer is True: return self.encoder.encode(value)
        if self.serializer:
            try:
                return self.serializer.dumps(value, **kwargs)
            except Exception:
                return self.encoder.encode(value)
        return value
    
    def deserialize(
        self, 
        value: typing.Any, 
        deserializer: typing.Optional[typing.Union[typing.Callable, bool]] = None,
        **kwargs
    ):
        """
        Handles deserialization
        """
        if deserializer and callable(deserializer): return deserializer(value, **kwargs)
        if deserializer is False: return value
        if deserializer is True: return self.encoder.decode(value)
        if self.serializer:
            try:
                return self.serializer.loads(value, **kwargs)
            except Exception:
                return self.encoder.decode(value)
        return value

    """
    Primary Functions
    """
    def set(
        self, 
        name: str, 
        value: typing.Any,
        ex: typing.Union[ExpiryT, None] = None,
        px: typing.Union[ExpiryT, None] = None,
        nx: bool = False,
        xx: bool = False,
        keepttl: bool = False,
        get: bool = False,
        exat: typing.Union[AbsExpiryT, None] = None,
        pxat: typing.Union[AbsExpiryT, None] = None,
        _serializer: typing.Optional[typing.Union[typing.Callable, bool]] = None,
        **kwargs
    ) -> typing.Any:
        """
        Serializes the Value using `serializer` and executes a set command
        """
        value = self.serialize(value, _serializer, **kwargs)
        return self.client.set(
            name = name,
            value = value,
            ex = ex,
            px = px,
            nx = nx,
            xx = xx,
            keepttl = keepttl,
            get = get,
            exat = exat,
            pxat = pxat,
            **kwargs
        )

    async def async_set(
        self, 
        name: str, 
        value: typing.Any,
        ex: typing.Union[ExpiryT, None] = None,
        px: typing.Union[ExpiryT, None] = None,
        nx: bool = False,
        xx: bool = False,
        keepttl: bool = False,
        get: bool = False,
        exat: typing.Union[AbsExpiryT, None] = None,
        pxat: typing.Union[AbsExpiryT, None] = None,
        _serializer: typing.Optional[typing.Union[typing.Callable, bool]] = None,
        **kwargs
    ) -> typing.Any:
        """
        Serializes the Value using `serializer` and executes a set command
        """
        value = self.serialize(value, _serializer, **kwargs)
        return await self.async_client.set(
            name = name,
            value = value,
            ex = ex,
            px = px,
            nx = nx,
            xx = xx,
            keepttl = keepttl,
            get = get,
            exat = exat,
            pxat = pxat,
            **kwargs
        )
    
    def get(
        self, 
        name: str, 
        default: typing.Any = None,
        _return_raw_value: typing.Optional[bool] = None,
        _serializer: typing.Optional[typing.Union[typing.Callable, bool]] = None,
        **kwargs
    ) -> typing.Any:
        """
        Serializes the Value using `serializer` and executes a set command

        - `default` is the value to return if the key is not found
        """
        val = self.client.get(name)
        if not val: return default
        if _return_raw_value: return val
        return self.deserialize(val, _serializer, **kwargs)

        # if _serializer: return _serializer(val)
        # if self.serializer:
        #     try:
        #         return self.serializer.loads(val, **kwargs)
        #     except Exception:
        #         return self.encoder.decode(val)
        # return val
    
    async def async_get(
        self, 
        name: str, 
        default: typing.Any = None, 
        _return_raw_value: typing.Optional[bool] = None,
        _serializer: typing.Optional[typing.Union[typing.Callable, bool]] = None,
        **kwargs
    ) -> typing.Any:
        """
        Serializes the Value using `serializer` and executes a set command

        - `default` is the value to return if the key is not found
        """
        val = await self.async_client.get(name)
        if not val: return default
        if _return_raw_value: return val
        return self.deserialize(val, _serializer, **kwargs)
        # if _serializer: return _serializer(val)
        # if self.serializer:
        #     try:
        #         return self.serializer.loads(val, **kwargs)
        #     except Exception:
        #         return self.encoder.decode(val)
        # return val
    
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
        key: typing.Optional[str] = None,
        value: typing.Optional[typing.Any] = None,
        mapping: typing.Optional[dict] = None,
        items: typing.Optional[list] = None,
        **kwargs
    ) -> bool:
        """
        Set the string value of a hash field
        """
        return self.client.hset(name, key = key, value = value, mapping = mapping, items = items, **kwargs)
    
    async def async_hset(
        self,
        name: str,
        key: typing.Optional[str] = None,
        value: typing.Optional[typing.Any] = None,
        mapping: typing.Optional[dict] = None,
        items: typing.Optional[list] = None,
        **kwargs
    ) -> bool:
        """
        Set the string value of a hash field
        """
        return await self.async_client.hset(name, key = key, value = value, mapping = mapping, items = items, **kwargs)
    
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
    
    def rpop(
        self,
        name: str,
        **kwargs
    ) -> typing.Any:
        """
        Remove and get the last element in a list
        """
        return self.client.rpop(name, **kwargs)
    
    async def async_rpop(
        self,
        name: str,
        **kwargs
    ) -> typing.Any:
        """
        Remove and get the last element in a list
        """
        return await self.async_client.rpop(name, **kwargs)

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
    
    def rpush(
        self,
        name: str,
        *values: typing.Any,
        **kwargs
    ) -> int:
        """
        Append one or multiple values to a list
        """
        return self.client.rpush(name, *values, **kwargs)
    
    async def async_rpush(
        self,
        name: str,
        *values: typing.Any,
        **kwargs
    ) -> int:
        """
        Append one or multiple values to a list
        """
        return await self.async_client.rpush(name, *values, **kwargs)
    
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
    
    def rpushx(
        self,
        name: str,
        value: typing.Any,
        **kwargs
    ) -> int:
        """
        Append a value to a list, only if the list exists
        """
        return self.client.rpushx(name, value, **kwargs)
    
    async def async_rpushx(
        self,
        name: str,
        value: typing.Any,
        **kwargs
    ) -> int:
        """
        Append a value to a list, only if the list exists
        """
        return await self.async_client.rpushx(name, value, **kwargs)

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
    
    def scan_iter(
        self,
        match: typing.Union[PatternT, None] = None,
        count: typing.Union[int, None] = None,
        _type: typing.Union[str, None] = None,
        **kwargs,
    ) -> typing.Iterator:
        """
        Make an iterator using the SCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` provides a hint to Redis about the number of keys to
            return per batch.

        ``_type`` filters the returned values by a particular Redis type.
            Stock Redis instances allow for the following types:
            HASH, LIST, SET, STREAM, STRING, ZSET
            Additionally, Redis modules can expose other types as well.
        """
        return self.client.scan_iter(match, count, _type, **kwargs)

    async def async_scan_iter(
        self,
        match: typing.Union[PatternT, None] = None,
        count: typing.Union[int, None] = None,
        _type: typing.Union[str, None] = None,
        **kwargs,
    ) -> typing.AsyncIterator:
        """
        Make an iterator using the SCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` provides a hint to Redis about the number of keys to
            return per batch.

        ``_type`` filters the returned values by a particular Redis type.
            Stock Redis instances allow for the following types:
            HASH, LIST, SET, STREAM, STRING, ZSET
            Additionally, Redis modules can expose other types as well.
        """
        async for key in self.async_client.scan_iter(match, count, _type, **kwargs):
            yield key
        

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
    
    def lmove(
        self,
        source_list: str,
        destination_list: str,
        source: typing.Optional[str] = 'LEFT',
        destination: typing.Optional[str] = 'RIGHT',
        **kwargs
    ) -> typing.Any:
        """
        Pop a value from a list, push it to another list and return it
        """
        return self.client.lmove(source_list, destination_list, source, destination, **kwargs)

    async def async_lmove(
        self,
        source_list: str,
        destination_list: str,
        source: typing.Optional[str] = 'LEFT',
        destination: typing.Optional[str] = 'RIGHT',
        **kwargs
    ) -> typing.Any:
        """
        Pop a value from a list, push it to another list and return it
        """
        return await self.async_client.lmove(source_list, destination_list, source, destination, **kwargs)


    def blmove(
        self,
        source_list: str,
        destination_list: str,
        source: typing.Optional[str] = 'LEFT',
        destination: typing.Optional[str] = 'RIGHT',
        timeout: int = 0,
        **kwargs
    ) -> typing.Any:
        """
        Pop a value from a list, push it to another list and return it; or block until one is available
        """
        return self.client.blmove(source_list, destination_list, timeout, source, destination, **kwargs)

    async def async_blmove(
        self,
        source_list: str,
        destination_list: str,
        source: typing.Optional[str] = 'LEFT',
        destination: typing.Optional[str] = 'RIGHT',
        timeout: int = 0,
        **kwargs
    ) -> typing.Any:
        """
        Pop a value from a list, push it to another list and return it; or block until one is available
        """
        return await self.async_client.blmove(source_list, destination_list, timeout, source, destination, **kwargs)

    def brpoplpush(
        self,
        source: str,
        destination: str,
        timeout: int = 0,
        **kwargs
    ) -> typing.Any:
        """
        Pop a value from a list, push it to another list and return it; or block until one is available
        """
        return self.client.brpoplpush(source, destination, timeout, **kwargs)
    
    async def async_brpoplpush(
        self,
        source: str,
        destination: str,
        timeout: int = 0,
        **kwargs
    ) -> typing.Any:
        """
        Pop a value from a list, push it to another list and return it; or block until one is available
        """
        return await self.async_client.brpoplpush(source, destination, timeout, **kwargs)
    
    def execute_command(
        self,
        *args,
        **kwargs
    ) -> typing.Any:
        """
        Execute a command
        """
        return self.client.execute_command(*args, **kwargs)
    
    async def async_execute_command(
        self,
        *args,
        **kwargs
    ) -> typing.Any:
        """
        Execute a command
        """
        return await self.async_client.execute_command(*args, **kwargs)

    def replicaof(
        self,
        host: str,
        port: typing.Optional[int] = 6379,
        **kwargs,
    ) -> bool:
        """
        Make the server a replica of another instance, or promote it as master
        """
        return self.client.replicaof(host, port, **kwargs)

    async def async_replicaof(
        self,
        host: str,
        port: typing.Optional[int] = 6379,
        **kwargs,
    ) -> bool:
        """
        Make the server a replica of another instance, or promote it as master
        """
        return await self.async_client.replicaof(host, port, **kwargs)
    
    """
    Set Commands
    """

    def sadd(
        self,
        name: str,
        *values: typing.Any,
        _serialize: typing.Optional[bool] = True,
        _serializer: typing.Optional[typing.Callable] = None,
        **kwargs
    ) -> int:
        """
        Add one or more members to a set
        """
        if _serialize: values = [self.serialize(value, _serializer, **kwargs) for value in values]
        return self.client.sadd(name, *values, **kwargs)
    
    async def async_sadd(
        self,
        name: str,
        *values: typing.Any,
        _serialize: typing.Optional[bool] = True,
        _serializer: typing.Optional[typing.Callable] = None,
        **kwargs
    ) -> int:
        """
        Add one or more members to a set
        """
        if _serialize: values = [self.serialize(value, _serializer, **kwargs) for value in values]
        return await self.async_client.sadd(name, *values, **kwargs)
    
    def scard(
        self,
        name: str,
        **kwargs
    ) -> int:
        """
        Get the number of members in a set
        """
        return self.client.scard(name, **kwargs)
    
    async def async_scard(
        self,
        name: str,
        **kwargs
    ) -> int:
        """
        Get the number of members in a set
        """
        return await self.async_client.scard(name, **kwargs)
    
    def sdiff(
        self,
        keys: typing.Union[str, typing.Iterable],
        *args: typing.Any,
        _return_raw_values: typing.Optional[bool] = None,
        _serializer: typing.Optional[typing.Callable] = None,
        **kwargs
    ) -> typing.Set:
        """
        Subtract multiple sets and return the resulting set
        """
        values = self.client.sdiff(keys, *args, **kwargs)
        if _return_raw_values: return values
        return {self.deserialize(value, _serializer, **kwargs) for value in values}
        
    
    async def async_sdiff(
        self,
        keys: typing.Union[str, typing.Iterable],
        *args: typing.Any,
        _return_raw_values: typing.Optional[bool] = None,
        _serializer: typing.Optional[typing.Callable] = None,
        **kwargs
    ) -> typing.Set:
        """
        Subtract multiple sets and return the resulting set
        """
        values = await self.async_client.sdiff(keys, *args, **kwargs)
        if _return_raw_values: return values
        return {self.deserialize(value, _serializer, **kwargs) for value in values}
    
    def sdiffstore(
        self,
        destination: str,
        keys: typing.Union[str, typing.Iterable],
        *args: typing.Any,
        **kwargs
    ) -> int:
        """
        Subtract multiple sets and store the resulting set in a key
        """
        return self.client.sdiffstore(destination, keys, *args, **kwargs)
    
    async def async_sdiffstore(
        self,
        destination: str,
        keys: typing.Union[str, typing.Iterable],
        *args: typing.Any,
        **kwargs
    ) -> int:
        """
        Subtract multiple sets and store the resulting set in a key
        """
        return await self.async_client.sdiffstore(destination, keys, *args, **kwargs)
    
    def sinter(
        self,
        keys: typing.Union[str, typing.Iterable],
        *args: typing.Any,
        _return_raw_values: typing.Optional[bool] = None,
        _serializer: typing.Optional[typing.Callable] = None,
        **kwargs
    ) -> typing.Set:
        """
        Intersect multiple sets
        """
        values = self.client.sinter(keys, *args, **kwargs)
        if _return_raw_values: return values
        return {self.deserialize(value, _serializer, **kwargs) for value in values}
    
    async def async_sinter(
        self,
        keys: typing.Union[str, typing.Iterable],
        *args: typing.Any,
        _return_raw_values: typing.Optional[bool] = None,
        _serializer: typing.Optional[typing.Callable] = None,
        **kwargs
    ) -> typing.Set:
        """
        Intersect multiple sets
        """
        values = await self.async_client.sinter(keys, *args, **kwargs)
        if _return_raw_values: return values
        return {self.deserialize(value, _serializer, **kwargs) for value in values}
    
    def sinterstore(
        self,
        destination: str,
        keys: typing.Union[str, typing.Iterable],
        *args: typing.Any,
        **kwargs
    ) -> int:
        """
        Intersect multiple sets and store the resulting set in a key
        """
        return self.client.sinterstore(destination, keys, *args, **kwargs)
    
    async def async_sinterstore(
        self,
        destination: str,
        keys: typing.Union[str, typing.Iterable],
        *args: typing.Any,
        **kwargs
    ) -> int:
        """
        Intersect multiple sets and store the resulting set in a key
        """
        return await self.async_client.sinterstore(destination, keys, *args, **kwargs)
    
    def sismember(
        self,
        name: str,
        value: typing.Any,
        _serialize: typing.Optional[bool] = True,
        _serializer: typing.Optional[typing.Callable] = None,
        **kwargs
    ) -> bool:
        """
        Determine if a given value is a member of a set
        """
        if _serialize: value = self.serialize(value, _serializer, **kwargs)
        return self.client.sismember(name, value, **kwargs)
    
    async def async_sismember(
        self,
        name: str,
        value: typing.Any,
        _serialize: typing.Optional[bool] = True,
        _serializer: typing.Optional[typing.Callable] = None,
        **kwargs
    ) -> bool:
        """
        Determine if a given value is a member of a set
        """
        if _serialize: value = self.serialize(value, _serializer, **kwargs)
        return await self.async_client.sismember(name, value, **kwargs)
    
    def smembers(
        self,
        name: str,
        _return_raw_values: typing.Optional[bool] = None,
        _serializer: typing.Optional[typing.Callable] = None,
        **kwargs
    ) -> typing.Set:
        """
        Get all the members in a set
        """
        values = self.client.smembers(name, **kwargs)
        if _return_raw_values: return values
        return {self.deserialize(value, _serializer, **kwargs) for value in values}
    
    async def async_smembers(
        self,
        name: str,
        _return_raw_values: typing.Optional[bool] = None,
        _serializer: typing.Optional[typing.Callable] = None,
        **kwargs
    ) -> typing.Set:
        """
        Get all the members in a set
        """
        values = await self.async_client.smembers(name, **kwargs)
        if _return_raw_values: return values
        return {self.deserialize(value, _serializer, **kwargs) for value in values}
    
    def smove(
        self,
        source: str,
        destination: str,
        value: typing.Any,
        _serialize: typing.Optional[bool] = True,
        _serializer: typing.Optional[typing.Callable] = None,
        **kwargs
    ) -> bool:
        """
        Move a member from one set to another
        """
        if _serialize: value = self.serialize(value, _serializer, **kwargs)
        return self.client.smove(source, destination, value, **kwargs)
    
    async def async_smove(
        self,
        source: str,
        destination: str,
        value: typing.Any,
        _serialize: typing.Optional[bool] = True,
        _serializer: typing.Optional[typing.Callable] = None,
        **kwargs
    ) -> bool:
        """
        Move a member from one set to another
        """
        if _serialize: value = self.serialize(value, _serializer, **kwargs)
        return await self.async_client.smove(source, destination, value, **kwargs)
    
    def spop(
        self,
        name: str,
        count: typing.Optional[int] = None,
        _return_raw_values: typing.Optional[bool] = None,
        _serializer: typing.Optional[typing.Callable] = None,
        **kwargs
    ) -> typing.Union[typing.Any, typing.List]:
        """
        Remove and return one or multiple random members from a set
        """
        values = self.client.spop(name, count, **kwargs)
        if _return_raw_values: return values
        if count: return [self.deserialize(value, _serializer, **kwargs) for value in values]
        return self.deserialize(values, _serializer, **kwargs)
    
    async def async_spop(
        self,
        name: str,
        count: typing.Optional[int] = None,
        _return_raw_values: typing.Optional[bool] = None,
        _serializer: typing.Optional[typing.Callable] = None,
        **kwargs
    ) -> typing.Union[typing.Any, typing.List]:
        """
        Remove and return one or multiple random members from a set
        """
        values = await self.async_client.spop(name, count, **kwargs)
        if _return_raw_values: return values
        if count: return [self.deserialize(value, _serializer, **kwargs) for value in values]
        return self.deserialize(values, _serializer, **kwargs)

    def srandmember(
        self,
        name: str,
        count: typing.Optional[int] = None,
        _return_raw_values: typing.Optional[bool] = None,
        _serializer: typing.Optional[typing.Callable] = None,
        **kwargs
    ) -> typing.Union[typing.Any, typing.List]:
        """
        Get one or multiple random members from a set
        """
        values = self.client.srandmember(name, count, **kwargs)
        if _return_raw_values: return values
        if count: return [self.deserialize(value, _serializer, **kwargs) for value in values]
        return self.deserialize(values, _serializer, **kwargs)
    
    async def async_srandmember(
        self,
        name: str,
        count: typing.Optional[int] = None,
        _return_raw_values: typing.Optional[bool] = None,
        _serializer: typing.Optional[typing.Callable] = None,
        **kwargs
    ) -> typing.Union[typing.Any, typing.List]:
        """
        Get one or multiple random members from a set
        """
        values = await self.async_client.srandmember(name, count, **kwargs)
        if _return_raw_values: return values
        if count: return [self.deserialize(value, _serializer, **kwargs) for value in values]
        return self.deserialize(values, _serializer, **kwargs)
    
    def srem(
        self,
        name: str,
        *values: typing.Any,
        _serialize: typing.Optional[bool] = True,
        _serializer: typing.Optional[typing.Callable] = None,
        **kwargs
    ) -> int:
        """
        Remove one or more members from a set
        """
        if _serialize: values = [self.serialize(value, _serializer, **kwargs) for value in values]
        return self.client.srem(name, *values, **kwargs)
    
    async def async_srem(
        self,
        name: str,
        *values: typing.Any,
        _serialize: typing.Optional[bool] = True,
        _serializer: typing.Optional[typing.Callable] = None,
        **kwargs
    ) -> int:
        """
        Remove one or more members from a set
        """
        if _serialize: values = [self.serialize(value, _serializer, **kwargs) for value in values]
        return await self.async_client.srem(name, *values, **kwargs)
    

    def sunion(
        self,
        *names: str,
        _return_raw_values: typing.Optional[bool] = None,
        _serializer: typing.Optional[typing.Callable] = None,
        **kwargs
    ) -> typing.Set:
        """
        Return the union of multiple sets
        """
        values = self.client.sunion(*names, **kwargs)
        if _return_raw_values: return values
        return {self.deserialize(value, _serializer, **kwargs) for value in values}

    async def async_sunion(
        self,
        *names: str,
        _return_raw_values: typing.Optional[bool] = None,
        _serializer: typing.Optional[typing.Callable] = None,
        **kwargs
    ) -> typing.Set:
        """
        Return the union of multiple sets
        """
        values = await self.async_client.sunion(*names, **kwargs)
        if _return_raw_values: return values
        return {self.deserialize(value, _serializer, **kwargs) for value in values}
    
    def sunionstore(
        self,
        destination: str,
        *names: str,
        **kwargs
    ) -> int:
        """
        Store the union of multiple sets in a key
        """
        return self.client.sunionstore(destination, *names, **kwargs)
    
    async def async_sunionstore(
        self,
        destination: str,
        *names: str,
        **kwargs
    ) -> int:
        """
        Store the union of multiple sets in a key
        """
        return await self.async_client.sunionstore(destination, *names, **kwargs)


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
        if self.state.active: return
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
                self.state.active = True
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
        if self.state.active: return
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
                self.state.active = True
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
    def cachify_v2(
        self,
        ttl: typing.Optional[int] = None,
        keybuilder: typing.Optional[typing.Callable] = None,
        name: typing.Optional[typing.Union[str, typing.Callable]] = None,
        typed: typing.Optional[bool] = True,
        
        exclude_keys: typing.Optional[typing.List[str]] = None,
        exclude_null: typing.Optional[bool] = True,
        exclude_exceptions: typing.Optional[typing.Union[bool, typing.List[Exception]]] = True,
        exclude_null_values_in_hash: typing.Optional[bool] = None,
        exclude_default_values_in_hash: typing.Optional[bool] = None,

        disabled: typing.Optional[typing.Union[bool, typing.Callable]] = None,
        invalidate_after: typing.Optional[typing.Union[int, typing.Callable]] = None,
        invalidate_if: typing.Optional[typing.Callable] = None,
        overwrite_if: typing.Optional[typing.Callable] = None,
        bypass_if: typing.Optional[typing.Callable] = None,

        timeout: typing.Optional[float] = 1.0,
        verbose: typing.Optional[bool] = False,
        super_verbose: typing.Optional[bool] = False,
        raise_exceptions: typing.Optional[bool] = True,

        encoder: typing.Optional[typing.Union[str, typing.Callable]] = None,
        decoder: typing.Optional[typing.Union[str, typing.Callable]] = None,

        hit_setter: typing.Optional[typing.Callable] = None,
        hit_getter: typing.Optional[typing.Callable] = None,
        **kwargs,
    ) -> typing.Callable[[FT], FT]:
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
        # Migrate the old TTL param to the new cache_ttl param
        if _ttl := kwargs.pop('cache_ttl', None):
            ttl = _ttl
        
        return cachify(
            ttl = ttl,
            keybuilder = keybuilder,
            name = name,
            typed = typed,
            exclude_keys = exclude_keys,
            exclude_null = exclude_null,
            exclude_exceptions = exclude_exceptions,
            exclude_null_values_in_hash = exclude_null_values_in_hash,
            exclude_default_values_in_hash = exclude_default_values_in_hash,
            disabled = disabled,
            invalidate_after = invalidate_after,
            invalidate_if = invalidate_if,
            overwrite_if = overwrite_if,
            bypass_if= bypass_if,
            timeout = timeout,
            verbose = verbose,
            super_verbose = super_verbose,
            raise_exceptions = raise_exceptions,
            encoder = encoder,
            decoder = decoder,
            hit_setter = hit_setter,
            hit_getter = hit_getter,
            session = self,
            **kwargs,
        )
    
    def create_cachify(
        self,
        ttl: typing.Optional[int] = None,
        keybuilder: typing.Optional[typing.Callable] = None,
        name: typing.Optional[typing.Union[str, typing.Callable]] = None,
        typed: typing.Optional[bool] = True,
        
        exclude_keys: typing.Optional[typing.List[str]] = None,
        exclude_null: typing.Optional[bool] = True,
        exclude_exceptions: typing.Optional[typing.Union[bool, typing.List[Exception]]] = True,
        exclude_null_values_in_hash: typing.Optional[bool] = None,
        exclude_default_values_in_hash: typing.Optional[bool] = None,

        disabled: typing.Optional[typing.Union[bool, typing.Callable]] = None,
        invalidate_after: typing.Optional[typing.Union[int, typing.Callable]] = None,
        invalidate_if: typing.Optional[typing.Callable] = None,
        overwrite_if: typing.Optional[typing.Callable] = None,
        bypass_if: typing.Optional[typing.Callable] = None,

        timeout: typing.Optional[float] = 1.0,
        verbose: typing.Optional[bool] = False,
        super_verbose: typing.Optional[bool] = False,
        raise_exceptions: typing.Optional[bool] = True,

        encoder: typing.Optional[typing.Union[str, typing.Callable]] = None,
        decoder: typing.Optional[typing.Union[str, typing.Callable]] = None,

        hit_setter: typing.Optional[typing.Callable] = None,
        hit_getter: typing.Optional[typing.Callable] = None,
        hset_enabled: typing.Optional[bool] = True,
        **kwargs,
    ) -> typing.Callable[[FT], FT]:
        """
        Creates a new `cachify` partial decorator with the given kwargs

        Args:
            ttl (typing.Optional[int], typing.Optional): The TTL for the cache. Defaults to None.
            keybuilder (typing.Optional[typing.Callable], typing.Optional): The keybuilder for the cache. Defaults to None.
            name (typing.Optional[Union[str, typing.Callable]], typing.Optional): The name for the cache. Defaults to None.
            typed (typing.Optional[bool], typing.Optional): Whether or not to include types in the cache key. Defaults to True.
            exclude_keys (typing.Optional[List[str]], typing.Optional): The keys to exclude from the cache key. Defaults to None.
            exclude_null (typing.Optional[bool], typing.Optional): Whether or not to exclude null values from the cache. Defaults to True.
            exclude_exceptions (typing.Optional[Union[bool, List[Exception]]], typing.Optional): Whether or not to exclude exceptions from the cache. Defaults to True.
            exclude_null_values_in_hash (typing.Optional[bool], typing.Optional): Whether or not to exclude null values from the cache hash. Defaults to None.
            exclude_default_values_in_hash (typing.Optional[bool], typing.Optional): Whether or not to exclude default values from the cache hash. Defaults to None.
            disabled (typing.Optional[Union[bool, typing.Callable]], typing.Optional): Whether or not the cache is disabled. Defaults to None.
            invalidate_after (typing.Optional[Union[int, typing.Callable]], typing.Optional): The number of hits after which the cache should be invalidated. Defaults to None.
            invalidate_if (typing.Optional[typing.Callable], typing.Optional): The function to determine whether or not the cache should be invalidated. Defaults to None.
            overwrite_if (typing.Optional[typing.Callable], typing.Optional): The function to determine whether or not the cache should be overwritten. Defaults to None.
            bypass_if (typing.Optional[typing.Callable], typing.Optional): The function to determine whether or not the cache should be bypassed. Defaults to None.
            timeout (typing.Optional[float], typing.Optional): The timeout for the cache. Defaults to 1.0.
            verbose (typing.Optional[bool], typing.Optional): Whether or not to log verbose messages. Defaults to False.
            super_verbose (typing.Optional[bool], typing.Optional): Whether or not to log super verbose messages. Defaults to False.
            raise_exceptions (typing.Optional[bool], typing.Optional): Whether or not to raise exceptions. Defaults to True.
            encoder (typing.Optional[Union[str, typing.Callable]], typing.Optional): The encoder for the cache. Defaults to None.
            decoder (typing.Optional[Union[str, typing.Callable]], typing.Optional): The decoder for the cache. Defaults to None.
            hit_setter (typing.Optional[typing.Callable], typing.Optional): The hit setter for the cache. Defaults to None.
            hit_getter (typing.Optional[typing.Callable], typing.Optional): The hit getter for the cache. Defaults to None.
            hset_enabled (typing.Optional[bool], typing.Optional): Whether or not to enable the hset cache. Defaults to True.
            
        """
        if _ttl := kwargs.pop('cache_ttl', None):
            ttl = _ttl
        
        return create_cachify(
            ttl = ttl,
            keybuilder = keybuilder,
            name = name,
            typed = typed,
            exclude_keys = exclude_keys,
            exclude_null = exclude_null,
            exclude_exceptions = exclude_exceptions,
            exclude_null_values_in_hash = exclude_null_values_in_hash,
            exclude_default_values_in_hash = exclude_default_values_in_hash,
            disabled = disabled,
            invalidate_after = invalidate_after,
            invalidate_if = invalidate_if,
            overwrite_if = overwrite_if,
            bypass_if= bypass_if,
            timeout = timeout,
            verbose = verbose,
            super_verbose = super_verbose,
            raise_exceptions = raise_exceptions,
            encoder = encoder,
            decoder = decoder,
            hit_setter = hit_setter,
            hit_getter = hit_getter,
            hset_enabled = hset_enabled,
            session = self,
            **kwargs,
        )



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
        invalidate_cache_key: typing.Optional[str] = None,
        _no_cache: typing.Optional[bool] = False,
        _no_cache_kwargs: typing.Optional[typing.List[str]] = None,
        _no_cache_validator: typing.Optional[typing.Callable] = None,
        _func_name: typing.Optional[str] = None,
        _validate_requests: typing.Optional[bool] = True,
        _exclude_request_headers: typing.Optional[typing.Union[typing.List[str], bool]] = True,
        _cache_invalidator: typing.Optional[typing.Union[bool, typing.Callable]] = None,
        _invalidate_after_n_hits: typing.Optional[int] = None,
        _cache_timeout: typing.Optional[float] = 5.0,
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
            wrapper_builder = build_cachify_async_func if iscoroutinefunction(func) else build_cachify_func
            return wrapper_builder(
                self,
                func,
                base,
                cache_ttl = cache_ttl,
                typed = typed,
                cache_prefix = cache_prefix,
                exclude = exclude,
                exclude_null = exclude_null,
                exclude_return_types = exclude_return_types,
                exclude_return_objs = exclude_return_objs,
                exclude_kwargs = exclude_kwargs,
                include_cache_hit = include_cache_hit,
                invalidate_cache_key = invalidate_cache_key,
                _no_cache = _no_cache,
                _no_cache_kwargs = _no_cache_kwargs,
                _no_cache_validator = _no_cache_validator,
                _validate_requests = _validate_requests,
                _exclude_request_headers = _exclude_request_headers,
                _cache_invalidator = _cache_invalidator,
                _invalidate_after_n_hits = _invalidate_after_n_hits,
                _cache_timeout = _cache_timeout,
                **kwargs
            )

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

    def __setitem__(self, key: KeyT, value: typing.Any) -> None:
        """
        Set the value at key ``name`` to ``value``
        """
        if self.dict_encoder is not False:
            value = self.dict_encoder(value)
        if self.state.dict_method == 'hash':
            self.client.hset(self.dict_hash_key, key, value)
        else:
            self.client.set(key, value)

    def __contains__(self, key: KeyT) -> bool:
        """
        Return a boolean indicating whether key ``name`` exists
        """
        if self.state.dict_method == 'hash':
            return self.client.hexists(self.dict_hash_key, key)
        else:
            return self.client.exists(key)

    def __getitem__(self, key: KeyT) -> typing.Any:
        if self.state.dict_method == 'hash':
            value = self.client.hget(self.dict_hash_key, key)
        else:
            value = self.client.get(key)
        if value is None: 
            key_value = f'{self.dict_hash_key}:{key}' if self.state.dict_method == 'hash' else key
            raise KeyError(key_value)
        if self.dict_decoder is not False:
            value = self.dict_decoder(value)
        return value
    
    def __delitem__(self, key: KeyT) -> None:
        """
        Delete one or more keys specified by ``names``
        """
        if self.state.dict_method  == 'hash':
            self.client.hdel(self.dict_hash_key, key)
        else:
            self.client.delete(key)
    
    def __repr__(self) -> str:
        return f'<{self.state._class__.__name__} {self.name}> {self.uri} @ {self.uri.db_id}'
    
    def __str__(self) -> str:
        return str(self.uri)
    
    def __len__(self) -> int:
        return self.dbsize()
    
    def __bool__(self) -> bool:
        return self.state.active

    async def _async_get_stats(self) -> typing.Dict[str, typing.Any]:
        stats = {
            'pool': getattr(self.client.connection_pool, '_stats', {}),
            'apool': getattr(self.async_client.connection_pool, '_stats', {}),
        }
        _info = await self.async_info()
        for key in {
            'maxclients',
            'connected_clients',
            'used_memory',
            'used_memory_human',
            'used_memory_peak_human',
            'used_memory_peak_perc',
            'maxmemory',
            'maxmemory_human',
            'maxmemory_policy',
            'total_system_memory',
            'total_system_memory_human',
            'instantaneous_input_kbps',
            'instantaneous_output_kbps',
        }:
            if key in _info: stats[key] = _info[key]

        stats['available_connections'] = stats['maxclients'] - stats['connected_clients']
        stats['max_connections_used'] = (stats['connected_clients'] / stats['maxclients']) * 100.0
        return stats
    
    def _get_stats(self) -> typing.Dict[str, typing.Any]:
        stats = {
            'pool': getattr(self.client.connection_pool, '_stats', {}),
            'apool': getattr(self.async_client.connection_pool, '_stats', {}),
        }
        _info = self.info()
        for key in {
            'maxclients',
            'connected_clients',
            'used_memory',
            'used_memory_human',
            'used_memory_peak_human',
            'used_memory_peak_perc',
            'maxmemory',
            'maxmemory_human',
            'maxmemory_policy',
            'total_system_memory',
            'total_system_memory_human',
            'instantaneous_input_kbps',
            'instantaneous_output_kbps',
        }:
            if key in _info: stats[key] = _info[key]

        stats['available_connections'] = stats['maxclients'] - stats['connected_clients']
        stats['max_connections_used'] = (stats['connected_clients'] / stats['maxclients']) * 100.0
        return stats
    
    def get_key_sizes(
        self, 
        match: typing.Union[PatternT, None] = None,
        count: typing.Union[int, None] = None,
        _type: typing.Union[str, None] = None,
        min_size: typing.Union[ByteSize, int, str, None] = None,
        max_size: typing.Union[ByteSize, int, str, None] = None,
        raise_error: typing.Optional[bool] = False,
        parse: typing.Optional[bool] = True,
        verbose: typing.Optional[bool] = True,
        **kwargs,
    ) -> typing.Iterator[typing.Tuple[str, typing.Union[ByteSize, int]]]:
        """
        Returns an iterator that yields a tuple of key name and size in bytes or a ByteSize object
        """
        if min_size is not None and not isinstance(min_size, ByteSize):
            min_size = ByteSize.validate(min_size)
        if max_size is not None and not isinstance(max_size, ByteSize):
            max_size = ByteSize.validate(max_size)

        for key in self.scan_iter(match=match, count=count, _type=_type, **kwargs):
            try:
                size = self.strlen(key)
                if parse: size = ByteSize.validate(size)
                if min_size is not None and size < min_size: continue
                if max_size is not None and size > max_size: continue
                yield key, size
            except Exception as e:
                if raise_error: raise e
                if verbose: logger.error(f'Error getting size of key {key}: {e}')
    
    async def async_get_key_sizes(
        self, 
        match: typing.Union[PatternT, None] = None,
        count: typing.Union[int, None] = None,
        _type: typing.Union[str, None] = None,
        min_size: typing.Union[ByteSize, int, str, None] = None,
        max_size: typing.Union[ByteSize, int, str, None] = None,
        raise_error: typing.Optional[bool] = False,
        parse: typing.Optional[bool] = True,
        verbose: typing.Optional[bool] = True,
        **kwargs,
    ) -> typing.AsyncIterator[typing.Tuple[bytes, typing.Union[ByteSize, int]]]:
        """
        Returns an iterator that yields a tuple of key name and size in bytes or a ByteSize object
        """
        
        if min_size is not None and not isinstance(min_size, ByteSize):
            min_size = ByteSize.validate(min_size)
        if max_size is not None and not isinstance(max_size, ByteSize):
            max_size = ByteSize.validate(max_size)
        
        async for key in self.async_scan_iter(match=match, count=count, _type=_type, **kwargs):
            try:
                size = await self.async_strlen(key)
                if parse: size = ByteSize.validate(size)
                if min_size is not None and size < min_size: continue
                if max_size is not None and size > max_size: continue
                yield key, size
            except Exception as e:
                if raise_error: raise e
                if verbose: logger.error(f'Error getting size of key {key}: {e}')
    

    """
    CLI Commands
    """

    def _cli(
        self, 
        args: typing.Union[str, typing.List[str]], 
        shell: bool = True, 
        raise_error: bool = True, 
        entrypoint: str = 'keydb-cli',
        **kwargs,
    ) -> str:
        """
        Runs a CLI command on the server
        """
        base_args = self.uri.connection_args.copy()
        if not isinstance(args, list):
            args = [args]
        base_args.extend(args)
        command = " ".join(base_args)
        if '-n' not in command:
            command = f'{command} -n {self.uri.db_id}'
        if entrypoint not in command:
            command = f'{entrypoint} {command}'
        import subprocess
        try:
            out = subprocess.check_output(command, shell=shell, **kwargs)
            if isinstance(out, bytes): out = out.decode('utf8')
            return out.strip()
        except Exception as e:
            if not raise_error: return ""
            raise e

    async def _async_cli(
        self,
        args: typing.Union[str, typing.List[str]],
        stdout = asyncio.subprocess.PIPE, 
        stderr = asyncio.subprocess.PIPE, 
        output_encoding: str = 'UTF-8', 
        output_errors: str = 'ignore',
        entrypoint: str = 'keydb-cli',
        **kwargs
    ) -> str:
        """
        Runs a CLI command on the server
        """
        base_args = self.uri.connection_args.copy()
        if not isinstance(args, list):
            args = [args]
        base_args.extend(args)
        command = " ".join(base_args)
        if '-n' not in command:
            command = f'{command} -n {self.uri.db_id}'
        if entrypoint not in command:
            command = f'{entrypoint} {command}'
        p = await asyncio.subprocess.create_subprocess_shell(command, stdout = stdout, stderr = stderr, **kwargs)
        stdout, _ = await p.communicate()
        return stdout.decode(encoding = output_encoding, errors = output_errors).strip()


def build_cachify_func(
    session: 'KeyDBSession',
    func: typing.Callable,
    base: str,
    cache_ttl: int = None, 
    typed: bool = False, 
    cache_prefix: str = None, 
    exclude: typing.List[str] = None,
    exclude_null: typing.Optional[bool] = False,
    exclude_return_types: typing.Optional[typing.List[type]] = None,
    exclude_return_objs: typing.Optional[typing.List[typing.Any]] = None,
    exclude_kwargs: typing.Optional[typing.List[str]] = None,
    include_cache_hit: typing.Optional[bool] = False,
    invalidate_cache_key: typing.Optional[str] = None,
    _no_cache: typing.Optional[bool] = False,
    _no_cache_kwargs: typing.Optional[typing.List[str]] = None,
    _no_cache_validator: typing.Optional[typing.Callable] = None,
    _validate_requests: typing.Optional[bool] = True,
    _exclude_request_headers: typing.Optional[typing.Union[typing.List[str], bool]] = True,
    _cache_invalidator: typing.Optional[typing.Union[bool, typing.Callable]] = None,
    _invalidate_after_n_hits: typing.Optional[int] = None,
    _cache_timeout: typing.Optional[float] = 5.0,
    **kwargs,
) -> typing.Callable:
    """
    Builds a cachify function
    """

    from lazyops.utils.helpers import fail_after

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Get the cache key to invalidate
        __invalidate_cache = kwargs.pop(invalidate_cache_key, False) if \
            invalidate_cache_key else False
        if __invalidate_cache is None: __invalidate_cache = False

        # If cache is disabled, return
        if session.cache_enabled is False:
            if include_cache_hit:
                return func(*args, **kwargs), False
            return func(*args, **kwargs)
        
        # if the max attempts for connection is reached, return the function
        if (session.state.cache_max_attempts and session.state.cache_max_attempts > 0) \
                and session.state.cache_failed_attempts >= session.state.cache_max_attempts:
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
                check_headers = {k.lower(): v for k, v in headers.items()}
                for key in {"cache-control", "x-cache-control", "x-no-cache"}:
                    if check_headers.get(key, "") in {"no-store", "no-cache", "true"}:
                        if include_cache_hit:
                            return func(*args, **kwargs), False
                        return func(*args, **kwargs)
        
            # Remove headers from keybuilder
            # This is dangerous. Dsiable for now.
            # if _exclude_request_headers:
            #     _nested_headers = None
            #     if keybuilder_kwargs.get("request", None) is not None and keybuilder_kwargs['request'].get('headers', None) is not None:
            #         headers = keybuilder_kwargs["request"].pop("headers", None)
            #         _nested_headers = True
            #     elif keybuilder_kwargs.get("headers", None) is not None:
            #         headers = keybuilder_kwargs.pop("headers", None)
            #         _nested_headers = False
                
            #     if _nested_headers is not None and isinstance(_exclude_request_headers, (list, tuple)):
            #         for key in _exclude_request_headers:
            #             _ = headers.pop(key, None)
            #             _ = headers.pop(key.lower(), None)
                
            #         if _nested_headers:
            #             keybuilder_kwargs["request"]["headers"] = headers
            #         else:
            #             keybuilder_kwargs["headers"] = headers
        
        if exclude_kwargs:
            for key in exclude_kwargs:
                _ = keybuilder_kwargs.pop(key, None)


        # Do the actual caching
        key = wrapper.__cache_key__(*args, **kwargs)
        
        # Handle invalidating the key
        _invalidate_key = __invalidate_cache
        if not _invalidate_key and _cache_invalidator:
            if isinstance(_cache_invalidator, bool):
                _invalidate_key = _cache_invalidator
            else:
                _invalidate_key = _cache_invalidator(*args, _cache_key = key, **kwargs)

        # Handle checking for num of hits
        if _invalidate_after_n_hits:
            _hits_key = f'{key}:hits'
            _num_hits = 0
            with contextlib.suppress(TimeoutError):
                with fail_after(_cache_timeout):
                    _num_hits = session.get(_hits_key)
                if _num_hits: _num_hits = int(_num_hits)
            if _num_hits and _num_hits > _invalidate_after_n_hits:
                _invalidate_key = True
                with contextlib.suppress(TimeoutError):
                    with fail_after(_cache_timeout):
                        session.delete(_hits_key)

        if _invalidate_key:
            if session.settings.debug_enabled:
                logger.info(f'[{session.name}] Invalidating cache key: {key}')
            try:
                with fail_after(_cache_timeout):
                    session.delete(key)
            except TimeoutError:
                logger.error(f'[{session.name}] Calling DELETE on KeyDB timed out. Cached function: {base}')
                session.state.cache_failed_attempts += 1
            except Exception as e:
                logger.error(f'[{session.name}] Calling DELETE on KeyDB failed. Cached function: {base}: {e}')
                session.state.cache_failed_attempts += 1
        
        try:
            with fail_after(_cache_timeout):
                result = session.get(key, default = ENOVAL)
        except TimeoutError:
            result = ENOVAL
            logger.error(f'[{session.name}] Calling GET on KeyDB timed out. Cached function: {base}')
            session.state.cache_failed_attempts += 1
        except Exception as e:
            result = ENOVAL
            logger.error(f'[{session.name}] Calling GET on KeyDB failed. Cached function: {base}: {e}')
            session.state.cache_failed_attempts += 1

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
                    with fail_after(_cache_timeout):
                        session.set(key, result, ex=cache_ttl)
                except TimeoutError:
                    logger.error(f'[{session.name}] Calling SET on KeyDB timed out. Cached function: {base}')
                    session.state.cache_failed_attempts += 1
                except Exception as e:
                    logger.error(f'[{session.name}] Calling SET on KeyDB failed. Cached function: {base}: {e}')
                    session.state.cache_failed_attempts += 1
        
        elif _invalidate_after_n_hits:
            with contextlib.suppress(TimeoutError):
                with fail_after(_cache_timeout):
                    session.incr(_hits_key, 1)
        
        return (result, is_cache_hit) if include_cache_hit else result

    def __cache_key__(*args, **kwargs):
        "Make key for cache given function arguments."
        return f'{cache_prefix}_{args_to_key(base, args, kwargs, typed, exclude)}'

    wrapper.__cache_key__ = __cache_key__
    return wrapper


def build_cachify_async_func(
    session: 'KeyDBSession',
    func: typing.Callable,
    base: str,
    cache_ttl: int = None, 
    typed: bool = False, 
    cache_prefix: str = None, 
    exclude: typing.List[str] = None,
    exclude_null: typing.Optional[bool] = False,
    exclude_return_types: typing.Optional[typing.List[type]] = None,
    exclude_return_objs: typing.Optional[typing.List[typing.Any]] = None,
    exclude_kwargs: typing.Optional[typing.List[str]] = None,
    include_cache_hit: typing.Optional[bool] = False,
    invalidate_cache_key: typing.Optional[str] = None,
    _no_cache: typing.Optional[bool] = False,
    _no_cache_kwargs: typing.Optional[typing.List[str]] = None,
    _no_cache_validator: typing.Optional[typing.Callable] = None,
    _validate_requests: typing.Optional[bool] = True,
    _exclude_request_headers: typing.Optional[typing.Union[typing.List[str], bool]] = True,
    _cache_invalidator: typing.Optional[typing.Union[bool, typing.Callable]] = None,
    _invalidate_after_n_hits: typing.Optional[int] = None,
    _cache_timeout: typing.Optional[float] = 5.0,
    **kwargs,
) -> typing.Callable:
    """
    Builds a cachify function
    """
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        "Wrapper for callable to cache arguments and return values."

        __invalidate_cache = kwargs.pop(invalidate_cache_key, False) if \
            invalidate_cache_key else False
        if __invalidate_cache is None: __invalidate_cache = False

        # If cache is disabled, return
        if session.cache_enabled is False:
            if include_cache_hit:
                return await func(*args, **kwargs), False
            return await func(*args, **kwargs)
        
        # if the max attempts for connection is reached, return the function
        if (session.state.cache_max_attempts and session.state.cache_max_attempts > 0) \
                and session.state.cache_failed_attempts >= session.state.cache_max_attempts:
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
                check_headers = {k.lower(): v for k, v in headers.items()}
                for key in {"cache-control", "x-cache-control", "x-no-cache"}:
                    if check_headers.get(key, "") in {"no-store", "no-cache", "true"}:
                        if include_cache_hit:
                            return await func(*args, **kwargs), False
                        return await func(*args, **kwargs)
            
            # Remove headers from keybuilder
            # This is dangerous. Disable for now.
            # if _exclude_request_headers:
            #     _nested_headers = None
            #     if keybuilder_kwargs.get("request", None) is not None:
            #         headers = getattr(keybuilder_kwargs["request"], "headers", None)
            #         if hasattr(keybuilder_kwargs["request"], "_headers"):
            #             del keybuilder_kwargs["request"]._headers
            #         elif hasattr(keybuilder_kwargs["request"], "headers"):
            #             with contextlib.suppress(Exception):
            #                 del keybuilder_kwargs["request"].headers

            #         # try:
            #         #     headers = keybuilder_kwargs["request"].pop("headers", None)
            #         #     # _nested_headers = True
            #         # except Exception as e:
            #         #     headers = getattr(keybuilder_kwargs["request"], "headers", None)
            #             # setattr(keybuilder_kwargs["request"], "headers", None)

            #         # and keybuilder_kwargs['request'].get('headers', None) is not None:
            #         # headers = keybuilder_kwargs["request"].pop("headers", None)
            #         # headers = keybuilder_kwargs["request"]
            #         _nested_headers = True
            #     elif keybuilder_kwargs.get("headers", None) is not None:
            #         headers = keybuilder_kwargs.pop("headers", None)
            #         _nested_headers = False
                
            #     if _nested_headers is not None and isinstance(_exclude_request_headers, (list, tuple)):
            #         for key in _exclude_request_headers:
            #             _ = headers.pop(key, None)
            #             _ = headers.pop(key.lower(), None)
            #         if _nested_headers:
            #             keybuilder_kwargs["request"] = {
            #                 "headers": headers,
            #                 "request": keybuilder_kwargs["request"]
            #             }
            #         else:
            #             keybuilder_kwargs["headers"] = headers
        
        if exclude_kwargs:
            for key in exclude_kwargs:
                _ = keybuilder_kwargs.pop(key, None)

        # Do the actual caching
        key = wrapper.__cache_key__(*args, **keybuilder_kwargs)

        # Handle invalidating the key
        _invalidate_key = __invalidate_cache
        if not _invalidate_key and _cache_invalidator:
            if isinstance(_cache_invalidator, bool):
                _invalidate_key = _cache_invalidator

            elif iscoroutinefunction(_cache_invalidator):
                _invalidate_key = await _cache_invalidator(*args, _cache_key = key, **kwargs)
            
            else:
                _invalidate_key = _cache_invalidator(*args, _cache_key = key, **kwargs)

        # Handle checking for num of hits
        if _invalidate_after_n_hits:
            _hits_key = f'{key}:hits'
            _num_hits = 0
            with contextlib.suppress(Exception):
                async with afail_after(_cache_timeout):
                # with anyio.fail_after(_cache_timeout):
                    _num_hits = await session.async_get(_hits_key)
                    if _num_hits: _num_hits = int(_num_hits)

            if _num_hits and _num_hits > _invalidate_after_n_hits:
                _invalidate_key = True
                # logger.info(f'[{self.name}] Invalidating cache key: {key} after {_num_hits} hits')
                with contextlib.suppress(Exception):
                    # with anyio.fail_after(_cache_timeout):
                    async with afail_after(_cache_timeout):
                        await session.async_delete(key)
                
        if _invalidate_key:
            if session.settings.debug_enabled:
                logger.info(f'[{session.name}] Invalidating cache key: {key}')
            with contextlib.suppress(Exception):
                # with anyio.fail_after(_cache_timeout):
                async with afail_after(_cache_timeout):
                    await session.async_delete(key)

        # result = ENOVAL
        try:
            # with anyio.fail_after(_cache_timeout):
            async with afail_after(_cache_timeout):
                result = await session.async_get(key, default = ENOVAL)
        
        except TimeoutError:
            result = ENOVAL
            logger.warning(f'[{session.name}] Calling GET on async KeyDB timed out. Cached function: {base}')
            session.state.cache_failed_attempts += 1
        
        except Exception as e:
            result = ENOVAL
            logger.warning(f'[{session.name}] Calling GET on async KeyDB failed. Cached function: {base}: {e}')
            session.state.cache_failed_attempts += 1

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
                    # with anyio.fail_after(_cache_timeout):
                    async with afail_after(_cache_timeout):
                        await session.async_set(key, result, ex=cache_ttl)
                except TimeoutError:
                    logger.error(f'[{session.name}] Calling SET on async KeyDB timed out. Cached function: {base}')
                    session.state.cache_failed_attempts += 1
                
                except Exception as e:
                    logger.error(f'[{session.name}] Calling SET on async KeyDB failed. Cached function: {base}: {e}')
                    session.state.cache_failed_attempts += 1

        
        elif _invalidate_after_n_hits:
            with contextlib.suppress(Exception):
                async with afail_after(_cache_timeout):
                # with anyio.fail_after(_cache_timeout):
                    await session.async_incr(_hits_key)
        
        return (result, is_cache_hit) if include_cache_hit else result
    
    def __cache_key__(*args, **kwargs):
        "Make key for cache given function arguments."
        return f'{cache_prefix}_{args_to_key(base, args, kwargs, typed, exclude)}'
    
    wrapper.__cache_key__ = __cache_key__
    return wrapper
