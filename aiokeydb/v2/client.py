from __future__ import annotations

"""
KeyDB Metaclass
"""
import anyio
import asyncio
import traceback
import typing
import logging
import contextlib
import functools
from pydantic.types import ByteSize
from aiokeydb.v2.lock import Lock, AsyncLock
from aiokeydb.v2.connection import (
    Encoder, 
    Connection,
    ConnectionPool, 
    BlockingConnectionPool, 
    AsyncConnection,
    AsyncConnectionPool,
    AsyncBlockingConnectionPool,
)
from aiokeydb.v2.core import (
    KeyDB, 
    PubSub, 
    Pipeline,
    AsyncKeyDB,
    AsyncPubSub,
    AsyncPipeline,
)

from aiokeydb.v2.typing import Number, KeyT, AbsExpiryT, ExpiryT, PatternT
from aiokeydb.v2.configs import KeyDBSettings, KeyDBWorkerSettings, settings as default_settings
from aiokeydb.v2.types import KeyDBUri
from aiokeydb.v2.types.session import KeyDBSession, ClientPools
from aiokeydb.v2.serializers import SerializerType, BaseSerializer


logger = logging.getLogger(__name__)

class KeyDBClientMeta(type):

    current: str = None
    sessions: typing.Dict[str, KeyDBSession] = {}

    pools: typing.Dict[str, ClientPools] = {}
    settings: KeyDBSettings = default_settings
    
    _ctx: typing.Type[KeyDBSession] = None
    _session: typing.Type[KeyDBSession] = None
    _encoder: typing.Type[Encoder] = None
    verbose: typing.Optional[bool] = True

    def get_settings(cls, **kwargs):
        return cls.settings
    
    @property
    def worker(cls) -> KeyDBWorkerSettings:
        return cls.settings.worker
    
    @property
    def ctx(cls) -> KeyDBSession:
        if not cls._ctx:
            cls.init_session()
        return cls._ctx 
    
    @property
    def encoder(cls) -> Encoder:
        if not cls._encoder:
            cls._encoder = Encoder(
                encoding = cls.settings.encoding,
                encoding_errors = cls.settings.encoding_errors,
                decode_responses = True,
            )
        return cls._encoder
    
    @property
    def is_leader_process(cls) -> bool:
        return cls.settings.is_leader_process if \
            cls.settings.is_leader_process is not None else True

    @property
    def has_session(cls) -> bool:
        return bool(cls.current) if cls.sessions else False
    
    @property
    def serializer(cls) -> typing.Type[BaseSerializer]:
        return cls.settings.get_serializer()

    def configure(
        cls,
        overwrite: typing.Optional[bool] = None,
        **kwargs,
    ):
        """
        Configures the global settings
        """
        cls.settings.configure(**kwargs)
        if overwrite is True: cls.init_session(overwrite = overwrite)
    
    def _set_ctx(
        cls,
        session: KeyDBSession,
        name: typing.Optional[str] = None,
        verbose: typing.Optional[bool] = True,
    ):
        """
        Sets the current session context
        """
        cls._ctx = session
        cls.current = name or session.name
        if cls.is_leader_process and verbose: logger.log(msg = f'Setting to Current Session: {cls.current}', level = cls.settings.loglevel)
    

    def _configure_pool(
        cls,
        name: str,
        uri: KeyDBUri,
        
        max_connections: int = None,
        # pool_class: typing.Type[ConnectionPool] = BlockingConnectionPool,
        pool_class: typing.Type[ConnectionPool] = ConnectionPool,
        connection_class: typing.Type[Connection] = Connection,
        connection_kwargs: typing.Dict[str, typing.Any] = None,

        amax_connections: int = None,
        # apool_class: typing.Type[AsyncConnectionPool] = AsyncBlockingConnectionPool,
        apool_class: typing.Type[AsyncConnectionPool] = AsyncConnectionPool,
        aconnection_class: typing.Type[AsyncConnection] = AsyncConnection,
        aconnection_kwargs: typing.Dict[str, typing.Any] = None,

        auto_pubsub: typing.Optional[bool] = True,
        pubsub_decode_responses: typing.Optional[bool] = True,
        decode_responses: typing.Optional[bool] = None,
        serializer: typing.Optional[typing.Any] = None,
        loop: asyncio.AbstractEventLoop = None,
        verbose: typing.Optional[bool] = None,

        **config,
    ) -> ClientPools:
        """
        Configures the pool for the given session
        """
        if uri.key in cls.pools and loop is None:
            return cls.pools[uri.key]

        verbose = verbose if verbose is not None else cls.verbose
        connection_kwargs = connection_kwargs or {}
        aconnection_kwargs = aconnection_kwargs or {}
        decode_responses = decode_responses if decode_responses is not None else \
            not bool(serializer or cls.serializer)

        # logger.log(msg = f"Configuring Pool for {name} w/ {uri.key} | decode = {decode_responses}", level = cls.settings.loglevel)
        if cls.is_leader_process and verbose: logger.log(msg = f"Configuring Pool for {name} w/ {uri.key}", level = cls.settings.loglevel)
        
        _pool = ClientPools(
            name = name,
            pool = pool_class.from_url(
                uri.connection,
                decode_responses = decode_responses,
                max_connections = max_connections,
                connection_class = connection_class,
                auto_pubsub = auto_pubsub,
                pubsub_decode_responses = pubsub_decode_responses,
                **cls.settings.get_retry_arg(False),
                **connection_kwargs,
                **config
            ),
            apool = apool_class.from_url(
                uri.connection,
                decode_responses = decode_responses,
                max_connections = amax_connections,
                connection_class = aconnection_class,
                auto_pubsub = auto_pubsub,
                pubsub_decode_responses = pubsub_decode_responses,
                **cls.settings.get_retry_arg(True),
                **aconnection_kwargs,
                **config
            ),
        )
        cls.pools[uri.key] = _pool
        return _pool


    def _configure_session(
        cls,
        name: str = 'default',
        uri: str = None,
        host: str = None,
        port: int = None,
        db_id: int = None,
        username: str = None,
        password: str = None,
        protocol: str = None,
        with_auth: bool = True,
        cache_enabled: typing.Optional[bool] = None,
        encoder: typing.Optional[typing.Any] = None,
        serializer: typing.Optional[typing.Any] = None,
        # decode_responses: typing.Optional[bool] = None,
        verbose: typing.Optional[bool] = None,
        loop: asyncio.AbstractEventLoop = None,
        **kwargs,
    ) -> KeyDBSession:
        """
        Configures a new session
        """

        verbose = verbose if verbose is not None else cls.verbose
        uri: KeyDBUri = cls.settings.create_uri(
            name = name,
            uri = uri,
            host = host,
            port = port,
            db_id = db_id,
            username = username,
            password = password,
            protocol = protocol,
            with_auth = with_auth,
        )
        db_id = db_id or uri.db_id
        db_id = db_id if db_id is not None else cls.settings.get_db_id(
            name = name, 
            db = db_id
        )
        config = cls.settings.get_config(**kwargs)
        config['db'] = db_id

        serializer = None if serializer is False else (serializer or cls.settings.get_serializer())
        pool = cls._configure_pool(
            name = name,
            uri = uri,
            # decode_responses = decode_responses,
            serializer = serializer,
            loop = loop,
            verbose = verbose,
            **config,
            # **kwargs,
        )
        return KeyDBSession(
            uri = uri,
            name = name, 
            client_pools = pool,
            # serializer = None if serializer is False else (serializer or cls.settings.get_serializer()),
            serializer = serializer,
            encoder = None if encoder is False else (encoder or cls.encoder),
            # serializer if serializer is not False else cls.serializer,
            # encoder = encoder if encoder is not False else cls.encoder,
            settings = cls.settings,
            cache_enabled = cache_enabled,
            # **kwargs,
            **config
        )

    
    def init_session(
        cls,
        name: str = 'default',
        uri: str = None,
        host: str = None,
        port: int = None,
        db_id: int = None,
        username: str = None,
        password: str = None,
        protocol: str = None,
        with_auth: bool = True,
        set_current: bool = False,
        cache_enabled: typing.Optional[bool] = None,
        overwrite: typing.Optional[bool] = None,
        verbose: typing.Optional[bool] = None,
        **config,
    ):
        
        verbose = verbose if verbose is not None else cls.verbose
        if name in cls.sessions and overwrite is not True:
            if verbose and name != 'default': logger.warning(f'Session {name} already exists')
            return cls.sessions[name]
        
        session = cls._configure_session(
            name = name,
            uri = uri,
            host = host,
            port = port,
            db_id = db_id,
            username = username,
            password = password,
            protocol = protocol,
            with_auth = with_auth,
            cache_enabled = cache_enabled,
            verbose = verbose,
            **config,
        )

        cls.sessions[name] = session
        if cls.is_leader_process and verbose: logger.log(msg = f'Initialized Session: {name} ({session.uri})', level = cls.settings.loglevel)
        if (set_current or overwrite) or cls._ctx is None:
            cls._set_ctx(session, name, verbose = verbose)
        return session

    def add_session(
        cls,
        session: KeyDBSession,
        overwrite: bool = False,
        set_current: bool = False,
        verbose: typing.Optional[bool] = None,
        # raise_errors: typing.Optional[bool] = True,
        **kwargs
    ):
        """
        Adds a session to the client.
        """

        verbose = verbose if verbose is not None else cls.verbose
        if not isinstance(session, KeyDBSession):
            raise TypeError('Session must be an instance of KeyDBSession')
        if session.name in cls.sessions and not overwrite:
            if verbose: logger.warning(f'Session {session.name} already exists')
            return
        cls.sessions[session.name] = session
        if cls.is_leader_process and verbose: logger.log(msg = f'Added Session: {session.name} ({session.uri})', level = cls.settings.loglevel)
        if set_current: cls._set_ctx(session, verbose = verbose)
    
    def create_session(
        cls,
        name: str,
        db_id: int = None,
        serializer: typing.Optional[typing.Any] = None,
        overwrite: bool = False,
        set_current: bool = False,
        cache_enabled: typing.Optional[bool] = None,
        _decode_responses: typing.Optional[bool] = None,
        verbose: typing.Optional[bool] = None,
        **kwargs,
    ):
        """
        Create a new Session instance
        - used in conjunction with aiokeydb.queues.TaskQueue
        - does not explicitly set the serializer.
        """

        verbose = verbose if verbose is not None else cls.verbose
        if name in cls.sessions and not overwrite:
            if cls.is_leader_process and verbose: logger.warning(f'Session {name} already exists')
            return cls.sessions[name]
        
        decode_responses = kwargs.pop('decode_responses', _decode_responses)
        session = cls._configure_session(
            name = name,
            db_id = db_id,
            cache_enabled = cache_enabled,
            serializer = serializer,
            decode_responses = decode_responses,
            verbose = verbose,
            **kwargs,
        )
        cls.sessions[name] = session
        if cls.is_leader_process and verbose: logger.log(msg = f'Created Session: {name} ({session.uri}) @ DB {db_id}', level = cls.settings.loglevel)
        if set_current: cls._set_ctx(session, name, verbose = verbose)
        return session
    
    def set_session(
        cls,
        name: str = None,
        verbose: typing.Optional[bool] = True,
        **kwargs,
    ):
        if name not in cls.sessions:
            return cls.init_session(name = name, set_current = True, verbose = verbose, **kwargs)
        cls._set_ctx(cls.sessions[name], name, verbose = verbose,)
    
    def get_session(
        cls,
        name: str = None,
        **kwargs,
    ) -> KeyDBSession:
        if not name: name = cls.current
        if not name: name = 'default'
        if name not in cls.sessions:
            cls.init_session(name = name, **kwargs)
        return cls.sessions[name]
        
    @property
    def session(cls) -> KeyDBSession:
        """
        returns the current `KeyDBSession`.
        """
        return cls.ctx
    
    @property
    def keydb(cls) -> KeyDB:
        """
        returns the underlying `KeyDB` client from within
        the current session.
        """
        return cls.ctx.client
    
    @property
    def async_keydb(cls) -> AsyncKeyDB:
        """
        returns the underlying `AsyncKeyDB` client from within
        the current session.
        """
        return cls.ctx.async_client
    

    @property
    def pubsub(cls) -> PubSub:
        """
        returns a `PubSub`.
        """
        return cls.ctx.pubsub
    
    @property
    def async_pubsub(cls) -> AsyncPubSub:
        """
        returns a `AsyncPubSub`.
        """
        return cls.ctx.async_pubsub
    
    @property
    def pipeline(cls) -> Pipeline:
        """
        returns a `Pipeline`.
        """
        return cls.ctx.pipeline
    
    @property
    def async_pipeline(cls) -> AsyncPipeline:
        """
        returns a `AsyncPipeline`.
        """
        return cls.ctx.async_pipeline
    
    @property
    def lock(cls) -> Lock:
        """
        returns a `Lock`.
        """
        return cls.ctx.lock
    
    @property
    def async_lock(cls) -> AsyncLock:
        """
        returns a `AsyncLock`.
        """
        return cls.ctx.async_lock
    
    def get_lock(
        cls, 
        name: str, 
        timeout: typing.Optional[Number] = None,
        sleep: Number = 0.1,
        blocking: bool = True,
        blocking_timeout: typing.Optional[Number] = None,
        thread_local: bool = True,
        _session: typing.Optional[str] = None, 
    ) -> Lock:
        """
        returns a `Lock` by name.
        """
        session = cls.get_session(_session)
        return session.get_lock(
            name = name,
            timeout = timeout,
            sleep = sleep,
            blocking = blocking,
            blocking_timeout = blocking_timeout,
            thread_local = thread_local,
        )
    
    def get_async_lock(
        cls, 
        name: str, 
        timeout: typing.Optional[Number] = None,
        sleep: Number = 0.1,
        blocking: bool = True,
        blocking_timeout: typing.Optional[Number] = None,
        thread_local: bool = True,
        _session: typing.Optional[str] = None, 
    ) -> AsyncLock:
        """
        returns a `Lock` by name.
        """
        session = cls.get_session(_session)
        return session.get_async_lock(
            name = name,
            timeout = timeout,
            sleep = sleep,
            blocking = blocking,
            blocking_timeout = blocking_timeout,
            thread_local = thread_local,
        )
    
    """
    PubSub
    """

    def publish(
        cls, 
        channel: str, 
        message: typing.Any, 
        _session: typing.Optional[str] = None, 
        **kwargs
    ):
        """
        [PubSub] Publishes a message to a channel
        """
        session = cls.get_session(_session)
        return session.publish(channel, message, **kwargs)

    async def async_publish(
        cls, 
        channel: str, 
        message: typing.Any, 
        _session: typing.Optional[str] = None, 
        **kwargs
    ):
        """
        [PubSub] Publishes a message to a channel
        """
        session = cls.get_session(_session)
        return await session.async_publish(channel, message, **kwargs)

    def subscribe(
        cls, 
        *channels: str, 
        _session: typing.Optional[str] = None, 
        **kwargs
    ):
        """
        [PubSub] Subscribes to a channel
        """
        session = cls.get_session(_session)
        return session.subscribe(*channels, **kwargs)
    
    async def async_subscribe(
        cls, 
        *channels: str, 
        _session: typing.Optional[str] = None, 
        **kwargs
    ):
        """
        [PubSub] Subscribes to a channel
        """
        session = cls.get_session(_session)
        return await session.async_subscribe(*channels, **kwargs)

    def unsubscribe(
        cls, 
        *channels: str, 
        _session: typing.Optional[str] = None, 
        **kwargs
    ):
        """
        [PubSub] Unsubscribes from a channel
        """
        session = cls.get_session(_session)
        return session.unsubscribe(*channels, **kwargs)

    async def async_unsubscribe(
        cls, 
        *channels: str, 
        _session: typing.Optional[str] = None, 
        **kwargs
    ):
        """
        [PubSub] Unsubscribes from a channel
        """
        session = cls.get_session(_session)
        return await session.async_unsubscribe(*channels, **kwargs)

    def psubscribe(
        cls, 
        *patterns: str, 
        _session: typing.Optional[str] = None,
        **kwargs
    ):
        """
        [PubSub] Subscribes to a pattern
        """
        session = cls.get_session(_session)
        return session.psubscribe(*patterns, **kwargs)

    async def async_psubscribe(
        cls, 
        *patterns: str, 
        _session: typing.Optional[str] = None,
        **kwargs
    ):
        """
        [PubSub] Subscribes to a pattern
        """
        session = cls.get_session(_session)
        return await session.async_psubscribe(*patterns, **kwargs)

    def punsubscribe(
        cls, 
        *patterns: str, 
        _session: typing.Optional[str] = None,
        **kwargs
    ):
        """
        [PubSub] Unsubscribes from a pattern
        """
        session = cls.get_session(_session)
        return session.punsubscribe(*patterns, **kwargs)
    
    async def async_punsubscribe(
        cls, 
        *patterns: str, 
        _session: typing.Optional[str] = None,
        **kwargs
    ):
        """
        [PubSub] Unsubscribes from a pattern
        """
        session = cls.get_session(_session)
        return await session.async_punsubscribe(*patterns, **kwargs)
    

    # @contextlib.contextmanager
    def plisten(
        cls, 
        *patterns: str,
        timeout: typing.Optional[float] = None,
        # decode_responses: typing.Optional[bool] = True,
        unsubscribe_after: typing.Optional[bool] = False,
        close_after: typing.Optional[bool] = False,
        listen_callback: typing.Optional[typing.Callable] = None,
        cancel_callback: typing.Optional[typing.Callable] = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.Iterator[typing.Any]:
        """
        [PubSub] Listens for messages
        """
        session = cls.get_session(_session)
        yield from session.plisten(
            *patterns, 
            timeout=timeout, 
            # decode_responses=decode_responses, 
            unsubscribe_after=unsubscribe_after, 
            close_after=close_after,
            listen_callback = listen_callback,
            cancel_callback = cancel_callback,
            **kwargs
        )
    
    # @contextlib.asynccontextmanager
    async def async_plisten(
        cls,
        *patterns: str,
        timeout: typing.Optional[float] = None,
        # decode_responses: typing.Optional[bool] = True,
        unsubscribe_after: typing.Optional[bool] = False,
        close_after: typing.Optional[bool] = False,
        listen_callback: typing.Optional[typing.Callable] = None,
        cancel_callback: typing.Optional[typing.Callable] = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.AsyncIterator[typing.Any]:
        """
        [PubSub] Listens for messages
        """
        session = cls.get_session(_session)
        async for message in session.async_plisten(
            *patterns,
            timeout=timeout,
            # decode_responses=decode_responses,
            unsubscribe_after=unsubscribe_after,
            close_after=close_after,
            listen_callback = listen_callback,
            cancel_callback = cancel_callback,
            **kwargs
        ):
            yield message


    """
    Primary Functions
    """
    
    def set(
        cls, 
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
        _serializer: typing.Optional[typing.Callable] = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.Any:
        """
        Serializes the Value using `serializer` and executes a set command
        """
        session = cls.get_session(_session)
        return session.set(
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
            _serializer = _serializer,
            **kwargs
        )
        
    
    async def async_set(
        cls, 
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
        _serializer: typing.Optional[typing.Callable] = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.Any:
        """
        Serializes the Value using `serializer` and executes a set command
        """
        session = cls.get_session(_session)
        return await session.async_set(
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
            _serializer = _serializer,
            **kwargs
        )
    
    
    def get(
        cls, 
        name: str, 
        default: typing.Any = None, 
        _return_raw_value: typing.Optional[bool] = None,
        _serializer: typing.Optional[typing.Callable] = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.Any:
        """
        Serializes the Value using `serializer` and executes a set command

        - `default` is the value to return if the key is not found
        """
        session = cls.get_session(_session)
        return session.get(
            name = name,
            default = default,
            _return_raw_value = _return_raw_value,
            _serializer = _serializer,
            **kwargs
        )
    
    
    async def async_get(
        cls, 
        name: str, 
        default: typing.Any = None, 
        _return_raw_value: typing.Optional[bool] = None,
        _serializer: typing.Optional[typing.Callable] = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.Any:
        """
        Serializes the Value using `serializer` and executes a set command

        - `default` is the value to return if the key is not found
        """
        session = cls.get_session(_session)
        return await session.async_get(
            name = name,
            default = default,
            _return_raw_value = _return_raw_value,
            _serializer = _serializer,
            **kwargs
        )
    
    
    def delete(
        cls, 
        names: typing.Union[typing.List[str], str],
        _session: typing.Optional[str] = None,
        **kwargs,
    ) -> typing.Any:
        """
        Delete one or more keys specified by names
        """
        session = cls.get_session(_session)
        return session.delete(
            names = names,
            **kwargs
        )

    
    async def async_delete(
        cls, 
        names: typing.Union[typing.List[str], str],
        _session: typing.Optional[str] = None,
        **kwargs,
    ) -> typing.Any:
        """
        Delete one or more keys specified by names
        """
        session = cls.get_session(_session)
        return await session.async_delete(
            names = names,
            **kwargs
        )

    
    def update(
        cls, 
        name: str, 
        data: typing.Dict[typing.Any, typing.Any], 
        overwrite: typing.Optional[bool] = False, 
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.Any:
        """
        Update the key: `name` with data

        equivilent to:
            key = 'mykey'; new_data = {'key2': 'value2'}
            x = KeyDBClient.get(key); -> x = {'key': 'value'}
            x.update(new_data); -> x.update({'key2': 'value2'})
            KeyDBClient.set(key, x); -> {'key': 'value', 'key2': 'value2'}
        """
        session = cls.get_session(_session)
        return session.update(
            name = name,
            data = data,
            overwrite = overwrite,
            **kwargs
        )
            

    
    async def async_update(
        cls, 
        name: str, 
        data: typing.Dict[typing.Any, typing.Any], 
        overwrite: typing.Optional[bool] = False, 
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.Any:
        """
        Update the key: `name` with data

        equivilent to:
            key = 'mykey'; new_data = {'key2': 'value2'}
            x = await KeyDBClient.async_get(key); -> x = {'key': 'value'}
            x.update(new_data); -> x.update({'key2': 'value2'})
            await KeyDBClient.async_set(key, x); -> {'key': 'value', 'key2': 'value2'}
        """
        session = cls.get_session(_session)
        return await session.async_update(
            name = name,
            data = data,
            overwrite = overwrite,
            **kwargs
        )
        

    
    def exists(
        cls, 
        keys: typing.Union[typing.List[KeyT], KeyT], 
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Check if a key exists
        """
        session = cls.get_session(_session)
        return session.exists(
            keys = keys,
            **kwargs
        )

    
    async def async_exists(
        cls, 
        keys: typing.Union[typing.List[KeyT], KeyT], 
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Check if a key exists
        """
        session = cls.get_session(_session)
        return await session.async_exists(
            keys = keys,
            **kwargs
        )
    

    
    def decr(
        cls,
        name: str,
        amount: typing.Optional[int] = 1,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Decrement the value of key `name` by `amount`
        """
        session = cls.get_session(_session)
        return session.decr(name, amount = amount, **kwargs)
    
    
    async def async_decr(
        cls,
        name: str,
        amount: typing.Optional[int] = 1,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Decrement the value of key `name` by `amount`
        """
        session = cls.get_session(_session)
        return await session.async_decr(name, amount = amount, **kwargs)
    
    
    def decrby(
        cls,
        name: str,
        amount: typing.Optional[int] = 1,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Decrement the value of key `name` by `amount`
        """
        session = cls.get_session(_session)
        return session.decrby(name, amount = amount, **kwargs)
    
    
    async def async_decrby(
        cls,
        name: str,
        amount: typing.Optional[int] = 1,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Decrement the value of key `name` by `amount`
        """
        session = cls.get_session(_session)
        return await session.async_decrby(name, amount = amount, **kwargs)
    
    
    def incr(
        cls,
        name: str,
        amount: typing.Optional[int] = 1,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Increment the value of key `name` by `amount`
        """
        session = cls.get_session(_session)
        return session.incr(name, amount = amount, **kwargs)
    
    
    async def async_incr(
        cls,
        name: str,
        amount: typing.Optional[int] = 1,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Increment the value of key `name` by `amount`
        """
        session = cls.get_session(_session)
        return await session.async_incr(name, amount = amount, **kwargs)
    
    
    def incrby(
        cls,
        name: str,
        amount: typing.Optional[int] = 1,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Increment the value of key `name` by `amount`
        """
        session = cls.get_session(_session)
        return session.incrby(name, amount = amount, **kwargs)
    
    
    async def async_incrby(
        cls,
        name: str,
        amount: typing.Optional[int] = 1,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Increment the value of key `name` by `amount`
        """
        session = cls.get_session(_session)
        return await session.async_incrby(name, amount = amount, **kwargs)
    
    
    def incrbyfloat(
        cls,
        name: str,
        amount: typing.Optional[float] = 1.0,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> float:
        """
        Increment the value of key `name` by `amount`
        """
        session = cls.get_session(_session)
        return session.incrbyfloat(name, amount = amount, **kwargs)
    
    
    async def async_incrbyfloat(
        cls,
        name: str,
        amount: typing.Optional[float] = 1.0,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> float:
        """
        Increment the value of key `name` by `amount`
        """
        session = cls.get_session(_session)
        return await session.async_incrbyfloat(name, amount = amount, **kwargs)
    
    
    def getbit(
        cls,
        name: str,
        offset: int,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Returns the bit value at offset in the string value stored at key
        """
        session = cls.get_session(_session)
        return session.getbit(name, offset, **kwargs)
    
    
    async def async_getbit(
        cls,
        name: str,
        offset: int,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Returns the bit value at offset in the string value stored at key
        """
        session = cls.get_session(_session)
        return await session.async_getbit(name, offset, **kwargs)
    
    
    def setbit(
        cls,
        name: str,
        offset: int,
        value: int,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Sets or clears the bit at offset in the string value stored at key
        """
        session = cls.get_session(_session)
        return session.setbit(name, offset, value, **kwargs)
    
    
    async def async_setbit(
        cls,
        name: str,
        offset: int,
        value: int,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Sets or clears the bit at offset in the string value stored at key
        """
        session = cls.get_session(_session)
        return await session.async_setbit(name, offset, value, **kwargs)
    
    
    def bitcount(
        cls,
        name: str,
        start: typing.Optional[int] = None,
        end: typing.Optional[int] = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Count the number of set bits (population counting) in a string
        """
        session = cls.get_session(_session)
        return session.bitcount(name, start = start, end = end, **kwargs)
    
    
    async def async_bitcount(
        cls,
        name: str,
        start: typing.Optional[int] = None,
        end: typing.Optional[int] = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Count the number of set bits (population counting) in a string
        """
        session = cls.get_session(_session)
        return await session.async_bitcount(name, start = start, end = end, **kwargs)
    
    
    def bitop(
        cls,
        operation: str,
        dest: str,
        *keys: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Perform bitwise operations between strings
        """
        session = cls.get_session(_session)
        return session.bitop(operation, dest, *keys, **kwargs)
    
    
    async def async_bitop(
        cls,
        operation: str,
        dest: str,
        *keys: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Perform bitwise operations between strings
        """
        session = cls.get_session(_session)
        return await session.async_bitop(operation, dest, *keys, **kwargs)
    
    
    def bitpos(
        cls,
        name: str,
        bit: int,
        start: typing.Optional[int] = None,
        end: typing.Optional[int] = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Find first bit set or clear in a string
        """
        session = cls.get_session(_session)
        return session.bitpos(name, bit, start = start, end = end, **kwargs)
    
    
    async def async_bitpos(
        cls,
        name: str,
        bit: int,
        start: typing.Optional[int] = None,
        end: typing.Optional[int] = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Find first bit set or clear in a string
        """
        session = cls.get_session(_session)
        return await session.async_bitpos(name, bit, start = start, end = end, **kwargs)
    
    
    def strlen(
        cls,
        name: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Get the length of the value stored in a key
        """
        session = cls.get_session(_session)
        return session.strlen(name, **kwargs)
    
    
    async def async_strlen(
        cls,
        name: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Get the length of the value stored in a key
        """
        session = cls.get_session(_session)
        return await session.async_strlen(name, **kwargs)
    
    
    def getrange(
        cls,
        name: str,
        start: int,
        end: int,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Get a substring of the string stored at a key
        """
        session = cls.get_session(_session)
        return session.getrange(name, start, end, **kwargs)
    
    
    async def async_getrange(
        cls,
        name: str,
        start: int,
        end: int,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Get a substring of the string stored at a key
        """
        session = cls.get_session(_session)
        return await session.async_getrange(name, start, end, **kwargs)
    
    
    def setrange(
        cls,
        name: str,
        offset: int,
        value: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Overwrite part of a string at key starting at the specified offset
        """
        session = cls.get_session(_session)
        return session.setrange(name, offset, value, **kwargs)
    
    
    async def async_setrange(
        cls,
        name: str,
        offset: int,
        value: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Overwrite part of a string at key starting at the specified offset
        """
        session = cls.get_session(_session)
        return await session.async_setrange(name, offset, value, **kwargs)
    
    
    def getset(
        cls,
        name: str,
        value: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Set the string value of a key and session = cls.get_session(_session)
        return its old value
        """
        session = cls.get_session(_session)
        return session.getset(name, value, **kwargs)

    
    async def async_getset(
        cls,
        name: str,
        value: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Set the string value of a key and session = cls.get_session(_session)
        return its old value
        """
        session = cls.get_session(_session)
        return await session.async_getset(name, value, **kwargs)
    
    
    def mget(
        cls,
        *names: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.List[typing.Optional[str]]:
        """
        Get the values of all the given keys
        """
        session = cls.get_session(_session)
        return session.mget(*names, **kwargs)
    
    
    async def async_mget(
        cls,
        *names: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.List[typing.Optional[str]]:
        """
        Get the values of all the given keys
        """
        session = cls.get_session(_session)
        return await session.async_mget(*names, **kwargs)
    
    
    def mset(
        cls,
        mapping: typing.Mapping[str, str],
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Set multiple keys to multiple values
        """
        session = cls.get_session(_session)
        return session.mset(mapping, **kwargs)
    
    
    async def async_mset(
        cls,
        mapping: typing.Mapping[str, str],
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Set multiple keys to multiple values
        """
        session = cls.get_session(_session)
        return await session.async_mset(mapping, **kwargs)
    
    
    def msetnx(
        cls,
        mapping: typing.Mapping[str, str],
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Set multiple keys to multiple values, only if none of the keys exist
        """
        session = cls.get_session(_session)
        return session.msetnx(mapping, **kwargs)
    
    
    async def async_msetnx(
        cls,
        mapping: typing.Mapping[str, str],
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Set multiple keys to multiple values, only if none of the keys exist
        """
        session = cls.get_session(_session)
        return await session.async_msetnx(mapping, **kwargs)
    
    
    def expire(
        cls,
        name: str,
        time: int,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Set a key's time to live in seconds
        """
        session = cls.get_session(_session)
        return session.expire(name, time, nx = nx, xx = xx, gt = gt, lt = lt, **kwargs)
    
    
    async def async_expire(
        cls,
        name: str,
        time: int,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Set a key's time to live in seconds
        """
        session = cls.get_session(_session)
        return await session.async_expire(name, time, nx = nx, xx = xx, gt = gt, lt = lt, **kwargs)
    
    
    def expireat(
        cls,
        name: str,
        when: int,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Set the expiration for a key as a UNIX timestamp
        """
        session = cls.get_session(_session)
        return session.expireat(name, when, nx = nx, xx = xx, gt = gt, lt = lt, **kwargs)
    
    
    async def async_expireat(
        cls,
        name: str,
        when: int,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Set the expiration for a key as a UNIX timestamp
        """
        session = cls.get_session(_session)
        return await session.async_expireat(name, when, nx = nx, xx = xx, gt = gt, lt = lt, **kwargs)
    
    
    def pexpire(
        cls,
        name: str,
        time: int,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Set a key's time to live in milliseconds
        """
        session = cls.get_session(_session)
        return session.pexpire(name, time, nx = nx, xx = xx, gt = gt, lt = lt, **kwargs)
    
    
    async def async_pexpire(
        cls,
        name: str,
        time: int,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Set a key's time to live in milliseconds
        """
        session = cls.get_session(_session)
        return await session.async_pexpire(name, time, nx = nx, xx = xx, gt = gt, lt = lt, **kwargs)
    
    
    def pexpireat(
        cls,
        name: str,
        when: int,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Set the expiration for a key as a UNIX timestamp specified in milliseconds
        """
        session = cls.get_session(_session)
        return session.pexpireat(name, when, nx = nx, xx = xx, gt = gt, lt = lt, **kwargs)
    
    
    async def async_pexpireat(
        cls,
        name: str,
        when: int,
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Set the expiration for a key as a UNIX timestamp specified in milliseconds
        """
        session = cls.get_session(_session)
        return await session.async_pexpireat(name, when, nx = nx, xx = xx, gt = gt, lt = lt, **kwargs)
    
    
    def ttl(
        cls,
        name: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Get the time to live for a key
        """
        session = cls.get_session(_session)
        return session.ttl(name, **kwargs)
    
    
    async def async_ttl(
        cls,
        name: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Get the time to live for a key
        """
        session = cls.get_session(_session)
        return await session.async_ttl(name, **kwargs)
    
    
    def pttl(
        cls,
        name: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Get the time to live for a key in milliseconds
        """
        session = cls.get_session(_session)
        return session.pttl(name, **kwargs)
    
    
    async def async_pttl(
        cls,
        name: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Get the time to live for a key in milliseconds
        """
        session = cls.get_session(_session)
        return await session.async_pttl(name, **kwargs)
    
    
    def persist(
        cls,
        name: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Remove the expiration from a key
        """
        session = cls.get_session(_session)
        return session.persist(name, **kwargs)
    
    
    async def async_persist(
        cls,
        name: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Remove the expiration from a key
        """
        session = cls.get_session(_session)
        return await session.async_persist(name, **kwargs)
    
    
    def psetex(
        cls,
        name: str,
        time_ms: int,
        value: typing.Any,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Set the value and expiration in milliseconds of a key
        """
        session = cls.get_session(_session)
        return session.psetex(name, time_ms, value, **kwargs)
    
    
    async def async_psetex(
        cls,
        name: str,
        time_ms: int,
        value: typing.Any,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Set the value and expiration in milliseconds of a key
        """
        session = cls.get_session(_session)
        return await session.async_psetex(name, time_ms, value, **kwargs)
    
    
    def setex(
        cls,
        name: str,
        time: int,
        value: typing.Any,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Set the value and expiration of a key
        """
        session = cls.get_session(_session)
        return session.setex(name, time, value, **kwargs)
    
    
    async def async_setex(
        cls,
        name: str,
        time: int,
        value: typing.Any,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Set the value and expiration of a key
        """
        session = cls.get_session(_session)
        return await session.async_setex(name, time, value, **kwargs)
    
    
    def getdel(
        cls,
        name: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.Any:
        """
        Get the value of a key and delete it
        """
        session = cls.get_session(_session)
        return session.getdel(name, **kwargs)
    
    
    async def async_getdel(
        cls,
        name: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.Any:
        """
        Get the value of a key and delete it
        """
        session = cls.get_session(_session)
        return await session.async_getdel(name, **kwargs)
    
    
    def hdel(
        cls,
        name: str,
        *keys: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Delete one or more hash fields
        """
        session = cls.get_session(_session)
        return session.hdel(name, *keys, **kwargs)
    
    
    async def async_hdel(
        cls,
        name: str,
        *keys: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Delete one or more hash fields
        """
        session = cls.get_session(_session)
        return await session.async_hdel(name, *keys, **kwargs)
    
    
    def hexists(
        cls,
        name: str,
        key: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Determine if a hash field exists
        """
        session = cls.get_session(_session)
        return session.hexists(name, key, **kwargs)
    
    
    async def async_hexists(
        cls,
        name: str,
        key: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Determine if a hash field exists
        """
        session = cls.get_session(_session)
        return await session.async_hexists(name, key, **kwargs)
    
    
    def hget(
        cls,
        name: str,
        key: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.Any:
        """
        Get the value of a hash field
        """
        session = cls.get_session(_session)
        return session.hget(name, key, **kwargs)
    
    
    async def async_hget(
        cls,
        name: str,
        key: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.Any:
        """
        Get the value of a hash field
        """
        session = cls.get_session(_session)
        return await session.async_hget(name, key, **kwargs)
    
    
    def hgetall(
        cls,
        name: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.Dict:
        """
        Get all the fields and values in a hash
        """
        session = cls.get_session(_session)
        return session.hgetall(name, **kwargs)
    
    
    async def async_hgetall(
        cls,
        name: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.Dict:
        """
        Get all the fields and values in a hash
        """
        session = cls.get_session(_session)
        return await session.async_hgetall(name, **kwargs)
    
    
    def hincrby(
        cls,
        name: str,
        key: str,
        amount: int = 1,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Increment the integer value of a hash field by the given number
        """
        session = cls.get_session(_session)
        return session.hincrby(name, key, amount, **kwargs)
    
    
    async def async_hincrby(
        cls,
        name: str,
        key: str,
        amount: int = 1,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Increment the integer value of a hash field by the given number
        """
        session = cls.get_session(_session)
        return await session.async_hincrby(name, key, amount, **kwargs)
    
    
    def hincrbyfloat(
        cls,
        name: str,
        key: str,
        amount: float = 1.0,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> float:
        """
        Increment the float value of a hash field by the given amount
        """
        session = cls.get_session(_session)
        return session.hincrbyfloat(name, key, amount, **kwargs)
    
    
    async def async_hincrbyfloat(
        cls,
        name: str,
        key: str,
        amount: float = 1.0,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> float:
        """
        Increment the float value of a hash field by the given amount
        """
        session = cls.get_session(_session)
        return await session.async_hincrbyfloat(name, key, amount, **kwargs)
    
    
    def hkeys(
        cls,
        name: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.List:
        """
        Get all the fields in a hash
        """
        session = cls.get_session(_session)
        return session.hkeys(name, **kwargs)
    
    
    async def async_hkeys(
        cls,
        name: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.List:
        """
        Get all the fields in a hash
        """
        session = cls.get_session(_session)
        return await session.async_hkeys(name, **kwargs)
    
    
    def hlen(
        cls,
        name: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Get the number of fields in a hash
        """
        session = cls.get_session(_session)
        return session.hlen(name, **kwargs)
    
    
    async def async_hlen(
        cls,
        name: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Get the number of fields in a hash
        """
        session = cls.get_session(_session)
        return await session.async_hlen(name, **kwargs)
    
    
    def hmget(
        cls,
        name: str,
        keys: typing.List,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.List:
        """
        Get the values of all the given hash fields
        """
        session = cls.get_session(_session)
        return session.hmget(name, keys, **kwargs)
    
    
    async def async_hmget(
        cls,
        name: str,
        keys: typing.List,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.List:
        """
        Get the values of all the given hash fields
        """
        session = cls.get_session(_session)
        return await session.async_hmget(name, keys, **kwargs)
    
    
    def hmset(
        cls,
        name: str,
        mapping: typing.Dict,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Set multiple hash fields to multiple values
        """
        session = cls.get_session(_session)
        return session.hmset(name, mapping, **kwargs)
    
    
    async def async_hmset(
        cls,
        name: str,
        mapping: typing.Dict,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Set multiple hash fields to multiple values
        """
        session = cls.get_session(_session)
        return await session.async_hmset(name, mapping, **kwargs)
    
    
    def hset(
        cls,
        name: str,
        key: str,
        value: typing.Any,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Set the string value of a hash field
        """
        session = cls.get_session(_session)
        return session.hset(name, key, value, **kwargs)
    
    
    async def async_hset(
        cls,
        name: str,
        key: str,
        value: typing.Any,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Set the string value of a hash field
        """
        session = cls.get_session(_session)
        return await session.async_hset(name, key, value, **kwargs)
    
    
    def hsetnx(
        cls,
        name: str,
        key: str,
        value: typing.Any,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Set the value of a hash field, only if the field does not exist
        """
        session = cls.get_session(_session)
        return session.hsetnx(name, key, value, **kwargs)
    
    
    async def async_hsetnx(
        cls,
        name: str,
        key: str,
        value: typing.Any,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Set the value of a hash field, only if the field does not exist
        """
        session = cls.get_session(_session)
        return await session.async_hsetnx(name, key, value, **kwargs)
    
    
    def hstrlen(
        cls,
        name: str,
        key: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Get the length of the value of a hash field
        """
        session = cls.get_session(_session)
        return session.hstrlen(name, key, **kwargs)
    
    
    async def async_hstrlen(
        cls,
        name: str,
        key: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Get the length of the value of a hash field
        """
        session = cls.get_session(_session)
        return await session.async_hstrlen(name, key, **kwargs)
    
    
    def hvals(
        cls,
        name: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.List:
        """
        Get all the values in a hash
        """
        session = cls.get_session(_session)
        return session.hvals(name, **kwargs)
    
    
    async def async_hvals(
        cls,
        name: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.List:
        """
        Get all the values in a hash
        """
        session = cls.get_session(_session)
        return await session.async_hvals(name, **kwargs)
    
    
    def lindex(
        cls,
        name: str,
        index: int,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.Any:
        """
        Get an element from a list by its index
        """
        session = cls.get_session(_session)
        return session.lindex(name, index, **kwargs)
    
    
    async def async_lindex(
        cls,
        name: str,
        index: int,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.Any:
        """
        Get an element from a list by its index
        """
        session = cls.get_session(_session)
        return await session.async_lindex(name, index, **kwargs)
    
    
    def linsert(
        cls,
        name: str,
        where: str,
        refvalue: typing.Any,
        value: typing.Any,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Insert an element before or after another element in a list
        """
    
        session = cls.get_session(_session)
        return session.linsert(name, where, refvalue, value, **kwargs)
    
    
    async def async_linsert(
        cls,
        name: str,
        where: str,
        refvalue: typing.Any,
        value: typing.Any,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Insert an element before or after another element in a list
        """
        session = cls.get_session(_session)
        return await session.async_linsert(name, where, refvalue, value, **kwargs)

    
    def llen(
        cls,
        name: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Get the length of a list
        """
        session = cls.get_session(_session)
        return session.llen(name, **kwargs)

    
    async def async_llen(
        cls,
        name: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Get the length of a list
        """
        session = cls.get_session(_session)
        return await session.async_llen(name, **kwargs)

    
    def lpop(
        cls,
        name: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.Any:
        """
        Remove and get the first element in a list
        """
        session = cls.get_session(_session)
        return session.lpop(name, **kwargs)
    
    
    async def async_lpop(
        cls,
        name: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.Any:
        """
        Remove and get the first element in a list
        """
        session = cls.get_session(_session)
        return await session.async_lpop(name, **kwargs)
    
    
    def rpop(
        cls,
        name: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.Any:
        """
        Remove and get the last element in a list
        """
        session = cls.get_session(_session)
        return session.rpop(name, **kwargs)

    
    async def async_rpop(
        cls,
        name: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.Any:
        """
        Remove and get the last element in a list
        """
        session = cls.get_session(_session)
        return await session.async_rpop(name, **kwargs)

    
    def lpush(
        cls,
        name: str,
        *values: typing.Any,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Prepend one or multiple values to a list
        """
        session = cls.get_session(_session)
        return session.lpush(name, *values, **kwargs)
    
    
    async def async_lpush(
        cls,
        name: str,
        *values: typing.Any,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Prepend one or multiple values to a list
        """
        session = cls.get_session(_session)
        return await session.async_lpush(name, *values, **kwargs)
    
    
    def rpush(
        cls,
        name: str,
        *values: typing.Any,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Append one or multiple values to a list
        """
        session = cls.get_session(_session)
        return session.rpush(name, *values, **kwargs)
    
    
    async def async_rpush(
        cls,
        name: str,
        *values: typing.Any,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Append one or multiple values to a list
        """
        session = cls.get_session(_session)
        return await session.async_rpush(name, *values, **kwargs)

    
    def lpushx(
        cls,
        name: str,
        value: typing.Any,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Prepend a value to a list, only if the list exists
        """
        session = cls.get_session(_session)
        return session.lpushx(name, value, **kwargs)
    
    
    async def async_lpushx(
        cls,
        name: str,
        value: typing.Any,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Prepend a value to a list, only if the list exists
        """
        session = cls.get_session(_session)
        return await session.async_lpushx(name, value, **kwargs)
    
    
    def rpushx(
        cls,
        name: str,
        value: typing.Any,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Append a value to a list, only if the list exists
        """
        session = cls.get_session(_session)
        return session.rpushx(name, value, **kwargs)
    
    
    async def async_rpushx(
        cls,
        name: str,
        value: typing.Any,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Append a value to a list, only if the list exists
        """
        session = cls.get_session(_session)
        return await session.async_rpushx(name, value, **kwargs)

    
    def lrange(
        cls,
        name: str,
        start: int,
        end: int,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.List:
        """
        Get a range of elements from a list
        """
        session = cls.get_session(_session)
        return session.lrange(name, start, end, **kwargs)
    
    
    async def async_lrange(
        cls,
        name: str,
        start: int,
        end: int,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.List:
        """
        Get a range of elements from a list
        """
        session = cls.get_session(_session)
        return await session.async_lrange(name, start, end, **kwargs)
    
    
    def lrem(
        cls,
        name: str,
        count: int,
        value: typing.Any,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Remove elements from a list
        """
        session = cls.get_session(_session)
        return session.lrem(name, count, value, **kwargs)
    
    
    async def async_lrem(
        cls,
        name: str,
        count: int,
        value: typing.Any,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Remove elements from a list
        """
        session = cls.get_session(_session)
        return await session.async_lrem(name, count, value, **kwargs)
    
    
    def lset(
        cls,
        name: str,
        index: int,
        value: typing.Any,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Set the value of an element in a list by its index
        """
        session = cls.get_session(_session)
        return session.lset(name, index, value, **kwargs)
    
    
    async def async_lset(
        cls,
        name: str,
        index: int,
        value: typing.Any,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Set the value of an element in a list by its index
        """
        session = cls.get_session(_session)
        return await session.async_lset(name, index, value, **kwargs)
    
    
    def ltrim(
        cls,
        name: str,
        start: int,
        end: int,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Trim a list to the specified range
        """
        session = cls.get_session(_session)
        return session.ltrim(name, start, end, **kwargs)
    
    
    async def async_ltrim(
        cls,
        name: str,
        start: int,
        end: int,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Trim a list to the specified range
        """
        session = cls.get_session(_session)
        return await session.async_ltrim(name, start, end, **kwargs)
    
    
    def zadd(
        cls,
        name: str,
        *args: typing.Any,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Add one or more members to a sorted set, or update its score if it already exists
        """
        session = cls.get_session(_session)
        return session.zadd(name, *args, **kwargs)
    
    
    async def async_zadd(
        cls,
        name: str,
        *args: typing.Any,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Add one or more members to a sorted set, or update its score if it already exists
        """
    
    
    def zcard(
        cls,
        name: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Get the number of members in a sorted set
        """
        session = cls.get_session(_session)
        return session.zcard(name, **kwargs)
    
    
    async def async_zcard(
        cls,
        name: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Get the number of members in a sorted set
        """
        session = cls.get_session(_session)
        return await session.async_zcard(name, **kwargs)
    
    
    def zcount(
        cls,
        name: str,
        min: typing.Any,
        max: typing.Any,
        _session: typing.Optional[str] = None,
        **kwargs

    ) -> int:
        """
        Count the members in a sorted set with scores within the given values
        """
        session = cls.get_session(_session)
        return session.zcount(name, min, max, **kwargs)
    
    
    async def async_zcount(
        cls,
        name: str,
        min: typing.Any,
        max: typing.Any,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Count the members in a sorted set with scores within the given values
        """
        session = cls.get_session(_session)
        return await session.async_zcount(name, min, max, **kwargs)
    
    
    def zincrby(
        cls,
        name: str,
        amount: float,
        value: typing.Any,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> float:
        """
        Increment the score of a member in a sorted set
        """
        session = cls.get_session(_session)
        return session.zincrby(name, amount, value, **kwargs)
    
    
    async def async_zincrby(
        cls,
        name: str,
        amount: float,
        value: typing.Any,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> float:
        """
        Increment the score of a member in a sorted set
        """
        session = cls.get_session(_session)
        return await session.async_zincrby(name, amount, value, **kwargs)
    
    
    def zinterstore(
        cls,
        dest: str,
        keys: typing.List[str],
        aggregate: str = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Intersect multiple sorted sets and store the resulting sorted set in a new key
        """
        session = cls.get_session(_session)
        return session.zinterstore(dest, keys, aggregate, **kwargs)
    
    
    async def async_zinterstore(
        cls,
        dest: str,
        keys: typing.List[str],
        aggregate: str = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Intersect multiple sorted sets and store the resulting sorted set in a new key
        """
        session = cls.get_session(_session)
        return await session.async_zinterstore(dest, keys, aggregate, **kwargs)
    
    
    def zlexcount(
        cls,
        name: str,
        min: typing.Any,
        max: typing.Any,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Count the number of members in a sorted set between a given lexicographical range
        """
        session = cls.get_session(_session)
        return session.zlexcount(name, min, max, **kwargs)
    
    
    async def async_zlexcount(
        cls,
        name: str,
        min: typing.Any,
        max: typing.Any,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Count the number of members in a sorted set between a given lexicographical range
        """
        session = cls.get_session(_session)
        return await session.async_zlexcount(name, min, max, **kwargs)
    
    
    def zpopmax(
        cls,
        name: str,
        count: int = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.List:
        """
        Remove and session = cls.get_session(_session)
        return members with the highest scores in a sorted set
        """
        session = cls.get_session(_session)
        return session.zpopmax(name, count, **kwargs)
    
    
    async def async_zpopmax(
        cls,
        name: str,
        count: int = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.List:
        """
        Remove and session = cls.get_session(_session)
        return members with the highest scores in a sorted set
        """
        session = cls.get_session(_session)
        return await session.async_zpopmax(name, count, **kwargs)
    
    
    def zpopmin(
        cls,
        name: str,
        count: int = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.List:
        """
        Remove and session = cls.get_session(_session)
        return members with the lowest scores in a sorted set
        """
        session = cls.get_session(_session)
        return session.zpopmin(name, count, **kwargs)
    
    
    async def async_zpopmin(
        cls,
        name: str,
        count: int = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.List:
        """
        Remove and session = cls.get_session(_session)
        return members with the lowest scores in a sorted set
        """
        session = cls.get_session(_session)
        return await session.async_zpopmin(name, count, **kwargs)
    
    
    def zrange(
        cls,
        name: str,
        start: int,
        stop: int,
        desc: bool = False,
        withscores: bool = False,
        score_cast_func: typing.Callable = float,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.List:
        """
        Return a range of members in a sorted set, by index
        """
        session = cls.get_session(_session)
        return session.zrange(name, start, stop, desc, withscores, score_cast_func, **kwargs)
    
    
    async def async_zrange(
        cls,
        name: str,
        start: int,
        stop: int,
        desc: bool = False,
        withscores: bool = False,
        score_cast_func: typing.Callable = float,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.List:
        """
        Return a range of members in a sorted set, by index
        """
        session = cls.get_session(_session)
        return await session.async_zrange(name, start, stop, desc, withscores, score_cast_func, **kwargs)
    
    
    def zrangebylex(
        cls,
        name: str,
        min: typing.Any,
        max: typing.Any,
        start: int = None,
        num: int = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.List:
        """
        Return a range of members in a sorted set, by lexicographical range
        """
        session = cls.get_session(_session)
        return session.zrangebylex(name, min, max, start, num, **kwargs)
    
    
    async def async_zrangebylex(
        cls,
        name: str,
        min: typing.Any,
        max: typing.Any,
        start: int = None,
        num: int = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.List:
        """
        Return a range of members in a sorted set, by lexicographical range
        """
        session = cls.get_session(_session)
        return await session.async_zrangebylex(name, min, max, start, num, **kwargs)
    
    
    def zrangebyscore(
        cls,
        name: str,
        min: typing.Any,
        max: typing.Any,
        start: int = None,
        num: int = None,
        withscores: bool = False,
        score_cast_func: typing.Callable = float,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.List:
        """
        Return a range of members in a sorted set, by score
        """
        session = cls.get_session(_session)
        return session.zrangebyscore(name, min, max, start, num, withscores, score_cast_func, **kwargs)
    
    
    async def async_zrangebyscore(
        cls,
        name: str,
        min: typing.Any,
        max: typing.Any,
        start: int = None,
        num: int = None,
        withscores: bool = False,
        score_cast_func: typing.Callable = float,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.List:
        """
        Return a range of members in a sorted set, by score
        """
        session = cls.get_session(_session)
        return await session.async_zrangebyscore(name, min, max, start, num, withscores, score_cast_func, **kwargs)
    
    
    def zrank(
        cls,
        name: str,
        value: typing.Any,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Determine the index of a member in a sorted set
        """
        session = cls.get_session(_session)
        return session.zrank(name, value, **kwargs)
    
    
    async def async_zrank(
        cls,
        name: str,
        value: typing.Any,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Determine the index of a member in a sorted set
        """
        session = cls.get_session(_session)
        return await session.async_zrank(name, value, **kwargs)
    
    
    def zrem(
        cls,
        name: str,
        *values: typing.Any,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Remove one or more members from a sorted set
        """
        session = cls.get_session(_session)
        return session.zrem(name, *values, **kwargs)
    
    
    async def async_zrem(
        cls,
        name: str,
        *values: typing.Any,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Remove one or more members from a sorted set
        """
        session = cls.get_session(_session)
        return await session.async_zrem(name, *values, **kwargs)
    
    
    def zremrangebylex(
        cls,
        name: str,
        min: typing.Any,
        max: typing.Any,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Remove all members in a sorted set between the given lexicographical range
        """
        session = cls.get_session(_session)
        return session.zremrangebylex(name, min, max, **kwargs)
    
    
    async def async_zremrangebylex(
        cls,
        name: str,
        min: typing.Any,
        max: typing.Any,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Remove all members in a sorted set between the given lexicographical range
        """
        session = cls.get_session(_session)
        return await session.async_zremrangebylex(name, min, max, **kwargs)
    
    
    def zremrangebyrank(
        cls,
        name: str,
        min: int,
        max: int,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Remove all members in a sorted set within the given indexes
        """
        session = cls.get_session(_session)
        return session.zremrangebyrank(name, min, max, **kwargs)
    
    
    async def async_zremrangebyrank(
        cls,
        name: str,
        min: int,
        max: int,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Remove all members in a sorted set within the given indexes
        """
        session = cls.get_session(_session)
        return await session.async_zremrangebyrank(name, min, max, **kwargs)
    
    
    def zremrangebyscore(
        cls,
        name: str,
        min: typing.Any,
        max: typing.Any,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Remove all members in a sorted set within the given scores
        """
        session = cls.get_session(_session)
        return session.zremrangebyscore(name, min, max, **kwargs)

    
    async def async_zremrangebyscore(
        cls,
        name: str,
        min: typing.Any,
        max: typing.Any,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Remove all members in a sorted set within the given scores
        """
        session = cls.get_session(_session)
        return await session.async_zremrangebyscore(name, min, max, **kwargs)
    
    
    def zrevrange(
        cls,
        name: str,
        start: int,
        num: int,
        withscores: bool = False,
        score_cast_func: typing.Callable = float,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.List:
        """
        Return a range of members in a sorted set, by index, with scores ordered from high to low
        """
        session = cls.get_session(_session)
        return session.zrevrange(name, start, num, withscores, score_cast_func, **kwargs)
    
    
    async def async_zrevrange(
        cls,
        name: str,
        start: int,
        num: int,
        withscores: bool = False,
        score_cast_func: typing.Callable = float,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.List:
        """
        Return a range of members in a sorted set, by index, with scores ordered from high to low
        """
        session = cls.get_session(_session)
        return await session.async_zrevrange(name, start, num, withscores, score_cast_func, **kwargs)
    
    
    def zrevrangebylex(
        cls,
        name: str,
        max: typing.Any,
        min: typing.Any,
        start: int = None,
        num: int = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.List:
        """
        Return a range of members in a sorted set, by lexicographical range, ordered from higher to lower strings.
        """
        session = cls.get_session(_session)
        return session.zrevrangebylex(name, max, min, start, num, **kwargs)
    
    
    async def async_zrevrangebylex(
        cls,
        name: str,
        max: typing.Any,
        min: typing.Any,
        start: int = None,
        num: int = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.List:
        """
        Return a range of members in a sorted set, by lexicographical range, ordered from higher to lower strings.
        """
        session = cls.get_session(_session)
        return await session.async_zrevrangebylex(name, max, min, start, num, **kwargs)
    
    
    def zrevrangebyscore(
        cls,
        name: str,
        max: typing.Any,
        min: typing.Any,
        start: int = None,
        num: int = None,
        withscores: bool = False,
        score_cast_func: typing.Callable = float,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.List:
        """
        Return a range of members in a sorted set, by score, with scores ordered from high to low
        """
        session = cls.get_session(_session)
        return session.zrevrangebyscore(name, max, min, start, num, withscores, score_cast_func, **kwargs)
    
    
    async def async_zrevrangebyscore(
        cls,
        name: str,
        max: typing.Any,
        min: typing.Any,
        start: int = None,
        num: int = None,
        withscores: bool = False,
        score_cast_func: typing.Callable = float,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.List:
        """
        Return a range of members in a sorted set, by score, with scores ordered from high to low
        """
        session = cls.get_session(_session)
        return await session.async_zrevrangebyscore(name, max, min, start, num, withscores, score_cast_func, **kwargs)
    
    
    def zrevrank(
        cls,
        name: str,
        value: typing.Any,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Determine the index of a member in a sorted set, with scores ordered from high to low
        """
        session = cls.get_session(_session)
        return session.zrevrank(name, value, **kwargs)
    
    
    async def async_zrevrank(
        cls,
        name: str,
        value: typing.Any,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Determine the index of a member in a sorted set, with scores ordered from high to low
        """
        session = cls.get_session(_session)
        return await session.async_zrevrank(name, value, **kwargs)
    
    
    def zscore(
        cls,
        name: str,
        value: typing.Any,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> float:
        """
        Get the score associated with the given member in a sorted set
        """
        session = cls.get_session(_session)
        return session.zscore(name, value, **kwargs)
    
    
    async def async_zscore(
        cls,
        name: str,
        value: typing.Any,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> float:
        """
        Get the score associated with the given member in a sorted set
        """
        session = cls.get_session(_session)
        return await session.async_zscore(name, value, **kwargs)
    
    
    def zunionstore(
        cls,
        dest: str,
        keys: typing.List[str],
        aggregate: str = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Add multiple sorted sets and store the resulting sorted set in a new key
        """
        session = cls.get_session(_session)
        return session.zunionstore(dest, keys, aggregate, **kwargs)
    
    
    async def async_zunionstore(
        cls,
        dest: str,
        keys: typing.List[str],
        aggregate: str = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Add multiple sorted sets and store the resulting sorted set in a new key
        """
        session = cls.get_session(_session)
        return await session.async_zunionstore(dest, keys, aggregate, **kwargs)
    
    
    def scan(
        cls,
        cursor: int = 0,
        match: str = None,
        count: int = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.Tuple[int, typing.List]:
        """
        Incrementally iterate the keys space
        """
        session = cls.get_session(_session)
        return session.scan(cursor, match, count, **kwargs)
    
    
    async def async_scan(
        cls,
        cursor: int = 0,
        match: str = None,
        count: int = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.Tuple[int, typing.List]:
        """
        Incrementally iterate the keys space
        """
        session = cls.get_session(_session)
        return await session.async_scan(cursor, match, count, **kwargs)
    
    
    def sscan(
        cls,
        name: str,
        cursor: int = 0,
        match: str = None,
        count: int = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.Tuple[int, typing.List]:
        """
        Incrementally iterate Set elements
        """
        session = cls.get_session(_session)
        return session.sscan(name, cursor, match, count, **kwargs)
    
    
    async def async_sscan(
        cls,
        name: str,
        cursor: int = 0,
        match: str = None,
        count: int = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.Tuple[int, typing.List]:
        """
        Incrementally iterate Set elements
        """
        session = cls.get_session(_session)
        return await session.async_sscan(name, cursor, match, count, **kwargs)
    
    
    def hscan(
        cls,
        name: str,
        cursor: int = 0,
        match: str = None,
        count: int = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.Tuple[int, typing.List]:
        """
        Incrementally iterate hash fields and associated values
        """
        session = cls.get_session(_session)
        return session.hscan(name, cursor, match, count, **kwargs)
    
    
    async def async_hscan(
        cls,
        name: str,
        cursor: int = 0,
        match: str = None,
        count: int = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.Tuple[int, typing.List]:
        """
        Incrementally iterate hash fields and associated values
        """
        session = cls.get_session(_session)
        return await session.async_hscan(name, cursor, match, count, **kwargs)
    
    
    def zscan(
        cls,
        name: str,
        cursor: int = 0,
        match: str = None,
        count: int = None,
        score_cast_func: typing.Callable = float,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.Tuple[int, typing.List]:
        """
        Incrementally iterate sorted sets elements and associated scores
        """
        session = cls.get_session(_session)
        return session.zscan(name, cursor, match, count, score_cast_func, **kwargs)
    
    
    async def async_zscan(
        cls,
        name: str,
        cursor: int = 0,
        match: str = None,
        count: int = None,
        score_cast_func: typing.Callable = float,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.Tuple[int, typing.List]:
        """
        Incrementally iterate sorted sets elements and associated scores
        """
        session = cls.get_session(_session)
        return await session.async_zscan(name, cursor, match, count, score_cast_func, **kwargs)
    
    
    def scan_iter(
        cls,
        match: typing.Union[PatternT, None] = None,
        count: typing.Union[int, None] = None,
        _type: typing.Union[str, None] = None,
        _session: typing.Optional[str] = None,
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
        session = cls.get_session(_session)
        return session.scan_iter(match, count, _type, **kwargs)

    async def async_scan_iter(
        cls,
        match: typing.Union[PatternT, None] = None,
        count: typing.Union[int, None] = None,
        _type: typing.Union[str, None] = None,
        _session: typing.Optional[str] = None,
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
        session = cls.get_session(_session)
        async for key in session.async_scan_iter(match, count, _type, **kwargs):
            yield key

    def zunion(
        cls,
        name: str,
        keys: typing.List[str],
        aggregate: str = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.List:
        """
        Add multiple sorted sets and store the resulting sorted set in a new key
        """
        session = cls.get_session(_session)
        return session.zunion(name, keys, aggregate, **kwargs)
    
    
    async def async_zunion(
        cls,
        name: str,
        keys: typing.List[str],
        aggregate: str = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.List:
        """
        Add multiple sorted sets and store the resulting sorted set in a new key
        """
        session = cls.get_session(_session)
        return await session.async_zunion(name, keys, aggregate, **kwargs)
    
    
    def zinter(
        cls,
        name: str,
        keys: typing.List[str],
        aggregate: str = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.List:
        """
        Intersect multiple sorted sets and store the resulting sorted set in a new key
        """
        session = cls.get_session(_session)
        return session.zinter(name, keys, aggregate, **kwargs)
    
    
    async def async_zinter(
        cls,
        name: str,
        keys: typing.List[str],
        aggregate: str = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.List:
        """
        Intersect multiple sorted sets and store the resulting sorted set in a new key
        """
        session = cls.get_session(_session)
        return await session.async_zinter(name, keys, aggregate, **kwargs)
    

    """
    Set Commands
    """

    def sadd(
        cls,
        name: str,
        *values: typing.Any,
        _serialize: typing.Optional[bool] = True,
        _serializer: typing.Optional[typing.Callable] = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Add one or more members to a set
        """
        session = cls.get_session(_session)
        return session.sadd(name, *values, _serialize = _serialize, _serializer = _serializer, **kwargs)
    
    async def async_sadd(
        cls,
        name: str,
        *values: typing.Any,
        _serialize: typing.Optional[bool] = True,
        _serializer: typing.Optional[typing.Callable] = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Add one or more members to a set
        """
        session = cls.get_session(_session)
        return await session.async_sadd(name, *values, _serialize = _serialize, _serializer = _serializer, **kwargs)
    
    def scard(
        cls,
        name: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Get the number of members in a set
        """
        session = cls.get_session(_session)
        return session.scard(name, **kwargs)
    
    async def async_scard(
        cls,
        name: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Get the number of members in a set
        """
        session = cls.get_session(_session)
        return await session.async_scard(name, **kwargs)
    
    def sdiff(
        cls,
        keys: typing.Union[str, typing.Iterable],
        *args: typing.Any,
        _return_raw_values: typing.Optional[bool] = None,
        _serializer: typing.Optional[typing.Callable] = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.Set:
        """
        Subtract multiple sets and return the resulting set
        """
        session = cls.get_session(_session)
        return session.sdiff(keys, *args, _return_raw_values = _return_raw_values, _serializer = _serializer, **kwargs)
        
    
    async def async_sdiff(
        cls,
        keys: typing.Union[str, typing.Iterable],
        *args: typing.Any,
        _return_raw_values: typing.Optional[bool] = None,
        _serializer: typing.Optional[typing.Callable] = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.Set:
        """
        Subtract multiple sets and return the resulting set
        """
        session = cls.get_session(_session)
        return await session.async_sdiff(keys, *args, _return_raw_values = _return_raw_values, _serializer = _serializer, **kwargs)
    
    def sdiffstore(
        cls,
        destination: str,
        keys: typing.Union[str, typing.Iterable],
        *args: typing.Any,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Subtract multiple sets and store the resulting set in a key
        """
        session = cls.get_session(_session)
        return session.sdiffstore(destination, keys, *args, **kwargs)
    
    async def async_sdiffstore(
        cls,
        destination: str,
        keys: typing.Union[str, typing.Iterable],
        *args: typing.Any,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Subtract multiple sets and store the resulting set in a key
        """
        session = cls.get_session(_session)
        return await session.async_sdiffstore(destination, keys, *args, **kwargs)
    
    def sinter(
        cls,
        keys: typing.Union[str, typing.Iterable],
        *args: typing.Any,
        _return_raw_values: typing.Optional[bool] = None,
        _serializer: typing.Optional[typing.Callable] = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.Set:
        """
        Intersect multiple sets
        """
        session = cls.get_session(_session)
        return session.sinter(keys, *args, _return_raw_values = _return_raw_values, _serializer = _serializer, **kwargs)

    
    async def async_sinter(
        cls,
        keys: typing.Union[str, typing.Iterable],
        *args: typing.Any,
        _return_raw_values: typing.Optional[bool] = None,
        _serializer: typing.Optional[typing.Callable] = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.Set:
        """
        Intersect multiple sets
        """
        session = cls.get_session(_session)
        return await session.async_sinter(keys, *args, _return_raw_values = _return_raw_values, _serializer = _serializer, **kwargs)
    
    def sinterstore(
        cls,
        destination: str,
        keys: typing.Union[str, typing.Iterable],
        *args: typing.Any,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Intersect multiple sets and store the resulting set in a key
        """
        session = cls.get_session(_session)
        return session.sinterstore(destination, keys, *args, **kwargs)
    
    async def async_sinterstore(
        cls,
        destination: str,
        keys: typing.Union[str, typing.Iterable],
        *args: typing.Any,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Intersect multiple sets and store the resulting set in a key
        """
        session = cls.get_session(_session)
        return await session.async_sinterstore(destination, keys, *args, **kwargs)
    
    def sismember(
        cls,
        name: str,
        value: typing.Any,
        _serialize: typing.Optional[bool] = True,
        _serializer: typing.Optional[typing.Callable] = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Determine if a given value is a member of a set
        """
        session = cls.get_session(_session)
        return session.sismember(name, value, _serialize = _serialize, _serializer = _serializer, **kwargs)
    
    async def async_sismember(
        cls,
        name: str,
        value: typing.Any,
        _serialize: typing.Optional[bool] = True,
        _serializer: typing.Optional[typing.Callable] = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Determine if a given value is a member of a set
        """
        session = cls.get_session(_session)
        return await session.async_sismember(name, value, _serialize = _serialize, _serializer = _serializer, **kwargs)
    
    def smembers(
        cls,
        name: str,
        _return_raw_values: typing.Optional[bool] = None,
        _serializer: typing.Optional[typing.Callable] = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.Set:
        """
        Get all the members in a set
        """
        session = cls.get_session(_session)
        return session.smembers(name, _return_raw_values = _return_raw_values, _serializer = _serializer,  **kwargs)
    
    async def async_smembers(
        cls,
        name: str,
        _return_raw_values: typing.Optional[bool] = None,
        _serializer: typing.Optional[typing.Callable] = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.Set:
        """
        Get all the members in a set
        """
        session = cls.get_session(_session)
        return await session.async_smembers(name, _return_raw_values = _return_raw_values, _serializer = _serializer,  **kwargs)
    
    def smove(
        cls,
        source: str,
        destination: str,
        value: typing.Any,
        _serialize: typing.Optional[bool] = True,
        _serializer: typing.Optional[typing.Callable] = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Move a member from one set to another
        """
        session = cls.get_session(_session)
        return session.smove(source, destination, value, _serialize = _serialize, _serializer = _serializer, **kwargs)
    
    async def async_smove(
        cls,
        source: str,
        destination: str,
        value: typing.Any,
        _serialize: typing.Optional[bool] = True,
        _serializer: typing.Optional[typing.Callable] = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Move a member from one set to another
        """
        session = cls.get_session(_session)
        return await session.async_smove(source, destination, value, _serialize = _serialize, _serializer = _serializer, **kwargs)
    
    def spop(
        cls,
        name: str,
        count: typing.Optional[int] = None,
        _return_raw_values: typing.Optional[bool] = None,
        _serializer: typing.Optional[typing.Callable] = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.Union[typing.Any, typing.List]:
        """
        Remove and return one or multiple random members from a set
        """
        session = cls.get_session(_session)
        return session.spop(name, count, _return_raw_values = _return_raw_values, _serializer = _serializer, **kwargs)
    
    async def async_spop(
        cls,
        name: str,
        count: typing.Optional[int] = None,
        _return_raw_values: typing.Optional[bool] = None,
        _serializer: typing.Optional[typing.Callable] = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.Union[typing.Any, typing.List]:
        """
        Remove and return one or multiple random members from a set
        """
        session = cls.get_session(_session)
        return await session.async_spop(name, count, _return_raw_values = _return_raw_values, _serializer = _serializer, **kwargs)

    def srandmember(
        cls,
        name: str,
        count: typing.Optional[int] = None,
        _return_raw_values: typing.Optional[bool] = None,
        _serializer: typing.Optional[typing.Callable] = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.Union[typing.Any, typing.List]:
        """
        Get one or multiple random members from a set
        """
        session = cls.get_session(_session)
        return session.srandmember(name, count, _return_raw_values = _return_raw_values, _serializer = _serializer, **kwargs)
    
    async def async_srandmember(
        cls,
        name: str,
        count: typing.Optional[int] = None,
        _return_raw_values: typing.Optional[bool] = None,
        _serializer: typing.Optional[typing.Callable] = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.Union[typing.Any, typing.List]:
        """
        Get one or multiple random members from a set
        """
        session = cls.get_session(_session)
        return await session.async_srandmember(name, count, _return_raw_values = _return_raw_values, _serializer = _serializer, **kwargs)
    
    def srem(
        cls,
        name: str,
        *values: typing.Any,
        _serialize: typing.Optional[bool] = True,
        _serializer: typing.Optional[typing.Callable] = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Remove one or more members from a set
        """
        session = cls.get_session(_session)
        return session.srem(name, *values, _serialize = _serialize, _serializer = _serializer, **kwargs)
    
    async def async_srem(
        cls,
        name: str,
        *values: typing.Any,
        _serialize: typing.Optional[bool] = True,
        _serializer: typing.Optional[typing.Callable] = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Remove one or more members from a set
        """
        session = cls.get_session(_session)
        return await session.async_srem(name, *values, _serialize = _serialize, _serializer = _serializer, **kwargs)
    

    def sunion(
        cls,
        *names: str,
        _return_raw_values: typing.Optional[bool] = None,
        _serializer: typing.Optional[typing.Callable] = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.Set:
        """
        Return the union of multiple sets
        """
        session = cls.get_session(_session)
        return session.sunion(*names, _return_raw_values = _return_raw_values, _serializer = _serializer, **kwargs)

    async def async_sunion(
        cls,
        *names: str,
        _return_raw_values: typing.Optional[bool] = None,
        _serializer: typing.Optional[typing.Callable] = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.Set:
        """
        Return the union of multiple sets
        """
        session = cls.get_session(_session)
        return await session.async_sunion(*names, _return_raw_values = _return_raw_values, _serializer = _serializer,  **kwargs)
    
    def sunionstore(
        cls,
        destination: str,
        *names: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Store the union of multiple sets in a key
        """
        session = cls.get_session(_session)
        return session.sunionstore(destination, *names, **kwargs)
    
    async def async_sunionstore(
        cls,
        destination: str,
        *names: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> int:
        """
        Store the union of multiple sets in a key
        """
        session = cls.get_session(_session)
        return await session.async_sunionstore(destination, *names, **kwargs)




    """
    Other utilities
    """
    
    
    def command(cls, _session: typing.Optional[str] = None, **kwargs):
        """
        Run a command
        """
        session = cls.get_session(_session)
        return session.command(**kwargs)
    
    
    async def async_command(cls, _session: typing.Optional[str] = None, **kwargs):
        """
        Run a command
        """
        session = cls.get_session(_session)
        return await session.async_command(**kwargs)
    
    def replicaof(
        cls,
        host: str,
        port: typing.Optional[int] = 6379,
        _session: typing.Optional[str] = None,
        **kwargs,
    ) -> bool:
        """
        Make the server a replica of another instance, or promote it as master
        """
        session = cls.get_session(_session)
        return session.replicaof(host, port, **kwargs)

    async def async_replicaof(
        cls,
        host: str,
        port: typing.Optional[int] = 6379,
        _session: typing.Optional[str] = None,
        **kwargs,
    ) -> bool:
        """
        Make the server a replica of another instance, or promote it as master
        """
        session = cls.get_session(_session)
        return await session.async_replicaof(host, port, **kwargs)
    
    
    
    def transaction(
        cls,
        func: Pipeline,
        *watches: typing.Any,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.List:
        """
        Run a transaction
        """
        session = cls.get_session(_session)
        return session.transaction(func, *watches, **kwargs)
    
    
    async def async_transaction(
        cls,
        func: AsyncPipeline,
        *watches: typing.Any,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.List:
        """
        Run a transaction
        """
        session = cls.get_session(_session)
        return await session.async_transaction(func, *watches, **kwargs)
    

    
    def config_get(
        cls,
        pattern: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.Dict:
        """
        Get the value of a configuration parameter
        """
        session = cls.get_session(_session)
        return session.config_get(pattern, **kwargs)

    
    async def async_config_get(
        cls,
        pattern: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.Dict:
        """
        Get the value of a configuration parameter
        """
        session = cls.get_session(_session)
        return await session.async_config_get(pattern, **kwargs)

    
    def config_set(
        cls,
        name: str,
        value: typing.Any,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Set a configuration parameter to the given value
        """
        session = cls.get_session(_session)
        return session.config_set(name, value, **kwargs)

    
    async def async_config_set(
        cls,
        name: str,
        value: typing.Any,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Set a configuration parameter to the given value
        """
        session = cls.get_session(_session)
        return await session.async_config_set(name, value, **kwargs)

    
    def config_resetstat(cls, _session: typing.Optional[str] = None, **kwargs):
        """
        Reset the stats session = cls.get_session(_session)
        returned by INFO
        """
        session = cls.get_session(_session)
        return session.config_resetstat(**kwargs)

    
    async def async_config_resetstat(cls, _session: typing.Optional[str] = None, **kwargs):
        """
        Reset the stats session = cls.get_session(_session)
        returned by INFO
        """
        session = cls.get_session(_session)
        return await session.async_config_resetstat(**kwargs)

    
    def config_rewrite(cls, _session: typing.Optional[str] = None, **kwargs):
        """
        Rewrite the configuration file with the in memory configuration
        """
        session = cls.get_session(_session)
        return session.config_rewrite(**kwargs)

    
    async def async_config_rewrite(cls, _session: typing.Optional[str] = None, **kwargs):
        """
        Rewrite the configuration file with the in memory configuration
        """
        session = cls.get_session(_session)
        return await session.async_config_rewrite(**kwargs)
    
    
    def keys(
        cls,
        pattern: typing.Optional[str] = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.List[KeyT]:
        """
        Get a List of all keys
        """
        session = cls.get_session(_session)
        return session.keys(pattern = pattern, **kwargs)
    
    
    async def async_keys(
        cls,
        pattern: typing.Optional[str] = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> typing.List[KeyT]:
        """
        Get a List of all keys
        """
        session = cls.get_session(_session)
        return await session.async_keys(pattern = pattern, **kwargs)

    
    def flushall(cls, _session: typing.Optional[str] = None, **kwargs):
        """
        Delete all keys in the current database
        """
        session = cls.get_session(_session)
        return session.flushall(**kwargs)
    
    
    async def async_flushall(cls, _session: typing.Optional[str] = None, **kwargs):
        """
        Delete all keys in the current database
        """
        session = cls.get_session(_session)
        return await session.async_flushall(**kwargs)
    
    
    def flushdb(cls, _session: typing.Optional[str] = None, **kwargs):
        """
        Delete all keys in the current database
        """
        session = cls.get_session(_session)
        return session.flushdb(**kwargs)
    
    
    async def async_flushdb(cls, _session: typing.Optional[str] = None, **kwargs):
        """
        Delete all keys in the current database
        """
        session = cls.get_session(_session)
        return await session.async_flushdb(**kwargs)
    
    
    def dbsize(cls, _session: typing.Optional[str] = None, **kwargs) -> int:
        """
        Return the number of keys in the current database
        """
        session = cls.get_session(_session)
        return session.dbsize(**kwargs)
    
    
    async def async_dbsize(cls, _session: typing.Optional[str] = None, **kwargs) -> int:
        """
        Return the number of keys in the current database
        """
        session = cls.get_session(_session)
        return await session.async_dbsize(**kwargs)
    
    
    def randomkey(cls, _session: typing.Optional[str] = None, **kwargs):
        """
        Return a random key from the current database
        """
        session = cls.get_session(_session)
        return session.randomkey(**kwargs)
    
    
    async def async_randomkey(cls, _session: typing.Optional[str] = None, **kwargs):
        """
        Return a random key from the current database
        """
        session = cls.get_session(_session)
        return await session.async_randomkey(**kwargs)
    
    
    def info(
        cls,  
        section: typing.Optional[str] = None,
        *args,
        _session: typing.Optional[str] = None,
        **kwargs
    ):
        """
        Return information and statistics about the server
        """
        session = cls.get_session(_session)
        return session.info(*args, section = section, **kwargs)
    
    
    async def async_info(
        cls,
        section: typing.Optional[str] = None,
        *args,
        _session: typing.Optional[str] = None,
        **kwargs
    ):
        """
        Return information and statistics about the server
        """
        session = cls.get_session(_session)
        return await session.async_info(*args, section = section, **kwargs)
    
    
    def move(
        cls,
        key: KeyT,
        db: int,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Move a key to another database
        """
        session = cls.get_session(_session)
        return session.move(key, db, **kwargs)
    
    
    async def async_move(
        cls,
        key: KeyT,
        db: int,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Move a key to another database
        """
        session = cls.get_session(_session)
        return await session.async_move(key, db, **kwargs)
    
    
    def rename(
        cls,
        key: KeyT,
        newkey: KeyT,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Rename a key
        """
        session = cls.get_session(_session)
        return session.rename(key, newkey, **kwargs)
    
    
    async def async_rename(
        cls,
        key: KeyT,
        newkey: KeyT,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Rename a key
        """
        session = cls.get_session(_session)
        return await session.async_rename(key, newkey, **kwargs)
    
    
    def renamenx(
        cls,
        key: KeyT,
        newkey: KeyT,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Rename a key, only if the new key does not exist
        """
        session = cls.get_session(_session)
        return session.renamenx(key, newkey, **kwargs)
    
    
    async def async_renamenx(
        cls,
        key: KeyT,
        newkey: KeyT,
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> bool:
        """
        Rename a key, only if the new key does not exist
        """
        session = cls.get_session(_session)
        return await session.async_renamenx(key, newkey, **kwargs)
    
    
    def migrate(
        cls,
        session: typing.Optional[KeyDBSession] = None,
        host: typing.Optional[str] = None,
        port: typing.Optional[int] = None,
        keys: typing.Optional[typing.Union[KeyT, typing.List[KeyT]]] = None,
        destination_db: typing.Optional[int] = None,
        timeout: typing.Optional[int] = None,
        copy: typing.Optional[bool] = None,
        replace: typing.Optional[bool] = None,
        auth: typing.Optional[str] = None,
        _session: typing.Optional[str] = None,
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
        
        session = cls.get_session(_session)
        return session.migrate(
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
        cls,
        session: typing.Optional[KeyDBSession] = None,
        host: typing.Optional[str] = None,
        port: typing.Optional[int] = None,
        keys: typing.Optional[typing.Union[KeyT, typing.List[KeyT]]] = None,
        destination_db: typing.Optional[int] = None,
        timeout: typing.Optional[int] = None,
        copy: typing.Optional[bool] = None,
        replace: typing.Optional[bool] = None,
        auth: typing.Optional[str] = None,
        _session: typing.Optional[str] = None,
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
        
        session = cls.get_session(_session)
        return await session.async_migrate(
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

    
    def module_list(cls, _session: typing.Optional[str] = None, **kwargs):
        """
        List all modules
        """
        session = cls.get_session(_session)
        return session.module_list(**kwargs)
    
    
    async def async_module_list(cls, _session: typing.Optional[str] = None, **kwargs):
        """
        List all modules
        """
        session = cls.get_session(_session)
        return await session.async_module_list(**kwargs)

    
    def module_load(
        cls,
        path: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ):
        """
        Load a module
        """
        session = cls.get_session(_session)
        return session.module_load(path, **kwargs)
    
    
    async def async_module_load(
        cls,
        path: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ):
        """
        Load a module
        """
        session = cls.get_session(_session)
        return await session.async_module_load(path, **kwargs)
    
    
    def module_loadex(
        cls,
        path: str,
        options: typing.Optional[typing.List[str]] = None,
        args: typing.Optional[typing.List[str]] = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ):
        """
        Load a module
        """
        session = cls.get_session(_session)
        return session.module_loadex(path, options = options, args = args, **kwargs)

    
    async def async_module_loadex(
        cls,
        path: str,
        options: typing.Optional[typing.List[str]] = None,
        args: typing.Optional[typing.List[str]] = None,
        _session: typing.Optional[str] = None,
        **kwargs
    ):
        """
        Load a module
        """
        session = cls.get_session(_session)
        return await session.async_module_loadex(path, options = options, args = args, **kwargs)
    
    
    def module_unload(
        cls,
        name: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ):
        """
        Unload a module
        """
        session = cls.get_session(_session)
        return session.module_unload(name, **kwargs)
    
    
    async def async_module_unload(
        cls,
        name: str,
        _session: typing.Optional[str] = None,
        **kwargs
    ):
        """
        Unload a module
        """
        session = cls.get_session(_session)
        return await session.async_module_unload(name, **kwargs)


    
    def ping(cls, _session: typing.Optional[str] = None, **kwargs):
        """
        Ping the keydb client
        """
        session = cls.get_session(_session)
        return session.ping(**kwargs)

    
    async def async_ping(cls, _session: typing.Optional[str] = None, **kwargs):
        """
        Ping the keydb client
        """
        session = cls.get_session(_session)
        return await session.async_ping(**kwargs)

    
    def wait_for_ready(
        cls, 
        interval: int = 1.0,
        max_attempts: typing.Optional[int] = None,
        timeout: typing.Optional[float] = 60.0,
        verbose: bool = False, 
        _session: typing.Optional[str] = None,
        **kwargs
    ):
        """
        Wait for the client to be ready
        """
        session = cls.get_session(_session)
        return session.wait_for_ready(
            interval = interval,
            max_attempts = max_attempts,
            timeout = timeout,
            verbose = verbose,
            **kwargs
        )
    
    
    async def async_wait_for_ready(
        cls, 
        interval: int = 1.0,
        max_attempts: typing.Optional[int] = None,
        timeout: typing.Optional[float] = 60.0,
        verbose: bool = False, 
        _session: typing.Optional[str] = None,
        **kwargs
    ):
        """
        Wait for the client to be ready
        """
        session = cls.get_session(_session)
        return await session.async_wait_for_ready(
            interval = interval,
            max_attempts = max_attempts,
            timeout = timeout,
            verbose = verbose,
            **kwargs
        )
    
    def get_key_sizes(
        cls, 
        match: typing.Union[PatternT, None] = None,
        count: typing.Union[int, None] = None,
        _type: typing.Union[str, None] = None,
        min_size: typing.Union[ByteSize, int, str, None] = None,
        max_size: typing.Union[ByteSize, int, str, None] = None,
        raise_error: typing.Optional[bool] = False,
        parse: typing.Optional[bool] = True,
        verbose: typing.Optional[bool] = True,
        _session: typing.Optional[str] = None,
        **kwargs,
    ) -> typing.Iterator[typing.Tuple[str, typing.Union[ByteSize, int]]]:
        """
        Returns an iterator that yields a tuple of key name and size in bytes or a ByteSize object
        """
        session = cls.get_session(_session)
        yield from session.get_key_sizes(
            match = match,
            count = count,
            _type = _type,
            min_size = min_size,
            max_size = max_size,
            raise_error = raise_error,
            parse = parse,
            verbose = verbose,
            **kwargs,
        )
    
    async def async_get_key_sizes(
        cls, 
        match: typing.Union[PatternT, None] = None,
        count: typing.Union[int, None] = None,
        _type: typing.Union[str, None] = None,
        min_size: typing.Union[ByteSize, int, str, None] = None,
        max_size: typing.Union[ByteSize, int, str, None] = None,
        raise_error: typing.Optional[bool] = False,
        parse: typing.Optional[bool] = True,
        verbose: typing.Optional[bool] = True,
        _session: typing.Optional[str] = None,
        **kwargs,
    ) -> typing.AsyncIterator[typing.Tuple[str, typing.Union[ByteSize, int]]]:
        """
        Returns an iterator that yields a tuple of key name and size in bytes or a ByteSize object
        """
        session = cls.get_session(_session)
        async for (key, size) in session.async_get_key_sizes(
            match = match,
            count = count,
            _type = _type,
            min_size = min_size,
            max_size = max_size,
            raise_error = raise_error,
            parse = parse,
            verbose = verbose,
            **kwargs,
        ):
            yield (key, size)
        
    @contextlib.asynccontextmanager
    async def afail_after(
        cls, 
        timeout: typing.Optional[float] = 5.0, 
        verbose: typing.Optional[bool] = True,
        raise_error: typing.Optional[bool] = True,
        trace_error: typing.Optional[bool] = False,
        **kwargs
    ):
        """
        Context manager that will fail after a certain timeout
        """
        try:
            async with anyio.fail_after(timeout):
                yield
        except Exception as e:
            if verbose:
                logger.error(f"Failed after {timeout} seconds: {e}")
            if trace_error:
                logger.error(traceback.format_exc())
            if raise_error:
                raise e

    async def aspawn_server(
        cls, 
        *args: typing.Iterable[str], 
        host: typing.Optional[str] = '0.0.0.0',
        port: typing.Optional[int] = 6379,
        password: typing.Optional[str] = None,
        active_replica: typing.Optional[bool] = False,
        multi_master: typing.Optional[bool] = False,
        register_session: typing.Optional[bool] = True,
        session_name: typing.Optional[str] = None,
        **kwargs
    ) -> asyncio.subprocess.Process:
        """
        Spawns a keydb server
        """
        cmd_args = list(args)
        for n, arg in enumerate(cmd_args):
            if not arg.startswith('--'):
                cmd_args[n] = f'--{arg}'

        cmd_args.append(
            f'--port {port}',
            f'--bind "{host}"',
        )
        if password:
            cmd_args.append(
                f'--requirepass "{password}"',
                f'--masterauth "{password}"',
            )
        if active_replica:
            cmd_args.append(
                '--active-replica yes'
            )
        if multi_master:
            cmd_args.append(
                '--multi-master yes'
            )
        cmd = ' '.join(cmd_args)
        logger.debug(f"Spawning server: {cmd}")
        pid = await asyncio.create_subprocess_shell(f"keydb-server {args}", **kwargs)
        if register_session:
            session_name = session_name or 'server'
            cls.init_session(
                session_name, host = host, port = port, password = password,
            )
        
        event = asyncio.Event()
        async def run_until_error():
            nonlocal pid
            while not event.is_set():
                try:
                    await asyncio.sleep(1)
                except Exception as e:
                    logger.error(f'KeyDB Error: {e}')
                    break
            
            logger.warning(f'Stopping KeyDB on PID: {pid.pid}')
            pid.terminate()
            pid.kill()
            pid = None    

        await asyncio.create_task(run_until_error())
        return pid


    """
    CLI Commands
    """

    def cli(
        cls,
        args: typing.Union[str, typing.List[str]], 
        shell: bool = True, 
        raise_error: bool = True, 
        entrypoint: str = 'keydb-cli',
        _session: typing.Optional[str] = None,
        **kwargs,
    ) -> str:
        """
        Runs a CLI command on the server
        """
        session = cls.get_session(_session)
        return session._cli(
            args = args,
            shell = shell,
            raise_error = raise_error,
            entrypoint = entrypoint,
            **kwargs
        )
    
    async def async_cli(
        cls,
        args: typing.Union[str, typing.List[str]],
        stdout = asyncio.subprocess.PIPE, 
        stderr = asyncio.subprocess.PIPE, 
        output_encoding: str = 'UTF-8', 
        output_errors: str = 'ignore',
        entrypoint: str = 'keydb-cli',
        _session: typing.Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Runs a CLI command on the server
        """
        session = cls.get_session(_session)
        return await session._async_cli(
            args = args,
            stdout = stdout,
            stderr = stderr,
            output_encoding = output_encoding,
            output_errors = output_errors,
            entrypoint = entrypoint,
            **kwargs
        )
    
    async def aclose(cls, verbose: bool = True):
        for name, ctx in cls.sessions.items():
            if verbose: logger.log(msg = f'Closing Session: {name}', level = cls.settings.loglevel)
            await ctx.aclose()
        
        cls._sessions = {}
        cls._ctx = None
        cls.current = None
    
    async def aclose_session(
        cls, 
        name: typing.Optional[str] = None, 
        session: typing.Optional[KeyDBSession] = None,
        raise_error: typing.Optional[bool] = False, 
        verbose: typing.Optional[bool] = False
    ):
        if session is None and name is None:
            raise ValueError('Must specify either name or session')
        name = name or session.name
        if name not in cls.sessions:
            if raise_error:
                raise KeyError(f'No session with name: {name}')
            return
        
        sess = cls.sessions[name]
        await sess.aclose()
        del cls.sessions[name]
        if verbose: logger.log(msg = f'Closed Session: {name}', level = cls.settings.loglevel)
    

    async def aclose_sessions(
        cls, 
        names: typing.Optional[typing.Union[str, typing.List[str]]] = None,
        sessions: typing.Optional[typing.Union[KeyDBSession, typing.List[KeyDBSession]]] = None, 
        raise_error: typing.Optional[bool] = False, 
        verbose: typing.Optional[bool] = False
    ):
        if sessions is None and names is None:
            raise ValueError('Must specify either names or sessions')
        if sessions is not None:
            if not isinstance(sessions, list):
                sessions = [sessions]
            names = [sess.name for sess in sessions]
        elif isinstance(names, str): names = [names]
        for name in names:
            await cls.aclose_session(name = name, raise_error = raise_error, verbose = verbose)

    
    def close(cls):
        for name, ctx in cls.sessions.items():
            logger.log(msg = f'Closing Session: {name}', level = cls.settings.loglevel)
            ctx.close()
        
        cls.sessions = {}
        cls._ctx = None
        cls.current = None
    
    def close_session(
        cls, 
        name: typing.Optional[str] = None, 
        session: typing.Optional[KeyDBSession] = None,
        raise_error: typing.Optional[bool] = False, 
        verbose: typing.Optional[bool] = False
    ):
        if session is None and name is None:
            raise ValueError('Must specify either name or session')
        name = name or session.name
        if name not in cls.sessions:
            if raise_error:
                raise KeyError(f'No session with name: {name}')
            return
        sess = cls.sessions[name]
        sess.close()
        del cls.sessions[name]
        if verbose: logger.log(msg = f'Closed Session: {name}', level = cls.settings.loglevel)
    
    def close_sessions(
        cls,
        names: typing.Optional[typing.Union[str, typing.List[str]]] = None,
        sessions: typing.Optional[typing.Union[KeyDBSession, typing.List[KeyDBSession]]] = None, 
        raise_error: typing.Optional[bool] = False, 
        verbose: typing.Optional[bool] = False
    ):
        if sessions is None and names is None: raise ValueError('Must specify either names or sessions')
        if sessions is not None:
            if not isinstance(sessions, list): sessions = [sessions]
            names = [sess.name for sess in sessions]
        elif isinstance(names, str): names = [names]
        for name in names:
            cls.close_session(name = name, raise_error = raise_error, verbose = verbose)
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.aclose()

    
    
    def cachify(
        cls,
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
        _session: typing.Optional[str] = None,
        _cache_fallback: typing.Optional[bool] = True,
        # _lazy_init: typing.Optional[bool] = None,
        **kwargs
    ):
        """Memoizing cache decorator. Repeated calls with the same arguments
        will look up the result in cache and avoid function evaluation.

        If `_func_name` is set to None (default), the callable name will be determined
        automatically.

        When expire is set to zero, function results will not be set in the
        cache. Store lookups still occur, however. 

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
        :type cache_prefix: str | None
        :param exclude: list of arguments to exclude from cache key
            (default None, no exclusion)
        :type exclude: list | None
        :param exclude_null: exclude arguments with null values from cache key
            (default False)
        :type exclude_null: bool
        :param exclude_return_types: list of return types to exclude from cache
            (default None, no exclusion)
        :type exclude_return_types: list | None
        :param exclude_return_objs: list of return objects to exclude from cache
            (default None, no exclusion)
        :type exclude_return_objs: list | None
        :param exclude_kwargs: list of kwargs to exclude from cache key
            (default None, no exclusion)
        :type exclude_kwargs: list | None
        :param include_cache_hit: include cache hit in return value
            (default False)
        :type include_cache_hit: bool
        :param bool _no_cache: disable cache for this function
            (default False)
        :param list _no_cache_kwargs: list of kwargs to disable cache for
            (default None, no exclusion)
        :param callable _no_cache_validator: callable to validate if cache should be disabled
            (default None, no validation)
        :param bool _validate_requests: validate requests
            (default True)
        :param _exclude_request_headers: list of headers to exclude from request validation
            (default True, exclude all headers)
        :type _exclude_request_headers: list | bool
        :param _cache_invalidator: callable to invalidate cache
            (default None, no invalidation)
        :type _cache_invalidator: callable | bool
        :param str _session: session name
            (default None, use default session)
        :param bool _lazy_init: lazy init session
            (default None, use default session)
        :param float _cache_timeout: timeout of cache operations
            (default 5.0)
        :param kwargs: additional arguments to pass to cache
        
        :return: callable decorator
        """
        from lazyops.utils import timed_cache, is_coro_func, fail_after
        _ = kwargs.pop('_lazy_init', None)
        _kwargs = dict(
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
            _func_name = _func_name,
            _validate_requests = _validate_requests,
            _exclude_request_headers = _exclude_request_headers,
            _cache_invalidator = _cache_invalidator,
            _invalidate_after_n_hits = _invalidate_after_n_hits,
            _cache_timeout = _cache_timeout,
        )

        def wrapper_func(func):

            _sess_ctx: 'KeyDBSession' = None
            if is_coro_func(func):
                # logger.info(f'Async Function: {func}')
                async def _get_sess_ctx():
                    nonlocal _sess_ctx
                    if _sess_ctx: return _sess_ctx
                    _sess = cls.get_session(_session)
                    with contextlib.suppress(Exception):
                        async with anyio.fail_after(1.0):
                            if await _sess.async_ping(): _sess_ctx = _sess
                    
                    return _sess_ctx
                

                @functools.wraps(func)
                async def inner_wrap(*args, **kwargs):
                    sess_ctx = await _get_sess_ctx()
                    if sess_ctx is None: 
                        if _cache_fallback is True: 
                            with contextlib.suppress(Exception):
                                return await timed_cache(secs = cache_ttl)(func)(*args, **kwargs)
                        return await func(*args, **kwargs)
                
                    # logger.info(f'Using async session {sess_ctx.uri} for cache')
                    return await sess_ctx.cachify(**_kwargs)(func)(*args, **kwargs)

            else:
                def _get_sess_ctx():
                    nonlocal _sess_ctx
                    if _sess_ctx: return _sess_ctx
                    _sess = cls.get_session(_session)
                    with contextlib.suppress(Exception):
                        with fail_after(1.0):
                            if _sess.ping(): _sess_ctx = _sess
                    
                    return _sess_ctx
                
                # logger.info(f'Sync Function: {func}')
                @functools.wraps(func)
                def inner_wrap(*args, **kwargs):
                    sess_ctx = _get_sess_ctx()
                    if sess_ctx is None: 
                        if _cache_fallback is True: 
                            with contextlib.suppress(Exception):
                                return timed_cache(secs = cache_ttl)(func)(*args, **kwargs)
                        return func(*args, **kwargs)
                    
                    # logger.info(f'Using session {sess_ctx.uri} for cache')
                    return sess_ctx.cachify(**_kwargs)(func)(*args, **kwargs)

            return inner_wrap
        return wrapper_func


    def cachify_v1(
        cls,
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
        _session: typing.Optional[str] = None,
        _lazy_init: typing.Optional[bool] = None,
        _cache_timeout: typing.Optional[float] = 5.0,
        **kwargs
    ):
        """Memoizing cache decorator. Repeated calls with the same arguments
        will look up the result in cache and avoid function evaluation.

        If `_func_name` is set to None (default), the callable name will be determined
        automatically.

        When expire is set to zero, function results will not be set in the
        cache. Store lookups still occur, however. 

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
        :type cache_prefix: str | None
        :param exclude: list of arguments to exclude from cache key
            (default None, no exclusion)
        :type exclude: list | None
        :param exclude_null: exclude arguments with null values from cache key
            (default False)
        :type exclude_null: bool
        :param exclude_return_types: list of return types to exclude from cache
            (default None, no exclusion)
        :type exclude_return_types: list | None
        :param exclude_return_objs: list of return objects to exclude from cache
            (default None, no exclusion)
        :type exclude_return_objs: list | None
        :param exclude_kwargs: list of kwargs to exclude from cache key
            (default None, no exclusion)
        :type exclude_kwargs: list | None
        :param include_cache_hit: include cache hit in return value
            (default False)
        :type include_cache_hit: bool
        :param bool _no_cache: disable cache for this function
            (default False)
        :param list _no_cache_kwargs: list of kwargs to disable cache for
            (default None, no exclusion)
        :param callable _no_cache_validator: callable to validate if cache should be disabled
            (default None, no validation)
        :param bool _validate_requests: validate requests
            (default True)
        :param _exclude_request_headers: list of headers to exclude from request validation
            (default True, exclude all headers)
        :type _exclude_request_headers: list | bool
        :param _cache_invalidator: callable to invalidate cache
            (default None, no invalidation)
        :type _cache_invalidator: callable | bool
        :param str _session: session name
            (default None, use default session)
        :param bool _lazy_init: lazy init session
            (default None, use default session)
        :param float _cache_timeout: timeout of cache operations
            (default 5.0)
        :param kwargs: additional arguments to pass to cache
        
        :return: callable decorator
        """

        # add _lazy_init to prevent loading the session
        # before the class is fully initialized
        if _lazy_init is True:
            return cls.cachify(
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
                _func_name = _func_name,
                _validate_requests = _validate_requests,
                _exclude_request_headers = _exclude_request_headers,
                _cache_invalidator = _cache_invalidator,
                _invalidate_after_n_hits = _invalidate_after_n_hits,
                _cache_timeout = _cache_timeout,
                **kwargs
            )

        # if _lazy_init is not None and _lazy_init is True and not cls.has_session:
        #     return
        session = cls.get_session(_session)
        return session.cachify(
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
            _func_name = _func_name,
            _validate_requests = _validate_requests,
            _exclude_request_headers = _exclude_request_headers,
            _cache_invalidator = _cache_invalidator,
            _invalidate_after_n_hits = _invalidate_after_n_hits,
            _cache_timeout = _cache_timeout,
            **kwargs
        )
    

            

class KeyDBClient(metaclass = KeyDBClientMeta):
    """
    The Global KeyDB Client Class
    """
    pass
