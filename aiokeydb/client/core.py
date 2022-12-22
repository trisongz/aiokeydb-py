import typing
import logging

from aiokeydb.lock import Lock
from aiokeydb.connection import Encoder
from aiokeydb.core import KeyDB, PubSub, Pipeline

from aiokeydb.typing import Number, KeyT
from aiokeydb.asyncio.lock import AsyncLock
from aiokeydb.asyncio.core import AsyncKeyDB, AsyncPubSub, AsyncPipeline

from aiokeydb.client.config import KeyDBSettings
from aiokeydb.client.types import classproperty, KeyDBUri

from aiokeydb.client.schemas.session import KeyDBSession
from aiokeydb.client.serializers import SerializerType

logger = logging.getLogger(__name__)

class KeyDBClient:
    ctx: typing.Type[KeyDBSession] = None
    current: str = None
    encoder: typing.Type[Encoder] = None
    sessions: typing.Dict[str, KeyDBSession] = {}
    settings: typing.Type[KeyDBSettings] = None

    @classmethod
    def configure(
        cls,
        url: typing.Optional[str] = None,
        host: typing.Optional[str] = None,
        port: typing.Optional[int] = None,
        db: typing.Optional[int] = None,
        username: typing.Optional[str] = None,
        password: typing.Optional[str] = None,
        ssl: typing.Optional[bool] = None,
        cache_ttl: typing.Optional[int] = None,
        cache_prefix: typing.Optional[str] = None,
        cache_enabled: typing.Optional[bool] = None,
        worker_enabled: typing.Optional[bool] = None,
        serializer: typing.Optional[SerializerType] = None,
        db_mapping: typing.Optional[typing.Union[str, typing.Dict[str, int]]] = None,
        socket_timeout: typing.Optional[float] = None,
        socket_connect_timeout: typing.Optional[float] = None,
        socket_keepalive: typing.Optional[bool] = None,
        connection_timeout: typing.Optional[int] = None,
        encoding: typing.Optional[str] = None,
        encoding_errors: typing.Optional[str] = None,
        config_kwargs: typing.Optional[typing.Union[str, typing.Dict[str, typing.Any]]] = None,
        debug_enabled: typing.Optional[bool] = None,
        log_level: typing.Optional[str] = None,
        queue_db: typing.Optional[int] = None,
        overwrite: typing.Optional[bool] = None,
        **kwargs,
    ):
        """
        Configures the global settings
        """
        if not cls.settings: cls.settings = KeyDBSettings()
        cls.settings.configure(
            url=url,
            host=host,
            port=port,
            db=db,
            username=username,
            password=password,
            ssl=ssl,
            cache_ttl=cache_ttl,
            cache_prefix=cache_prefix,
            cache_enabled=cache_enabled,
            worker_enabled=worker_enabled,
            serializer=serializer,
            db_mapping=db_mapping,
            socket_timeout=socket_timeout,
            socket_connect_timeout=socket_connect_timeout,
            socket_keepalive=socket_keepalive,
            connection_timeout=connection_timeout,
            encoding=encoding,
            encoding_errors=encoding_errors,
            config_kwargs=config_kwargs,
            log_level=log_level,
            debug_enabled=debug_enabled,
            queue_db=queue_db,
            **kwargs,
        )
        if overwrite is True: cls.init_session(overwrite=overwrite)



    @classmethod
    def get_settings(cls) -> KeyDBSettings:
        if not cls.settings: cls.settings = KeyDBSettings()
        return cls.settings

    @classproperty
    def has_session(cls) -> bool:
        if not cls.settings: return False
        return bool(cls.ctx) if cls.sessions else False

    @classmethod
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
        **config,
    ):
        if name in cls.sessions and overwrite is not True:
            logger.warning(f'Session {name} already exists')
            return
        if not cls.settings: cls.settings = KeyDBSettings()
        if not cls.encoder: cls.encoder = Encoder(
            encoding = cls.settings.encoding,
            encoding_errors = cls.settings.encoding_errors,
            decode_responses = True,
        )
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
        db_id = uri.db_id
        config = cls.settings.get_config(
            **config,
        )
        ctx = KeyDBSession(
            uri = uri,
            name = name,
            db_id = db_id,
            serializer = cls.settings.get_serializer(),
            encoder = cls.encoder,
            settings = cls.settings,
            cache_enabled = cache_enabled,
            **config,
        )
        cls.sessions[name] = ctx    
        logger.log(msg = f'Initialized Session: {name} ({uri})', level = cls.settings.loglevel)
        if (set_current or overwrite) or cls.ctx is None:
            cls.ctx = ctx
            cls.current = name
            logger.log(msg = f'Setting to Current Session: {name}', level = cls.settings.loglevel)
        return ctx

    @classmethod
    def add_session(
        cls,
        session: KeyDBSession,
        overwrite: bool = False,
        set_current: bool = False,
        **kwargs
    ):
        """
        Adds a session to the client.
        """
        if not isinstance(session, KeyDBSession):
            raise TypeError('Session must be an instance of KeyDBSession')
        if session.name in cls.sessions and not overwrite:
            logger.warning(f'Session {session.name} already exists')
            return
        cls.sessions[session.name] = session
        logger.log(msg = f'Added Session: {session.name} ({session.uri})', level = cls.settings.loglevel)
        if set_current:
            cls.ctx = session
            cls.current = session.name
            logger.log(msg = f'Setting to Current Session: {session.name}', level = cls.settings.loglevel)

    @classmethod
    def create_session(
        cls,
        name: str,
        db_id: int = None,
        serializer: typing.Optional[typing.Any] = None,
        overwrite: bool = False,
        set_current: bool = False,
        cache_enabled: typing.Optional[bool] = None,
        _decode_responses: typing.Optional[bool] = None,
        **kwargs,
    ):
        """
        Create a new Session instance
        - used in conjunction with aiokeydb.queues.TaskQueue
        - does not explicitly set the serializer.
        """
        if name in cls.sessions and not overwrite:
            logger.warning(f'Session {name} already exists')
            return cls.sessions[name]
        if not cls.settings: cls.get_settings()
        db_id = db_id if db_id is not None else cls.settings.get_db_id(
            name = name, db = db_id
        )
        uri: KeyDBUri = cls.settings.create_uri(
            name = name,
            db_id = db_id,
            with_auth = True,
        )
        # logger.info(f'Creating Session: {name} ({uri})')
        config = cls.settings.get_config(**kwargs)
        ctx = KeyDBSession(
            uri,
            name = name,
            db_id = db_id,
            serializer = serializer,
            settings = cls.settings,
            cache_enabled = cache_enabled,
            _decode_responses = _decode_responses,
            **config,
        )
        cls.sessions[name] = ctx
        logger.log(msg = f'Created Session: {name} ({uri}) @ DB {db_id}', level = cls.settings.loglevel)
        if set_current:
            cls.ctx = ctx
            cls.current = name
            logger.log(msg = f'Setting to Current Session: {name}', level = cls.settings.loglevel)
        return ctx

    @classmethod
    def set_session(
        cls,
        name: str = None,
        **kwargs,
    ):
        if name not in cls.sessions:
            return cls.init_session(name = name, set_current = True, **kwargs)
        cls.ctx = cls.sessions[name]
        cls.current = name
        logger.log(msg = f'Setting to Current Session: {name}', level = cls.settings.loglevel)

    @classmethod
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

    @classmethod
    def close_session(
        cls,
        name: str,
        **kwargs,
    ):
        """
        Remove a session from the client.
        """
        if name not in cls.sessions:
            logger.log(msg = f'Session {name} does not exist', level = cls.settings.loglevel)
            return
        session = cls.sessions[name]
        session.close()
        del cls.sessions[name]
        logger.log(msg = f'Removed Session: {name}', level = cls.settings.loglevel)
        if name == cls.current:
            cls.current = None
            cls.ctx = None
            logger.log(msg = f'Removed Current Session: {name}', level = cls.settings.loglevel)
    
    @classmethod
    async def async_close_session(
        cls,
        name: str,
        **kwargs,
    ):
        """
        Remove a session from the client.
        """
        if name not in cls.sessions:
            logger.log(msg = f'Session {name} does not exist', level = cls.settings.loglevel)
            return
        session = cls.sessions[name]
        await session.aclose()
        del cls.sessions[name]
        logger.log(msg = f'Removed Session: {name}', level = cls.settings.loglevel)
        if name == cls.current:
            cls.current = None
            cls.ctx = None
            logger.log(msg = f'Removed Current Session: {name}', level = cls.settings.loglevel)
    
    @classproperty
    def session(cls) -> KeyDBSession:
        """
        returns the current `KeyDBSession`.
        """
        if not cls.ctx:
            cls.init_session()
        return cls.ctx
    
    @classproperty
    def keydb(cls) -> KeyDB:
        """
        returns the underlying `KeyDB` client from within
        the current session.
        """
        return cls.get_session().client
    
    @classproperty
    def async_keydb(cls) -> AsyncKeyDB:
        """
        returns the underlying `AsyncKeyDB` client from within
        the current session.
        """
        return cls.get_session().async_client

    @classproperty
    def pubsub(cls) -> PubSub:
        """
        returns a `PubSub`.
        """
        return cls.get_session().pubsub
    
    @classproperty
    def async_pubsub(cls) -> AsyncPubSub:
        """
        returns a `AsyncPubSub`.
        """
        return cls.get_session().async_pubsub
    
    @classproperty
    def pipeline(cls) -> Pipeline:
        """
        returns a `Pipeline`.
        """
        return cls.get_session().pipeline
    
    @classproperty
    def async_pipeline(cls) -> AsyncPipeline:
        """
        returns a `AsyncPipeline`.
        """
        return cls.get_session().async_pipeline
    
    @classproperty
    def lock(cls) -> Lock:
        """
        returns a `Lock`.
        """
        return cls.get_session().lock
    
    @classproperty
    def async_lock(cls) -> AsyncLock:
        """
        returns a `AsyncLock`.
        """
        return cls.get_session().async_lock
    
    @classmethod
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
            # _session = _session,
        )
    
    @classmethod
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
            # _session = _session,
        )
    

    """
    Primary Functions
    """
    @classmethod
    def set(
        cls, 
        name: str, 
        value: typing.Any,
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
            **kwargs
        )
        
    @classmethod
    async def async_set(
        cls, 
        name: str, 
        value: typing.Any,
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
            **kwargs
        )
    
    @classmethod
    def get(
        cls, 
        name: str, 
        default: typing.Any = None, 
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
            **kwargs
        )
    
    @classmethod
    async def async_get(
        cls, 
        name: str, 
        default: typing.Any = None, 
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
            **kwargs
        )
    
    @classmethod
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

    @classmethod
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

    @classmethod
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
            

    @classmethod
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
        

    @classmethod
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

    @classmethod
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
    

    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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

    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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

    @classmethod
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

    @classmethod
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

    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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

    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    Other utilities
    """
    
    @classmethod
    def command(cls, _session: typing.Optional[str] = None, **kwargs):
        """
        Run a command
        """
        session = cls.get_session(_session)
        return session.command(**kwargs)
    
    @classmethod
    async def async_command(cls, _session: typing.Optional[str] = None, **kwargs):
        """
        Run a command
        """
        session = cls.get_session(_session)
        return await session.async_command(**kwargs)
    
    @classmethod
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
    
    @classmethod
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
    

    @classmethod
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

    @classmethod
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

    @classmethod
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

    @classmethod
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

    @classmethod
    def config_resetstat(cls, _session: typing.Optional[str] = None, **kwargs):
        """
        Reset the stats session = cls.get_session(_session)
        returned by INFO
        """
        session = cls.get_session(_session)
        return session.config_resetstat(**kwargs)

    @classmethod
    async def async_config_resetstat(cls, _session: typing.Optional[str] = None, **kwargs):
        """
        Reset the stats session = cls.get_session(_session)
        returned by INFO
        """
        session = cls.get_session(_session)
        return await session.async_config_resetstat(**kwargs)

    @classmethod
    def config_rewrite(cls, _session: typing.Optional[str] = None, **kwargs):
        """
        Rewrite the configuration file with the in memory configuration
        """
        session = cls.get_session(_session)
        return session.config_rewrite(**kwargs)

    @classmethod
    async def async_config_rewrite(cls, _session: typing.Optional[str] = None, **kwargs):
        """
        Rewrite the configuration file with the in memory configuration
        """
        session = cls.get_session(_session)
        return await session.async_config_rewrite(**kwargs)
    
    @classmethod
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
    
    @classmethod
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

    @classmethod
    def flushall(cls, _session: typing.Optional[str] = None, **kwargs):
        """
        Delete all keys in the current database
        """
        session = cls.get_session(_session)
        return session.flushall(**kwargs)
    
    @classmethod
    async def async_flushall(cls, _session: typing.Optional[str] = None, **kwargs):
        """
        Delete all keys in the current database
        """
        session = cls.get_session(_session)
        return await session.async_flushall(**kwargs)
    
    @classmethod
    def flushdb(cls, _session: typing.Optional[str] = None, **kwargs):
        """
        Delete all keys in the current database
        """
        session = cls.get_session(_session)
        return session.flushdb(**kwargs)
    
    @classmethod
    async def async_flushdb(cls, _session: typing.Optional[str] = None, **kwargs):
        """
        Delete all keys in the current database
        """
        session = cls.get_session(_session)
        return await session.async_flushdb(**kwargs)
    
    @classmethod
    def dbsize(cls, _session: typing.Optional[str] = None, **kwargs) -> int:
        """
        Return the number of keys in the current database
        """
        session = cls.get_session(_session)
        return session.dbsize(**kwargs)
    
    @classmethod
    async def async_dbsize(cls, _session: typing.Optional[str] = None, **kwargs) -> int:
        """
        Return the number of keys in the current database
        """
        session = cls.get_session(_session)
        return await session.async_dbsize(**kwargs)
    
    @classmethod
    def randomkey(cls, _session: typing.Optional[str] = None, **kwargs):
        """
        Return a random key from the current database
        """
        session = cls.get_session(_session)
        return session.randomkey(**kwargs)
    
    @classmethod
    async def async_randomkey(cls, _session: typing.Optional[str] = None, **kwargs):
        """
        Return a random key from the current database
        """
        session = cls.get_session(_session)
        return await session.async_randomkey(**kwargs)
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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

    @classmethod
    def module_list(cls, _session: typing.Optional[str] = None, **kwargs):
        """
        List all modules
        """
        session = cls.get_session(_session)
        return session.module_list(**kwargs)
    
    @classmethod
    async def async_module_list(cls, _session: typing.Optional[str] = None, **kwargs):
        """
        List all modules
        """
        session = cls.get_session(_session)
        return await session.async_module_list(**kwargs)

    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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

    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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


    @classmethod
    def ping(cls, _session: typing.Optional[str] = None, **kwargs):
        """
        Ping the keydb client
        """
        session = cls.get_session(_session)
        return session.ping(**kwargs)

    @classmethod
    async def async_ping(cls, _session: typing.Optional[str] = None, **kwargs):
        """
        Ping the keydb client
        """
        session = cls.get_session(_session)
        return await session.async_ping(**kwargs)

    @classmethod
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
    
    @classmethod
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
    
    @classmethod
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
        _no_cache: typing.Optional[bool] = False,
        _no_cache_kwargs: typing.Optional[typing.List[str]] = None,
        _no_cache_validator: typing.Optional[typing.Callable] = None,
        _func_name: typing.Optional[str] = None,
        _validate_requests: typing.Optional[bool] = True,
        _exclude_request_headers: typing.Optional[typing.Union[typing.List[str], bool]] = True,
        _cache_invalidator: typing.Optional[typing.Union[bool, typing.Callable]] = None,
        _session: typing.Optional[str] = None,
        _lazy_init: typing.Optional[bool] = None,
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
        :param kwargs: additional arguments to pass to cache
        
        :return: callable decorator
        """

        # add _lazy_init to prevent loading the session
        # before the class is fully initialized
        if _lazy_init is not None and _lazy_init is True and not cls.has_session:
            return

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
            _no_cache = _no_cache,
            _no_cache_kwargs = _no_cache_kwargs,
            _no_cache_validator = _no_cache_validator,
            _func_name = _func_name,
            _validate_requests = _validate_requests,
            _exclude_request_headers = _exclude_request_headers,
            _cache_invalidator = _cache_invalidator,
            **kwargs
        )
            

    @classmethod
    async def aclose(cls):
        for name, ctx in cls.sessions.items():
            logger.log(msg = f'Closing Session: {name}', level = cls.settings.loglevel)
            await ctx.aclose()
        
        cls.sessions = {}
        cls.ctx = None
        cls.current = None
    
    @classmethod
    def close(cls):
        for name, ctx in cls.sessions.items():
            logger.log(msg = f'Closing Session: {name}', level = cls.settings.loglevel)
            ctx.close()
        
        cls.sessions = {}
        cls.ctx = None
        cls.current = None

    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.aclose()
    





