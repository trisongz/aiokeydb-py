import typing
import logging

from aiokeydb.lock import Lock
from aiokeydb.connection import Encoder
from aiokeydb.core import KeyDB, PubSub, Pipeline

from aiokeydb.typing import Number, KeyT
from aiokeydb.asyncio.lock import AsyncLock
from aiokeydb.asyncio.core import AsyncKeyDB, AsyncPubSub, AsyncPipeline

from aiokeydb.client.config import KeyDBSettings
from aiokeydb.client.types import classproperty

from aiokeydb.client.schemas.session import KeyDBSession

logger = logging.getLogger(__name__)

class KeyDBClient:
    ctx: typing.Type[KeyDBSession] = None
    current: str = None
    encoder: typing.Type[Encoder] = None
    sessions: typing.Dict[str, KeyDBSession] = {}
    settings: typing.Type[KeyDBSession] = None

    @classmethod
    async def aclose(cls):
        for name, ctx in cls.sessions.items():
            logger.info(f'Closing Session: {name}')
            await ctx.aclose()
        
        cls.sessions = {}
        cls.ctx = None
        cls.current = None
    
    @classmethod
    def close(cls):
        for name, ctx in cls.sessions.items():
            logger.info(f'Closing Session: {name}')
            ctx.close()
        
        cls.sessions = {}
        cls.ctx = None
        cls.current = None

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
        **config,
    ):
        if name in cls.sessions:
            logger.warning(f'Session {name} already exists')
            return
        if not cls.settings: cls.settings = KeyDBSettings()
        if not cls.encoder: cls.encoder = Encoder(
            encoding = cls.settings.encoding,
            encoding_errors = cls.settings.encoding_errors,
            decode_responses = True,
        )
        uri, db_id = cls.settings.create_uri(
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
            **config,
        )
        cls.sessions[name] = ctx
        cls.ctx = ctx
        cls.current = name
        logger.info(f'Created Session: {name} ({uri})')
    
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
        returns a `KeyDB`.
        """
        return cls.get_session().client
    
    @classproperty
    def async_keydb(cls) -> AsyncKeyDB:
        """
        returns a `AsyncKeyDB`.
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
            _session = _session,
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
            _session = _session,
        )
    
    
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
            x = await KeyDBClient.get(key); -> x = {'key': 'value'}
            x.update(new_data); -> x.update({'key2': 'value2'})
            await KeyDBClient.set(key, x); -> {'key': 'value', 'key2': 'value2'}
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
            x = await KeyDBClient.get(key); -> x = {'key': 'value'}
            x.update(new_data); -> x.update({'key2': 'value2'})
            await KeyDBClient.set(key, x); -> {'key': 'value', 'key2': 'value2'}
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

    """
    Other utilities
    """

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
        include_cache_hit: typing.Optional[bool] = False,
        _no_cache: typing.Optional[bool] = False,
        _no_cache_kwargs: typing.Optional[typing.List[str]] = None,
        _no_cache_validator: typing.Optional[typing.Callable] = None,
        _func_name: typing.Optional[str] = None,
        _validate_requests: typing.Optional[bool] = True,
        _session: typing.Optional[str] = None,
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
        session = cls.get_session(_session)
        return session.cachify(
            cache_ttl = cache_ttl,
            typed = typed,
            cache_prefix = cache_prefix,
            exclude = exclude,
            exclude_null = exclude_null,
            exclude_return_types = exclude_return_types,
            exclude_return_objs = exclude_return_objs,
            include_cache_hit = include_cache_hit,
            _no_cache = _no_cache,
            _no_cache_kwargs = _no_cache_kwargs,
            _no_cache_validator = _no_cache_validator,
            _func_name = _func_name,
            _validate_requests = _validate_requests,
            **kwargs
        )
            







