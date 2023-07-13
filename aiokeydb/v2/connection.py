import os
import sys
import logging
import asyncio
import typing
import contextlib
import threading
import inspect
from itertools import chain
from queue import Empty, Full, Queue, LifoQueue
from urllib.parse import parse_qs, unquote, urlparse, ParseResult
from redis.connection import (
    URL_QUERY_ARGUMENT_PARSERS,
    Connection,
    UnixDomainSocketConnection,
    SSLConnection,
    Encoder,
    ConnectionPool as _ConnectionPool,
)

from redis.asyncio.connection import (
    Connection as AsyncConnection,
    UnixDomainSocketConnection as AsyncUnixDomainSocketConnection,
    SSLConnection as AsyncSSLConnection,
    Encoder as AsyncEncoder,
    ConnectionPool as _AsyncConnectionPool,
)

import aiokeydb.v2.exceptions as exceptions

# the functionality is available in 3.11.x but has a major issue before
# 3.11.3. See https://github.com/redis/redis-py/issues/2633
if sys.version_info >= (3, 11, 3):
    from asyncio import timeout as async_timeout
else:
    from async_timeout import timeout as async_timeout

from aiokeydb.v2.utils import set_ulimits

logger = logging.getLogger(__name__)

def parse_url(url: str, _is_async: bool = False):
    if not (
        url.startswith("keydb://")
        or url.startswith("keydbs://")
        or url.startswith("redis://")
        or url.startswith("rediss://")
        or url.startswith("unix://")
    ):
        raise ValueError(
            "KeyDB URL must specify one of the following "
            "schemes (keydb://, keydbs://, redis://, rediss://, unix://)"
        )

    url: ParseResult = urlparse(url)
    kwargs = {}

    for name, value in parse_qs(url.query).items():
        if value and len(value) > 0:
            value = unquote(value[0])
            parser = URL_QUERY_ARGUMENT_PARSERS.get(name)
            if parser:
                try:
                    kwargs[name] = parser(value)
                except (TypeError, ValueError) as e:
                    raise ValueError(f"Invalid value for `{name}` in connection URL.") from e
            else:
                kwargs[name] = value

    if url.username:
        kwargs["username"] = unquote(url.username)
    if url.password:
        kwargs["password"] = unquote(url.password)

    # We only support redis://, rediss:// and unix:// schemes.
    if url.scheme == "unix":
        if url.path:
            kwargs["path"] = unquote(url.path)
        kwargs["connection_class"] = AsyncUnixDomainSocketConnection if _is_async else UnixDomainSocketConnection

    else:  # implied:  url.scheme in ("redis", "rediss"):
        if url.hostname:
            kwargs["host"] = unquote(url.hostname)
        if url.port:
            kwargs["port"] = int(url.port)

        # If there's a path argument, use it as the db argument if a
        # querystring value wasn't specified
        if url.path and "db" not in kwargs:
            with contextlib.suppress(AttributeError, ValueError):
                kwargs["db"] = int(unquote(url.path).replace("/", ""))
        if url.scheme in {"rediss", "keydbs"}:
            kwargs["connection_class"] = AsyncSSLConnection if _is_async else SSLConnection

    return kwargs


_conn_valid_kwarg_keys: typing.Dict[str, typing.List[str]] = {}

def _extract_cls_init_kwargs(obj: object) -> typing.List[str]:
    """
    Extracts the kwargs that are valid for a connection class
    """
    argspec = inspect.getfullargspec(obj.__init__)
    _args = []
    for arg in argspec.args:
        _args.append(arg)
    for arg in argspec.kwonlyargs:
        _args.append(arg)
    
    # Check if subclass of Connection
    if hasattr(obj, "__bases__"):
        for base in obj.__bases__:
            _args.extend(_extract_cls_init_kwargs(base))

    return list(set(_args))

def filter_kwargs_for_connection(conn_cls: typing.Type[Connection], kwargs: typing.Dict[str, typing.Any]) -> typing.Dict[str, typing.Any]:
    # sourcery skip: dict-comprehension
    """
    Filter out kwargs that aren't valid for a connection class
    """
    global _conn_valid_kwarg_keys
    if conn_cls.__name__ not in _conn_valid_kwarg_keys:
        _conn_valid_kwarg_keys[conn_cls.__name__] = _extract_cls_init_kwargs(conn_cls)
    return {k: v for k, v in kwargs.items() if k in _conn_valid_kwarg_keys[conn_cls.__name__]}


class ConnectionPool(_ConnectionPool):
    """
    Create a connection pool. ``If max_connections`` is set, then this
    object raises :py:class:`~redis.exceptions.ConnectionError` when the pool's
    limit is reached.

    By default, TCP connections are created unless ``connection_class``
    is specified. Use class:`.UnixDomainSocketConnection` for
    unix sockets.

    Any additional keyword arguments are passed to the constructor of
    ``connection_class``.
    """

    @classmethod
    def from_url(cls, url, **kwargs):
        """
        Return a connection pool configured from the given URL.

        For example::
            keydb://[[username]:[password]]@localhost:6379/0
            keydbs://[[username]:[password]]@localhost:6379/0
            redis://[[username]:[password]]@localhost:6379/0
            rediss://[[username]:[password]]@localhost:6379/0
            unix://[[username]:[password]]@/path/to/socket.sock?db=0

        Five URL schemes are supported:
        - `keydb://` creates a TCP socket connection.
        - `keydbs://` creates a SSL wrapped TCP socket connection.
        - `redis://` creates a TCP socket connection. See more at:
          <https://www.iana.org/assignments/uri-schemes/prov/redis>
        - `rediss://` creates a SSL wrapped TCP socket connection. See more at:
          <https://www.iana.org/assignments/uri-schemes/prov/rediss>
        - ``unix://``: creates a Unix Domain Socket connection.

        The username, password, hostname, path and all querystring values
        are passed through urllib.parse.unquote in order to replace any
        percent-encoded values with their corresponding characters.

        There are several ways to specify a database number. The first value
        found will be used:

            1. A ``db`` querystring option, e.g. redis://localhost?db=0
            2. If using the redis:// or rediss:// schemes, the path argument
               of the url, e.g. redis://localhost/0
            3. A ``db`` keyword argument to this function.

        If none of these options are specified, the default db=0 is used.

        All querystring options are cast to their appropriate Python types.
        Boolean arguments can be specified with string values "True"/"False"
        or "Yes"/"No". Values that cannot be properly cast cause a
        ``ValueError`` to be raised. Once parsed, the querystring arguments
        and keyword arguments are passed to the ``ConnectionPool``'s
        class initializer. In the case of conflicting arguments, querystring
        arguments always win.
        """
        url_options = parse_url(url, _is_async = False)
        if "connection_class" in kwargs:
            url_options["connection_class"] = kwargs["connection_class"]
        kwargs.update(url_options)
        return cls(**kwargs)
    

    def __init__(
        self, 
        connection_class: typing.Type[Connection] = Connection, 
        max_connections: typing.Optional[int] = None, 
        auto_pubsub: typing.Optional[bool] = True,
        pubsub_decode_responses: typing.Optional[bool] = True, # automatically set decode_responses to True
        auto_reset_enabled: typing.Optional[bool] = True,
        **connection_kwargs
    ):
        max_connections = max_connections or 2**31
        if not isinstance(max_connections, int) or max_connections < 0:
            raise ValueError('"max_connections" must be a positive integer')
        try:
            set_ulimits(max_connections)
        except Exception as e:
            logger.debug(f"Unable to set ulimits for connection: {e}")
        self.connection_class = connection_class
        # self.connection_kwargs = connection_kwargs
        self.connection_kwargs = filter_kwargs_for_connection(self.connection_class, connection_kwargs)
        self.max_connections = max_connections

        self._lock: threading.Lock = None
        self._created_connections: int = None
        self._available_connections: typing.List[typing.Type[Connection]] = None
        self._in_use_connections: typing.Set[typing.Type[Connection]] = None

        # a lock to protect the critical section in _checkpid().
        # this lock is acquired when the process id changes, such as
        # after a fork. during this time, multiple threads in the child
        # process could attempt to acquire this lock. the first thread
        # to acquire the lock will reset the data structures and lock
        # object of this pool. subsequent threads acquiring this lock
        # will notice the first thread already did the work and simply
        # release the lock.
        
        self._fork_lock: threading.Lock = threading.Lock()
        self.reset()
        self.encoder_class = self.connection_kwargs.get("encoder_class", Encoder)

        self.auto_pubsub = auto_pubsub
        self.pubsub_decode_responses = pubsub_decode_responses
        self._pubsub_encoder = self.get_pubsub_encoder() if self.auto_pubsub else None
        self._auto_reset_enabled = auto_reset_enabled

    @property
    def db_id(self):
        return self.connection_kwargs.get("db", 0)
    
    def with_db_id(self, db_id: int) -> 'ConnectionPool':
        """
        Return a new connection pool with the specified db_id.
        """
        kwargs = self.connection_kwargs.copy()
        kwargs["db"] = db_id
        return self.__class__(
            connection_class = self.connection_class,
            max_connections = self.max_connections,
            auto_pubsub = self.auto_pubsub,
            pubsub_decode_responses = self.pubsub_decode_responses,
            auto_reset_enabled = self._auto_reset_enabled,
            **kwargs
        )

    def reset(self):
        "Reset the pool"
        if not self._lock:
            self._lock = threading.Lock()
        
        self._created_connections = 0
        self._available_connections = []
        self._in_use_connections = set()

        # this must be the last operation in this method. while reset() is
        # called when holding _fork_lock, other threads in this process
        # can call _checkpid() which compares self.pid and os.getpid() without
        # holding any lock (for performance reasons). keeping this assignment
        # as the last operation ensures that those other threads will also
        # notice a pid difference and block waiting for the first thread to
        # release _fork_lock. when each of these threads eventually acquire
        # _fork_lock, they will notice that another thread already called
        # reset() and they will immediately release _fork_lock and continue on.
        self.pid = os.getpid()
    
    def get_connection(self, command_name, *keys, **options):
        "Get a connection from the pool"
        self._checkpid()
        with self._lock:
            try:
                connection = self._available_connections.pop()
            except IndexError:
                try:
                    connection = self.make_connection()
                except exceptions.ConnectionError as e:
                    if not self._auto_reset_enabled: raise e
                    logger.warning(f'Resetting Connection Pool: {self._created_connections}/{self.max_connections}')
                    self.reset_pool()
                    connection = self.make_connection()

            self._in_use_connections.add(connection)

            if command_name in {'PUBLISH', 'SUBSCRIBE', 'UNSUBSCRIBE'} and self.auto_pubsub:
                connection.encoder = self._pubsub_encoder

        try:
            # ensure this connection is connected to Redis
            connection.connect()
            # connections that the pool provides should be ready to send
            # a command. if not, the connection was either returned to the
            # pool before all data has been read or the socket has been
            # closed. either way, reconnect and verify everything is good.
            try:
                if connection.can_read():
                    raise ConnectionError("Connection has data")
            except (exceptions.ConnectionError, ConnectionError, OSError) as exc:
                connection.disconnect()
                connection.connect()
                if connection.can_read():
                    raise ConnectionError("Connection not ready") from exc
        except BaseException:
            # release the connection back to the pool so that we don't
            # leak it
            self.release(connection)
            raise

        return connection
    

    def get_pubsub_encoder(self):
        """
        Returns the pubsub encoder based on encoding settings
        """
        kwargs = self.connection_kwargs
        return self.encoder_class(
            encoding = kwargs.get("encoding", "utf-8"),
            encoding_errors = kwargs.get("encoding_errors", "strict"),
            decode_responses = self.pubsub_decode_responses or kwargs.get("decode_responses", True),
        )

    def disconnect(
        self, 
        inuse_connections: bool = True,
        raise_exceptions: bool = True,
        with_lock: bool = True,
    ):
        """
        Disconnects connections in the pool

        If ``inuse_connections`` is True, disconnect connections that are
        current in use, potentially by other threads. Otherwise only disconnect
        connections that are idle in the pool.
        """
        self._checkpid()
        if with_lock:
            self._lock.acquire()
        
        # with self._lock:
        if inuse_connections:
            connections = chain(
                self._available_connections, self._in_use_connections
            )
        else:
            connections = self._available_connections

        for connection in connections:
            try:
                connection.disconnect()
            except Exception as e:
                if raise_exceptions: raise e
        if with_lock:
            self._lock.release()
        
    def reset_pool(
        self, 
        inuse_connections: bool = True, 
        raise_exceptions: bool = False,
    ):
        """
        Resets the connection pool
        """
        self.disconnect(inuse_connections = inuse_connections, raise_exceptions = raise_exceptions, with_lock = False)
        self.reset()
    
    @property
    def _stats(self):
        return {
            "pid": self.pid,
            "created_connections": self._created_connections,
            "available_connections": len(self._available_connections),
            "in_use_connections": len(self._in_use_connections),
            "max_connections": self.max_connections,
        }

_ACP = typing.TypeVar("_ACP", bound="AsyncConnectionPool")

class AsyncConnectionPool(_AsyncConnectionPool):

    """
    Create a connection pool. ``If max_connections`` is set, then this
    object raises :py:class:`~redis.ConnectionError` when the pool's
    limit is reached.

    By default, TCP connections are created unless ``connection_class``
    is specified. Use :py:class:`~redis.UnixDomainSocketConnection` for
    unix sockets.

    Any additional keyword arguments are passed to the constructor of
    ``connection_class``.
    """

    @classmethod
    def from_url(cls: typing.Type[_ACP], url: str, **kwargs) -> _ACP:
        """
        Return a connection pool configured from the given URL.

        For example::
            keydb://[[username]:[password]]@localhost:6379/0
            keydbs://[[username]:[password]]@localhost:6379/0
            redis://[[username]:[password]]@localhost:6379/0
            rediss://[[username]:[password]]@localhost:6379/0
            unix://[[username]:[password]]@/path/to/socket.sock?db=0

        Five URL schemes are supported:
        - `keydb://` creates a TCP socket connection.
        - `keydbs://` creates a SSL wrapped TCP socket connection.
        - `redis://` creates a TCP socket connection. See more at:
          <https://www.iana.org/assignments/uri-schemes/prov/redis>
        - `rediss://` creates a SSL wrapped TCP socket connection. See more at:
          <https://www.iana.org/assignments/uri-schemes/prov/rediss>
        - ``unix://``: creates a Unix Domain Socket connection.

        The username, password, hostname, path and all querystring values
        are passed through urllib.parse.unquote in order to replace any
        percent-encoded values with their corresponding characters.

        There are several ways to specify a database number. The first value
        found will be used:
            1. A ``db`` querystring option, e.g. redis://localhost?db=0
            2. If using the redis:// or rediss:// schemes, the path argument
               of the url, e.g. redis://localhost/0
            3. A ``db`` keyword argument to this function.

        If none of these options are specified, the default db=0 is used.

        All querystring options are cast to their appropriate Python types.
        Boolean arguments can be specified with string values "True"/"False"
        or "Yes"/"No". Values that cannot be properly cast cause a
        ``ValueError`` to be raised. Once parsed, the querystring arguments
        and keyword arguments are passed to the ``ConnectionPool``'s
        class initializer. In the case of conflicting arguments, querystring
        arguments always win.
        """
        url_options = parse_url(url, _is_async = True)
        if "connection_class" in kwargs:
            url_options["connection_class"] = kwargs["connection_class"]
        kwargs.update(url_options)
        return cls(**kwargs)
    
    def __init__(
        self, 
        connection_class: typing.Type[AsyncConnection] = AsyncConnection, 
        max_connections: typing.Optional[int] = None, 
        auto_pubsub: typing.Optional[bool] = True,
        pubsub_decode_responses: typing.Optional[bool] = True, # automatically set decode_responses to True
        auto_reset_enabled: typing.Optional[bool] = True,
        **connection_kwargs
    ):
        max_connections = max_connections or 2**31
        if not isinstance(max_connections, int) or max_connections < 0:
            raise ValueError('"max_connections" must be a positive integer')
        try:
            set_ulimits(max_connections)
            # logger.info(f"Set ulimits to {max_connections}")
        except Exception as e:
            logger.debug(f"Unable to set ulimits for connection: {e}")
        self.connection_class: typing.Type[AsyncConnection] = connection_class
        self.connection_kwargs = filter_kwargs_for_connection(self.connection_class, connection_kwargs or {})
        # self.connection_kwargs: typing.Dict[str, typing.Any] = connection_kwargs or {}
        self.max_connections = max_connections

        self._lock: asyncio.Lock = None
        self._created_connections: int = None
        self._available_connections: typing.List[typing.Type[AsyncConnection]] = None
        self._in_use_connections: typing.Set[typing.Type[AsyncConnection]] = None

        # a lock to protect the critical section in _checkpid().
        # this lock is acquired when the process id changes, such as
        # after a fork. during this time, multiple threads in the child
        # process could attempt to acquire this lock. the first thread
        # to acquire the lock will reset the data structures and lock
        # object of this pool. subsequent threads acquiring this lock
        # will notice the first thread already did the work and simply
        # release the lock.
        
        self._fork_lock: threading.Lock = threading.Lock()
        self.reset()
        self.encoder_class = self.connection_kwargs.get("encoder_class", AsyncEncoder)

        self.auto_pubsub = auto_pubsub
        self.pubsub_decode_responses = pubsub_decode_responses
        self._pubsub_encoder = self.get_pubsub_encoder() if self.auto_pubsub else None
        self._auto_reset_enabled = auto_reset_enabled

    @property
    def db_id(self):
        return self.connection_kwargs.get("db", 0)
    
    def with_db_id(self, db_id: int) -> 'AsyncConnectionPool':
        """
        Return a new connection pool with the specified db_id.
        """
        kwargs = self.connection_kwargs.copy()
        kwargs["db"] = db_id
        return self.__class__(
            connection_class=self.connection_class,
            max_connections=self.max_connections,
            auto_pubsub=self.auto_pubsub,
            pubsub_decode_responses=self.pubsub_decode_responses,
            auto_reset_enabled=self._auto_reset_enabled,
            **kwargs,
        )
        

    def reset(self):
        if not self._lock:
            self._lock = asyncio.Lock()
        self._created_connections = 0
        self._available_connections = []
        self._in_use_connections = set()

        # this must be the last operation in this method. while reset() is
        # called when holding _fork_lock, other threads in this process
        # can call _checkpid() which compares self.pid and os.getpid() without
        # holding any lock (for performance reasons). keeping this assignment
        # as the last operation ensures that those other threads will also
        # notice a pid difference and block waiting for the first thread to
        # release _fork_lock. when each of these threads eventually acquire
        # _fork_lock, they will notice that another thread already called
        # reset() and they will immediately release _fork_lock and continue on.
        self.pid = os.getpid()
    
    def get_pubsub_encoder(self):
        """
        Returns the pubsub encoder based on encoding settings
        """
        kwargs = self.connection_kwargs
        return self.encoder_class(
            encoding = kwargs.get("encoding", "utf-8"),
            encoding_errors = kwargs.get("encoding_errors", "strict"),
            decode_responses = self.pubsub_decode_responses or kwargs.get("decode_responses", True),
        )
    

    async def get_connection(self, command_name, *keys, **options):
        """Get a connection from the pool"""
        self._checkpid()
        async with self._lock:
            try:
                connection: typing.Type[AsyncConnection] = self._available_connections.pop()
                # logger.info(f"{command_name} {keys} {options} | Got connection: {connection}: {connection.pid} {self._created_connections}/{self.max_connections}")
            except IndexError:
                try:
                    connection = self.make_connection()
                
                except exceptions.ConnectionError as e:
                    if not self._auto_reset_enabled: raise e

                    logger.warning(f'Resetting Connection Pool: {self._created_connections}/{self.max_connections}')
                    await self.reset_pool()
                    connection = self.make_connection()

            self._in_use_connections.add(connection)
            if command_name in {'PUBLISH', 'SUBSCRIBE', 'UNSUBSCRIBE'} and self.auto_pubsub:
                connection.encoder = self._pubsub_encoder

        try:
            # ensure this connection is connected to Redis
            await connection.connect()
            # connections that the pool provides should be ready to send
            # a command. if not, the connection was either returned to the
            # pool before all data has been read or the socket has been
            # closed. either way, reconnect and verify everything is good.
            try:
                if await connection.can_read_destructive():
                    raise ConnectionError("Connection has data") from None
            except (exceptions.ConnectionError, OSError, ConnectionError):
                await connection.disconnect()
                await connection.connect()
                if await connection.can_read_destructive():
                    raise ConnectionError("Connection not ready") from None
        except BaseException:
            # release the connection back to the pool so that we don't
            # leak it
            await self.release(connection)
            raise

        return connection
    
    async def disconnect(    
        self, 
        inuse_connections: bool = True,
        raise_exceptions: bool = False,
        with_lock: bool = True,
        return_connections: bool = False,
    ):
        """
        Disconnects connections in the pool

        If ``inuse_connections`` is True, disconnect connections that are
        current in use, potentially by other tasks. Otherwise only disconnect
        connections that are idle in the pool.
        """
        self._checkpid()
        if with_lock:
            await self._lock.acquire()
        if inuse_connections:
            connections: typing.Iterable[Connection] = chain(
                self._available_connections, self._in_use_connections
            )
        else:
            connections = self._available_connections
        resp = await asyncio.gather(
            *(connection.disconnect() for connection in connections),
            return_exceptions=True,
        )
        exc = next((r for r in resp if isinstance(r, BaseException)), None)
        if exc and raise_exceptions:
            raise exc
        if with_lock:
            self._lock.release()
        if return_connections:
            return [connection for connection, r in zip(connections, resp) if not isinstance(r, BaseException)]
        

    async def reset_pool(
        self, 
        inuse_connections: bool = True, 
        raise_exceptions: bool = False,
        with_lock: bool = False,
    ):
        """
        Resets the connection pool
        """
        await self.disconnect(inuse_connections = inuse_connections, raise_exceptions = raise_exceptions, with_lock = with_lock, return_connections = True)
        # logger.warning(f"Resetting Connection Pool: {self._created_connections}/{self.max_connections}")
        # for conn in conns:
        #     logger.error(f"Resetting Connection: {conn}: {conn.pid}")
        #     await self.release(conn)
        self.reset()


    @property
    def _stats(self):
        return {
            "pid": self.pid,
            "created_connections": self._created_connections,
            "available_connections": len(self._available_connections),
            "in_use_connections": len(self._in_use_connections),
            "max_connections": self.max_connections,
        }
    

## Blocking Pools

class BlockingConnectionPool(ConnectionPool):
    """
    Thread-safe blocking connection pool::

        >>> from redis.client import Redis
        >>> client = Redis(connection_pool=BlockingConnectionPool())

    It performs the same function as the default
    :py:class:`~redis.ConnectionPool` implementation, in that,
    it maintains a pool of reusable connections that can be shared by
    multiple redis clients (safely across threads if required).

    The difference is that, in the event that a client tries to get a
    connection from the pool when all of connections are in use, rather than
    raising a :py:class:`~redis.ConnectionError` (as the default
    :py:class:`~redis.ConnectionPool` implementation does), it
    makes the client wait ("blocks") for a specified number of seconds until
    a connection becomes available.

    Use ``max_connections`` to increase / decrease the pool size::

        >>> pool = BlockingConnectionPool(max_connections=10)

    Use ``timeout`` to tell it either how many seconds to wait for a connection
    to become available, or to block forever:

        >>> # Block forever.
        >>> pool = BlockingConnectionPool(timeout=None)

        >>> # Raise a ``ConnectionError`` after five seconds if a connection is
        >>> # not available.
        >>> pool = BlockingConnectionPool(timeout=5)
    """

    def __init__(
        self,
        max_connections: typing.Optional[int] = None,
        timeout: typing.Optional[int] = 20,
        connection_class: typing.Type[Connection] = Connection,
        queue_class: typing.Type[Queue] = LifoQueue,
        **connection_kwargs,
    ):
        max_connections = max_connections or 50
        self.queue_class = queue_class
        self.timeout = timeout
        self._connections: typing.List[Connection]
        connection_kwargs = filter_kwargs_for_connection(connection_class, connection_kwargs or {})
        super().__init__(
            connection_class = connection_class,
            max_connections = max_connections,
            **connection_kwargs,
        )

    @property
    def db_id(self):
        return self.connection_kwargs.get("db", 0)
    
    def with_db_id(self, db_id: int) -> 'BlockingConnectionPool':
        """
        Return a new connection pool with the specified db_id.
        """
        kwargs = self.connection_kwargs.copy()
        kwargs["db"] = db_id
        return self.__class__(
            max_connections=self.max_connections,
            timeout=self.timeout,
            connection_class=self.connection_class,
            queue_class=self.queue_class,
            auto_pubsub=self.auto_pubsub,
            pubsub_decode_responses=self.pubsub_decode_responses,
            auto_reset_enabled=self._auto_reset_enabled,
            **kwargs,
        )
    
    def reset(self):
        # Create and fill up a thread safe queue with ``None`` values.
        self.pool = self.queue_class(self.max_connections)
        while True:
            try:
                self.pool.put_nowait(None)
            except Full:
                break

        # Keep a list of actual connection instances so that we can
        # disconnect them later.
        self._connections: typing.List[typing.Type[Connection]] = []

        # this must be the last operation in this method. while reset() is
        # called when holding _fork_lock, other threads in this process
        # can call _checkpid() which compares self.pid and os.getpid() without
        # holding any lock (for performance reasons). keeping this assignment
        # as the last operation ensures that those other threads will also
        # notice a pid difference and block waiting for the first thread to
        # release _fork_lock. when each of these threads eventually acquire
        # _fork_lock, they will notice that another thread already called
        # reset() and they will immediately release _fork_lock and continue on.
        self.pid = os.getpid()

    def make_connection(self) -> typing.Type[Connection]:
        "Make a fresh connection."
        connection = self.connection_class(**self.connection_kwargs)
        self._connections.append(connection)
        return connection

    def get_connection(self, command_name, *keys, **options):
        """
        Get a connection, blocking for ``self.timeout`` until a connection
        is available from the pool.

        If the connection returned is ``None`` then creates a new connection.
        Because we use a last-in first-out queue, the existing connections
        (having been returned to the pool after the initial ``None`` values
        were added) will be returned before ``None`` values. This means we only
        create new connections when we need to, i.e.: the actual number of
        connections will only increase in response to demand.
        """
        # Make sure we haven't changed process.
        self._checkpid()

        # Try and get a connection from the pool. If one isn't available within
        # self.timeout then raise a ``ConnectionError``.
        connection = None
        try:
            connection = self.pool.get(block=True, timeout=self.timeout)
        except Empty as e:
            # Note that this is not caught by the redis client and will be
            # raised unless handled by application code. If you want never to
            if not self._auto_reset_enabled:
                raise ConnectionError(
                    "No connection available. Try enabling `auto_reset_enabled`."
                ) from e
            logger.warning(f'Resetting Pool: {len(self._connections)}/{self.pool.qsize()}/{self.max_connections} due to error: {e}')
            self.reset_pool()

        # If the ``connection`` is actually ``None`` then that's a cue to make
        # a new connection to add to the pool.
        if connection is None:
            connection = self.make_connection()

        try:
            # ensure this connection is connected to Redis
            connection.connect()
            # connections that the pool provides should be ready to send
            # a command. if not, the connection was either returned to the
            # pool before all data has been read or the socket has been
            # closed. either way, reconnect and verify everything is good.

            if command_name in {'PUBLISH', 'SUBSCRIBE', 'UNSUBSCRIBE'} and self.auto_pubsub:
                connection.encoder = self._pubsub_encoder

            try:
                if connection.can_read():
                    raise ConnectionError("Connection has data")
            except (exceptions.ConnectionError, OSError, ConnectionError) as exc:
                connection.disconnect()
                connection.connect()
                if connection.can_read():
                    raise ConnectionError("Connection not ready") from exc
        except BaseException:
            # release the connection back to the pool so that we don't leak it
            self.release(connection)
            raise

        return connection

    def release(self, connection):
        "Releases the connection back to the pool."
        # Make sure we haven't changed process.
        self._checkpid()
        if not self.owns_connection(connection):
            # pool doesn't own this connection. do not add it back
            # to the pool. instead add a None value which is a placeholder
            # that will cause the pool to recreate the connection if
            # its needed.
            connection.disconnect()
            self.pool.put_nowait(None)
            return

        # Put the connection back into the pool.
        with contextlib.suppress(Full):
            self.pool.put_nowait(connection)

    def disconnect(self):
        "Disconnects all connections in the pool."
        self._checkpid()
        for connection in self._connections:
            connection.disconnect()


    @property
    def _stats(self):
        return {
            "pid": self.pid,
            "available_connections": self.pool.qsize(),
            "in_use_connections": len(self._connections),
            "max_connections": self.max_connections,
        }

class AsyncBlockingConnectionPool(AsyncConnectionPool):
    """
    Thread-safe blocking connection pool::

        >>> from redis.client import Redis
        >>> client = Redis(connection_pool=BlockingConnectionPool())

    It performs the same function as the default
    :py:class:`~redis.ConnectionPool` implementation, in that,
    it maintains a pool of reusable connections that can be shared by
    multiple redis clients (safely across threads if required).

    The difference is that, in the event that a client tries to get a
    connection from the pool when all of connections are in use, rather than
    raising a :py:class:`~redis.ConnectionError` (as the default
    :py:class:`~redis.ConnectionPool` implementation does), it
    makes the client wait ("blocks") for a specified number of seconds until
    a connection becomes available.

    Use ``max_connections`` to increase / decrease the pool size::

        >>> pool = BlockingConnectionPool(max_connections=10)

    Use ``timeout`` to tell it either how many seconds to wait for a connection
    to become available, or to block forever:

        >>> # Block forever.
        >>> pool = BlockingConnectionPool(timeout=None)

        >>> # Raise a ``ConnectionError`` after five seconds if a connection is
        >>> # not available.
        >>> pool = BlockingConnectionPool(timeout=5)
    """

    def __init__(
        self,
        max_connections: typing.Optional[int] = None,
        timeout: typing.Optional[int] = 20,
        connection_class: typing.Type[AsyncConnection] = AsyncConnection,
        queue_class: typing.Type[asyncio.Queue] = asyncio.LifoQueue,
        **connection_kwargs,
    ):
        max_connections = max_connections or 50
        self.queue_class = queue_class
        self.timeout = timeout
        self._connections: typing.List[AsyncConnection]
        connection_kwargs = filter_kwargs_for_connection(connection_class, connection_kwargs or {})
        super().__init__(
            connection_class = connection_class,
            max_connections = max_connections,
            **connection_kwargs,
        )


    @property
    def db_id(self):
        return self.connection_kwargs.get("db", 0)
    
    def with_db_id(self, db_id: int) -> 'AsyncBlockingConnectionPool':
        """
        Return a new connection pool with the specified db_id.
        """
        kwargs = self.connection_kwargs.copy()
        kwargs["db"] = db_id
        return self.__class__(
            max_connections=self.max_connections,
            timeout=self.timeout,
            connection_class=self.connection_class,
            queue_class=self.queue_class,
            auto_pubsub=self.auto_pubsub,
            pubsub_decode_responses=self.pubsub_decode_responses,
            auto_reset_enabled=self._auto_reset_enabled,
            **kwargs,
        )
    
    def reset(self):
        # Create and fill up a thread safe queue with ``None`` values.
        self.pool = self.queue_class(self.max_connections)
        while True:
            try:
                self.pool.put_nowait(None)
            except asyncio.QueueFull:
                break

        # Keep a list of actual connection instances so that we can
        # disconnect them later.
        self._connections = []

        # this must be the last operation in this method. while reset() is
        # called when holding _fork_lock, other threads in this process
        # can call _checkpid() which compares self.pid and os.getpid() without
        # holding any lock (for performance reasons). keeping this assignment
        # as the last operation ensures that those other threads will also
        # notice a pid difference and block waiting for the first thread to
        # release _fork_lock. when each of these threads eventually acquire
        # _fork_lock, they will notice that another thread already called
        # reset() and they will immediately release _fork_lock and continue on.
        self.pid = os.getpid()

    def make_connection(self) -> typing.Type[AsyncConnection]:
        """Make a fresh connection."""
        connection = self.connection_class(**self.connection_kwargs)
        self._connections.append(connection)
        return connection

    async def get_connection(self, command_name, *keys, **options):
        """
        Get a connection, blocking for ``self.timeout`` until a connection
        is available from the pool.

        If the connection returned is ``None`` then creates a new connection.
        Because we use a last-in first-out queue, the existing connections
        (having been returned to the pool after the initial ``None`` values
        were added) will be returned before ``None`` values. This means we only
        create new connections when we need to, i.e.: the actual number of
        connections will only increase in response to demand.
        """
        # Make sure we haven't changed process.
        self._checkpid()

        # Try and get a connection from the pool. If one isn't available within
        # self.timeout then raise a ``ConnectionError``.
        connection = None
        try:
            async with async_timeout(self.timeout):
                connection = await self.pool.get()
        except (asyncio.QueueEmpty, asyncio.TimeoutError) as e:
            # Note that this is not caught by the redis client and will be
            # raised unless handled by application code. If you want never to
            # raise ConnectionError("No connection available.")
            if not self._auto_reset_enabled:
                raise ConnectionError(
                    "No connection available. Try enabling `auto_reset_enabled`"
                ) from e

            logger.warning(f'Resetting Pool: {len(self._connections)}/{self.pool.qsize()}/{self.max_connections} due to error: {e}')
            await self.reset_pool()

        # If the ``connection`` is actually ``None`` then that's a cue to make
        # a new connection to add to the pool.
        if connection is None:
            connection = self.make_connection()

        try:
            # ensure this connection is connected to Redis
            await connection.connect()
            # connections that the pool provides should be ready to send
            # a command. if not, the connection was either returned to the
            # pool before all data has been read or the socket has been
            # closed. either way, reconnect and verify everything is good.

            if command_name in {'PUBLISH', 'SUBSCRIBE', 'UNSUBSCRIBE'} and self.auto_pubsub:
                connection.encoder = self._pubsub_encoder

            try:
                if await connection.can_read_destructive():
                    raise ConnectionError("Connection has data") from None
            except (exceptions.ConnectionError, OSError, ConnectionError):
                await connection.disconnect()
                await connection.connect()
                if await connection.can_read_destructive():
                    raise ConnectionError("Connection not ready") from None
        
        except BaseException:
            # release the connection back to the pool so that we don't leak it
            await self.release(connection)
            raise

        return connection

    async def release(self, connection: AsyncConnection):
        """Releases the connection back to the pool."""
        # Make sure we haven't changed process.
        self._checkpid()
        if not self.owns_connection(connection):
            # pool doesn't own this connection. do not add it back
            # to the pool. instead add a None value which is a placeholder
            # that will cause the pool to recreate the connection if
            # its needed.
            await connection.disconnect()
            self.pool.put_nowait(None)
            return

        # Put the connection back into the pool.
        try:
            self.pool.put_nowait(connection)
        except asyncio.QueueFull:
            # perhaps the pool has been reset() after a fork? regardless,
            # we don't want this connection
            pass

    async def disconnect(self, inuse_connections: bool = True, raise_exceptions: bool = False):
        """Disconnects all connections in the pool."""
        self._checkpid()
        async with self._lock:
            resp = await asyncio.gather(
                *(connection.disconnect() for connection in self._connections),
                return_exceptions=True,
            )
            exc = next((r for r in resp if isinstance(r, BaseException)), None)
            if exc:
                raise exc

    @property
    def _stats(self):
        return {
            "pid": self.pid,
            "available_connections": self.pool.qsize(),
            "in_use_connections": len(self._connections),
            "max_connections": self.max_connections,
        }