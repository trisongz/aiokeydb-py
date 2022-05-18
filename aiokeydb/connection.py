
"""
Modified from 

https://github.com/aio-libs/aioredis-py/blob/master/aioredis/connection.py
"""

import os
import threading
import asyncio
import async_timeout

from aioredis.connection import (
    TypeVar,
    Type,
    Optional,
    Encoder,
    List,
    Set,

)
from aioredis.connection import (
    Connection,
    ConnectKwargs,
    ParseResult,
    urlparse,
    parse_qs,
    unquote,
    URL_QUERY_ARGUMENT_PARSERS,
    UnixDomainSocketConnection,
    SSLConnection,
    EncodableT,
)

from aioredis.connection import ConnectionPool as _ConnectionPool

def parse_url(url: str) -> ConnectKwargs:
    parsed: ParseResult = urlparse(url)
    kwargs: ConnectKwargs = {}

    for name, value_list in parse_qs(parsed.query).items():
        if value_list and len(value_list) > 0:
            value = unquote(value_list[0])
            parser = URL_QUERY_ARGUMENT_PARSERS.get(name)
            if parser:
                try:
                    # We can't type this.
                    kwargs[name] = parser(value)  # type: ignore[misc]
                except (TypeError, ValueError):
                    raise ValueError(f"Invalid value for `{name}` in connection URL.")
            else:
                kwargs[name] = value  # type: ignore[misc]

    if parsed.username:
        kwargs["username"] = unquote(parsed.username)
    if parsed.password:
        kwargs["password"] = unquote(parsed.password)

    # We only support keydb://, keydbs://, redis://, rediss:// and unix:// schemes.
    if parsed.scheme == "unix":
        if parsed.path:
            kwargs["path"] = unquote(parsed.path)
        kwargs["connection_class"] = UnixDomainSocketConnection

    elif parsed.scheme in {"keydb", "keydbs", "redis", "rediss"}:
        if parsed.hostname:
            kwargs["host"] = unquote(parsed.hostname)
        if parsed.port:
            kwargs["port"] = int(parsed.port)

        # If there's a path argument, use it as the db argument if a
        # querystring value wasn't specified
        if parsed.path and "db" not in kwargs:
            try:
                kwargs["db"] = int(unquote(parsed.path).replace("/", ""))
            except (AttributeError, ValueError):
                pass

        if parsed.scheme in {"rediss", "keydbs"}:
            kwargs["connection_class"] = SSLConnection
    else:
        valid_schemes = "keydb://, keydbs://, redis://, rediss://, unix:// "
        raise ValueError(
            f"KeyDB URL must specify one of the following schemes ({valid_schemes})"
        )

    return kwargs


_CP = TypeVar("_CP", bound="ConnectionPool")


class ConnectionPool(_ConnectionPool):
    """
    Create a connection pool. ``If max_connections`` is set, then this
    object raises :py:class:`~keydb.ConnectionError` when the pool's
    limit is reached.

    By default, TCP connections are created unless ``connection_class``
    is specified. Use :py:class:`~keydb.UnixDomainSocketConnection` for
    unix sockets.

    Any additional keyword arguments are passed to the constructor of
    ``connection_class``.
    """

    @classmethod
    def from_url(cls: Type[_CP], url: str, **kwargs) -> _CP:
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
            1. A ``db`` querystring option, e.g. keydb://localhost?db=0 / redis://localhost?db=0
            2. If using the keydb://, keydbs://, redis:// or rediss:// schemes, the path argument
               of the url, e.g.  keydb://localhost/0 / redis://localhost/0
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
        url_options = parse_url(url)
        kwargs.update(url_options)
        return cls(**kwargs)

    def __init__(
        self,
        connection_class: Type[Connection] = Connection,
        max_connections: Optional[int] = None,
        **connection_kwargs,
    ):
        max_connections = max_connections or 2 ** 31
        if not isinstance(max_connections, int) or max_connections < 0:
            raise ValueError('"max_connections" must be a positive integer')

        self.connection_class = connection_class
        self.connection_kwargs = connection_kwargs
        self.max_connections = max_connections

        # a lock to protect the critical section in _checkpid().
        # this lock is acquired when the process id changes, such as
        # after a fork. during this time, multiple threads in the child
        # process could attempt to acquire this lock. the first thread
        # to acquire the lock will reset the data structures and lock
        # object of this pool. subsequent threads acquiring this lock
        # will notice the first thread already did the work and simply
        # release the lock.
        self._fork_lock = threading.Lock()
        self._lock = asyncio.Lock()
        self._created_connections: int
        self._available_connections: List[Connection]
        self._in_use_connections: Set[Connection]
        self.reset()  # lgtm [py/init-calls-subclass]
        self.encoder_class = self.connection_kwargs.get("encoder_class", Encoder)



class BlockingConnectionPool(ConnectionPool):
    """
    Thread-safe blocking connection pool::

        >>> from aiokeydb.client import KeyDB
        >>> client = KeyDB(connection_pool=BlockingConnectionPool())

    It performs the same function as the default
    :py:class:`~keydb.ConnectionPool` implementation, in that,
    it maintains a pool of reusable connections that can be shared by
    multiple keydb clients (safely across threads if required).

    The difference is that, in the event that a client tries to get a
    connection from the pool when all of connections are in use, rather than
    raising a :py:class:`~keydb.ConnectionError` (as the default
    :py:class:`~keydb.ConnectionPool` implementation does), it
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
        max_connections: int = 50,
        timeout: Optional[int] = 20,
        connection_class: Type[Connection] = Connection,
        queue_class: Type[asyncio.Queue] = asyncio.LifoQueue,
        **connection_kwargs,
    ):

        self.queue_class = queue_class
        self.timeout = timeout
        self._connections: List[Connection]
        super().__init__(
            connection_class=connection_class,
            max_connections=max_connections,
            **connection_kwargs,
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

    def make_connection(self):
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
            async with async_timeout.timeout(self.timeout):
                connection = await self.pool.get()
        except (asyncio.QueueEmpty, asyncio.TimeoutError):
            # Note that this is not caught by the redis client and will be
            # raised unless handled by application code. If you want never to
            raise ConnectionError("No connection available.")

        # If the ``connection`` is actually ``None`` then that's a cue to make
        # a new connection to add to the pool.
        if connection is None:
            connection = self.make_connection()

        try:
            # ensure this connection is connected to KeyDB
            await connection.connect()
            # connections that the pool provides should be ready to send
            # a command. if not, the connection was either returned to the
            # pool before all data has been read or the socket has been
            # closed. either way, reconnect and verify everything is good.
            try:
                if await connection.can_read():
                    raise ConnectionError("Connection has data") from None
            except ConnectionError:
                await connection.disconnect()
                await connection.connect()
                if await connection.can_read():
                    raise ConnectionError("Connection not ready") from None
        except BaseException:
            # release the connection back to the pool so that we don't leak it
            await self.release(connection)
            raise

        return connection

    async def release(self, connection: Connection):
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

    async def disconnect(self, inuse_connections: bool = True):
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
