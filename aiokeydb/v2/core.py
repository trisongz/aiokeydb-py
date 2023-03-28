import typing
import logging

from redis.commands import (
    CoreCommands,
    AsyncCoreCommands,
    RedisModuleCommands,
    SentinelCommands,
    AsyncSentinelCommands,
)
from redis.client import (
    #AbstractRedis,
    Redis,
    Pipeline,
    PubSub,
    CaseInsensitiveDict,
    parse_scan,
    
)
from redis.asyncio.client import (
    Redis as AsyncRedis,
    PubSub as AsyncPubSub,
    Pipeline as AsyncPipeline,
)


from aiokeydb.v2.connection import (
    ConnectionPool,
    AsyncConnectionPool,
)


logger = logging.getLogger(__name__)


class KeyDB(Redis):
    """
    Implementation of the KeyDB protocol.

    This abstract class provides a Python interface to all KeyDB commands
    and an implementation of the KeyDB protocol.

    Pipelines derive from this, implementing how
    the commands are sent and received to the KeyDB server. Based on
    configuration, an instance will either use a ConnectionPool, or
    Connection object to talk to keydb.

    It is not safe to pass PubSub or Pipeline objects between threads.
    """

    @property
    def is_async(self):
        return False

    @classmethod
    def from_url(
        cls, 
        url, 
        pool_class: typing.Optional[typing.Type[ConnectionPool]] = ConnectionPool,
        **kwargs
    ):
        """
        Return a Redis client object configured from the given URL

        For example::

            keydb://[[username]:[password]]@localhost:6379/0
            keydbs://[[username]:[password]]@localhost:6379/0
            redis://[[username]:[password]]@localhost:6379/0
            rediss://[[username]:[password]]@localhost:6379/0
            unix://[username@]/path/to/socket.sock?db=0[&password=password]

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
        if not pool_class: pool_class = ConnectionPool
        connection_pool = pool_class.from_url(url, **kwargs)
        return cls(connection_pool=connection_pool)


class AsyncKeyDB(AsyncRedis):
    """
    Implementation of the KeyDB protocol.

    This abstract class provides a Python interface to all KeyDB commands
    and an implementation of the KeyDB protocol.

    Pipelines derive from this, implementing how
    the commands are sent and received to the KeyDB server. Based on
    configuration, an instance will either use a AsyncConnectionPool, or
    AsyncConnection object to talk to redis.
    """

    @property
    def is_async(self):
        return True
    
    @classmethod
    def from_url(
        cls, 
        url: str, 
        pool_class: typing.Optional[typing.Type[AsyncConnectionPool]] = AsyncConnectionPool,
        single_connection_client: bool = False,
        **kwargs
    ):
        """
        Return a KeyDB client object configured from the given URL

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
        and keyword arguments are passed to the ``AsyncConnectionPool``'s
        class initializer. In the case of conflicting arguments, querystring
        arguments always win.

        """
        if pool_class is None: pool_class = AsyncConnectionPool
        connection_pool = pool_class.from_url(url, **kwargs)
        return cls(
            connection_pool = connection_pool,
            single_connection_client = single_connection_client,
        )


