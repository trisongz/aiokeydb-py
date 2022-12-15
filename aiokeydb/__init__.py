from __future__ import absolute_import

import sys

from aiokeydb.core import KeyDB, StrictKeyDB
from aiokeydb.cluster import KeyDBCluster
from aiokeydb.connection import (
    BlockingConnectionPool,
    Connection,
    ConnectionPool,
    SSLConnection,
    UnixDomainSocketConnection,
)
from aiokeydb.exceptions import (
    AuthenticationError,
    AuthenticationWrongNumberOfArgsError,
    BusyLoadingError,
    ChildDeadlockedError,
    ConnectionError,
    DataError,
    InvalidResponse,
    PubSubError,
    ReadOnlyError,
    KeyDBError,
    ResponseError,
    TimeoutError,
    WatchError,
)
from aiokeydb.sentinel import (
    Sentinel,
    SentinelConnectionPool,
    SentinelManagedConnection,
    SentinelManagedSSLConnection,
)
from aiokeydb.utils import from_url

# Handle Async

from aiokeydb.asyncio import (
    AsyncKeyDB, 
    StrictAsyncKeyDB,
    AsyncBlockingConnectionPool,
    AsyncConnection,
    AsyncConnectionPool,
    AsyncSSLConnection,
    AsyncUnixDomainSocketConnection,
    AsyncSentinel,
    AsyncSentinelConnectionPool,
    AsyncSentinelManagedConnection,
    AsyncSentinelManagedSSLConnection,
    async_from_url
)

# Handle Client

from aiokeydb.client.serializers import SerializerType
from aiokeydb.client.config import KeyDBSettings
from aiokeydb.client.schemas.session import KeyDBSession
from aiokeydb.client.core import KeyDBClient

if sys.version_info >= (3, 8):
    from importlib import metadata
else:
    import importlib_metadata as metadata


def int_or_str(value):
    try:
        return int(value)
    except ValueError:
        return value

from aiokeydb.version import VERSION as __version__


# try:
#     __version__ = metadata.version("aiokeydb")
# except metadata.PackageNotFoundError:
#     __version__ = "99.99.99"


VERSION = tuple(map(int_or_str, __version__.split(".")))

__all__ = [
    "AuthenticationError",
    "AuthenticationWrongNumberOfArgsError",
    "BlockingConnectionPool",
    "BusyLoadingError",
    "ChildDeadlockedError",
    "Connection",
    "ConnectionError",
    "ConnectionPool",
    "DataError",
    "from_url",
    "InvalidResponse",
    "PubSubError",
    "ReadOnlyError",
    "KeyDB",
    "KeyDBCluster",
    "KeyDBError",
    "ResponseError",
    "Sentinel",
    "SentinelConnectionPool",
    "SentinelManagedConnection",
    "SentinelManagedSSLConnection",
    "SSLConnection",
    "StrictKeyDB",
    "TimeoutError",
    "UnixDomainSocketConnection",
    "WatchError",
    # Async
    "AsyncKeyDB", 
    "StrictAsyncKeyDB",
    "AsyncBlockingConnectionPool",
    "AsyncConnection",
    "AsyncConnectionPool",
    "AsyncSSLConnection",
    "AsyncUnixDomainSocketConnection",
    "AsyncSentinel",
    "AsyncSentinelConnectionPool",
    "AsyncSentinelManagedConnection",
    "AsyncSentinelManagedSSLConnection",
    "async_from_url",

    # Client
    "SerializerType",
    "KeyDBSettings",
    "KeyDBSession",
    "KeyDBClient",
]
