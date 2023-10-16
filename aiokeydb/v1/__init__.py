"""
Backwards compatible import for aiokeydb.v1.v1
"""

from __future__ import absolute_import

import sys

from aiokeydb.v1.core import KeyDB, StrictKeyDB
from aiokeydb.v1.cluster import KeyDBCluster
from aiokeydb.v1.connection import (
    BlockingConnectionPool,
    Connection,
    ConnectionPool,
    SSLConnection,
    UnixDomainSocketConnection,
)
from aiokeydb.v1.credentials import CredentialProvider, UsernamePasswordCredentialProvider
from aiokeydb.v1.exceptions import (
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
from aiokeydb.v1.sentinel import (
    Sentinel,
    SentinelConnectionPool,
    SentinelManagedConnection,
    SentinelManagedSSLConnection,
)
from aiokeydb.v1.utils import from_url

# Handle Async

from aiokeydb.v1.asyncio import (
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

from aiokeydb.v1.client.serializers import SerializerType
from aiokeydb.v1.client.config import KeyDBSettings
from aiokeydb.v1.client.schemas.session import KeyDBSession
from aiokeydb.v1.client.meta import KeyDBClient

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
    "CredentialProvider",
    "UsernamePasswordCredentialProvider",
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
