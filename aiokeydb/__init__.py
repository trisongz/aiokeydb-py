from __future__ import absolute_import

from aiokeydb.core import KeyDB, AsyncKeyDB
from aiokeydb.cluster import KeyDBCluster, AsyncKeyDBCluster
from aiokeydb.connection import (
    BlockingConnectionPool,
    Connection,
    ConnectionPool,
    SSLConnection,
    UnixDomainSocketConnection,
    AsyncBlockingConnectionPool,
    AsyncConnection,
    AsyncConnectionPool,
    AsyncSSLConnection,
    AsyncUnixDomainSocketConnection,
)
from redis.credentials import CredentialProvider, UsernamePasswordCredentialProvider
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
    ResponseError,
    TimeoutError,
    WatchError,
    JobError,
)
from aiokeydb.sentinel import (
    Sentinel,
    SentinelConnectionPool,
    SentinelManagedConnection,
    AsyncSentinel,
    AsyncSentinelConnectionPool,
    AsyncSentinelManagedConnection,
)

from aiokeydb.utils.base import from_url
from aiokeydb.utils.lazy import get_keydb_settings

# Handle Client
from aiokeydb.serializers import SerializerType
from aiokeydb.configs import KeyDBSettings, KeyDBWorkerSettings, settings
from aiokeydb.types.session import KeyDBSession
from aiokeydb.client import KeyDBClient

# Handle Queues
from aiokeydb.types.jobs import Job, CronJob
from aiokeydb.types.task_queue import TaskQueue
from aiokeydb.types.worker import Worker

# Add KeyDB Index Types
from aiokeydb.types.indexes import (
    KDBIndex,
    KDBDict,
    AsyncKDBDict,
)

from aiokeydb.version import VERSION as __version__

def int_or_str(value):
    try:
        return int(value)
    except ValueError:
        return value


VERSION = tuple(map(int_or_str, __version__.split(".")))

# Job.update_forward_refs()

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
    # "KeyDBError",
    "ResponseError",
    "Sentinel",
    "SentinelConnectionPool",
    "SentinelManagedConnection",
    "SentinelManagedSSLConnection",
    "SSLConnection",
    # "StrictKeyDB",
    "TimeoutError",
    "UnixDomainSocketConnection",
    "WatchError",
    "JobError",
    "CredentialProvider",
    "UsernamePasswordCredentialProvider",
    # Async
    "AsyncKeyDB", 
    "AsyncKeyDBCluster",
    # "StrictAsyncKeyDB",
    "AsyncBlockingConnectionPool",
    "AsyncConnection",
    "AsyncConnectionPool",
    "AsyncSSLConnection",
    "AsyncUnixDomainSocketConnection",
    "AsyncSentinel",
    "AsyncSentinelConnectionPool",
    "AsyncSentinelManagedConnection",
    # "AsyncSentinelManagedSSLConnection",

    # Client
    "SerializerType",
    "KeyDBSettings",
    "KeyDBWorkerSettings",
    "KeyDBSession",
    "KeyDBClient",

    # Queues
    "TaskQueue",
    "Worker",
]
