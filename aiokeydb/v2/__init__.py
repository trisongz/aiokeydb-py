from __future__ import absolute_import

from aiokeydb.v2.core import KeyDB, AsyncKeyDB
from aiokeydb.v2.cluster import KeyDBCluster, AsyncKeyDBCluster
from aiokeydb.v2.connection import (
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
from aiokeydb.v2.exceptions import (
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
from aiokeydb.v2.sentinel import (
    Sentinel,
    SentinelConnectionPool,
    SentinelManagedConnection,
    AsyncSentinel,
    AsyncSentinelConnectionPool,
    AsyncSentinelManagedConnection,
)

from aiokeydb.v2.utils import from_url

# Handle Client
from aiokeydb.v2.serializers import SerializerType
from aiokeydb.v2.configs import KeyDBSettings, KeyDBWorkerSettings, settings
from aiokeydb.v2.types.session import KeyDBSession
from aiokeydb.v2.client import KeyDBClient

# Handle Queues
from aiokeydb.v2.types.jobs import Job, CronJob
from aiokeydb.v2.types.task_queue import TaskQueue
from aiokeydb.v2.types.worker import Worker

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
