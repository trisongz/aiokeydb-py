from aiokeydb.client import KeyDB, StrictKeyDB
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
from aiokeydb.utils import from_url


def int_or_str(value):
    try:
        return int(value)
    except ValueError:
        return value


__version__ = "0.0.1"
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
    "KeyDBError",
    "ResponseError",
    "SSLConnection",
    "StrictKeyDB",
    "TimeoutError",
    "UnixDomainSocketConnection",
    "WatchError",
]