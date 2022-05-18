"""Core exceptions raised by the KeyDB client"""
import asyncio
import builtins


class KeyDBError(Exception):
    pass


class ConnectionError(KeyDBError):
    pass


class TimeoutError(asyncio.TimeoutError, builtins.TimeoutError, KeyDBError):
    pass


class AuthenticationError(ConnectionError):
    pass


class BusyLoadingError(ConnectionError):
    pass


class InvalidResponse(KeyDBError):
    pass


class ResponseError(KeyDBError):
    pass


class DataError(KeyDBError):
    pass


class PubSubError(KeyDBError):
    pass


class WatchError(KeyDBError):
    pass


class NoScriptError(ResponseError):
    pass


class ExecAbortError(ResponseError):
    pass


class ReadOnlyError(ResponseError):
    pass


class NoPermissionError(ResponseError):
    pass


class ModuleError(ResponseError):
    pass


class LockError(KeyDBError, ValueError):
    """Errors acquiring or releasing a lock"""

    # NOTE: For backwards compatibility, this class derives from ValueError.
    # This was originally chosen to behave like threading.Lock.
    pass


class LockNotOwnedError(LockError):
    """Error trying to extend or release a lock that is (no longer) owned"""

    pass


class ChildDeadlockedError(Exception):
    """Error indicating that a child process is deadlocked after a fork()"""

    pass


class AuthenticationWrongNumberOfArgsError(ResponseError):
    """
    An error to indicate that the wrong number of args
    were sent to the AUTH command
    """

    pass
