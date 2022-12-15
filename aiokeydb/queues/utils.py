
import time
import uuid
import random
import typing
import traceback
import contextlib
import contextvars
import asyncio
import functools
from concurrent import futures

if typing.TYPE_CHECKING:
    from aiokeydb.client.config import KeyDBSettings

from lazyops.utils.logs import default_logger as logger

_QueueSettings: 'KeyDBSettings' = None
_NodeName: str = None
_ThreadPool: futures.ThreadPoolExecutor = None

def get_settings() -> 'KeyDBSettings':
    """
    Lazily initialize the worker settings
    """
    global _QueueSettings
    if _QueueSettings is None:
        from aiokeydb.client.core import KeyDBClient
        _QueueSettings = KeyDBClient.get_settings()
    return _QueueSettings

def get_hostname() -> str:
    """
    Lazily initialize the worker node name
    """
    global _NodeName
    if _NodeName is None:
        import socket
        while _NodeName is None:
            with contextlib.suppress(Exception):
                _NodeName = socket.gethostname()
            time.sleep(0.5)
    return _NodeName

def get_thread_pool(
    n_workers: int = None
) -> futures.ThreadPoolExecutor:
    """
    Lazily initialize the worker thread pool
    """
    global _ThreadPool
    if _ThreadPool is None:
        if n_workers is None:
            settings = get_settings()
            n_workers = settings.worker.threadpool_size
        _ThreadPool = futures.ThreadPoolExecutor(max_workers = n_workers)
    return _ThreadPool



async def run_in_executor(ctx: typing.Dict[str, typing.Any], func: typing.Callable, *args, **kwargs):
    blocking = functools.partial(func, *args, **kwargs)
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(ctx['pool'], blocking)

def get_and_log_exc():
    error = traceback.format_exc()
    logger.error(f'node={get_hostname()}, {error}')
    return error


def now():
    return int(time.time() * 1000)

def uuid1():
    return str(uuid.uuid1())

def uuid4():
    return str(uuid.uuid4())

def millis(s):
    return s * 1000

def seconds(ms):
    return ms / 1000


def exponential_backoff(
    attempts,
    base_delay,
    max_delay=None,
    jitter=True,
):
    """
    Get the next delay for retries in exponential backoff.

    attempts: Number of attempts so far
    base_delay: Base delay, in seconds
    max_delay: Max delay, in seconds. If None (default), there is no max.
    jitter: If True, add a random jitter to the delay
    """
    if max_delay is None:
        max_delay = float("inf")
    backoff = min(max_delay, base_delay * 2 ** max(attempts - 1, 0))
    if jitter:
        backoff = backoff * random.random()
    return backoff


_JobKeyMethod = {
    'uuid1': uuid1,
    'uuid4': uuid4,
}

_JobKeyFunc: typing.Callable = None

def _get_jobkey_func():
    global _JobKeyFunc
    if _JobKeyFunc is None:
        _JobKeyFunc = _JobKeyMethod[get_settings().worker.job_key_method]
    return _JobKeyFunc()

def get_default_job_key():
    return _get_jobkey_func()


def ensure_coroutine_function(func):
    if asyncio.iscoroutinefunction(func):
        return func

    async def wrapped(*args, **kwargs):
        loop = asyncio.get_running_loop()
        ctx = contextvars.copy_context()
        return await loop.run_in_executor(
            executor = None, func = lambda: ctx.run(func, *args, **kwargs)
        )

    return wrapped