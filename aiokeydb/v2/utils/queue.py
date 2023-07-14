from __future__ import annotations

import os
import time
import uuid
import random
import typing
import logging
import traceback
import contextlib
import contextvars
import asyncio
import functools
import inspect

from concurrent import futures
from lazyops.utils.helpers import import_function, timer, build_batches
from lazyops.utils.ahelpers import amap_v2 as concurrent_map

logger = logging.getLogger(__name__)

_NodeName: str = None
_ThreadPool: futures.ThreadPoolExecutor = None

if typing.TYPE_CHECKING:
    from aiokeydb.v2.types.jobs import Job

def get_hostname() -> str:
    """
    Lazily initialize the worker node name
    """
    global _NodeName
    if _NodeName is None:
        if os.getenv('HOSTNAME'):
            _NodeName = os.getenv('HOSTNAME')
        else:
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
            from aiokeydb.v2.configs import settings
            n_workers = settings.worker.threadpool_size
        _ThreadPool = futures.ThreadPoolExecutor(max_workers = n_workers)
    return _ThreadPool


async def run_in_executor(ctx: typing.Dict[str, typing.Any], func: typing.Callable, *args, **kwargs):
    blocking = functools.partial(func, *args, **kwargs)
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(ctx['pool'], blocking)


def get_func_name(func: typing.Union[str, typing.Callable]) -> str:
    """
    Returns the function name
    """
    return func.__name__ if callable(func) else func


def get_func_full_name(func: typing.Union[str, typing.Callable]) -> str:
    """
    Returns the function name
    """
    return f"{func.__module__}.{func.__qualname__}" if callable(func) else func

def get_and_log_exc(
    job: typing.Optional['Job'] = None, 
    func: typing.Optional[typing.Union[str, typing.Callable]] = None
):
    error = traceback.format_exc()
    err_msg = f'node={get_hostname()}, {error}'
    if func: err_msg = f'func={get_func_name(func)}, {err_msg}'
    elif job: err_msg = f'job={job.short_repr}, {err_msg}'
    logger.error(err_msg)
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
        from aiokeydb.v2.configs import settings
        _JobKeyFunc = _JobKeyMethod[settings.worker.job_key_method]
    return _JobKeyFunc()

def get_default_job_key():
    return _get_jobkey_func()

def isclassmethod(method):
    bound_to = getattr(method, '__self__', None)
    if not isinstance(bound_to, type):
        # must be bound to a class
        return False
    name = method.__name__
    for cls in bound_to.__mro__:
        descriptor = vars(cls).get(name)
        if descriptor is not None:
            return isinstance(descriptor, classmethod)
    return False

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


_bg_tasks: typing.Set[asyncio.Task] = set()


def create_background_task(func: typing.Union[typing.Callable, typing.Coroutine], *args, **kwargs):
    """
    Creates a background task and adds it to the global set of background tasks
    """
    if inspect.isawaitable(func):
        task = asyncio.create_task(func)
    else:
        task = asyncio.create_task(func(*args, **kwargs))
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)
    return task
