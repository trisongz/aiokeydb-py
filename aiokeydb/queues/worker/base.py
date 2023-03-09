
"""
Base Worker Class
"""
import os
import asyncio
import signal
import typing
import contextlib

from lazyops.utils.logs import default_logger as logger
from lazyops.utils.helpers import is_coro_func
from aiokeydb.exceptions import ConnectionError
from aiokeydb.queues.types import Job, CronJob, JobStatus, TaskType
from aiokeydb.queues.imports.cron import croniter, resolve_croniter
from aiokeydb.queues.utils import (
    millis,
    now,
    seconds,
    get_settings,
    get_hostname,
    get_and_log_exc,
    ensure_coroutine_function,
    get_thread_pool,
    run_in_executor,
)


if typing.TYPE_CHECKING:
    from aiokeydb.queues.queue import TaskQueue
    from aiokeydb.client.config import KeyDBSettings

_is_verbose: bool = None

def get_verbose() -> bool:
    """
    Lazily initialize the worker verbose setting
    """
    global _is_verbose
    if _is_verbose is None:
        settings = get_settings()
        _is_verbose = settings.worker.debug_enabled
    return _is_verbose

class WorkerTasks:

    context: typing.Dict[str, typing.Any] = {}
    functions: typing.List[typing.Callable] = []
    cronjobs: typing.List[typing.Dict] = []

    dependencies: typing.Dict[str, typing.Tuple[typing.Union[typing.Any, typing.Callable], typing.Dict]] = {}

    context_funcs: typing.Dict[str, typing.Callable] = {}
    startup_funcs: typing.Dict[str, typing.Tuple[typing.Union[typing.Any, typing.Callable], typing.Dict]] = {}
    shutdown_funcs: typing.Dict[str, typing.Tuple[typing.Union[typing.Any, typing.Callable], typing.Dict]] = {}

    silenced_functions: typing.List[str] = []

    @classmethod
    def get_functions(
        cls,
        verbose: typing.Optional[bool] = None,
    ) -> typing.List[typing.Callable]:
        """
        Compiles all the worker functions
        that are enabled from `KOpsWorkerParams.functions`.
        """
        if verbose is None: verbose = get_verbose()
        worker_ops = [run_in_executor]
        worker_ops.extend(cls.functions)
        if verbose:
            for f in worker_ops:
                if verbose:
                    logger.info(f'Worker Function: {f.__name__}')
        return worker_ops

    @classmethod
    def get_cronjobs(
        cls, 
        verbose: typing.Optional[bool] = None,
    ) -> typing.List[CronJob]:
        """
        Compiles all the worker cron functions
        that are enabled.
        WorkerCronFuncs = [CronJob(cron, cron="* * * * * */5")]
        """
        if verbose is None: verbose = get_verbose()

        cronjobs = []
        for cron_op in cls.cronjobs:
            if isinstance(cron_op, dict): 
                silenced = cron_op.pop('silenced', None)
                cron_op = CronJob(**cron_op)
                if silenced is True: cls.silenced_functions.append(cron_op.function.__name__)
            if verbose: 
                logger.info(f'Worker CronJob: {cron_op.function.__name__}: {cron_op.cron}')
            cronjobs.append(cron_op)
        return cronjobs
    
    @classmethod
    async def run_dependencies(
        cls, 
        ctx: typing.Dict,
        verbose: bool = False,    
    ) -> typing.Dict[str, typing.Any]:
        """
        Runs all the worker dependencies
        """
        for name, dep in cls.dependencies.items():
            func, kwargs = dep
            if verbose: logger.info(f'[dependency] setting ctx[{name}]: result of {func.__name__}')
            if kwargs.get('_set_ctx'):
                ctx = await func(ctx, **kwargs) if is_coro_func(func) else func(ctx, **kwargs)
            else:
                ctx[name] = await func(**kwargs) if is_coro_func(func) else func(**kwargs)
        
        return ctx
    
    @classmethod
    async def run_context(
        cls, 
        ctx: typing.Dict,
        verbose: bool = False,    
    ) -> typing.Dict[str, typing.Any]:
        """
        Runs all the worker context functions
        """
        for name, dep in cls.context_funcs.items():
            func, kwargs = dep
            if verbose: logger.info(f'[context] setting ctx[{name}]: result of {func.__name__}')
            if kwargs.get('_set_ctx'):
                ctx = await func(ctx, **kwargs) if is_coro_func(func) else func(ctx, **kwargs)
            else:
                ctx[name] = await func(**kwargs) if is_coro_func(func) else func(**kwargs)
        return ctx

    @classmethod
    async def run_startup_funcs(
        cls,
        ctx: typing.Dict,
        verbose: typing.Optional[bool] = True,
    ) -> typing.Dict[str, typing.Any]:
        """
        Runs all the worker startup functions
        """
        if verbose is None: verbose = get_verbose()
        for name, dep in cls.startup_funcs.items():
            func, kwargs = dep
            if verbose: logger.info(f'[startup] Setting ctx[{name}] = result of: {func.__name__}')
            if kwargs.get('_set_ctx'):
                # logger.info('Passing ctx to function')
                ctx = await func(ctx, **kwargs) if is_coro_func(func) else func(ctx, **kwargs)
            else:
                ctx[name] = await func(**kwargs) if is_coro_func(func) else func(**kwargs)
        return ctx
    
    @classmethod
    async def run_shutdown_funcs(
        cls,
        ctx: typing.Dict,
        verbose: typing.Optional[bool] = None,
    ) -> typing.Dict[str, typing.Any]:
        """
        Runs all the worker shutdown functions
        """
        if verbose is None: verbose = get_verbose()
        for name, dep in cls.shutdown_funcs.items():
            func, kwargs = dep
            if verbose: logger.info(f'[shutdown] task: {name} = {func.__name__}')
            await func(ctx = ctx, **kwargs) if is_coro_func(func) else func(ctx = ctx, **kwargs)
        return ctx

    @classmethod
    async def get_startup_context(
        cls,
        ctx: typing.Dict[str, typing.Any],
        verbose: typing.Optional[bool] = None,
    ) -> typing.Dict[str, typing.Any]:
        """
        Loads all Runtime Deps
        """
        if verbose is None: verbose = get_verbose()
        for name, obj in cls.context.items():
            if name not in ctx: 
                ctx[name] = obj
                if verbose: logger.info(f'[context] setting ctx[{name}]: {obj}')
        
        ctx = await cls.run_dependencies(ctx, verbose)
        ctx = await cls.run_context(ctx, verbose)
        ctx = await cls.run_startup_funcs(ctx, verbose)
        
        return ctx


class Worker:
    """
    Worker is used to process and monitor jobs.

    queue: instance of saq.queue.Queue
    functions: list of async functions
    concurrency: number of jobs to process concurrently
    startup: async function to call on startup
    shutdown: async function to call on shutdown
    before_process: async function to call before a job processes
    after_process: async function to call after a job processes
    timers: dict with various timer overrides in seconds
        schedule: how often we poll to schedule jobs
        stats: how often to update stats
        sweep: how often to cleanup stuck jobs
        abort: how often to check if a job is aborted
    """

 

    SIGNALS = [signal.SIGINT, signal.SIGTERM] if os.name != "nt" else [signal.SIGTERM]

    def __init__(
        self,
        queue: 'TaskQueue',
        functions: typing.Optional[typing.List[typing.Callable]] = None,
        *,
        name: typing.Optional[str] = None,
        settings: typing.Optional['KeyDBSettings'] = None,
        concurrency: typing.Optional[int] = None,
        cron_jobs: typing.Optional[typing.List[typing.Callable]] = None,
        startup: typing.Optional[typing.Union[typing.List[typing.Callable], typing.Callable,]] = None,
        shutdown: typing.Optional[typing.Union[typing.List[typing.Callable], typing.Callable,]] = None,
        before_process: typing.Optional[typing.Callable] = None,
        after_process: typing.Optional[typing.Callable] = None,
        timers: typing.Optional[typing.Dict[str, typing.Union[int, float, typing.Any]]] = None,
        dequeue_timeout: int = 0,
        selectors: typing.Optional[typing.Dict[str, typing.Any]] = None,
        threadpool_size: typing.Optional[int] = None,
        debug_enabled: typing.Optional[bool] = None,
        silenced_functions: typing.Optional[typing.List[str]] = None,
        worker_attributes: typing.Optional[typing.Dict[str, typing.Any]] = None,
        heartbeat_ttl: typing.Optional[int] = None,
    ):
        self.queue = queue
        self.settings = settings or get_settings()
        self.name = name if name is not None else (self.settings.worker.name or get_hostname())
        self.concurrency = concurrency if concurrency is not None else self.settings.worker.concurrency
        self.debug_enabled = debug_enabled if debug_enabled is not None else self.settings.worker.debug_enabled

        self.silenced_functions = silenced_functions if silenced_functions is not None else list(set(WorkerTasks.silenced_functions))
        self.queue.add_silenced_functions(*self.silenced_functions)

        self.startup = startup or [
            WorkerTasks.get_startup_context
        ]
        if not isinstance(self.startup, list):
            self.startup = [self.startup]
        self.shutdown = shutdown or [
            WorkerTasks.run_shutdown_funcs
        ]
        if not isinstance(self.shutdown, list):
            self.shutdown = [self.shutdown]
        
        self.before_process = before_process
        self.after_process = after_process
        self.timers = {
            "schedule": 1,
            "stats": 10,
            "sweep": 180,
            "abort": 1,
            "heartbeat": 10,
            "broadcast": 10,
        }
        if timers: self.timers.update(timers)
        self.selectors = selectors or {}
        self.event = asyncio.Event()
        functions: typing.Set[typing.Callable] = set(functions or WorkerTasks.get_functions(verbose = self.debug_enabled))
        self.functions = {}
        self.cron_jobs: typing.List[CronJob] = cron_jobs or WorkerTasks.get_cronjobs()
        # self.context = {"worker": self, "queue": self.queue, "keydb": self.queue.ctx, "pool": get_thread_pool(threadpool_size), "vars": {}}
        self.tasks: typing.Set[asyncio.Task] = set()
        self.job_task_contexts: typing.Dict['Job', typing.Dict[str, typing.Any]] = {}
        self.dequeue_timeout = dequeue_timeout
        self.worker_attributes = worker_attributes or {}
        self.worker_attributes.update({
            # "name": self.name,
            "selectors": self.selectors,
            "worker_id": self.queue.uuid,
            "queue_name": self.queue.queue_name,
        })
        self.heartbeat_ttl = heartbeat_ttl if heartbeat_ttl is not None else self.queue.heartbeat_ttl

        for job in self.cron_jobs:
            resolve_croniter(required = True)
            if not croniter.is_valid(job.cron):
                raise ValueError(f"Cron is invalid {job.cron}")
            functions.add(job.function)

        for function in functions:
            if isinstance(function, tuple): name, function = function
            else: name = function.__qualname__
            self.functions[name] = function
    

    def logger(self, job: 'Job' = None, kind: str = "enqueue"):
        if job:
            return logger.bind(
                worker_name = self.name,
                job_id=job.id,
                status=job.status,
                queue_name = getattr(job.queue, 'queue_name', self.queue_name) or 'unknown queue',
                kind=kind,
            )
        else:
            return logger.bind(
                worker_name = self.name,
                queue_name = self.queue_name,
                kind=kind,
            )

    async def _before_process(self, ctx):
        if self.before_process:
            await self.before_process(ctx)

    async def _after_process(self, ctx):
        if self.after_process:
            await self.after_process(ctx)
    
    async def prepare_server(self):
        """
        Prepares the keydb server to ensure that the maximum number of concurrent
        connections are available.
        """
        await self.queue.prepare_server()


    async def start(self):
        """Start processing jobs and upkeep tasks."""
        self.logger(kind = "startup").info(f'Queue Name: {self.queue_name} @ {self.queue.uri} DB: {self.queue.db_id}')
        if not self.settings.worker_enabled:
            self.logger(kind = "startup").warning(
                f'{self.settings.app_name or "KeyDBWorker"}: {self.name} is disabled.')
            return
        
        self.context = {"worker": self, "queue": self.queue, "keydb": self.queue.ctx, "pool": get_thread_pool(), "vars": {}}
        self.worker_attributes['name'] = self.name
        self.queue._worker_name = self.name
        self.logger(kind = "startup").info(
            f'{self.settings.app_name or "KeyDBWorker"}: {self.name} | WorkerID: {self.worker_id} | {self.settings.app_version} | Build ID: {self.settings.build_id} | Worker Attributes: {self.worker_attributes}')
        try:
            self.event = asyncio.Event()
            loop = asyncio.get_running_loop()

            for signum in self.SIGNALS: loop.add_signal_handler(signum, self.event.set)
            await self.queue.ctx.async_client.initialize()

            if self.startup: 
                for func in self.startup:
                    await func(self.context)

            self.logger(kind = "startup").info(
                f"Registered {len(self.functions)} functions, {len(self.cron_jobs)} cron jobs, {self.concurrency} concurrency. Functions: {list(self.functions.keys())}"
            )
            if self.silenced_functions:
                self.logger(kind = "startup").info(
                    f"Silenced functions: {self.silenced_functions}"
                )

            # Register the queue
            # await self.queue.register_queue()
            # Send the first heartbeat
            await self.heartbeat()

            # Prepare the server
            await self.prepare_server()

            self.tasks.update(await self.upkeep())
            for _ in range(self.concurrency):
                self._process()

            await self.event.wait()
        finally:
            self.logger(kind = "shutdown").warning(
                f'{self.settings.app_name or "KeyDBWorker"}: {self.name} is shutting down.'
                )
            if self.shutdown:
                for func in self.shutdown:
                    await func(self.context)
            await self.stop()
    

    async def stop(self):
        """Stop the worker and cleanup."""
        self.event.set()
        all_tasks = list(self.tasks)
        self.tasks.clear()
        for task in all_tasks:
            task.cancel()
        await asyncio.gather(*all_tasks, return_exceptions=True)
    

    async def schedule(self, lock=1):
        for cron_job in self.cron_jobs:
            kwargs = cron_job.__dict__.copy()
            function = kwargs.pop("function").__qualname__
            kwargs["key"] = f"cron:{function}" if kwargs.pop("unique") else None
            scheduled = croniter(kwargs.pop("cron"), seconds(now())).get_next()

            await self.queue.enqueue(
                function,
                scheduled=int(scheduled),
                **{k: v for k, v in kwargs.items() if v is not None},
            )

        scheduled = await self.queue.schedule(lock)
        if scheduled and self.queue.verbose_results:
            self.logger(kind = "scheduled").info(
                f'↻ node={self.queue.node_name}, {scheduled}'
            )

    async def heartbeat(self, ttl: int = 10):
        """
        Send a heartbeat to the queue.
        """
        await self.queue.add_heartbeat(
            worker_id = self.worker_id,
            worker_attributes = self.worker_attributes,
            heartbeat_ttl = ttl,
        )

    async def upkeep(self):
        """Start various upkeep tasks async."""

        async def poll(func, sleep, arg=None):
            while not self.event.is_set():
                try:
                    if asyncio.iscoroutinefunction(func):
                        await func(arg or sleep)
                    else:
                        func(arg or sleep)
                except (Exception, asyncio.CancelledError):
                    if self.event.is_set():
                        return
                    get_and_log_exc()

                await asyncio.sleep(sleep)

        return [
            asyncio.create_task(poll(self.abort, self.timers["abort"])),
            asyncio.create_task(poll(self.schedule, self.timers["schedule"])),
            asyncio.create_task(poll(self.queue.sweep, self.timers["sweep"])),
            asyncio.create_task(
                poll(self.queue.stats, self.timers["stats"], self.timers["stats"] + 1)
            ),
            asyncio.create_task(
                poll(self.heartbeat, self.timers["heartbeat"], self.heartbeat_ttl)
            ),
            asyncio.create_task(poll(self._broadcast_process, self.timers["broadcast"])
            )
        ]

    async def abort(self, abort_threshold: int):
        jobs = [
            job
            for job in self.job_task_contexts
            if job.duration("running") >= millis(abort_threshold)
        ]

        if not jobs: return
        aborted = await self.queue.ctx.async_mget(job.abort_id for job in jobs)
        for job, abort in zip(jobs, aborted):
            if not abort: continue

            task_data = self.job_task_contexts.get(job, {})
            task: asyncio.Task = task_data.get("task")

            if task and not task.done():
                task_data["aborted"] = True
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

            await job.finish(JobStatus.ABORTED, error = abort.decode("utf-8"))
            await self.queue.ctx.async_delete(job.abort_id)
            self.logger(job = job, kind = "abort").info(f"⊘ {job.duration('running')}ms, node={self.node_name}, func={job.function}, id={job.id}")
    
    async def process(self, broadcast: typing.Optional[bool] = False):
        # sourcery skip: low-code-quality
        # pylint: disable=too-many-branches
        job, context = None, None
        try:
            with contextlib.suppress(ConnectionError):
                job = await self.queue.dequeue(
                    self.dequeue_timeout, 
                    worker_id = self.worker_id if broadcast else None, 
                    worker_name = self.name if broadcast else None,
                )

            if not job: return
            if job.worker_id and job.worker_id != self.worker_id:
                if self.debug_enabled or self.queue.verbose_results:
                    self.logger(job = job, kind = "process").info(f"⊘ Rejected job, queued_key={job.queued_key}, func={job.function}, id={job.id} | worker_id={job.worker_id} != {self.worker_id}")
                return
            if job.worker_name and job.worker_name != self.name:
                if self.debug_enabled or self.queue.verbose_results:
                    self.logger(job = job, kind = "process").info(f"⊘ Rejected job, queued_key={job.queued_key}, func={job.function}, id={job.id} | worker_name={job.worker_name} != {self.name}")
                return
                
            if job.worker_name or job.worker_id and (self.debug_enabled or self.queue.verbose_results):
                self.logger(job = job, kind = "process").info(f"☑ Accepted job, func={job.function}, id={job.id}, worker_name={job.worker_name}, worker_id={job.worker_id}")

            job.started = now()
            job.status = JobStatus.ACTIVE
            job.attempts += 1
            await job.update()
            context = {**self.context, "job": job}
            await self._before_process(context)
            if job.function not in self.silenced_functions:
                self.logger(job = job, kind = "process").info(
                    f"← duration={job.duration('running')}ms, node={self.node_name}, func={job.function}"
                )
                

            function = ensure_coroutine_function(self.functions[job.function])
            task = asyncio.create_task(function(context, **(job.kwargs or {})))
            self.job_task_contexts[job] = {"task": task, "aborted": False}
            result = await asyncio.wait_for(task, job.timeout)
            await job.finish(JobStatus.COMPLETE, result=result)
        except asyncio.CancelledError:
            if job and not self.job_task_contexts.get(job, {}).get("aborted"):
                await job.retry("cancelled")
        except Exception:
            error = get_and_log_exc()

            if job:
                if job.attempts >= job.retries:
                    await job.finish(JobStatus.FAILED, error=error)
                else: await job.retry(error)
        finally:
            if context:
                self.job_task_contexts.pop(job, None)
                try: await self._after_process(context)
                except (Exception, asyncio.CancelledError): 
                    get_and_log_exc()

    def _process(self, previous_task=None):
        if previous_task:
            self.tasks.discard(previous_task)

        if not self.event.is_set():
            new_task = asyncio.create_task(self.process())
            self.tasks.add(new_task)
            new_task.add_done_callback(self._process)
    
    def _broadcast_process(self, previous_task = None):
        if previous_task and isinstance(previous_task, asyncio.Task):
            self.tasks.discard(previous_task)

        if not self.event.is_set():
            new_task = asyncio.create_task(self.process(broadcast=True))
            self.tasks.add(new_task)
            new_task.add_done_callback(self._broadcast_process)

    """
    Static Methods to add functions and context to the worker queue.
    """

    @staticmethod
    def add_context(
        obj: typing.Optional[typing.Any] = None,
        name: typing.Optional[str] = None,
        verbose: typing.Optional[bool] = False,
        silenced: typing.Optional[bool] = None,
        _fx: typing.Optional[typing.Callable] = None,
        **kwargs,
    ):
        """
        Add a context object to the worker queue.

        Args:
            name: name of the job
            obj: object to pass to the function
            verbose: whether to print the function's output
            kwargs: additional arguments to pass to the function

        >> @Worker.add_context("client")
        >> async def init_client():
        >>     return Client()

        >> ctx['client'] = Client
            
        """
        if obj is not None:
            name = name or obj.__name__
            if callable(obj):
                WorkerTasks.context_funcs[name] = (obj, kwargs)
                if verbose: logger.info(f"Registered context function {name}: {obj}")
            else:
                WorkerTasks.context[name] = obj
                if verbose: logger.info(f"Registered context {name}: {obj}")
            if silenced is True:
                WorkerTasks.silenced_functions.append(name)
            return
        
        if _fx is not None:
            name = name or _fx.__name__
            WorkerTasks.context_funcs[name] = (_fx, kwargs)
            if verbose: logger.info(f"Registered context function {name}: {_fx}")
            if silenced is True: WorkerTasks.silenced_functions.append(name)
            return

        # Create a wrapper
        def wrapper(func: typing.Callable):
            func_name = name or func.__name__
            WorkerTasks.context_funcs[func_name] = (func, kwargs)
            if verbose: logger.info(f"Registered context function {func_name}: {func}")
            if silenced is True: WorkerTasks.silenced_functions.append(func_name)
            return func
        
        return wrapper

    @staticmethod
    def add_dependency(
        obj: typing.Optional[typing.Any] = None,
        name: typing.Optional[str] = None,
        verbose: typing.Optional[bool] = False,
        _fx: typing.Optional[typing.Callable] = None,
        silenced: typing.Optional[bool] = None,
        **kwargs,
    ):
        """
        Adds a dependency function to the worker queue.
        Args:
            name: name of the job
            obj: object to pass to the function
            verbose: whether to print the function's output
            kwargs: additional arguments to pass to the function
        """
        if obj is not None:
            name = name or obj.__name__
            WorkerTasks.dependencies[name] = (obj, kwargs)
            if verbose: logger.info(f"Registered dependency {name}: {obj}")
            if silenced is True: WorkerTasks.silenced_functions.append(name)
            return
        
        if _fx is not None:
            name = name or _fx.__name__
            WorkerTasks.dependencies[name] = (_fx, kwargs)
            if verbose: logger.info(f"Registered dependency {name}: {_fx}")
            if silenced is True: WorkerTasks.silenced_functions.append(name)
            return
        
        # Create a wrapper
        def wrapper(func: typing.Callable):
            func_name = name or func.__name__
            WorkerTasks.dependencies[func_name] = (func, kwargs)
            if verbose: logger.info(f"Registered depency{func_name}: {func}")
            if silenced is True: WorkerTasks.silenced_functions.append(func_name)
            return func
        
        return wrapper


    @staticmethod
    def on_startup(
        name: typing.Optional[str] = None,
        verbose: typing.Optional[bool] = False,
        _fx: typing.Optional[typing.Callable] = None,
        silenced: typing.Optional[bool] = None,
        **kwargs,
    ):
        """
        Add a startup function to the worker queue.


        >> @Worker.on_startup("client")
        >> async def init_client():
        >>     return Client()

        >> ctx['client'] = Client


        >> @Worker.on_startup(_set_ctx = True)
        >> async def init_client(ctx: Dict[str, Any], **kwargs):
        >>   ctx['client'] = Client()
        >>   return ctx

        >> ctx['client'] = Client

        
        """
        if _fx is not None:
            name = name or _fx.__name__
            WorkerTasks.startup_funcs[name] = (_fx, kwargs)
            if verbose: logger.info(f"Registered startup function {name}: {_fx}")
            if silenced is True: WorkerTasks.silenced_functions.append(name)
            return
        
        def decorator(func: typing.Callable):
            func_name = name or func.__name__
            WorkerTasks.startup_funcs[func_name] = (func, kwargs)
            if verbose: logger.info(f"Registered startup function {func_name}: {func}")
            if silenced is True: WorkerTasks.silenced_functions.append(func_name)
            # logger.info(f"Registered startup function {func_name}: {func}: {kwargs}")
            return func
        
        return decorator

    @staticmethod
    def on_shutdown(
        name: typing.Optional[str] = None,
        verbose: typing.Optional[bool] = False,
        _fx: typing.Optional[typing.Callable] = None,
        silenced: typing.Optional[bool] = None,
        **kwargs,
    ):
        """
        Add a shutdown function to the worker queue.
        """
        if _fx is not None:
            name = name or _fx.__name__
            WorkerTasks.shutdown_funcs[name] = (_fx, kwargs)
            if verbose: logger.info(f"Registered shutdown function {name}: {_fx}")
            if silenced is True: WorkerTasks.silenced_functions.append(name)
            return
        
        def decorator(func: typing.Callable):
            func_name = name or func.__name__
            WorkerTasks.shutdown_funcs[func_name] = (func, kwargs)
            if verbose: logger.info(f"Registered shutdown function {func_name}: {func}")
            if silenced is True: WorkerTasks.silenced_functions.append(func_name)
            return func
        return decorator

    @staticmethod
    def add_function(
        name: typing.Optional[str] = None,
        _fx: typing.Optional[typing.Callable] = None,
        verbose: typing.Optional[bool] = False,
        silenced: typing.Optional[bool] = None,
        **kwargs,
    ):
        """
        Add a function to the worker queue.

        >> @Worker.add_function()
        >> async def my_function(*args, **kwargs):
        >>     return "Hello World"

        >> res = await queue.apply('my_function', *args, **kwargs)
        >> assert res == "Hello World"
        
        """
        if _fx is not None:
            name = name or _fx.__name__
            WorkerTasks.functions.append(_fx)
            if verbose:
                logger.info(f"Registered function {name}: {_fx}")
            if silenced is True: WorkerTasks.silenced_functions.append(name)
            return
        
        def decorator(func: typing.Callable):
            WorkerTasks.functions.append(func)
            if verbose:
                logger.info(f"Registered function {func.__name__}")
            if silenced is True: WorkerTasks.silenced_functions.append(func.__name__)
            return func
        
        return decorator
    

    @staticmethod
    def add_cronjob(        
        schedule: typing.Optional[typing.Union[typing.Dict, typing.List, str]] = None,
        _fx: typing.Optional[typing.Callable] = None,
        verbose: typing.Optional[bool] = False,
        silenced: typing.Optional[bool] = None,
        **kwargs,
    ):
        """
        Adds a function to `WorkerTask.cronjobs`.
        WorkerCronFuncs = {
            {'coroutine': refresh_spot_data, 'name': 'refresh_spot_data', 'minute': {10, 30, 50}},
        }
        """
        if _fx is not None:
            cron = {'function': _fx, **kwargs, 'cron': schedule, 'silenced': silenced}
            WorkerTasks.cronjobs.append(cron)
            if verbose: logger.info(f'Registered CronJob: {cron}')
            return
        
        def decorator(func: typing.Callable):
            nonlocal schedule
            cron = {'function': func, **kwargs, 'cron': schedule, 'silenced': silenced}
            WorkerTasks.cronjobs.append(cron)
            if verbose: logger.info(f'Registered CronJob: {cron}')
            return func
        return decorator
    
    
    @staticmethod
    def add(
        task: TaskType = TaskType.default,
        name: typing.Optional[str] = None,
        obj: typing.Optional[typing.Any] = None,
        verbose: typing.Optional[bool] = False,
        **kwargs,
    ):
        """
        Add a function to the worker queue.

        Args:
            func: function to call
            name: name of the job
            obj: object to pass to the function
            verbose: whether to print the function's output
            kwargs: additional arguments to pass to the function
        """
        task = TaskType(task) if isinstance(task, str) else task
        if obj is not None:
            if task in {TaskType.default, TaskType.function}:
                return Worker.add_function(_fx = obj, name = name, verbose = verbose, **kwargs)
            if task == TaskType.cronjob:
                return Worker.add_cronjob(_fx = obj, name = name, verbose = verbose, **kwargs)
            if task == TaskType.context:
                return Worker.add_context(obj = obj, name = name, verbose = verbose, **kwargs)
            if task == TaskType.dependency:
                return Worker.add_dependency(obj = obj, name = name, verbose = verbose, **kwargs)
        
        def wrapper(func: typing.Callable):
            if task in {TaskType.default, TaskType.function}:
                return Worker.add_function(_fx = func, name = name, verbose = verbose, **kwargs)
            if task == TaskType.cronjob:
                return Worker.add_cronjob(_fx = func, name = name, verbose = verbose, **kwargs)
            if task == TaskType.context:
                return Worker.add_context(_fx = func, name = name, verbose = verbose, **kwargs)
            if task == TaskType.dependency:
                return Worker.add_dependency(_fx = func, name = name, verbose = verbose, **kwargs)
            if task == TaskType.startup:
                return Worker.on_startup(_fx = func, name = name, verbose = verbose, **kwargs)
            if task == TaskType.shutdown:
                return Worker.on_shutdown(_fx = func, name = name, verbose = verbose, **kwargs)
        
        return wrapper
        
    
    @classmethod
    def from_settings(
        cls,
        settings: typing.Union[str, typing.Dict],
        queue: typing.Optional['TaskQueue'] = None,
        verbose: typing.Optional[bool] = None,
        **kwargs
    ) -> "Worker":
        """
        Create a Worker from a settings file or dict.
        """
        verbose = verbose or get_verbose()
        if isinstance(settings, str):
            import importlib

            if verbose: logger.info(f"Importing settings from {settings}")
            module_path, name = settings.strip().rsplit(".", 1)
            module = importlib.import_module(module_path)
            settings = getattr(module, name)
        elif verbose: logger.info(f"Using settings dict {settings}")
        if kwargs: settings.update(kwargs)
        if queue is not None: settings['queue'] = queue
        return cls(**settings)


    """
    Properties
    """
    @property
    def worker_id(self) -> str:
        """
        Returns a unique worker id.
        """
        return self.queue.uuid

    @property
    def queue_name(self) -> str:
        """
        Returns the queue name.
        """
        return self.queue.queue_name

    @property
    def node_name(self) -> str:
        """
        Returns the node name.
        """
        return self.queue.node_name
    