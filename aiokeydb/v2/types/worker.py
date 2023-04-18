
"""
Base Worker Class
"""
import os
import asyncio
import signal
import typing
import contextlib
from croniter import croniter
from lazyops.utils.logs import default_logger as logger

from aiokeydb.v2.exceptions import ConnectionError
from aiokeydb.v2.configs import settings as default_settings
from aiokeydb.v2.types.jobs import Job, CronJob, JobStatus, TaskType
from aiokeydb.v2.utils.queue import (
    millis,
    now,
    seconds,
    get_hostname,
    get_and_log_exc,
    ensure_coroutine_function,
    get_thread_pool,
)

if typing.TYPE_CHECKING:
    from aiokeydb.v2.types.task_queue import TaskQueue
    from aiokeydb.v2.configs import KeyDBSettings


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
        broadcast_concurrency: typing.Optional[int] = None,
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
        is_leader_process: typing.Optional[bool] = None,
    ):
        self.queue = queue
        self.settings = settings or default_settings
        self.worker_host = get_hostname()
        self.name = name if name is not None else (self.settings.worker.name or self.worker_host)
        self.concurrency = concurrency if concurrency is not None else self.settings.worker.concurrency
        self.broadcast_concurrency = broadcast_concurrency if broadcast_concurrency is not None else self.settings.worker.max_broadcast_concurrency
        if not self.broadcast_concurrency:
            self.broadcast_concurrency = 3
        
        self.is_leader_process = is_leader_process if is_leader_process is not None else (self.settings.worker.is_leader_process if self.settings.worker.is_leader_process is not None else True)
        self.debug_enabled = debug_enabled if debug_enabled is not None else self.settings.worker.debug_enabled
        self.silenced_functions = silenced_functions if silenced_functions is not None else list(set(self.settings.worker.tasks.silenced_functions))
        self.queue.add_silenced_functions(*self.silenced_functions)

        self.startup = startup or [
            self.settings.worker.get_startup_context
        ]
        if not isinstance(self.startup, list):
            self.startup = [self.startup]
        self.shutdown = shutdown or [
            self.settings.worker.run_shutdown_funcs
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
            "heartbeat": 5,
            "broadcast": 10,
        }
        if timers: self.timers.update(timers)
        self.selectors = selectors or {}
        self.event = asyncio.Event()
        functions: typing.Set[typing.Callable] = set(functions or self.settings.worker.get_functions(verbose = self.debug_enabled))
        self.functions = {}
        self.cron_jobs: typing.List[CronJob] = cron_jobs or self.settings.worker.get_cronjobs()
        # self.context = {"worker": self, "queue": self.queue, "keydb": self.queue.ctx, "pool": get_thread_pool(threadpool_size), "vars": {}}
        self.tasks: typing.Set[asyncio.Task] = set()
        self.job_task_contexts: typing.Dict['Job', typing.Dict[str, typing.Any]] = {}
        self.dequeue_timeout = dequeue_timeout
        self.worker_pid: int = os.getpid()
        self.worker_attributes = worker_attributes or {}
        self.worker_attributes.update({
            # "name": self.name,
            "selectors": self.selectors,
            "worker_host": self.worker_host,
            "worker_id": self.queue.uuid,
            "queue_name": self.queue.queue_name,
            "is_leader_process": self.is_leader_process,
            "worker_pid": self.worker_pid,
        })
        self.heartbeat_ttl = heartbeat_ttl if heartbeat_ttl is not None else self.queue.heartbeat_ttl

        for job in self.cron_jobs:
            if not croniter.is_valid(job.cron):
                raise ValueError(f"Cron is invalid {job.cron}")
            # functions.add(job.function)
            self.functions[job.function.__qualname__] = job.function
            # self.logger(kind="startup").info(f"Added cron job {job.function.__qualname__} with cron {job.cron} to worker {self.name}.")

        for function in functions:
            if isinstance(function, tuple): name, function = function
            # elif isinstance(function, Job):
            else: name = function.__qualname__
            self.functions[name] = function
        # self.logger(kind="startup").info(f"Added {len(self.functions)} functions to worker {self.name}: {self.functions.keys()}")
        self._worker_identity: str = f'[{self.worker_pid}] {self.settings.app_name or "KeyDBWorker"}'
    
    @property
    def _worker_name(self):
        return f"{self.worker_host}.{self.name}.{self.worker_pid}"

    def logger(self, job: 'Job' = None, kind: str = "enqueue"):
        if job:
            return logger.bind(
                # worker_name = self.name,
                worker_name = self._worker_name,
                job_id=job.id,
                status=job.status,
                queue_name = getattr(job.queue, 'queue_name', self.queue_name) or 'unknown queue',
                kind=kind,
            )
        else:
            return logger.bind(
                worker_name = self._worker_name,
                # worker_name = self.name,
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
        # if self.is_leader_process:
        os.environ['IS_WORKER_PROCESS'] = 'true'
        self.logger(kind = "startup").info(f'{self._worker_identity}: Queue Name: {self.queue_name} @ {self.queue.uri} DB: {self.queue.db_id}')
        if not self.settings.worker_enabled:
            self.logger(kind = "startup").warning(
                f'{self._worker_identity}: {self.worker_host}.{self.name} is disabled.')
            return
        
        self.context = {"worker": self, "queue": self.queue, "keydb": self.queue.ctx, "pool": get_thread_pool(), "vars": {}}
        self.worker_attributes['name'] = self.name
        # self.queue._worker_name = self.name
        self.queue._worker_name = self._worker_name
        self.logger(kind = "startup").info(
            f'{self._worker_identity}: {self.worker_host}.{self.name} v{self.settings.version} | WorkerID: {self.worker_id} | Concurrency: {self.concurrency}/jobs, {self.broadcast_concurrency}/broadcasts | Worker Attributes: {self.worker_attributes} ')
        try:
            self.event = asyncio.Event()
            loop = asyncio.get_running_loop()

            for signum in self.SIGNALS: loop.add_signal_handler(signum, self.event.set)
            await self.queue.ctx.async_client.initialize()

            if self.startup: 
                for func in self.startup:
                    await func(self.context)

            if self.is_leader_process: self.logger(kind = "startup").info(
                f"{self._worker_identity}: Registered {len(self.functions)} functions, {len(self.cron_jobs)} cron jobs, {self.concurrency} concurrency. Functions: {list(self.functions.keys())}"
            )
            if self.silenced_functions and self.is_leader_process:
                self.logger(kind = "startup").info(
                    f"{self._worker_identity}: Silenced functions: {self.silenced_functions}"
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
            
            for _ in range(self.broadcast_concurrency):
                self._broadcast_process()

            await self.event.wait()
        finally:
            self.logger(kind = "shutdown").warning(
                f'{self._worker_identity}: {self.worker_host}.{self.name} is shutting down.'
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

    async def heartbeat(self, ttl: int = 20):
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

        async def poll(func, sleep, arg=None, **kwargs):
            while not self.event.is_set():
                try:
                    if asyncio.iscoroutinefunction(func):
                        await func(arg or sleep, **kwargs)
                    else:
                        func(arg or sleep, **kwargs)
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
            # asyncio.create_task(
            #     poll(self._broadcast_process, self.timers["broadcast"])
            # ),

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
            # self.logger(job = None, kind = "process").error(f"JOB: {job}")
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

    async def process_broadcast(self):
        # await self.heartbeat(self.heartbeat_ttl)
        await self.process(broadcast = True)

        await self.queue.schedule(lock = 1, worker_id = self.worker_id)
        await self.queue.schedule(lock = 1, worker_id = self.name)

        await self.queue.sweep(worker_id = self.worker_id)
        await self.queue.sweep(worker_id = self.name)


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
            new_task = asyncio.create_task(self.process_broadcast())
            self.tasks.add(new_task)
            new_task.add_done_callback(self._broadcast_process)
        
    
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
        verbose = verbose or default_settings.worker.debug_enabled
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
    

    """
    Static Methods to add functions and context to the worker queue.
    """

    @staticmethod
    def add_context(
        obj: typing.Optional[typing.Any] = None,
        name: typing.Optional[str] = None,
        verbose: typing.Optional[bool] = None,
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
        return default_settings.worker.add_context(
            obj = obj, name = name, verbose = verbose, silenced = silenced, _fx = _fx, **kwargs
        )

    @staticmethod
    def add_dependency(
        obj: typing.Optional[typing.Any] = None,
        name: typing.Optional[str] = None,
        verbose: typing.Optional[bool] = None,
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
        return default_settings.worker.add_dependency(
            obj = obj, name = name, verbose = verbose, silenced = silenced, _fx = _fx, **kwargs
        )


    @staticmethod
    def on_startup(
        name: typing.Optional[str] = None,
        verbose: typing.Optional[bool] = None,
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
        return default_settings.worker.on_startup(
            name = name, verbose = verbose, _fx = _fx, silenced = silenced, **kwargs
        )

    @staticmethod
    def on_shutdown(
        name: typing.Optional[str] = None,
        verbose: typing.Optional[bool] = None,
        _fx: typing.Optional[typing.Callable] = None,
        silenced: typing.Optional[bool] = None,
        **kwargs,
    ):
        """
        Add a shutdown function to the worker queue.
        """
        return default_settings.worker.on_shutdown(
            name = name, verbose = verbose, _fx = _fx, silenced = silenced, **kwargs
        )

    @staticmethod
    def add_function(
        name: typing.Optional[str] = None,
        _fx: typing.Optional[typing.Callable] = None,
        verbose: typing.Optional[bool] = None,
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
        return default_settings.worker.add_function(
            name = name, _fx = _fx, verbose = verbose, silenced = silenced, **kwargs
        )
    

    @staticmethod
    def add_cronjob(        
        schedule: typing.Optional[typing.Union[typing.Dict, typing.List, str]] = None,
        _fx: typing.Optional[typing.Callable] = None,
        verbose: typing.Optional[bool] = None,
        silenced: typing.Optional[bool] = None,
        **kwargs,
    ):
        """
        Adds a function to `WorkerTask.cronjobs`.
        WorkerCronFuncs = {
            {'coroutine': refresh_spot_data, 'name': 'refresh_spot_data', 'minute': {10, 30, 50}},
        }
        """
        return default_settings.worker.add_cronjob(
            schedule = schedule, _fx = _fx, verbose = verbose, silenced = silenced, **kwargs
        )
    
    
    @staticmethod
    def add(
        task: TaskType = TaskType.default,
        name: typing.Optional[str] = None,
        obj: typing.Optional[typing.Any] = None,
        verbose: typing.Optional[bool] = None,
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
        return default_settings.worker.add(
            task = task, 
            name = name, 
            obj = obj, 
            verbose = verbose, 
            **kwargs
        )
    
    @staticmethod
    def set_queue_func(
        queue_func: typing.Union[typing.Callable, 'TaskQueue'],
    ):
        """
        Set the queue function to use for the worker queue.
        """
        return default_settings.worker.set_queue_func(queue_func = queue_func)

    @staticmethod
    def add_fallback_function(
        verbose: typing.Optional[bool] = True,
        silenced: typing.Optional[bool] = None,
        method: typing.Optional[str] = "apply",
        timeout: typing.Optional[int] = None,
        suppressed_exceptions: typing.Optional[typing.List[typing.Type[Exception]]] = None,
        failed_results: typing.Optional[typing.List] = None,
        queue_func: typing.Optional[typing.Union[typing.Callable, 'TaskQueue']] = None,
        **kwargs,
    ):
        """
        Add a fallback function to the worker queue.
        The function will first be attempted in the worker queue
        and if it fails, it will be called directly.
        This is designed to reduce redundancy in writing fallback functions.
        
        **IMPORTANT**: The function must be a coroutine function, and
        and should only have kwargs

        >> @Worker.add_fallback_function()
        >> async def run_func(ctx: Optional[dict] = None, **kwargs):
        >>     if ctx:
        >>         print("Calling from Worker Queue. Will Fail.")
        >>         raise Exception("Failed")
        >>     print("Calling Directly")
        >>     return "Hello World"
        """
        return default_settings.worker.add_fallback_function(
            verbose = verbose, 
            silenced = silenced, 
            method = method, 
            timeout = timeout,
            suppressed_exceptions = suppressed_exceptions, 
            failed_results = failed_results,
            queue_func = queue_func, 
            **kwargs
        )