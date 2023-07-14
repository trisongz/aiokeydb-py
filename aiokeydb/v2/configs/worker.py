import os
import json
import socket
import contextlib
import functools
from lazyops.utils.helpers import is_coro_func
from lazyops.utils.logs import default_logger as logger
from typing import Optional, Dict, Any, Union, Callable, List, Tuple, TYPE_CHECKING

from aiokeydb.v2.types import BaseSettings, validator, lazyproperty, KeyDBUri
from aiokeydb.v2.types.static import TaskType
from aiokeydb.v2.serializers import SerializerType
from aiokeydb.v2.utils.queue import run_in_executor

if TYPE_CHECKING:
    from aiokeydb.v2.types.jobs import CronJob
    from aiokeydb.v2.types.task_queue import TaskQueue


class WorkerTasks(object):

    context: Dict[str, Any] = {}
    functions: List[Callable] = []
    cronjobs: List[Dict] = []

    dependencies: Dict[str, Tuple[Union[Any, Callable], Dict]] = {}

    context_funcs: Dict[str, Callable] = {}
    startup_funcs: Dict[str, Tuple[Union[Any, Callable], Dict]] = {}
    shutdown_funcs: Dict[str, Tuple[Union[Any, Callable], Dict]] = {}

    silenced_functions: List[str] = []
    silenced_functions_by_stage: Dict[str, List[str]] = {
        'enqueue': [],
        'dequeue': [],
        'process': [],
        'finish': [],
        'sweep': [],
        'retry': [],
        'abort': [],
    }
    queue_func: Union[Callable, 'TaskQueue'] = None


_schedule_fmt = {
    'minutes': '*/{value} * * * *',
    'hours': '* */{value} * * *',
    'days': '* * */{value} * *',
    'weeks': '* * * */{value} *',
}

def validate_cron_schedule(cron_schedule: str) -> str:
    """
    Validates a cron schedule and tries to fix it if it's invalid
    super basic, but it works
    """
    import croniter
    if croniter.croniter.is_valid(cron_schedule):
        return cron_schedule
    cron_schedule = cron_schedule.lower()
    if 'every' in cron_schedule:
        cron_schedule = cron_schedule.replace('every', '').strip()
    value, unit = cron_schedule.split(' ')
    if not unit.endswith('s'): unit += 's'
    return _schedule_fmt[unit].format(value=value)





class KeyDBWorkerSettings(BaseSettings):
    """
    KeyDB Worker Settings

    - db: The KeyDB DB                                  | Default: 2
    - prefix: the absolute prefix                       | Default: 'queue'
    - queue_name: The queue name                        | Default: 'workers'
    - job_key_method: The UUID Method                   | Default: 'uuid4'
    - job_serializer: The default serializer            | Default: 'dill'
    - job_prefix: The job prefix                        | Default: 'job'
    - job_timeout: The job timeout                      | Default: 120
    - concurrency: The concurrency per worker           | Default: 100
    - max_concurrency: The queue's max concurrency      | Default: 100
    - management_url: The management URL                | Default: None
    - management_enabled: Whether to enable management  | Default: True

    """
    db: Optional[int] = None
    name: Optional[str] = None
    prefix: Optional[str] = 'queue'
    queue_name: Optional[str] = 'workers'
    job_key_method: str = 'uuid4'
    job_serializer: Optional[SerializerType] = SerializerType.dill # Optional[Union[str, SerializerType]] = SerializerType.dill

    # Sets the defaults for the job
    job_prefix: Optional[str] = 'job'
    job_timeout: Optional[int] = 120 # Controls the default job timeout
    job_ttl: Optional[int] = 60
    job_retries: Optional[int] = 1
    job_retry_delay: Optional[float] = 2.0

    concurrency: Optional[int] = 100
    max_concurrency: Optional[int] = 100
    max_broadcast_concurrency: Optional[int] = 5
    threadpool_size: Optional[int] = 100
    debug_enabled: Optional[bool] = False

    socket_keepalive: Optional[bool] = True
    socket_connect_timeout: Optional[int] = 60
    heartbeat_interval: Optional[int] = 20
    retry_on_timeout: Optional[bool] = True
    is_leader_process: Optional[bool] = None

    function_tracker_enabled: Optional[bool] = False
    function_tracker_ttl: Optional[int] = 60 * 60 * 24 * 7 # 7 days
    verbose_startup: Optional[bool] = False

    # Worker Timers
    # These mirror the default timers
    schedule_timer: Optional[int] = 1
    stats_timer: Optional[int] = 60
    sweep_timer: Optional[int] = 180
    abort_timer: Optional[int] = 1
    heartbeat_timer: Optional[int] = 5
    broadcast_timer: Optional[int] = 10

    class Config:
        case_sensitive = False
        env_prefix = 'KEYDB_WORKER_'
    

    @validator('job_serializer', pre = True, always = True)
    def validate_job_serializer(cls, v, values: Dict) -> SerializerType:
        return SerializerType(v) if isinstance(v, str) else v
    
    def configure(
        self,
        **kwargs
    ):
        """
        Configures the settings
        """
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)
    
    @lazyproperty
    def tasks(self) -> WorkerTasks:
        """
        Returns the worker tasks
        """
        return WorkerTasks()

    @property
    def timers(self) -> Dict[str, int]:
        """
        Returns the worker timers
        """
        return {
            'schedule': self.schedule_timer,
            'stats': self.stats_timer,
            'sweep': self.sweep_timer,
            'abort': self.abort_timer,
            'heartbeat': self.heartbeat_timer,
            'broadcast': self.broadcast_timer,
        }

    def get_functions(
        self,
        verbose: Optional[bool] = None,
    ) -> List[Callable]:
        """
        Compiles all the worker functions
        that are enabled from `KOpsWorkerParams.functions`.
        """
        if verbose is None: verbose = self.debug_enabled
        worker_ops = [run_in_executor]
        worker_ops.extend(self.tasks.functions)
        if verbose:
            for f in worker_ops:
                if verbose:
                    logger.info(f'Worker Function: {f.__name__}')
        return worker_ops

    def get_cronjobs(
        self, 
        verbose: Optional[bool] = None,
    ) -> List['CronJob']:
        """
        Compiles all the worker cron functions
        that are enabled.
        WorkerCronFuncs = [CronJob(cron, cron="* * * * * */5")]
        """

        if verbose is None: verbose = self.debug_enabled
        
        from aiokeydb.v2.types.jobs import CronJob
        cronjobs = []
        for cron_op in self.tasks.cronjobs:
            if isinstance(cron_op, dict): 
                silenced = cron_op.pop('silenced', None)
                cron_op = CronJob(**cron_op)
                if silenced is True: 
                    self.add_function_to_silenced(cron_op.function_name)
                    # self.tasks.silenced_functions.append(cron_op.function_name)
                # if silenced is True: self.tasks.silenced_functions.append(cron_op.function.__name__)
            if verbose: 
                # logger.info(f'Worker CronJob: {cron_op.function.__name__}: {cron_op.cron}')
                logger.info(f'Worker CronJob: {cron_op.function_name}: {cron_op.cron}')
            cronjobs.append(cron_op)
        return cronjobs
    
    async def run_dependencies(
        self, 
        ctx: Dict,
        verbose: Optional[bool] = None, 
    ) -> Dict[str, Any]:
        """
        Runs all the worker dependencies
        """
        if verbose is None: verbose = self.debug_enabled
        for name, dep in self.tasks.dependencies.items():
            func, kwargs = dep
            if verbose: logger.info(f'[dependency] setting ctx[{name}]: result of {func.__name__}')
            if kwargs.get('_set_ctx'):
                ctx = await func(ctx, **kwargs) if is_coro_func(func) else func(ctx, **kwargs)
            else:
                ctx[name] = await func(**kwargs) if is_coro_func(func) else func(**kwargs)
        
        return ctx
    
    async def run_context(
        self, 
        ctx: Dict,
        verbose: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """
        Runs all the worker context functions
        """
        if verbose is None: verbose = self.debug_enabled
        for name, dep in self.tasks.context_funcs.items():
            func, kwargs = dep
            if verbose: logger.info(f'[context] setting ctx[{name}]: result of {func.__name__}')
            if kwargs.get('_set_ctx'):
                ctx = await func(ctx, **kwargs) if is_coro_func(func) else func(ctx, **kwargs)
            else:
                ctx[name] = await func(**kwargs) if is_coro_func(func) else func(**kwargs)
        return ctx

    async def run_startup_funcs(
        self,
        ctx: Dict,
        verbose: Optional[bool] = True,
    ) -> Dict[str, Any]:
        """
        Runs all the worker startup functions
        """
        if verbose is None: verbose = self.debug_enabled
        for name, dep in self.tasks.startup_funcs.items():
            func, kwargs = dep
            if verbose: logger.info(f'[startup] Setting ctx[{name}] = result of: {func.__name__}')
            if kwargs.get('_set_ctx'):
                # logger.info('Passing ctx to function')
                ctx = await func(ctx, **kwargs) if is_coro_func(func) else func(ctx, **kwargs)
            else:
                ctx[name] = await func(**kwargs) if is_coro_func(func) else func(**kwargs)
        return ctx
    
    async def run_shutdown_funcs(
        self,
        ctx: Dict,
        verbose: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """
        Runs all the worker shutdown functions
        """
        if verbose is None: verbose = self.debug_enabled
        for name, dep in self.tasks.shutdown_funcs.items():
            func, kwargs = dep
            if verbose: logger.info(f'[shutdown] task: {name} = {func.__name__}')
            await func(ctx = ctx, **kwargs) if is_coro_func(func) else func(ctx = ctx, **kwargs)
        return ctx

    async def get_startup_context(
        self,
        ctx: Dict[str, Any],
        verbose: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """
        Loads all Runtime Deps
        """
        if verbose is None: verbose = self.debug_enabled
        for name, obj in self.tasks.context.items():
            if name not in ctx: 
                ctx[name] = obj
                if verbose: logger.info(f'[context] setting ctx[{name}]: {obj}')
        
        ctx = await self.run_dependencies(ctx, verbose)
        ctx = await self.run_context(ctx, verbose)
        ctx = await self.run_startup_funcs(ctx, verbose)
        return ctx
    
    # Handler Methods to add
    # to worker queue tasks

    def add_context(
        self,
        obj: Optional[Any] = None,
        name: Optional[str] = None,
        verbose: Optional[bool] = None,
        silenced: Optional[bool] = None,
        _fx: Optional[Callable] = None,
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
        if verbose is None: verbose = self.debug_enabled
        if obj is not None:
            name = name or obj.__name__
            if callable(obj):
                self.tasks.context_funcs[name] = (obj, kwargs)
                if verbose: logger.info(f"Registered context function {name}: {obj}")
            else:
                self.tasks.context[name] = obj
                if verbose: logger.info(f"Registered context {name}: {obj}")
            if silenced is True:
                self.add_function_to_silenced(name)
                # self.tasks.silenced_functions.append(name)
            return
        
        if _fx is not None:
            name = name or _fx.__name__
            self.tasks.context_funcs[name] = (_fx, kwargs)
            if verbose: logger.info(f"Registered context function {name}: {_fx}")
            if silenced is True: 
                self.add_function_to_silenced(name)
                # self.tasks.silenced_functions.append(name)
            return

        # Create a wrapper
        def wrapper(func: Callable):
            func_name = name or func.__name__
            self.tasks.context_funcs[func_name] = (func, kwargs)
            if verbose: logger.info(f"Registered context function {func_name}: {func}")
            if silenced is True: 
                self.add_function_to_silenced(func_name)
                # self.tasks.silenced_functions.append(func_name)
            return func
        
        return wrapper


    def add_dependency(
        self,
        obj: Optional[Any] = None,
        name: Optional[str] = None,
        verbose: Optional[bool] = None,
        _fx: Optional[Callable] = None,
        silenced: Optional[bool] = None,
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
        if verbose is None: verbose = self.debug_enabled
        if obj is not None:
            name = name or obj.__name__
            self.tasks.dependencies[name] = (obj, kwargs)
            if verbose: logger.info(f"Registered dependency {name}: {obj}")
            if silenced is True: 
                self.add_function_to_silenced(name)
                # self.tasks.silenced_functions.append(name)
            return
        
        if _fx is not None:
            name = name or _fx.__name__
            self.tasks.dependencies[name] = (_fx, kwargs)
            if verbose: logger.info(f"Registered dependency {name}: {_fx}")
            if silenced is True: 
                self.add_function_to_silenced(name)
                # self.tasks.silenced_functions.append(name)
            return
        
        # Create a wrapper
        def wrapper(func: Callable):
            func_name = name or func.__name__
            self.tasks.dependencies[func_name] = (func, kwargs)
            if verbose: logger.info(f"Registered depency{func_name}: {func}")
            if silenced is True: 
                self.add_function_to_silenced(func_name)
                # self.tasks.silenced_functions.append(func_name)
            return func
        
        return wrapper


    def on_startup(
        self,
        name: Optional[str] = None,
        verbose: Optional[bool] = None,
        _fx: Optional[Callable] = None,
        silenced: Optional[bool] = None,
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
        if verbose is None: verbose = self.debug_enabled
        if _fx is not None:
            name = name or _fx.__name__
            self.tasks.startup_funcs[name] = (_fx, kwargs)
            if verbose: logger.info(f"Registered startup function {name}: {_fx}")
            if silenced is True: 
                self.add_function_to_silenced(name)
                # self.tasks.silenced_functions.append(name)
            return
        
        def decorator(func: Callable):
            func_name = name or func.__name__
            self.tasks.startup_funcs[func_name] = (func, kwargs)
            if verbose: logger.info(f"Registered startup function {func_name}: {func}")
            if silenced is True: 
                self.add_function_to_silenced(name)
                # self.tasks.silenced_functions.append(func_name)
            # logger.info(f"Registered startup function {func_name}: {func}: {kwargs}")
            return func
        
        return decorator

    def on_shutdown(
        self,
        name: Optional[str] = None,
        verbose: Optional[bool] = None,
        _fx: Optional[Callable] = None,
        silenced: Optional[bool] = None,
        **kwargs,
    ):
        """
        Add a shutdown function to the worker queue.
        """
        if verbose is None: verbose = self.debug_enabled
        if _fx is not None:
            name = name or _fx.__name__
            self.tasks.shutdown_funcs[name] = (_fx, kwargs)
            if verbose: logger.info(f"Registered shutdown function {name}: {_fx}")
            if silenced is True: 
                self.add_function_to_silenced(name)
                # self.tasks.silenced_functions.append(name)
            return
        
        def decorator(func: Callable):
            func_name = name or func.__name__
            self.tasks.shutdown_funcs[func_name] = (func, kwargs)
            if verbose: logger.info(f"Registered shutdown function {func_name}: {func}")
            if silenced is True: 
                self.add_function_to_silenced(name)
                # self.tasks.silenced_functions.append(func_name)
            return func
        return decorator

    def add_function(
        self,
        name: Optional[str] = None,
        _fx: Optional[Callable] = None,
        verbose: Optional[bool] = None,
        silenced: Optional[bool] = None,
        silenced_stages: Optional[List[str]] = None,
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
            if name:
                self.tasks.functions.append((name, _fx))
            else:
                self.tasks.functions.append(_fx)
            if verbose:
                logger.info(f"Registered function {name}: {_fx}")
            if silenced is True or silenced_stages: 
                self.add_function_to_silenced(name, silenced_stages = silenced_stages)
            return
        
        def decorator(func: Callable):
            nonlocal name
            func_name = name or func.__name__
            if name:
                self.tasks.functions.append((name, func))
            else:
                self.tasks.functions.append(func)
            if verbose:
                logger.info(f"Registered function {func_name}")
            if silenced is True or silenced_stages: 
                self.add_function_to_silenced(func_name, silenced_stages = silenced_stages)
            return func
        
        return decorator
    
    def add_function_to_silenced(
        self,
        name: str,
        silenced_stages: Optional[List[str]] = None,
        **kwargs
    ):
        """
        Adds a function to the silenced functions
        """
        if silenced_stages:
            for stage in silenced_stages:
                if stage not in self.tasks.silenced_functions_by_stage: continue
                if name not in self.tasks.silenced_functions_by_stage[stage]:
                    self.tasks.silenced_functions_by_stage[stage].append(name)
        
        elif name not in self.tasks.silenced_functions:
            self.tasks.silenced_functions.append(name)
    
    def is_silenced_function(
        self,
        name: str,
        stage: Optional[str] = None,
    ) -> bool:
        """
        Checks if a function is silenced
        """
        if name in self.tasks.silenced_functions: return True
        if stage: return name in self.tasks.silenced_functions_by_stage.get(stage, [])
        return False
    
    @property
    def has_silenced_functions(self) -> bool:
        """
        Returns whether there are silenced functions
        """
        if self.tasks.silenced_functions: return True
        return any(self.tasks.silenced_functions_by_stage.values())

    @property
    def silenced_function_dict(self) -> Dict[str, List[str]]:
        """
        Returns the silenced functions dict
        """
        return {
            'global': self.tasks.silenced_functions,
            **self.tasks.silenced_functions_by_stage,
        }


    def set_queue_func(
        self,
        queue_func: Union[Callable, 'TaskQueue'],
    ):
        """
        Sets the queue function to use.
        """
        self.tasks.queue_func = queue_func
    
    def get_queue_func(
        self,
        queue_func: Optional[Union[Callable, 'TaskQueue']] = None,
    ) -> 'TaskQueue':
        """
        Gets the queue function to use.
        """
        queue_func = queue_func or self.tasks.queue_func
        return queue_func() if callable(queue_func) else queue_func

    def add_fallback_function(
        self,
        verbose: Optional[bool] = None,
        silenced: Optional[bool] = None,
        method = "apply",
        timeout: Optional[int] = None,
        suppressed_exceptions: Optional[list] = None,
        failed_results: Optional[list] = None,
        queue_func: Optional[Union[Callable, 'TaskQueue']] = None,
        silenced_stages: Optional[List[str]] = None,
        **kwargs,
    ):
        """
        Creates a fallback function for the worker.

        - attempts to apply the function to the queue, if it fails, it will
        attempt to run the function locally.
        """
        if not suppressed_exceptions: suppressed_exceptions = [Exception]
        if verbose is None: verbose = self.debug_enabled
        if timeout is None: timeout = self.job_timeout
        def decorator(func: Callable):
            self.tasks.functions.append(func)
            name = func.__name__
            if verbose: logger.info(f"Registered fallback function {name}")
            if silenced is True or silenced_stages: self.add_function_to_silenced(name, silenced_stages = silenced_stages)

            @functools.wraps(func)
            async def wrapper(**kwargs):
                with contextlib.suppress(*suppressed_exceptions):
                    queue = self.get_queue_func(queue_func)
                    res = await getattr(queue, method)(
                        name, 
                        timeout = timeout, 
                        **kwargs
                    )
                    if failed_results and res in failed_results: raise ValueError(res)
                    return res
                return await func(ctx = None, **kwargs)
            return wrapper
        return decorator
        

    def add_cronjob(        
        self,
        schedule: Optional[Union[Dict, List, str]] = None,
        _fx: Optional[Callable] = None,
        name: Optional[str] = None,
        verbose: Optional[bool] = None,
        silenced: Optional[bool] = None,
        silenced_stages: Optional[List[str]] = None,
        default_kwargs: Optional[dict] = None,
        callback: Optional[Union[str, Callable]] = None,
        callback_kwargs: Optional[dict] = None,
        **kwargs,
    ):
        """
        Adds a function to `WorkerTask.cronjobs`.
        WorkerCronFuncs = {
            {'coroutine': refresh_spot_data, 'name': 'refresh_spot_data', 'minute': {10, 30, 50}},
        }
        """
        if verbose is None: verbose = self.debug_enabled
        if schedule and isinstance(schedule, str): schedule = validate_cron_schedule(schedule)
        if _fx is not None:
            cron = {'function': _fx, 'cron_name': name, 'default_kwargs': default_kwargs, 'cron': schedule, 'silenced': silenced, 'callback': callback, **kwargs}
            if callback_kwargs: cron['callback_kwargs'] = callback_kwargs

            self.tasks.cronjobs.append(cron)
            if silenced or silenced_stages: 
                self.add_function_to_silenced(name or _fx.__qualname__, silenced_stages = silenced_stages)
            if verbose: logger.info(f'Registered CronJob: {cron}')
            return
        
        def decorator(func: Callable):
            nonlocal schedule
            cron = {'function': func, 'cron': schedule, 'cron_name': name, 'default_kwargs': default_kwargs, 'silenced': silenced, 'callback': callback, **kwargs}
            if callback_kwargs: cron['callback_kwargs'] = callback_kwargs
            
            self.tasks.cronjobs.append(cron)
            if silenced or silenced_stages: 
                self.add_function_to_silenced(name or func.__qualname__, silenced_stages = silenced_stages)
            if verbose: logger.info(f'Registered CronJob: {cron}')
            return func
        return decorator
    
    
    def add(
        self,
        task: TaskType = TaskType.default,
        name: Optional[str] = None,
        obj: Optional[Any] = None,
        verbose: Optional[bool] = None,
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
                return self.add_function(_fx = obj, name = name, verbose = verbose, **kwargs)
            if task == TaskType.cronjob:
                return self.add_cronjob(_fx = obj, name = name, verbose = verbose, **kwargs)
            if task == TaskType.context:
                return self.add_context(obj = obj, name = name, verbose = verbose, **kwargs)
            if task == TaskType.dependency:
                return self.add_dependency(obj = obj, name = name, verbose = verbose, **kwargs)
        
        def wrapper(func: Callable):
            if task in {TaskType.default, TaskType.function}:
                return self.add_function(_fx = func, name = name, verbose = verbose, **kwargs)
            if task == TaskType.cronjob:
                return self.add_cronjob(_fx = func, name = name, verbose = verbose, **kwargs)
            if task == TaskType.context:
                return self.add_context(_fx = func, name = name, verbose = verbose, **kwargs)
            if task == TaskType.dependency:
                return self.add_dependency(_fx = func, name = name, verbose = verbose, **kwargs)
            if task == TaskType.startup:
                return self.on_startup(_fx = func, name = name, verbose = verbose, **kwargs)
            if task == TaskType.shutdown:
                return self.on_shutdown(_fx = func, name = name, verbose = verbose, **kwargs)
        
        return wrapper