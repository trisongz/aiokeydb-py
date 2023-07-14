import os
import gc
import json
import time
import asyncio
import typing
import anyio
import threading

from contextlib import asynccontextmanager, suppress
from lazyops.utils.logs import default_logger as logger
# from lazyops.utils import logger
from lazyops.utils.serialization import ObjectEncoder
from aiokeydb.v2.types.base import BaseModel, Field, validator
from aiokeydb.v2.types.base import KeyDBUri, lazyproperty
from aiokeydb.v2.types.session import KeyDBSession

from aiokeydb.v2.serializers import SerializerType
from aiokeydb.v2.client import KeyDBClient
from aiokeydb.v2.commands import AsyncScript

from aiokeydb.v2.connection import (
    ConnectionPool, 
    BlockingConnectionPool,
    AsyncConnectionPool,
    AsyncBlockingConnectionPool
)
from aiokeydb.v2.types.jobs import (
    Job,
    JobStatus,
    FunctionTracker,
    TERMINAL_STATUSES,
    UNSUCCESSFUL_TERMINAL_STATUSES,
)
import aiokeydb.v2.exceptions as exceptions
from aiokeydb.v2.exceptions import JobError
from aiokeydb.v2.utils.queue import (
    millis,
    now,
    seconds,
    uuid4,
    get_default_job_key,
    ensure_coroutine_function,
    get_hostname,
    concurrent_map,
    timer,
    build_batches,
)

from aiokeydb.v2.configs import settings
from aiokeydb.v2.utils import set_ulimits, get_ulimits
from aiokeydb.v2.backoff import default_backoff
from redis.asyncio.retry import Retry



class QueueStats(BaseModel):
    """
    Holds the state for the queue
    """
    
    name: str
    prefix: typing.Optional[str] = 'queue'
    uuid: typing.Optional[str] = Field(default_factory = uuid4)
    started: typing.Optional[int] = Field(default_factory = now)
    last_job: typing.Optional[int] = Field(default_factory = now)
    complete: typing.Optional[int] = 0
    failed: typing.Optional[int] = 0
    aborted: typing.Optional[int] = 0
    retried: typing.Optional[int] = 0
    num_workers: typing.Optional[int] = 0
    active_workers: typing.Optional[typing.List[str]] = []
    worker_attributes: typing.Optional[typing.Dict[str, typing.Dict[str, typing.Any]]] = {}

    @validator('uuid')
    def validate_uuid(cls, v):
        return uuid4() if v is None else v

    @lazyproperty
    def incomplete_key(self) -> str:
        return f"{self.prefix}:{self.name}:incomplete"
    
    @lazyproperty
    def queued_key(self) -> str:
        return f"{self.prefix}:{self.name}:queued"
    
    @lazyproperty
    def active_key(self) -> str:
        return f"{self.prefix}:{self.name}:active"
    
    @lazyproperty
    def scheduled_key(self) -> str:
        return f"{self.prefix}:{self.name}:scheduled"
    
    @lazyproperty
    def sweep_key(self) -> str:
        return f"{self.prefix}:{self.name}:sweep"
    
    @lazyproperty
    def stats_key(self) -> str:
        return f"{self.prefix}:{self.name}:stats"
    
    @lazyproperty
    def complete_key(self) -> str:
        return f"{self.prefix}:{self.name}:complete"
    
    @lazyproperty
    def queue_info_key(self) -> str:
        return f"{self.prefix}:{self.name}:queue_info"
    
    @lazyproperty
    def worker_token_key(self) -> str:
        return f"{self.prefix}:{self.name}:token"
    
    @lazyproperty
    def management_queue_key(self) -> str:
        return f"{self.prefix}:{self.name}:mtg"
    
    @lazyproperty
    def abort_id_prefix(self) -> str:
        return f"{self.prefix}:{self.name}:abort"
    
    @lazyproperty
    def heartbeat_key(self) -> str:
        return f"{self.prefix}:{self.name}:heartbeat"
    
    @lazyproperty
    def function_tracker_key(self) -> str:
        return f"{self.prefix}:{self.name}:functiontracker"
    
    @lazyproperty
    def queue_job_ids_key(self) -> str:
        return f"{self.prefix}:{self.name}:jobids"

    def create_namespace(self, namespace: str) -> str:
        return f"{self.prefix}:{self.name}:{namespace}"
    
    @lazyproperty
    def node_name(self) -> str:
        get_hostname()


class TaskQueue:
    """
    TaskQueue is used to interact with aiokeydb.

    queue_name: name of the job queue
    prefix: prefix for the job queue
    db: database id for the job queue

    serializer: serializer for the job queue. If it is not specified, the default
        serializer will be used.
        Expects the following methods:
            dump: lambda that takes a dictionary and outputs bytes (default json.dumps)
            load: lambda that takes bytes and outputs a python dictionary (default json.loads)
    
    max_concurrency: maximum concurrent operations.
        This throttles calls to `enqueue`, `job`, and `abort` to prevent the Queue
        from consuming too many KeyDB connections.

    client_mode: whether to run in client mode. If True, the queue will not
        create any keys in the database, and will not create any management
        keys. This is useful for running a worker in a client-only mode.
    
    uuid: uuid of the queue
    """
    def __init__(  # pylint: disable=too-many-arguments
        self,
        queue_name: typing.Optional[str] = None,
        prefix: typing.Optional[str] = None,
        db: typing.Optional[int] = None,
        serializer: typing.Optional[typing.Union[object, SerializerType]] = None,
        max_concurrency: typing.Optional[int] = None,
        max_broadcast_concurrency: typing.Optional[int] = None,
        client_mode: typing.Optional[bool] = False,
        debug_enabled: typing.Optional[bool] = None,
        uuid: typing.Optional[str] = None,
        metrics_func: typing.Optional[typing.Callable] = None,
        verbose_results: typing.Optional[bool] = False,
        truncate_logs: typing.Optional[bool] = True,
        logging_max_length: typing.Optional[int] = 500,
        silenced_functions: typing.Optional[typing.List[str]] = None, # Don't log these functions
        heartbeat_ttl: typing.Optional[int] = None, # 10,
        is_leader_process: typing.Optional[bool] = None,
        function_tracker_enabled: typing.Optional[bool] = None,
        function_tracker_ttl: typing.Optional[int] = None,
        **kwargs
    ): # sourcery skip: low-code-quality
        self.settings = settings
        self.prefix = prefix if prefix is not None else self.settings.worker.prefix
        self.queue_name = queue_name if queue_name is not None else self.settings.worker.queue_name
        self.serializer = serializer if serializer is not None else self.settings.worker.job_serializer.get_serializer()
        
        self._ctx_kwargs = {
            'db_id': db if db is not None else self.settings.worker.db,
            **kwargs        
        }

        self.client_mode = client_mode
        self.max_concurrency = max_concurrency or self.settings.worker.max_concurrency
        self.max_broadcast_concurrency = max_broadcast_concurrency or self.settings.worker.max_broadcast_concurrency or 3
        
        self.debug_enabled = debug_enabled if debug_enabled is not None else self.settings.worker.debug_enabled
        self.function_tracker_enabled = function_tracker_enabled if function_tracker_enabled is not None else self.settings.worker.function_tracker_enabled
        self.function_tracker_ttl = function_tracker_ttl if function_tracker_ttl is not None else self.settings.worker.function_tracker_ttl

        self._stats = QueueStats(
            name = self.queue_name,
            prefix = self.prefix,
            uuid = uuid,
        )
        self.uuid = self._stats.uuid
        self._version = None
        self._schedule_script: AsyncScript = None
        self._enqueue_script: AsyncScript = None
        self._cleanup_script: AsyncScript = None

        self._before_enqueues = {}
        self._op_sem = asyncio.Semaphore(self.max_concurrency)
        self._metrics_func = metrics_func

        self.verbose_results = verbose_results
        self.truncate_logs = truncate_logs
        self.logging_max_length = logging_max_length
        self.heartbeat_ttl = heartbeat_ttl or self.settings.worker.heartbeat_interval
        self._worker_name: str = None
        self.is_leader_process = is_leader_process if is_leader_process is not None else (self.settings.worker.is_leader_process if self.settings.worker.is_leader_process is not None else True)
        self.queue_pid: int = os.getpid()
        if silenced_functions:
            self.add_silenced_functions(*silenced_functions)
    
    @lazyproperty
    def _lock(self) -> asyncio.Lock:
        """
        Returns a lock for the queue.
        """
        return asyncio.Lock()
    
    @lazyproperty
    def ctx(self) -> KeyDBSession:
        """
        Returns the KeyDBSession for the queue.
        """
        if 'socket_connect_timeout' not in self._ctx_kwargs:
            self._ctx_kwargs['socket_connect_timeout'] = self.settings.worker.socket_connect_timeout
        if 'socket_keepalive' not in self._ctx_kwargs:
            self._ctx_kwargs['socket_keepalive'] = self.settings.worker.socket_keepalive
        if 'health_check_interval' not in self._ctx_kwargs:
            self._ctx_kwargs['health_check_interval'] = self.heartbeat_ttl
        
        return KeyDBClient.create_session(
            name = self.queue_name,
            serializer = False,
            cache_enabled = False,
            decode_responses = False,
            auto_pubsub = False,
            set_current = False,

            max_connections = self.max_concurrency * 10,
            pool_class = ConnectionPool,

            amax_connections = self.max_concurrency ** 2,
            apool_class = AsyncConnectionPool,
            **self._ctx_kwargs
        )

    async def _get_stats(self, log_stats: bool = False, include_jobs: bool = True, include_conn_kwargs: bool = True, include_retries: bool = False):
        """
        Fetches the stats for the queue.
        """
        _stats = await self.ctx._async_get_stats()
        _stats['version'] = self.version
        _stats['workers'] = {
            'num': self.num_workers,
            'active': self.active_workers,
        }
        _stats['queue'] = await self.info()
        if not include_jobs:
            _stats['queue']['jobs'] = len(_stats['queue'].get('jobs', []))
        if include_conn_kwargs: 
            _stats['connection_kwargs'] = {**self.ctx.async_client.connection_pool.connection_kwargs, **self._ctx_kwargs}
            if not include_retries:
                _stats['connection_kwargs']['retry_on_error'] = len(_stats['connection_kwargs']['retry_on_error'])
            
            # Remove password
            _ = _stats['connection_kwargs'].pop('password', None)

        if log_stats: 
            self.logger(kind = 'stats').info(f"{_stats}")
            if _stats.get('max_connections_used') > 95.0:
                self.logger(kind = 'stats').warning(f"Nearing Maximum connection client Limit: {_stats.get('max_connections_used')}")
        return _stats
    
    @lazyproperty
    def version(self):
        """
        Returns the version of the redis server.
        """
        info = self.ctx.info()
        return tuple(int(i) for i in info["redis_version"].split("."))
    
    @lazyproperty
    def uri(self) -> KeyDBUri:
        """
        Returns the connection for the queue.
        """
        return self.ctx.uri

    @lazyproperty
    def db_id(self):
        """
        Returns the database id for the queue.
        """
        return self.ctx.db_id
    
    @property
    def _should_debug_log(self):
        """
        Returns whether the queue should debug log.
        """
        return self.debug_enabled and self.is_leader_process
    

    def is_silenced_function(
        self,
        name: str,
        stage: typing.Optional[str] = None,
    ) -> bool:
        """
        Checks if a function is silenced
        """
        return self.settings.worker.is_silenced_function(name, stage = stage)


    def add_silenced_functions(self, *functions):
        """
        Adds functions to the list of silenced functions.
        """
        for func in functions:
            self.settings.worker.add_function_to_silenced(func)

    def logger(self, job: 'Job' = None, kind: str = "enqueue", job_id: typing.Optional[str] = None,):
        """
        Returns a logger for the queue.
        """
        if job:
            return logger.bind(
                job_id = job.id,
                status = job.status,
                worker_name = self._worker_name,
                queue_name = getattr(job.queue, 'queue_name', None) or 'unknown queue',
                kind = kind,
            )
        elif job_id:
            return logger.bind(
                job_id = job_id,
                worker_name = self._worker_name,
                queue_name = self.queue_name,
                kind = kind,
            )
        else:
            return logger.bind(
                worker_name = self._worker_name,
                queue_name = self.queue_name,
                kind = kind,
            )
    
    @property
    def job_id_prefix(self) -> str:
        """
        Returns the job id prefix.
        """
        return f"queue:{self.queue_name}:{self.settings.worker.job_prefix}"

    def job_id(self, job_key):
        """
        Returns the job id for a job key.
        """
        return f"{self.job_id_prefix}:{job_key}" if \
            self.job_id_prefix not in job_key else job_key
    
    def register_before_enqueue(self, callback):
        """
        Registers a callback to run before enqueue.
        """
        self._before_enqueues[id(callback)] = callback

    def unregister_before_enqueue(self, callback):
        """
        Unregisters a callback to run before enqueue.
        """
        self._before_enqueues.pop(id(callback), None)

    async def _before_enqueue(self, job: Job):
        """
        Helper function to run before enqueue.
        """
        for cb in self._before_enqueues.values():
            await cb(job)

    @asynccontextmanager
    async def pipeline(self, transaction: bool = True, shard_hint: typing.Optional[str] = None, raise_on_error: typing.Optional[bool] = None):
        """
        Context manager to run a pipeline.

        This context manager is a wrapper around the redis pipeline context manager to ensure that it is
        properly deleted.
        """
        yield self.ctx.async_pipeline
        # pipe = self.ctx.async_client.pipeline(transaction = transaction, shard_hint = shard_hint)
        # pipe = self.ctx.async_pipeline
        # yield pipe
        # await pipe.execute(raise_on_error = raise_on_error)
        # await pipe.close(close_connection_pool = False)
        # del pipe

    def serialize(self, job: Job):
        """
        Dumps a job.
        """
        return self.serializer.dumps(job.to_dict())

    def deserialize(self, job_bytes: bytes):
        """
        Deserializes a job.
        """
        if not job_bytes: return None
        job_dict: typing.Dict = self.serializer.loads(job_bytes)
        assert (
                job_dict.pop("queue") == self.queue_name
        ), f"Job {job_dict} fetched by wrong queue: {self.queue_name}"
        return Job(**job_dict, queue=self)

    async def disconnect(self):
        """
        Disconnects the queue.
        """
        await self.ctx.aclose()

    
    async def prepare_server(self):
        """
        Prepares the keydb server to ensure that the maximum number of concurrent
        connections are available.
        """
        self.ctx.config['transaction'] = True
        info = await self.ctx.async_info()
        await self.prepare_for_broadcast()
        if 'maxclients' not in info:
            if self._should_debug_log: logger.warning(f'Unable to configure the maxclients to {self.max_concurrency}: maxclients not supported: {info}')
            return

        # Each Worker spawns a connection
        min_connections = ((self.num_workers or 1) * (self.max_concurrency + self.max_broadcast_concurrency)) * 10

        curr_max_connections = info['maxclients']
        curr_connected = info['connected_clients']

        new_max_connections = max(
            min_connections,
            (curr_connected + min_connections),
            curr_max_connections
        )
        curr_ulimits = get_ulimits()
        if curr_ulimits < new_max_connections and self._should_debug_log:
            logger.debug(f'The maximum number of concurrent connections may not be supported as ulimits: {curr_ulimits} < desired max connections: {new_max_connections}')

        if self._should_debug_log: logger.info(f'Configuring Server: Current Max Connections: {curr_max_connections}, Current Connected: {curr_connected}, Min Connections: {min_connections} -> New Max: {new_max_connections}')
        if new_max_connections > curr_max_connections:
            try:
                await self.ctx.config_set('maxclients', new_max_connections)
                info = await self.ctx.async_info()
                if 'maxclients' in info:
                    new_set_max_connections = info['maxclients']
                    if self._should_debug_log: logger.debug(f'New Max Connections: {new_set_max_connections}')
            except Exception as e:
                if self._should_debug_log:
                    logger.warning(f'Unable to configure the maxclients to {new_max_connections}: {e}')
        # logger.info(f'Pool Class: {self.ctx.client_pools.apool.__class__.__name__}')


    """
    Primary APIs
    """

    async def schedule(self, lock: int = 1, worker_id: typing.Optional[str] = None):
        """
        Schedule jobs.
        """
        if not self._schedule_script:
            self._schedule_script = self.ctx.async_client.register_script(
                """
                if redis.call('EXISTS', KEYS[1]) == 0 then
                    redis.call('SETEX', KEYS[1], ARGV[1], 1)
                    local jobs = redis.call('ZRANGEBYSCORE', KEYS[2], 1, ARGV[2])

                    if next(jobs) then
                        local scores = {}
                        for _, v in ipairs(jobs) do
                            table.insert(scores, 0)
                            table.insert(scores, v)
                        end
                        redis.call('ZADD', KEYS[2], unpack(scores))
                        redis.call('RPUSH', KEYS[3], unpack(jobs))
                    end

                    return jobs
                end
                """
            )

        scheduled_key = self.scheduled_key if worker_id is None else f"{self.scheduled_key}:{worker_id}"
        incomplete_key = self.incomplete_key if worker_id is None else f"{self.incomplete_key}:{worker_id}"
        queued_key = self.queued_key if worker_id is None else f"{self.queued_key}:{worker_id}"

        return await self._schedule_script(
            keys=[scheduled_key, incomplete_key, queued_key],
            args=[lock, seconds(now())],
        )

    async def sweep(self, lock: int = 60, worker_id: typing.Optional[str] = None):
        """
        Sweep jobs.
        """
        # sourcery skip: low-code-quality
        if not self._cleanup_script:
            self._cleanup_script = self.ctx.async_client.register_script(
                """
                if redis.call('EXISTS', KEYS[1]) == 0 then
                    redis.call('SETEX', KEYS[1], ARGV[1], 1)
                    return redis.call('LRANGE', KEYS[2], 0, -1)
                end
                """
            )

        active_key = self.active_key if worker_id is None else f"{self.active_key}:{worker_id}"
        sweep_key = self.sweep_key if worker_id is None else f"{self.sweep_key}:{worker_id}"
        incomplete_key = self.incomplete_key if worker_id is None else f"{self.incomplete_key}:{worker_id}"

        job_ids = await self._cleanup_script(
            # keys = [self.sweep_key, self.active_key] if worker_id is None else [self.sweep_key, f"{self.active_key}:{worker_id}"],
            keys = [sweep_key, active_key],
            args = [lock], 
            client = self.ctx.async_client,
        )

        swept = []
        if job_ids:
            for job_id, job_bytes in zip(job_ids, await self.ctx.async_mget(job_ids)):
                try:
                    job = self.deserialize(job_bytes)
                except Exception as e:
                    job = None
                    self.logger(kind = "sweep").warning(f"Unable to deserialize job {job_id}: {e}")
                if not job: 
                    swept.append(job_id)
                    # async with self.pipeline(transaction = True) as pipe:
                    async with self.ctx.async_client.pipeline(transaction = True) as pipe:
                        await (
                            pipe.lrem(active_key, 0, job_id)
                            .zrem(incomplete_key, job_id)
                            .execute()
                        )
                        # await (
                        #     pipe.lrem(self.active_key, 0, job_id)
                        #     .zrem(self.incomplete_key, job_id)
                        #     .execute()
                        # )
                    self.logger(kind = "sweep").info(f"Sweeping missing job {job_id}")
                elif job.status != JobStatus.ACTIVE or job.stuck:
                    swept.append(job_id)
                    await job.finish(JobStatus.ABORTED, error="swept")
                    if self.is_silenced_function(job.function, stage = 'sweep'):
                        pass
                    elif self.verbose_results:
                        self.logger(job=job, kind = "sweep").info(f"☇ duration={job.duration('total')}ms, node={self.node_name}, func={job.function}, result={job.result}")
                    elif self.truncate_logs:
                        job_result = (
                            f'{str(job.result)[:self.logging_max_length]}...'
                            if self.logging_max_length
                            else str(job.result)
                        )
                        self.logger(job=job, kind = "sweep").info(f"☇ duration={job.duration('total')}ms, node={self.node_name}, func={job.function}, result={job_result}")
                    else:
                        self.logger(job=job, kind = "sweep").info(f"☇ duration={job.duration('total')}ms, node={self.node_name}, func={job.function}")
        gc.collect()
        return swept
    
    async def sweep_job(self, job_id: typing.Optional[str] = None, worker_id: typing.Optional[str] = None, job: typing.Optional[Job] = None, verbose: typing.Optional[bool] = None):
        # sourcery skip: low-code-quality
        """
        Sweep a single job.
        """
        assert job_id or job, "Must provide either job_id or job"
        job = await self._get_job_by_id(job_id) if job is None else job
        job_id = job_id if job_id is not None else job.id
        verbose = verbose if verbose is not None else self.verbose_results
        if not job:
            if verbose: self.logger(kind = "sweep").info(f"Sweeping missing job {job_id}")
        elif job.status != JobStatus.ACTIVE or job.stuck:
            await job.finish(JobStatus.ABORTED, error="swept")
            if self.is_silenced_function(job.function, stage = 'sweep'):
                pass
            elif verbose is False:
                self.logger(job=job, kind = "sweep").info(f"☇ duration={job.duration('total')}ms, node={self.node_name}, func={job.function}")
            elif self.verbose_results:
                self.logger(job=job, kind = "sweep").info(f"☇ duration={job.duration('total')}ms, node={self.node_name}, func={job.function}, result={job.result}")
            elif self.truncate_logs:
                job_result = (
                    f'{str(job.result)[:self.logging_max_length]}...'
                    if self.logging_max_length
                    else str(job.result)
                )
                self.logger(job=job, kind = "sweep").info(f"☇ duration={job.duration('total')}ms, node={self.node_name}, func={job.function}, result={job_result}")
                
        active_key = self.active_key if worker_id is None else f"{self.active_key}:{worker_id}"
        incomplete_key = self.incomplete_key if worker_id is None else f"{self.incomplete_key}:{worker_id}"
        async with self.ctx.async_client.pipeline(transaction = True) as pipe:
            await (
                pipe.lrem(active_key, 0, job_id)
                .zrem(incomplete_key, job_id)
                .execute()
            )
        return True

    async def listen(self, job_keys: typing.List[str], callback: typing.Callable, timeout: int = 10):
        """
        Listen to updates on jobs.

        job_keys: sequence of job keys
        callback: callback function, if it returns truthy, break
        timeout: if timeout is truthy, wait for timeout seconds
        """
        pubsub = self.ctx.async_client.pubsub(retryable=True)
        
        job_ids = [self.job_id(job_key) for job_key in job_keys]
        await pubsub.subscribe(*job_ids)

        async def listen():
            async for message in pubsub.listen():
                if message["type"] == "message":
                    job_id = message["channel"].decode("utf-8")
                    job_key = Job.key_from_id(job_id)
                    status = JobStatus[message["data"].decode("utf-8").upper()]
                    if asyncio.iscoroutinefunction(callback):
                        stop = await callback(job_key, status)
                    else:
                        stop = callback(job_key, status)
                    if stop: break

        try:
            if timeout:
                await asyncio.wait_for(listen(), timeout)
            else:
                await listen()
        finally:
            await pubsub.unsubscribe(*job_ids)

    async def notify(self, job: Job):
        """
        Notify subscribers of a job update.
        """
        await self.ctx.async_client.publish(job.id, job.status)
        await job.run_job_callback()


    async def update(self, job: Job):
        """
        Update a job.
        """
        job.touched = now()
        await self.ctx.async_set(job.id, self.serialize(job))
        await self.notify(job)
        if self.function_tracker_enabled: 
            await self.track_job_id(job)

    async def job(self, job_key: str) -> Job:
        """
        Fetch a Job by key.
        """
        job_id = self.job_id(job_key)
        return await self._get_job_by_id(job_id)
    
    async def job_exists(self, job_key: str) -> bool:
        """
        Check if a job exists
        """
        job_id = self.job_id(job_key)
        return await self.ctx.async_exists(job_id)

    async def _get_job_by_id(self, job_id):
        async with self._op_sem:
            return self.deserialize(await self.ctx.async_get(job_id))

    async def wait_for_job_completion(self, job_id: str, results_only: typing.Optional[bool] = False, interval: float = 0.5) -> typing.Any:
        """
        Waits for a job completion
        """
        job = await self.job(job_id)
        if not job: raise ValueError(f"Job {job_id} not found")
        while job.status not in TERMINAL_STATUSES:
            await asyncio.sleep(interval)
            job = await self.job(job_id)
        if not results_only: return job
        return job.result if job.status == JobStatus.COMPLETE else job.error


    async def abort(self, job: Job, error: typing.Any, ttl: int = 5):
        """
        Abort a job.
        """
        async with self._op_sem:
            async with self.ctx.async_client.pipeline(transaction = True, retryable = True) as pipe:
            # async with self.pipeline(transaction = True) as pipe:
                dequeued, *_ = await (
                    pipe.lrem(job.queued_key, 0, job.id)
                    .zrem(job.incomplete_key, job.id)
                    .expire(job.id, ttl + 1)
                    .setex(job.abort_id, ttl, error)
                    .execute()
                )

            if dequeued:
                await job.finish(JobStatus.ABORTED, error = error)
                await self.ctx.async_delete(job.abort_id)
            else:
                await self.ctx.async_lrem(job.active_key, 0, job.id)
            
            await self.track_job(job)


    async def retry(self, job: Job, error: typing.Any):
        """
        Retry an errored job.
        """
        job_id = job.id
        job.status = JobStatus.QUEUED
        job.error = error
        job.completed = 0
        job.started = 0
        job.progress = 0
        job.touched = now()
        next_retry_delay = job.next_retry_delay()

        # async with self.pipeline(transaction = True) as pipe:
        async with self.ctx.async_client.pipeline(transaction=True, retryable = True) as pipe:
            pipe = pipe.lrem(job.active_key, 1, job_id)
            pipe = pipe.lrem(job.queued_key, 1, job_id)
            if next_retry_delay:
                scheduled = time.time() + next_retry_delay
                pipe = pipe.zadd(job.incomplete_key, {job_id: scheduled})
            else:
                pipe = pipe.zadd(job.incomplete_key, {job_id: job.scheduled})
                pipe = pipe.rpush(job.queued_key, job_id)
            await pipe.set(job_id, self.serialize(job)).execute()
            self.retried += 1
            await self.notify(job)
            if not self.is_silenced_function(job.function, stage = 'retry'):
                self.logger(job=job, kind = "retry").info(f"↻ duration={job.duration('running')}ms, node={self.node_name}, func={job.function}, error={job.error}")

    async def finish(
        self, 
        job: Job, 
        status: typing.Union[JobStatus, str], 
        *, 
        result: typing.Any = None, 
        error: typing.Any = None
    ):
        """
        Publish a job result.
        """
        job_id = job.id
        job.status = status
        job.result = result
        job.error = error
        job.completed = now()

        if status == JobStatus.COMPLETE:
            job.progress = 1.0

        async with self.ctx.async_client.pipeline(transaction=True, retryable = True) as pipe:
        # async with self.pipeline(transaction = True) as pipe:
            pipe = pipe.lrem(job.active_key, 1, job_id).zrem(job.incomplete_key, job_id)
            if job.ttl > 0:
                pipe = pipe.setex(job_id, job.ttl, self.serialize(job))
            elif job.ttl == 0:
                pipe = pipe.set(job_id, self.serialize(job))
            else:
                pipe.delete(job_id)

            await pipe.execute()

            if status == JobStatus.COMPLETE:
                self.complete += 1
            elif status == JobStatus.FAILED:
                self.failed += 1
            elif status == JobStatus.ABORTED:
                self.aborted += 1

            await self.notify(job)
            if self.debug_enabled:
                self.logger(job=job, kind = "finish").info(f"Finished {job}")
            elif self.is_silenced_function(job.function, stage = 'finish'):
                pass
            elif self.verbose_results or not self.truncate_logs:
                self.logger(job=job, kind="finish").info(f"● duration={job.duration('total')}ms, node={self.node_name}, func={job.function}")
            else:
                job_result = (
                    f'{str(job.result)[:self.logging_max_length]}...'
                    if self.logging_max_length
                    else str(job.result)
                )
                self.logger(job=job, kind="finish").info(f"● duration={job.duration('total')}ms, node={self.node_name}, func={job.function}, result={job_result}")
            
            await self.track_job(job)

    async def _dequeue_job_id(
        self,
        timeout: int = 0,
        postfix: str = None,
    ):
        """
        Dequeue a job from the queue.
        """
        queued_key, active_key = self.queued_key, self.active_key
        if postfix:
            queued_key = f'{self.queued_key}:{postfix}'
            active_key = f'{self.active_key}:{postfix}'

        if self.version < (6, 2, 0):
            return await self.ctx.async_brpoplpush(
                queued_key, 
                active_key, 
                timeout
            )
        return await self.ctx.async_blmove(
            source_list = queued_key, 
            destination_list = active_key, 
            source = "RIGHT", 
            destination = "LEFT", 
            timeout = timeout
        )

    async def dequeue(self, timeout = 0, worker_id: str = None, worker_name: str = None):
        """
        Dequeue a job from the queue.
        """
        job_id = await self._dequeue_job_id(timeout) if \
            worker_id is None and worker_name is None else None
        
        if job_id is None and worker_id:
            job_id = await self._dequeue_job_id(timeout, worker_id)
            
        if job_id is None and worker_name:
            job_id = await self._dequeue_job_id(timeout, worker_name)
        
        if job_id is not None:
            # logger.info(f'Fetched job id {job_id}: {queued_key}: {active_key}')
            return await self._get_job_by_id(job_id)

        if not worker_id and not worker_name: self.logger(kind="dequeue").info("Dequeue timed out")
        return None
    
    
    async def _deferred(
        self, 
        job_or_func: typing.Union[Job, str],
        wait_time: float = 10.0, 
        **kwargs
    ):
        """
        Defer a job by instance or string after a certain amount of time
        """
        await asyncio.sleep(wait_time)
        return await self.enqueue(job_or_func, **kwargs)

    async def defer(
        self, 
        job_or_func: typing.Union[Job, str],
        wait_time: float = 10.0, 
        **kwargs
    ):
        """
        Enqueue a job by instance or string after a certain amount of time

        Kwargs can be arguments of the function or properties of the job.
        If a job instance is passed in, it's properties are overriden.

        If the job has already been enqueued, this returns None.
        will execute asyncronously and return immediately.
        """
        asyncio.create_task(self._deferred(job_or_func, wait_time = wait_time, **kwargs))


    async def enqueue(
        self, 
        job_or_func: typing.Union[Job, str, typing.Callable],
        **kwargs
    ):
        """
        Enqueue a job by instance or string.

        Kwargs can be arguments of the function or properties of the job.
        If a job instance is passed in, it's properties are overriden.

        If the job has already been enqueued, this returns None.
        """
        job = Job.from_kwargs(job_or_func = job_or_func, **kwargs)

        if not self._enqueue_script:
            self._enqueue_script = self.ctx.async_client.register_script(
                """
                if not redis.call('ZSCORE', KEYS[1], KEYS[2]) and redis.call('EXISTS', KEYS[4]) == 0 then
                    redis.call('SET', KEYS[2], ARGV[1])
                    redis.call('ZADD', KEYS[1], ARGV[2], KEYS[2])
                    if ARGV[2] == '0' then redis.call('RPUSH', KEYS[3], KEYS[2]) end
                    return 1
                else
                    return nil
                end
                """
            )

        self.last_job = now()
        job.queue = self
        job.queued = now()
        job.status = JobStatus.QUEUED

        await self._before_enqueue(job)

        async with self._op_sem:
            if not await self._enqueue_script(
                    keys=[job.incomplete_key, job.id, job.queued_key, job.abort_id],
                    args=[self.serialize(job), job.scheduled],
                    client = self.ctx.async_client,
            ):
                return None
        if not self.client_mode and self.debug_enabled:
            self.logger(job=job, kind="enqueue").info(f"Enqueuing {job}")
        elif self.is_silenced_function(job.function, stage = 'enqueue'):
            pass
        # elif job.function in self.silenced_functions:
        #     pass
        elif self.verbose_results:
            self.logger(job=job, kind="enqueue").info(
                f"→ duration={now() - job.queued}ms, node={self.node_name}, func={job.function}, timeout={job.timeout}, kwargs={job.kwargs}"
            )
        elif self.truncate_logs:
            job_kwargs = f"{str({k: str(v)[:self.logging_max_length] for k, v in job.kwargs.items()})[:self.logging_max_length]}..." if job.kwargs else ""
            self.logger(job=job, kind="enqueue").info(
                f"→ duration={now() - job.queued}ms, node={self.node_name}, func={job.function}, timeout={job.timeout}, kwargs={job_kwargs}"
            )
        else:
            self.logger(job=job, kind="enqueue").info(
                f"→ duration={now() - job.queued}ms, node={self.node_name}, func={job.function}, timeout={job.timeout}"
            )

        return job

    async def apply(
        self, 
        job_or_func: typing.Union[Job, str],
        timeout: int = None, 
        broadcast: typing.Optional[bool] = None,
        worker_names: typing.Optional[typing.List[str]] = None,
        worker_selector: typing.Optional[typing.Callable] = None,
        worker_selector_args: typing.Optional[typing.List] = None,
        worker_selector_kwargs: typing.Optional[typing.Dict] = None,
        workers_selected: typing.Optional[typing.List[typing.Dict[str, str]]] = None,
        return_all_results: typing.Optional[bool] = False,
        **kwargs
    ):
        """
        Enqueue a job and wait for its result.

        If the job is successful, this returns its result.
        If the job is unsuccessful, this raises a JobError.

        Example::
            try:
                assert await queue.apply("add", a=1, b=2) == 3
            except JobError:
                print("job failed")

        job_or_func: Same as Queue.enqueue
        kwargs: Same as Queue.enqueue
        timeout: How long to wait for the job to complete before raising a TimeoutError
        broadcast: Broadcast the job to all workers. If True, the job will be run on all workers.
        worker_names: List of worker names to run the job on. If provided, will run on these specified workers.
        worker_selector: Function that takes in a list of workers and returns a list of workers to run the job on. If provided, worker_names will be ignored.
        """
        results = await self.map(
            job_or_func, 
            timeout = timeout, 
            iter_kwargs = [kwargs],
            broadcast = broadcast,
            worker_names = worker_names,
            worker_selector = worker_selector,
            worker_selector_args = worker_selector_args,
            worker_selector_kwargs = worker_selector_kwargs,
            workers_selected = workers_selected,
        )
        return results if return_all_results else results[0]
    
    async def select_workers(
        self,
        func: typing.Callable,
        *args,
        **kwargs,
    ) -> typing.List[typing.Dict[str, str]]:
        """
        Passes the dict of the worker attributes to the 
        provided function and returns a list of workers that
        are able to run the job.

        Function should expect kw: `worker_attributes`: Dict[str, Dict[str, Any]]
        and should return List[Dict[str, str]] where the Dict is either
            {'worker_id': str}
        or
            {'worker_name': str}
        """
        await self.prepare_for_broadcast()
        func = ensure_coroutine_function(func)
        return await func(
            *args,
            worker_attributes = self.worker_attributes,
            **kwargs
        )

    async def prepare_job_kws(
        self,
        iter_kwargs: typing.Iterable[typing.Dict], 
        timeout: int = None,
        broadcast: typing.Optional[bool] = False,
        worker_names: typing.Optional[typing.List[str]] = None,
        worker_selector: typing.Optional[typing.Callable] = None,
        worker_selector_args: typing.Optional[typing.List] = None,
        worker_selector_kwargs: typing.Optional[typing.Dict] = None,
        workers_selected: typing.Optional[typing.List[typing.Dict[str, str]]] = None,
        **kwargs
    ) -> typing.Tuple[typing.List[typing.Dict[str, typing.Any]], typing.Optional[typing.List[typing.Dict[str, str]]]]:
        """
        Prepare jobs for broadcast or map.

        Returns:
            iter_kwargs: List of kwargs to pass to the job
            worker_kws: List of worker kwargs to pass to the job
        """
        if (
            not broadcast
            and not worker_selector
            and not worker_names
            and not workers_selected
        ):
            return [
                {
                    "timeout": timeout,
                    "key": kwargs.get("key") or get_default_job_key(),
                    **kwargs,
                    **kw,
                }
                for kw in iter_kwargs
            ], None
        if workers_selected: worker_kws = workers_selected
        elif worker_selector:
            worker_selector_args = worker_selector_args or []
            worker_selector_kwargs = worker_selector_kwargs or {}
            worker_kws = await self.select_workers(worker_selector, *worker_selector_args, **worker_selector_kwargs)
        elif worker_names: worker_kws = [{"worker_name": worker_name} for worker_name in worker_names]

        else:
            await self.prepare_for_broadcast()
            # worker_kws = [{"worker_id": worker_id} for worker_id in self.active_workers]
            worker_kws = [{"worker_id": worker_id} for worker_id in await self.get_worker_ids()]

        broadcast_kwargs = []
        for kw in iter_kwargs:
            broadcast_kwargs.extend(
                {
                    "timeout": timeout,
                    "key": get_default_job_key(),
                    **kwargs,
                    **kw,
                    **worker_kw,
                }
                for worker_kw in worker_kws
            )
        return broadcast_kwargs, worker_kws

    async def broadcast(
        self,
        job_or_func: typing.Union[Job, str],
        timeout: int = None,
        enqueue: typing.Optional[bool] = True,
        worker_names: typing.Optional[typing.List[str]] = None,
        worker_selector: typing.Optional[typing.Callable] = None,
        worker_selector_args: typing.Optional[typing.List] = None,
        worker_selector_kwargs: typing.Optional[typing.Dict] = None,
        workers_selected: typing.Optional[typing.List[typing.Dict[str, str]]] = None,
        **kwargs
    ):
        """
        Broadcast a job to all nodes and collect all of their results.
        
        job_or_func: Same as Queue.enqueue
        kwargs: Same as Queue.enqueue
        timeout: How long to wait for the job to complete before raising a TimeoutError
        worker_names: List of worker names to run the job on. If provided, will run on these specified workers.
        worker_selector: Function that takes in a list of workers and returns a list of workers to run the job on. If provided, worker_names will be ignored.
        """

        if not enqueue:
            return await self.map(
                job_or_func, 
                timeout = timeout, 
                iter_kwargs = [kwargs], 
                broadcast = True, 
                worker_names = worker_names,
                worker_selector = worker_selector,
                worker_selector_args = worker_selector_args,
                worker_selector_kwargs = worker_selector_kwargs,
                workers_selected = workers_selected,
            )
    
        iter_kwargs, worker_kws = await self.prepare_job_kws(
            iter_kwargs = [kwargs], 
            timeout = timeout, 
            broadcast = True, 
            worker_names = worker_names, 
            worker_selector = worker_selector,
            worker_selector_args = worker_selector_args,
            worker_selector_kwargs = worker_selector_kwargs,
            workers_selected = workers_selected,
            **kwargs
        )
        # if worker_kws:
        #     # logger.info(f"Enqueueing {job_or_func} to {len(worker_kws)} workers: {worker_kws}: {iter_kwargs}")
        jobs: typing.List[Job] = []
        for kw in iter_kwargs:
            jobs.append(
                await self.enqueue(
                    job_or_func,
                    **kw
                )
            )
        return jobs



    async def map(
        self, 
        job_or_func: typing.Union[Job, str],
        iter_kwargs: typing.Iterable[typing.Dict], 
        timeout: int = None, 
        return_exceptions: bool = False, 
        broadcast: typing.Optional[bool] = False,
        worker_names: typing.Optional[typing.List[str]] = None,
        worker_selector: typing.Optional[typing.Callable] = None,
        worker_selector_args: typing.Optional[typing.List] = None,
        worker_selector_kwargs: typing.Optional[typing.Dict] = None,
        workers_selected: typing.Optional[typing.List[typing.Dict[str, str]]] = None,
        **kwargs
    ):
        """
        Enqueue multiple jobs and collect all of their results.

        Example::
            try:
                assert await queue.map(
                    "add",
                    [
                        {"a": 1, "b": 2},
                        {"a": 3, "b": 4},
                    ]
                ) == [3, 7]
            except JobError:
                print("any of the jobs failed")

        job_or_func: Same as Queue.enqueue
        iter_kwargs: Enqueue a job for each item in this sequence. Each item is the same
            as kwargs for Queue.enqueue.
        timeout: Total seconds to wait for all jobs to complete. If None (default) or 0, wait forever.
        return_exceptions: If False (default), an exception is immediately raised as soon as any jobs
            fail. Other jobs won't be cancelled and will continue to run.
            If True, exceptions are treated the same as successful results and aggregated in the result list.
        broadcast: If True, broadcast the job to all nodes. Otherwise, only enqueue the job on this node.
        kwargs: Default kwargs for all jobs. These will be overridden by those in iter_kwargs.
        """
        iter_kwargs, worker_kws = await self.prepare_job_kws(
            iter_kwargs = iter_kwargs, 
            timeout = timeout, 
            broadcast = broadcast, 
            worker_names = worker_names, 
            worker_selector = worker_selector, 
            workers_selected = workers_selected,
            worker_selector_args = worker_selector_args,
            worker_selector_kwargs = worker_selector_kwargs,
            **kwargs
        )
        if worker_kws and (self.verbose_results or self.debug_enabled):
            logger.info(f"Broadcasting {job_or_func} to {len(worker_kws)} workers: {worker_kws}")
            
        job_keys = [key["key"] for key in iter_kwargs]
        pending_job_keys = set(job_keys)
        async def callback(job_key, status):
            if status in TERMINAL_STATUSES:
                pending_job_keys.discard(job_key)

            if status in UNSUCCESSFUL_TERMINAL_STATUSES and not return_exceptions:
                return True

            if not pending_job_keys:
                return True

        # Start listening before we enqueue the jobs.
        # This ensures we don't miss any updates.
        task = asyncio.create_task(
            self.listen(pending_job_keys, callback, timeout=None)
        )

        try:
            await asyncio.gather(
                *[self.enqueue(job_or_func, **kw) for kw in iter_kwargs]
            )
        except:
            task.cancel()
            raise

        await asyncio.wait_for(task, timeout=timeout)

        jobs: typing.List[Job] = await asyncio.gather(*[self.job(job_key) for job_key in job_keys])

        results = []
        for job, job_key in zip(jobs, job_keys):
            if job is None:
                if not return_exceptions: raise ValueError(f"Job {job_key} not found")
                results.append(None)
            elif job.status in UNSUCCESSFUL_TERMINAL_STATUSES:
                exc = JobError(job)
                if not return_exceptions: raise exc
                results.append(exc)
            else: results.append(job.result)
        return results


    """
    Batch Methods
    """

    
    async def batch_map(
        self, 
        batch_func_kwargs: typing.Union[typing.List, typing.Dict],
        timeout: int = None, 
        return_exceptions: bool = False, 
        **kwargs
    ):
        """
        Enqueue multiple individual as a result of multiple funcs collect all of their results.

        Example::
            try:
                assert await queue.batch_map(
                    [
                        "add", {"a": 1, "b": 2},
                        "subtract", {"a": 3, "b": 2},
                    ]
                ) == [3, 1]
            except JobError:
                print("any of the jobs failed")
            
            try:
                assert await queue.batch_map(
                    {
                        "result_1": ["add", {"a": 1, "b": 2}],
                        "result_2": ["subtract", {"a": 3, "b": 2}],
                    }
                ) == {"result_1": 3, "result_2": 1}
            except JobError:
                print("any of the jobs failed")

        job_or_func: Same as Queue.enqueue
        iter_kwargs: Enqueue a job for each item in this sequence. Each item is the same
            as kwargs for Queue.enqueue.
        timeout: Total seconds to wait for all jobs to complete. If None (default) or 0, wait forever.
        return_exceptions: If False (default), an exception is immediately raised as soon as any jobs
            fail. Other jobs won't be cancelled and will continue to run.
            If True, exceptions are treated the same as successful results and aggregated in the result list.
        kwargs: Default kwargs for all jobs. These will be overridden by those in iter_kwargs.
        """
        if isinstance(batch_func_kwargs, dict):
            return await self.batch_map_dict(
                batch_func_kwargs, timeout = timeout, return_exceptions = return_exceptions, **kwargs
            )
        return await self.batch_map_list(
            batch_func_kwargs, timeout = timeout, return_exceptions = return_exceptions, **kwargs
        )

    async def batch_map_dict(
        self, 
        batch_func_kwargs: typing.Dict[str, typing.Tuple[str, typing.Dict]], 
        timeout: int = None, 
        return_exceptions: bool = False, 
        **kwargs
    ):
        """
        Enqueue multiple individual as a result of multiple funcs collect all of their results.

        Example::
            try:
                assert await queue.batch_map(
                    {
                        "result_1": ("add", {"a": 1, "b": 2}),
                        "result_2": ("subtract", {"a": 3, "b": 2}),
                    }
                ) == {"result_1": 3, "result_2": 1}
            except JobError:
                print("any of the jobs failed")

        batch_func_kwargs: Same as Queue.enqueue
        iter_kwargs: Enqueue a job for each item in this sequence. Each item is the same
            as kwargs for Queue.enqueue.
        timeout: Total seconds to wait for all jobs to complete. If None (default) or 0, wait forever.
        return_exceptions: If False (default), an exception is immediately raised as soon as any jobs
            fail. Other jobs won't be cancelled and will continue to run.
            If True, exceptions are treated the same as successful results and aggregated in the result list.
        kwargs: Default kwargs for all jobs. These will be overridden by those in iter_kwargs.
        """
        batch_kwargs = {
            key: {
                "job_or_func": func,
                "timeout": timeout,
                "key": kw.get("key") or get_default_job_key(),
                **kwargs,
                **kw,
            }
            for key, (func, kw) in batch_func_kwargs.items()
        }
        job_dict = {kw["key"]: key  for key, kw in batch_kwargs.items()}
        pending_job_keys = set(job_dict.keys())

        async def callback(job_key, status):
            if status in TERMINAL_STATUSES: pending_job_keys.discard(job_key)
            if status in UNSUCCESSFUL_TERMINAL_STATUSES and not return_exceptions: return True
            if not pending_job_keys: return True

        # Start listening before we enqueue the jobs.
        # This ensures we don't miss any updates.
        task = asyncio.create_task(self.listen(pending_job_keys, callback, timeout=None))

        try:
            await asyncio.gather(
                *[self.enqueue(**kw) for kw in list(batch_kwargs.values())]
            )
        except:
            task.cancel()
            raise

        await asyncio.wait_for(task, timeout=timeout)
        jobs: typing.List[Job] = await asyncio.gather(*[self.job(job_key) for job_key in job_dict])
        results = {}
        
        for job, job_key in zip(jobs, job_dict):
            if job is None:
                if not return_exceptions: raise ValueError(f"Job {job_key} not found")
                results[job_dict[job_key]] = None
            elif job.status in UNSUCCESSFUL_TERMINAL_STATUSES:
                exc = JobError(job)
                if not return_exceptions: raise exc
                results[job_dict[job.key]] = exc
            else: results[job_dict[job.key]] = job.result
        return results

    async def batch_map_list(
        self, 
        batch_func_kwargs: typing.List[typing.Tuple[str, typing.Dict[str, typing.Any]]], 
        timeout: int = None, 
        return_exceptions: bool = False, 
        **kwargs
    ):
        """
        Enqueue multiple individual as a result of multiple funcs collect all of their results.

        Example::
            try:
                assert await queue.batch_map(
                    [
                        ("add", {"a": 1, "b": 2}),
                        ("subtract", {"a": 3, "b": 2}),
                    ]
                ) == [3, 1]
            except JobError:
                print("any of the jobs failed")

        batch_func_kwargs: Same as Queue.enqueue
        iter_kwargs: Enqueue a job for each item in this sequence. Each item is the same
            as kwargs for Queue.enqueue.
        timeout: Total seconds to wait for all jobs to complete. If None (default) or 0, wait forever.
        return_exceptions: If False (default), an exception is immediately raised as soon as any jobs
            fail. Other jobs won't be cancelled and will continue to run.
            If True, exceptions are treated the same as successful results and aggregated in the result list.
        kwargs: Default kwargs for all jobs. These will be overridden by those in iter_kwargs.
        """
        batch_kwargs = [
            {
                "job_or_func": func,
                "timeout": timeout,
                "key": kw.get("key") or get_default_job_key(),
                **kwargs,
                **kw,
            }
            for (func, kw) in batch_func_kwargs
        ]
        job_keys = [key["key"] for key in batch_kwargs]
        pending_job_keys = set(job_keys)

        async def callback(job_key, status):
            if status in TERMINAL_STATUSES: pending_job_keys.discard(job_key)
            if status in UNSUCCESSFUL_TERMINAL_STATUSES and not return_exceptions: return True
            if not pending_job_keys: return True

        # Start listening before we enqueue the jobs.
        # This ensures we don't miss any updates.
        task = asyncio.create_task(self.listen(pending_job_keys, callback, timeout=None))

        try:
            await asyncio.gather(
                *[self.enqueue(**kw) for kw in batch_kwargs]
            )
        except:
            task.cancel()
            raise

        await asyncio.wait_for(task, timeout=timeout)
        jobs: typing.List[Job] = await asyncio.gather(*[self.job(job_key) for job_key in job_keys])
        results = []
        for job, job_key in zip(jobs, job_keys):
            if job is None:
                if not return_exceptions: raise ValueError(f"Job {job_key} not found")
                results.append(None)
            elif job.status in UNSUCCESSFUL_TERMINAL_STATUSES:
                exc = JobError(job)
                if not return_exceptions: raise exc
                results.append(exc)
            else: results.append(job.result)
        return results

    @asynccontextmanager
    async def batch(self):
        """
        Context manager to batch enqueue jobs.

        This tracks all jobs enqueued within the context manager scope and ensures that
        all are aborted if any exception is raised.

        Example::
            async with queue.batch():
                await queue.enqueue("test")  # This will get cancelled
                raise asyncio.CancelledError
        """
        children = set()

        async def track_child(job):
            children.add(job)

        self.register_before_enqueue(track_child)
        try: yield
        except:
            await asyncio.gather(
                *[self.abort(child, "cancelled") for child in children],
                return_exceptions=True,
            )
            raise
        finally: self.unregister_before_enqueue(track_child)

    """
    New Helper Methods
    """

    async def wait_for_jobs(
        self,
        jobs: typing.List[Job],
        source_job: typing.Optional[Job] = None,
        verbose: typing.Optional[bool] = False,
        raise_exceptions: typing.Optional[bool] = False,
        refresh_interval: typing.Optional[float] = 0.5,
        **kwargs,
    ) -> typing.List[typing.Any]:
        """
        Waits for jobs to finish
        """
        results, num_jobs = [], len(jobs)
        t = timer()
        while jobs:
            for job in jobs:
                try: await job.refresh()
                except RuntimeError: await job.enqueue()
                if job.status == JobStatus.COMPLETE:
                    results.append(job.result)
                    if source_job: await source_job.incr_progress()
                    jobs.remove(job)
                elif job.status == JobStatus.FAILED:
                    if verbose: logger.error(f'Job {job.id} failed: {job.error}')
                    if raise_exceptions: raise job.error
                    jobs.remove(job)
                if not jobs: break
            await asyncio.sleep(refresh_interval)
        
        if verbose: 
            if source_job: logger.info(f'Completed {source_job.id} w/ {len(results)}/{num_jobs} in {timer(t):.2f}s')
            else: logger.info(f'Completed {len(results)}/{num_jobs} jobs in {timer(t):.2f}s')
        return results        

    
    async def as_jobs_complete(
        self,
        jobs: typing.List[Job],
        source_job: typing.Optional[Job] = None,
        verbose: typing.Optional[bool] = False,
        raise_exceptions: typing.Optional[bool] = False,
        refresh_interval: typing.Optional[float] = 0.5,
        return_results: typing.Optional[bool] = True,
        **kwargs,
    ) -> typing.AsyncGenerator[typing.Any, None]:
        # sourcery skip: low-code-quality
        """
        Generator that yields results as they complete
        """
        t = timer()
        num_results, num_jobs = 0, len(jobs)
        while jobs:
            for job in jobs:
                try: await job.refresh()
                except RuntimeError: await job.enqueue()
                if job.status == JobStatus.COMPLETE:
                    yield job.result if return_results else job
                    num_results += 1
                    if source_job: await source_job.incr_progress()
                    jobs.remove(job)
                
                elif job.status == JobStatus.FAILED:
                    if verbose: logger.error(f'Job {job.id} failed: {job.error}')
                    if raise_exceptions: raise job.error
                    jobs.remove(job)
                
                if not jobs: break
            await asyncio.sleep(refresh_interval)
        
        if verbose: 
            if source_job: logger.info(f'Completed {source_job.id} w/ {num_results}/{num_jobs} in {timer(t):.2f}s')
            else: logger.info(f'Completed {num_results}/{num_jobs} jobs in {timer(t):.2f}s')

    @property
    def batch_iterator_concurrency_limit(self) -> int:
        """
        Returns the batch iterator concurrency limit
        """
        return self.max_concurrency - 5

    async def batch_enqueue_iterator(
        self,
        job_or_func: typing.Union[Job, str],
        iterable: typing.Iterable[typing.Any],
        iterable_key: typing.Optional[str] = None,
        concurrency_limit: typing.Optional[int] = None,
        enqueue_func: typing.Optional[typing.Callable] = None,
        _verbose: typing.Optional[bool] = False,
        _raise_exceptions: typing.Optional[bool] = False,
        _return_results: typing.Optional[bool] = True,
        _key_func: typing.Optional[typing.Callable] = None,
        **kwargs,
    ) -> typing.AsyncGenerator[typing.Any, None]:
        """
        Helper method to batch enqueue jobs with a concurrency limit.
        and yields the results as they complete.
        """
        enqueue_func = enqueue_func or self.enqueue
        concurrency_limit = concurrency_limit or self.max_concurrency - 5
        n_items, t = 0, timer()
        for batch in build_batches(iterable, concurrency_limit, fixed_batch_size = True):
            batch_jobs = []
            bt = timer()
            for item in batch:
                if _key_func: kwargs['key'] = _key_func(item)
                if iterable_key: kwargs[iterable_key] = item
                batch_jobs.append(
                    await enqueue_func(
                        job_or_func,
                        *(() if iterable_key else (item)),
                        **kwargs,
                    )
                )
            async for completed_job in self.as_jobs_complete(
                jobs = batch_jobs,
                verbose = _verbose,
                raise_exceptions = _raise_exceptions,
                return_results = _return_results,
            ):
                yield completed_job
                n_items += 1
            
            if _verbose: logger.info(f'[{job_or_func}] Completed {len(batch_jobs)} Batch Results in {timer(bt):.2f}s ({len(batch_jobs) / timer(bt):.2f} items/s)')
        if _verbose: logger.info(f'[{job_or_func}] Completed {n_items} Results in {timer(t):.2f}s ({n_items / timer(t):.2f} items/s)')


    @property
    async def _all_queue_job_keys(self) -> typing.List[str]:
        """
        Returns all job ids in the queue
        """
        keys = await self.ctx.async_keys(f'queue:{self.queue_name}:job:*')
        return [key.decode('utf-8') for key in keys]

    @property
    async def _all_queue_job_ids(self) -> typing.List[str]:
        """
        Returns all job ids in the queue
        """
        keys = await self._all_queue_job_keys
        return [key.split(':')[-1] for key in keys]
    
    @property
    async def _all_queue_function_tracker_keys(self) -> typing.List[str]:
        """
        Returns all function tracker keys in the queue
        """
        keys = await self.ctx.async_keys(f'queue:{self.queue_name}:functiontracker.*')
        return [key.decode('utf-8') for key in keys]

    async def _reset_queue_jobs(
        self, 
        error: typing.Optional[typing.Union[str, Exception]] = 'Resetting Queue',
        ttl: typing.Optional[int] = 5,
        status: typing.Optional[JobStatus] = JobStatus.ABORTED,
        verbose: typing.Optional[bool] = True,
        **kwargs,
    ):
        """
        Helper method to reset all jobs in the queue
        """
        job_keys = await self._all_queue_job_keys
        for job_key in job_keys:
            job = await self.job(job_key)
            try:
                await self.abort(job, error = error, ttl = ttl)
                await self.ctx.async_client.publish(job.id, status)
            except Exception as e:
                logger.error(f'Error resetting job {job_key}: {e}')
        if verbose: logger.info(f'Reset {len(job_keys)} jobs in {self.queue_name}')

    async def _reset_queue_trackers(
        self,
        verbose: typing.Optional[bool] = True,
        **kwargs,
    ):
        """
        Helper method to reset all function trackers in the queue
        """
        tracker_keys = await self._all_queue_function_tracker_keys
        for tracker_key in tracker_keys:
            await self.ctx.async_delete(tracker_key)
        if verbose: logger.info(f'Reset {len(tracker_keys)} function trackers in {self.queue_name}')

    async def reset_queue(
        self,
        jobs: typing.Optional[bool] = False,
        # scheduled: typing.Optional[bool] = False,
        tracker: typing.Optional[bool] = False,
        verbose: typing.Optional[bool] = True,
        **kwargs,
    ):
        """
        Purges all keys from the queue
        """
        if jobs: await self._reset_queue_jobs(verbose = verbose, **kwargs)
        if tracker: await self._reset_queue_trackers(verbose = verbose, **kwargs)


    """
    Queue Metadata
    """
    
    async def fetch_job_info(
        self,
        offset: int = 0, 
        limit: int = 10,
        **kwargs
    ) -> typing.List[typing.Union[Job, typing.Dict]]:
        """
        Fetch all job info
        """
        job_info = []
        for job_bytes in await self.ctx.async_mget(
            (
                await self.ctx.async_lrange(self.active_key, offset, limit - 1)
            )
            + (
                await self.ctx.async_lrange(self.queued_key, offset, limit - 1)
            )
        ):
            with suppress(Exception):
                job_info.append(self.deserialize(job_bytes).to_dict())
        return job_info

    async def get_worker_ids(self) -> typing.List[str]:
        """
        Get all worker ids
        """
        worker_ids = []
        for key in await self.ctx.async_zrangebyscore(self.heartbeat_key, now(), "inf"):
            key: bytes = key.decode("utf-8")
            *_, worker_uuid = key.split(":")
            worker_ids.append(worker_uuid)
        return worker_ids

    async def info(self, jobs: bool = False, offset: int = 0, limit: int = 10):
        # pylint: disable=too-many-locals
        worker_uuids = []

        for key in await self.ctx.async_zrangebyscore(self.stats_key, now(), "inf"):
            key: bytes = key.decode("utf-8")
            *_, worker_uuid = key.split(":")
            worker_uuids.append(worker_uuid)

        worker_stats = await self.ctx.async_mget(
            self.create_namespace(f"stats:{worker_uuid}") for worker_uuid in worker_uuids
        )
        worker_info = {}
        for worker_uuid, stats in zip(worker_uuids, worker_stats):
            if stats:
                stats = json.loads(stats.decode("UTF-8"))
                worker_info[worker_uuid] = stats
        

        metrics = await self.prepare_queue_metrics(worker_info)
        job_info = await self.fetch_job_info(offset = offset, limit = limit) if jobs else []
        metrics['jobs'] = job_info
        return metrics

    async def sync_queue_info(self):
        """
        Syncs the current queue info with keydb
        """
        queue_info = await self.info(jobs = True)
        await self.ctx.async_set(self.queue_info_key, json.dumps(queue_info, cls = ObjectEncoder), ex = 60)
    
    async def fetch_queue_info(self):
        """
        Fetches the current queue info from keydb
        """
        queue_info = await self.ctx.async_get(self.queue_info_key)
        return json.loads(queue_info) if queue_info else {}

    async def stats(self, ttl: int = 60):
        """
        Logs the current queue stats to keydb
        """
        try:
            async with anyio.fail_after(10):
                stats = await self.prepare_worker_metrics()
                current = now()
                # async with self.pipeline(transaction = True) as pipe:
                async with self.ctx.async_client.pipeline(transaction = True, retryable = True) as pipe:
                    key = self.create_namespace(f"stats:{self.uuid}")
                    await (
                        pipe.setex(key, ttl, json.dumps(stats))
                        .zremrangebyscore(self.stats_key, 0, current)
                        .zadd(self.stats_key, {key: current + millis(ttl)})
                        .expire(self.stats_key, ttl)
                        .execute()
                    )
                return stats
        except TimeoutError as e:
            logger.error(f"TimeoutError while logging stats: {e}")
        except Exception as e:
            logger.error(f"Error while logging stats: {e}")

    
    async def add_heartbeat(
        self, 
        worker_id: str, 
        worker_attributes: typing.Dict[str, typing.Any] = None,
        heartbeat_ttl: typing.Optional[int] = None,
    ):
        """
        Registers a heartbeat for the current worker.
        """
        current = now()
        worker_attributes = worker_attributes or {}
        heartbeat_ttl = heartbeat_ttl or self.heartbeat_ttl
        async with self.ctx.async_client.pipeline(transaction = True, retryable = True) as pipe:
        # async with self.pipeline(transaction = True) as pipe:
            key = self.create_namespace(f"worker:attr:{worker_id}")
            await (
                pipe.setex(key, heartbeat_ttl, json.dumps(worker_attributes))
                .zremrangebyscore(self.heartbeat_key, 0, current)
                .zadd(self.heartbeat_key, {worker_id: current + millis(heartbeat_ttl)})
                .expire(self.heartbeat_key, heartbeat_ttl)
                .execute()
            )
        self.active_workers = await self.get_worker_ids()
        self.num_workers = len(self.active_workers)


    async def count(self, kind: str, postfix: typing.Optional[str] = None):
        """
        Returns the number of jobs in the given queue.
        """
        if kind == "queued":
            return await self.ctx.async_llen(f"{self.queued_key}:{postfix}" if postfix else self.queued_key)
        if kind == "active":
            return await self.ctx.async_llen(f"{self.active_key}:{postfix}" if postfix else self.active_key)
        if kind == "scheduled":
            return await self.ctx.async_zcount(f"{self.scheduled_key}:{postfix}" if postfix else self.scheduled_key, 1, "inf")
        if kind == "incomplete":
            return await self.ctx.async_zcard(f"{self.incomplete_key}:{postfix}" if postfix else self.incomplete_key)
        raise ValueError(f"Can't count unknown type {kind} {postfix}")

    @property
    def _queue_kind_key_map(self) -> typing.Dict[str, str]:
        """
        Returns the mapping of queue kind to key
        """
        return {
            "queued": self.queued_key,
            "active": self.active_key,
            "scheduled": self.scheduled_key,
            "incomplete": self.incomplete_key,
        }

    @property
    async def _all_queue_jobs(self) -> typing.Dict[str, JobStatus]:
        """
        Returns all jobs in all queues
        """
        job_ids = await self.ctx.async_hgetall(self._stats.queue_job_ids_key)
        if not job_ids: return {}
        return {
            job_id.decode("utf-8"): JobStatus(job_status.decode("utf-8")) for job_id, job_status in job_ids.items()
        } 

    async def get_job_ids_in_queue(self, kind: typing.Optional[str] = None, key: typing.Optional[str] = None, postfix: typing.Optional[str] = None, encode: typing.Optional[bool] = True) -> typing.List[str]:
        """
        Returns all jobs in the given queue.
        """
        if not kind:
            job_ids = await self.ctx.async_hgetall(self._stats.queue_job_ids_key)
            # logger.info(f"Job ids: {job_ids}")
            return [job_id.decode("utf-8") for job_id in job_ids.keys()] if encode else job_ids.keys()

        if not key:
            base_key = self._queue_kind_key_map[kind]
            key = f"{base_key}:{postfix}" if postfix else base_key
        results: typing.List[bytes] = await self.ctx.async_lrange(key, 0, -1)
        return [result.decode("utf-8") for result in results] if encode else results
    
    async def sweep_jobs_by_status(self, status: JobStatus, kind: typing.Optional[str] = "active", key: typing.Optional[str] = None, postfix: typing.Optional[str] = None, verbose: typing.Optional[bool] = None, **kwargs):
        """
        Sweeps all jobs with the given status.
        """
        verbose = verbose or self._should_debug_log
        job_ids = await self.get_job_ids_in_queue(kind, key = key, postfix = postfix)
        if not job_ids: 
            if verbose: logger.info(f"No jobs found in {kind} queue")
            return
        if verbose: logger.info(f"Sweeping {len(job_ids)} jobs from {kind} queue with status {status}")

        async def _sweep_job(job_id: str):
            """
            Inner sweep job function
            """
            job = await self._get_job_by_id(job_id)
            if job.status == status:
                if verbose: logger.info(f"Sweeping job {job_id} with status {status}")
                await self.sweep_job(job = job, job_id = job_id, verbose = verbose)
            else:
                if verbose: logger.info(f"Skipping job {job_id} with status {job.status}")

        async for _ in concurrent_map(
            _sweep_job,
            job_ids,
        ):
            pass
        return job_ids


    """
    Metrics Handler
    """
    async def prepare_for_broadcast(self, ttl: typing.Optional[int] = None):
        """
        Fetches the current workers
        """
        worker_data = await self.ctx.async_mget(
            self.create_namespace(f"worker:attr:{worker_id}") for worker_id in self.active_workers
        )
        self.worker_attributes = {
            worker_id: json.loads(worker_attr.decode("UTF-8"))
            for worker_id, worker_attr in zip(self.active_workers, worker_data)
            if worker_attr
        }
        

    async def prepare_queue_metrics(
        self, 
        worker_info: typing.Optional[typing.Dict[str, typing.Any]] = None,
        **kwargs
    ) -> typing.Dict[str, typing.Any]:
        """
        Parse worker metrics from worker info.
        """
        return worker_info
    
    async def prepare_worker_metrics(
        self, 
        **kwargs
    ) -> typing.Dict:
        """
        Parse worker metrics from worker info.
        """
        pass

    """
    Function Tracker
    """

    async def setup_function_tracker(self, functions: typing.List[str]):
        """
        Sets up the function tracker for the given functions.
        """
        funcs = {}
        for func in functions:
            _existing = await self.ctx.async_get(f'{self._stats.function_tracker_key}.{func}', default = None)
            funcs[func] = FunctionTracker.deserialize(_existing) if _existing else FunctionTracker(function = func)
        self._function_tracker = funcs
        logger.info(f"Function Tracker: {self._function_tracker}")

    @asynccontextmanager
    async def _fail_ok(self, duration: typing.Optional[float] = 7.0):
        """
        Allows a block of code to fail without raising an exception.
        """
        try:
            async with anyio.fail_after(duration):
                yield
        except Exception as e:
            logger.trace(f"Failed OK: {e}", error = e, level = "WARNING")

    async def _get_function_tracker(self, function: str, none_ok: typing.Optional[bool] = True) -> typing.Optional[FunctionTracker]:
        """
        Returns the function tracker for the given function.
        """
        if not self.function_tracker_enabled: return None
        function_tracker = await self.ctx.async_get(f'{self._stats.function_tracker_key}.{function}', default = None)
        if function_tracker:
            function_tracker = FunctionTracker.deserialize(function_tracker)
        elif none_ok: return None
        else:
            function_tracker = FunctionTracker(function = function)
        return function_tracker
    
    async def track_job_id(self, job: Job):
        """
        Inserts the job id into the queue
        """
        async with self._fail_ok():
            if job.status in TERMINAL_STATUSES:
                await self.ctx.async_hdel(self._stats.queue_job_ids_key, job.id)
                # logger.info(f"Dropping job id {job.id}")
            else:
                await self.ctx.async_hset(self._stats.queue_job_ids_key, job.id, job.status.value)
                # logger.info(f"Inserting job id {job.id} with status {job.status.value}")
                # logger.info(f"Job ids: {await self.ctx.async_hgetall(self._stats.queue_job_ids_key)}")

    async def track_job(self, job: Job):
        """
        Tracks the job results
        """
        if not self.function_tracker_enabled: return
        async with self._fail_ok():
            function_tracker = await self._get_function_tracker(job.function, none_ok = False)
            function_tracker.track_job(job)
            await self.ctx.async_set(f'{self._stats.function_tracker_key}.{job.function}', function_tracker.serialize(), ex = self.function_tracker_ttl)
            await self.track_job_id(job)

    async def get_function_trackers(self) -> typing.Dict[str, FunctionTracker]:
        """
        Returns the function trackers
        """
        if not self.function_tracker_enabled: return {}
        _keys = await self.ctx.async_keys(f'{self._stats.function_tracker_key}.*')
        _function_trackers = {}
        for key in _keys:
            function_tracker = await self.ctx.async_get(key, default = None)
            if function_tracker:
                function_tracker = FunctionTracker.deserialize(function_tracker)
                _function_trackers[function_tracker.function] = function_tracker
        return _function_trackers
    
    async def reset_function_trackers(self, functions: typing.Optional[typing.List[str]] = None):
        """
        Resets the function trackers
        """
        if not self.function_tracker_enabled: return
        async with self._fail_ok():
            if functions is None: 
                functions = await self.ctx.async_keys(f'{self._stats.function_tracker_key}.*')
            else:
                functions = [f'{self._stats.function_tracker_key}.{function}' for function in functions]
            for function in functions:
                await self.ctx.async_delete(f'{self._stats.function_tracker_key}.{function}')

    """
    Management Methods
    """

    """
    Alias Properties
    """
    # Properties
    @lazyproperty
    def node_name(self) -> str: return self._stats.node_name

    
    # Counts
    @lazyproperty
    def complete(self) -> int: return self._stats.complete
    @lazyproperty
    def failed(self) -> int: return self._stats.failed
    @lazyproperty
    def retried(self) -> int: return self._stats.retried
    @lazyproperty
    def aborted(self) -> int: return self._stats.aborted
    @lazyproperty
    def num_workers(self) -> int: return self._stats.num_workers
    
    # Key Namespaces
    @lazyproperty
    def active_key(self) -> str: return self._stats.active_key
    @lazyproperty
    def queued_key(self) -> str: return self._stats.queued_key
    @lazyproperty
    def scheduled_key(self) -> str: return self._stats.scheduled_key
    @lazyproperty
    def incomplete_key(self) -> str: return self._stats.incomplete_key
    @lazyproperty
    def function_tracker_key(self) -> str: return self._stats.function_tracker_key
    @lazyproperty
    def stats_key(self) -> str: return self._stats.stats_key
    @lazyproperty
    def sweep_key(self) -> str: return self._stats.sweep_key
    @lazyproperty
    def queue_info_key(self) -> str: return self._stats.queue_info_key
    @lazyproperty
    def worker_token_key(self) -> str: return self._stats.worker_token_key
    @lazyproperty
    def management_queue_key(self) -> str: return self._stats.management_queue_key
    @lazyproperty
    def heartbeat_key(self) -> str: return self._stats.heartbeat_key
    @lazyproperty
    def abort_id_prefix(self) -> str: return self._stats.abort_id_prefix

    @lazyproperty
    def active_workers(self) -> typing.List[str]: return self._stats.active_workers
    @lazyproperty
    def worker_attributes(self) -> typing.Dict[str, typing.Dict[str, typing.Any]]: return self._stats.worker_attributes

    def create_namespace(self, key: str) -> str: return self._stats.create_namespace(key)

# Job.update_forward_refs(queue = TaskQueue)
# Job.update_forward_refs()
# Job.__try_update_forward_refs__()