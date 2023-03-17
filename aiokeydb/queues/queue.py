import json
import time
import asyncio
import typing
import threading
from contextlib import asynccontextmanager, suppress
from lazyops.utils.logs import default_logger as logger

from aiokeydb.client.types import KeyDBUri, lazyproperty
from lazyops.utils.serialization import ObjectEncoder
from aiokeydb.client.serializers import SerializerType
from aiokeydb.client.meta import KeyDBClient
from aiokeydb.client.schemas.session import KeyDBSession
from aiokeydb.connection import ConnectionPool, BlockingConnectionPool
from aiokeydb.asyncio.connection import AsyncConnectionPool, AsyncBlockingConnectionPool
from aiokeydb.queues.errors import JobError
from aiokeydb.queues.types import (
    Job,
    JobStatus,
    QueueStats,
    TERMINAL_STATUSES,
    UNSUCCESSFUL_TERMINAL_STATUSES,
)
from aiokeydb.queues.utils import (
    millis,
    now,
    seconds,
    uuid1,
    uuid4,
    get_default_job_key,
    get_settings,
    ensure_coroutine_function,
)

from lazyops.imports._aiohttpx import (
    aiohttpx,
    require_aiohttpx
)

from aiokeydb.utils import set_ulimits, get_ulimits


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
    
    management_url: URL
    management_enabled: whether to enable management
    uuid: uuid of the queue
    """
    def __init__(  # pylint: disable=too-many-arguments
        self,
        queue_name: typing.Optional[str] = None,
        prefix: typing.Optional[str] = None,
        db: typing.Optional[int] = None,
        serializer: typing.Optional[typing.Union[object, SerializerType]] = None,
        max_concurrency: typing.Optional[int] = None,
        client_mode: typing.Optional[bool] = False,
        debug_enabled: typing.Optional[bool] = None,
        management_url: typing.Optional[str] = None,
        management_enabled: typing.Optional[bool] = True,
        management_register_api_path: typing.Optional[str] = None,
        uuid: typing.Optional[str] = None,
        metrics_func: typing.Optional[typing.Callable] = None,
        verbose_results: typing.Optional[bool] = False,
        truncate_logs: typing.Optional[bool] = True,
        logging_max_length: typing.Optional[int] = 500,
        silenced_functions: typing.Optional[typing.List[str]] = None, # Don't log these functions
        heartbeat_ttl: typing.Optional[int] = None, # 10,
        **kwargs
    ):
    
        self.settings = get_settings()
        self.prefix = prefix if prefix is not None else self.settings.worker.prefix
        self.queue_name = queue_name if queue_name is not None else self.settings.worker.queue_name
        self.serializer = serializer if serializer is not None else self.settings.worker.job_serializer.get_serializer()
        
        self._ctx_kwargs = {
            'db_id': db if db is not None else self.settings.worker.db,
            **kwargs        
        }


        self.client_mode = client_mode

        self.max_concurrency = max_concurrency or self.settings.worker.max_concurrency
        self.debug_enabled = debug_enabled if debug_enabled is not None else self.settings.worker.debug_enabled
        self.management_url = management_url if management_url is not None else self.settings.worker.management_url
        self.management_enabled = management_enabled if management_enabled is not None else self.settings.worker.management_enabled
        self.management_register_api_path = management_register_api_path if management_register_api_path is not None else self.settings.worker.management_register_api_path
        
        self._stats = QueueStats(
            name = self.queue_name,
            prefix = self.prefix,
            uuid = uuid,
        )
        self.uuid = self._stats.uuid
        self._version = None
        self._schedule_script = None
        self._enqueue_script = None
        self._cleanup_script = None

        self._before_enqueues = {}
        self._op_sem = asyncio.Semaphore(self.max_concurrency)
        self._metrics_func = metrics_func

        self.verbose_results = verbose_results
        self.truncate_logs = truncate_logs
        self.logging_max_length = logging_max_length
        self.silenced_functions = silenced_functions or []
        self.heartbeat_ttl = heartbeat_ttl or self.settings.worker.heartbeat_interval
        self._worker_name: str = None
        if not self.silenced_functions:
            self._set_silenced_functions()
    
    @lazyproperty
    def _lock(self) -> asyncio.Lock:
        return asyncio.Lock()
    
    @lazyproperty
    def ctx(self) -> KeyDBSession:
        if 'socket_connect_timeout' not in self._ctx_kwargs:
            self._ctx_kwargs['socket_connect_timeout'] = self.settings.worker.socket_connect_timeout
        if 'socket_keepalive' not in self._ctx_kwargs:
            self._ctx_kwargs['socket_keepalive'] = self.settings.worker.socket_keepalive
        if 'health_check_interval' not in self._ctx_kwargs:
            self._ctx_kwargs['health_check_interval'] = self.heartbeat_ttl
        if 'retry_on_timeout' not in self._ctx_kwargs:
            self._ctx_kwargs['retry_on_timeout'] = self.settings.worker.retry_on_timeout

        return KeyDBClient.create_session(
            name = self.queue_name,
            serializer = False,
            cache_enabled = False,
            decode_responses = False,
            auto_pubsub = False,
            set_current = False,
            # retry_on_timeout = True,
            # health_check_interval = self.heartbeat_ttl,
            # socket_connect_timeout = self.settings. # 60,
            # socket_keepalive = True,

            max_connections = self.max_concurrency * 10,
            pool_class = BlockingConnectionPool,

            amax_connections = self.max_concurrency ** 2,
            apool_class = AsyncBlockingConnectionPool,
            **self._ctx_kwargs
        )

    
    @lazyproperty
    def version(self):
        # if not self._version:
        #     self.
        info = self.ctx.info()
        return tuple(int(i) for i in info["redis_version"].split("."))
    
    @lazyproperty
    def uri(self) -> KeyDBUri:
        return self.ctx.uri

    @lazyproperty
    def db_id(self):
        return self.ctx.db_id

    def _set_silenced_functions(self):
        from aiokeydb.queues.worker import WorkerTasks
        self.silenced_functions = list(set(WorkerTasks.silenced_functions))


    def add_silenced_functions(self, *functions):
        self.silenced_functions.extend(functions)
        self.silenced_functions = list(set(self.silenced_functions))

    def logger(self, job: 'Job' = None, kind: str = "enqueue"):
        if job:
            return logger.bind(
                job_id = job.id,
                status = job.status,
                worker_name = self._worker_name,
                queue_name = getattr(job.queue, 'queue_name', None) or 'unknown queue',
                kind = kind,
            )
        else:
            return logger.bind(
                worker_name = self._worker_name,
                queue_name = self.queue_name,
                kind = kind,
            )
    

    def job_id(self, job_key):
        return f"queue:{self.queue_name}:{self.settings.worker.job_prefix}:{job_key}"
    
    def register_before_enqueue(self, callback):
        self._before_enqueues[id(callback)] = callback

    def unregister_before_enqueue(self, callback):
        self._before_enqueues.pop(id(callback), None)

    async def _before_enqueue(self, job: Job):
        for cb in self._before_enqueues.values():
            await cb(job)

    def serialize(self, job: Job):
        return self.serializer.dumps(job.to_dict())

    def deserialize(self, job_bytes: bytes):
        if not job_bytes: return None
        job_dict: typing.Dict = self.serializer.loads(job_bytes)
        assert (
                job_dict.pop("queue") == self.queue_name
        ), f"Job {job_dict} fetched by wrong queue: {self.queue_name}"
        return Job(**job_dict, queue=self)

    async def disconnect(self):
        await self.ctx.aclose()

    
    async def prepare_server(self):
        """
        Prepares the keydb server to ensure that the maximum number of concurrent
        connections are available.
        """
        info = await self.ctx.async_info()
        await self.prepare_for_broadcast()

        # Each Worker spawns a connection
        min_connections = ((self.num_workers or 1) * self.max_concurrency) * 10

        curr_max_connections = info['maxclients']
        curr_connected = info['connected_clients']

        new_max_connections = max(
            min_connections,
            (curr_connected + min_connections),
            curr_max_connections
        )
        curr_ulimits = get_ulimits()
        if curr_ulimits < new_max_connections:
            logger.debug(f'The maximum number of concurrent connections may not be supported as ulimits: {curr_ulimits} < desired max connections: {new_max_connections}')

        logger.info(f'Configuring Server: Current Max Connections: {curr_max_connections}, Current Connected: {curr_connected}, Min Connections: {min_connections} -> New Max: {new_max_connections}')
        if new_max_connections > curr_max_connections:
            try:
                await self.ctx.config_set('maxclients', new_max_connections)
                info = await self.ctx.async_info()
                new_set_max_connections = info['maxclients']
                logger.debug(f'New Max Connections: {new_set_max_connections}')
            except Exception as e:
                logger.warning(f'Unable to configure the maxclients to {new_max_connections}: {e}')


    """
    Primary APIs
    """

    async def schedule(self, lock: int = 1):
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

        return await self._schedule_script(
            keys=[self.scheduled_key, self.incomplete_key, self.queued_key],
            args=[lock, seconds(now())],
        )

    async def sweep(self, lock: int = 60):
        if not self._cleanup_script:
            self._cleanup_script = self.ctx.async_client.register_script(
                """
                if redis.call('EXISTS', KEYS[1]) == 0 then
                    redis.call('SETEX', KEYS[1], ARGV[1], 1)
                    return redis.call('LRANGE', KEYS[2], 0, -1)
                end
                """
            )

        job_ids = await self._cleanup_script(
            keys = [self.sweep_key, self.active_key], 
            args = [lock], 
            client = self.ctx.async_client,
        )

        swept = []
        if job_ids:
            for job_id, job_bytes in zip(job_ids, await self.ctx.async_mget(job_ids)):
                job = self.deserialize(job_bytes)
                if not job: 
                    swept.append(job_id)
                    async with self.ctx.async_client.pipeline(transaction = True) as pipe:
                        await (
                            pipe.lrem(self.active_key, 0, job_id)
                            .zrem(self.incomplete_key, job_id)
                            .execute()
                        )
                    self.logger(kind = "sweep").info(f"Sweeping missing job {job_id}")
                elif job.status != JobStatus.ACTIVE or job.stuck:
                    swept.append(job_id)
                    await job.finish(JobStatus.ABORTED, error="swept")
                    if job.function in self.silenced_functions:
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
        return swept

    async def listen(self, job_keys: typing.List[str], callback: typing.Callable, timeout: int = 10):
        """
        Listen to updates on jobs.

        job_keys: sequence of job keys
        callback: callback function, if it returns truthy, break
        timeout: if timeout is truthy, wait for timeout seconds
        """
        pubsub = self.ctx.async_client.pubsub()
        
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

    async def update(self, job: Job):
        """
        Update a job.
        """
        job.touched = now()
        await self.ctx.async_set(job.id, self.serialize(job))
        await self.notify(job)

    async def job(self, job_key: str):
        """
        Fetch a Job by key.
        """
        job_id = self.job_id(job_key)
        return await self._get_job_by_id(job_id)

    async def _get_job_by_id(self, job_id):
        async with self._op_sem:
            return self.deserialize(await self.ctx.async_get(job_id))

    async def abort(self, job: Job, error: typing.Any, ttl: int = 5):
        """
        Abort a job.
        """
        async with self._op_sem:
            async with self.ctx.async_client.pipeline(transaction = True) as pipe:
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


        async with self.ctx.async_client.pipeline(transaction=True) as pipe:
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
            if not self.debug_enabled and job.function not in self.silenced_functions:
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

        async with self.ctx.async_client.pipeline(transaction=True) as pipe:
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
            
            elif job.function in self.silenced_functions:
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
            return await self.ctx.async_client.brpoplpush(
                queued_key, 
                active_key, 
                timeout
            )
        # logger.info(f'BLMOVE: {queued_key}')
        return await self.ctx.async_client.execute_command(
            "BLMOVE", queued_key, active_key, "RIGHT", "LEFT", timeout
        )

    async def dequeue(self, timeout = 0, worker_id: str = None, worker_name: str = None):
        job_id = await self._dequeue_job_id(timeout) if \
            worker_id is None and worker_name is None else None
        
        if job_id is None and worker_id:
            job_id = await self._dequeue_job_id(timeout, worker_id)
            
        if job_id is None and worker_name:
            job_id = await self._dequeue_job_id(timeout, worker_name)
        
        if job_id is not None:
            # logger.info(f'Fetched job id {job_id}: {queued_key}: {active_key}')
            return await self._get_job_by_id(job_id)

        self.logger(kind="dequeue").info("Dequeue timed out")
        return None
    
    
    async def _deferred(
        self, 
        job_or_func: typing.Union[Job, str],
        wait_time: float = 10.0, 
        **kwargs
    ):
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
        job_or_func: typing.Union[Job, str],
        **kwargs
    ):
        """
        Enqueue a job by instance or string.

        Kwargs can be arguments of the function or properties of the job.
        If a job instance is passed in, it's properties are overriden.

        If the job has already been enqueued, this returns None.
        """
        job_kwargs = {}
        job_fields = Job.get_fields()
        for k, v in kwargs.items():
            if k in job_fields:
                job_kwargs[k] = v
            else:
                if "kwargs" not in job_kwargs:
                    job_kwargs["kwargs"] = {}
                job_kwargs["kwargs"][k] = v

        if isinstance(job_or_func, str):
            job = Job(function=job_or_func, **job_kwargs)
        else:
            job = job_or_func

            for k, v in job_kwargs.items():
                setattr(job, k, v)

        if job.queue and job.queue.queue_name != self.queue_name:
            raise ValueError(f"Job {job} registered to a different queue")

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
        elif job.function in self.silenced_functions:
            pass
        elif self.verbose_results:
            self.logger(job=job, kind="enqueue").info(
                f"→ duration={now() - job.queued}ms, node={self.node_name}, func={job.function}, timeout={job.timeout}, kwargs={job.kwargs}"
            )
        elif self.truncate_logs:
            job_kwargs = f"{str({k: str(v)[:self.logging_max_length] for k, v in job.kwargs.items()})[:self.logging_max_length]}..."
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
            worker_kws = [{"worker_id": worker_id} for worker_id in self.active_workers]

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
        for job in jobs:
            if job.status in UNSUCCESSFUL_TERMINAL_STATUSES:
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
        for job in jobs:
            if job.status in UNSUCCESSFUL_TERMINAL_STATUSES:
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
        for job in jobs:
            if job.status in UNSUCCESSFUL_TERMINAL_STATUSES:
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
    Queue Metadata
    """
    


    # async def version(self):
    #     if not self._version:
    #         async with self._lock:
    #             info = await self.ctx.async_info()
    #             self._version = tuple(int(i) for i in info["redis_version"].split("."))
    #         logger.info(f"{self.queue_name} | Fetched Version: {self._version}")
    #     return self._version

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
        stats = await self.prepare_worker_metrics()
        current = now()
        async with self.ctx.async_client.pipeline(transaction = True) as pipe:
            key = self.create_namespace(f"stats:{self.uuid}")
            await (
                pipe.setex(key, ttl, json.dumps(stats))
                .zremrangebyscore(self.stats_key, 0, current)
                .zadd(self.stats_key, {key: current + millis(ttl)})
                .expire(self.stats_key, ttl)
                .execute()
            )
        return stats
    

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
        async with self.ctx.async_client.pipeline(transaction = True) as pipe:
            key = self.create_namespace(f"worker:attr:{worker_id}")
            await (
                pipe.setex(key, heartbeat_ttl, json.dumps(worker_attributes))
                .zremrangebyscore(self.heartbeat_key, 0, current)
                .zadd(self.heartbeat_key, {worker_id: current + millis(heartbeat_ttl)})
                .expire(self.heartbeat_key, heartbeat_ttl)
                .execute()
            )
        # self.num_workers = await self.ctx.async_client.zcount(self.heartbeat_key, 1, "-inf")
        # self.active_workers = [worker_id.decode("utf-8") for worker_id in (await self.ctx.async_client.zrange(self.heartbeat_key, 0, current + millis(heartbeat_ttl)))]
        # logger.info(f"Active workers: {self.active_workers}")


    async def count(self, kind: str, postfix: typing.Optional[str] = None):
        if kind == "queued":
            return await self.ctx.async_llen(f"{self.queued_key}:{postfix}" if postfix else self.queued_key)
        if kind == "active":
            return await self.ctx.async_llen(f"{self.active_key}:{postfix}" if postfix else self.active_key)
        if kind == "scheduled":
            return await self.ctx.async_zcount(f"{self.scheduled_key}:{postfix}" if postfix else self.scheduled_key, 1, "inf")
        if kind == "incomplete":
            return await self.ctx.async_zcard(f"{self.incomplete_key}:{postfix}" if postfix else self.incomplete_key)
        raise ValueError(f"Can't count unknown type {kind} {postfix}")


    """
    Metrics Handler
    """
    async def prepare_for_broadcast(self, ttl: typing.Optional[int] = None):
        """
        Fetches the current workers
        """
        current = now()
        ttl = ttl or self.heartbeat_ttl
        self.num_workers = await self.ctx.async_client.zcount(self.heartbeat_key, 1, "-inf")
        self.active_workers = [worker_id.decode("utf-8") for worker_id in (await self.ctx.async_client.zrange(self.heartbeat_key, 0, current + millis(ttl)))]
        worker_data = await self.ctx.async_mget(
            self.create_namespace(f"worker:attr:{worker_id}") for worker_id in self.active_workers
        )
        self.worker_attributes = {
            worker_id: json.loads(worker_attr.decode("UTF-8"))
            for worker_id, worker_attr in zip(self.active_workers, worker_data)
            if worker_attr
        }
        # logger.info(f"Active workers: {self.active_workers}: {self.worker_attributes}")
        

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
    Management Methods
    """
    async def fetch_worker_token(self) -> str:
        """
        Fetches the worker token from keydb
        """
        token = await self.ctx.async_get(self.worker_token_key, self.settings.worker.token)
        if token: token = self.serializer.loads(token)
        return token

    async def register_queue(self):
        """
        Updates the Queue Key to Register Worker
        """
        queues: typing.List[str] = await self.ctx.async_get(self.management_queue_key, [])
        if queues: queues = self.serializer.loads(queues)
        if self.queue_name not in queues: 
            queues.append(self.queue_name)
            await self.ctx.async_set(self.management_queue_key, self.serializer.dumps(queues))
            self.logger(kind = "startup").info(f'Registering queue "{self.queue_name}:{self.uuid}" with Management API')
        else:
            self.logger(kind = "startup").info(f'Queue "{self.queue_name}:{self.uuid}" already registered with Management API')
        return {'registered': True}

    @require_aiohttpx()
    async def register_worker(self):
        """
        Register this worker with the Management API.
        """
        if not self.management_enabled or not self.management_endpoint: return
        await self.ctx.async_wait_for_ready(max_attempts = 20)
        async with aiohttpx.Client() as client:
            attempts = 0
            token = await self.fetch_worker_token()
            self.logger(kind = "startup").info(
                f'Registering queue "{self.queue_name}:{self.uuid}" \
                with Management API @ "{self.management_url}" \
                with token: {token}')
            
            while attempts < 5:
                try:
                    await client.async_post(
                        url = self.management_endpoint,
                        timeout = 5.0, 
                        json = {
                            "queue_name": self.queue_name,
                            "uuid": self.uuid,
                            "token": token,
                        }
                    )
                    self.logger(kind = "startup").info(
                        f'Succesfully registered queue "{self.queue_name}:{self.uuid}"'
                    )
                    return {'registered': True}
                
                except (aiohttpx.HTTPError, asyncio.TimeoutError, aiohttpx.NetworkError, aiohttpx.ConnectError):
                    attempts += 1
                    await asyncio.sleep(2.5)
                    continue

                except Exception as e:
                    self.logger(kind = "startup").error(
                        f'Failed to register queue "{self.queue_name}:{self.uuid}" with Management API: {e}'
                    )
                    return {'registered': False}
        return {'registered': False}

    """
    Alias Properties
    """
    # Properties
    @lazyproperty
    def node_name(self) -> str: return self._stats.node_name

    @lazyproperty
    def management_endpoint(self) -> str: 
        """
        Returns the management endpoint
        """
        if self.management_url is None:
            return None
        from urllib.parse import urljoin
        return urljoin(self.management_url, self.management_register_api_path)

    
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

    
# Job.update_forward_refs()
# Job.__try_update_forward_refs__()