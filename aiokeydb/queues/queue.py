import json
import time
import asyncio
import typing

from contextlib import asynccontextmanager, suppress
from lazyops.utils.logs import default_logger as logger

from aiokeydb.client.types import KeyDBUri, lazyproperty
from lazyops.utils.serialization import ObjectEncoder
from aiokeydb.client.serializers import SerializerType
from aiokeydb.client.core import KeyDBClient
from aiokeydb.client.schemas.session import KeyDBSession

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
)

from lazyops.imports._aiohttpx import (
    aiohttpx,
    require_aiohttpx
)



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
        **kwargs
    ):
    
        self.settings = get_settings()
        self.prefix = prefix if prefix is not None else self.settings.worker.prefix
        self.queue_name = queue_name if queue_name is not None else self.settings.worker.queue_name
        self.serializer = serializer if serializer is not None else self.settings.worker.job_serializer.get_serializer()
        db = db if db is not None else self.settings.worker.db
        self.ctx: KeyDBSession = KeyDBClient.create_session(
            name = self.queue_name,
            db_id = db,
            serializer = None,
            cache_enabled = False,
            _decode_responses = False,
            set_current = False,
            **kwargs,
        )
        self.uri: KeyDBUri = self.ctx.uri
        self.db_id = self.uri.db_id

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

    def logger(self, job: 'Job' = None, kind: str = "enqueue"):
        if job:
            return logger.bind(
                job_id=job.id,
                status=job.status,
                queue_name=getattr(job.queue, 'queue_name', None) or 'unknown queue',
                kind=kind,
            )
        else:
            return logger.bind(
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
                    self.logger(job=job, kind = "sweep").info(f"☇ duration={job.duration('total')}ms, node={self._node_name}, func={job.function}, result={job.result}")
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

                    if stop:
                        break

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
                    pipe.lrem(self.queued_key, 0, job.id)
                    .zrem(self.incomplete_key, job.id)
                    .expire(job.id, ttl + 1)
                    .setex(job.abort_id, ttl, error)
                    .execute()
                )

            if dequeued:
                await job.finish(JobStatus.ABORTED, error = error)
                await self.ctx.async_delete(job.abort_id)
            else:
                await self.ctx.async_lrem(self.active_key, 0, job.id)


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
            pipe = pipe.lrem(self._stats.active_key, 1, job_id)
            pipe = pipe.lrem(self._stats.queued_key, 1, job_id)
            if next_retry_delay:
                scheduled = time.time() + next_retry_delay
                pipe = pipe.zadd(self.incomplete_key, {job_id: scheduled})
            else:
                pipe = pipe.zadd(self.incomplete_key, {job_id: job.scheduled})
                pipe = pipe.rpush(self.queued_key, job_id)
            await pipe.set(job_id, self.serialize(job)).execute()
            self.retried += 1
            await self.notify(job)
            if not self.debug_enabled:
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
            pipe = pipe.lrem(self.active_key, 1, job_id).zrem(self.incomplete_key, job_id)

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
            else:
                self.logger(job=job, kind="finish").info(f"● duration={job.duration('total')}ms, node={self.node_name}, func={job.function}, result={job.result}")

    async def dequeue(self, timeout=0):
        if await self.version() < (6, 2, 0):
            job_id = await self.ctx.async_client.brpoplpush(self.queued_key, self.active_key, timeout)
        else:
            job_id = await self.ctx.async_client.execute_command(
                "BLMOVE", self.queued_key, self.active_key, "RIGHT", "LEFT", timeout
            )
        if job_id is not None:
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
                    keys=[self.incomplete_key, job.id, self.queued_key, job.abort_id],
                    args=[self.serialize(job), job.scheduled],
                    client = self.ctx.async_client,
            ):
                return None
        if not self.client_mode and self.debug_enabled:
            self.logger(job=job, kind="enqueue").info(f"Enqueuing {job}")
        else:
            
            self.logger(job=job, kind="enqueue").info(
                f"→ duration={now() - job.queued}ms, \
                node={self.node_name}, \
                func={job.function}, \
                timeout={job.timeout}, \
                kwargs={job.kwargs}"
            )

        return job

    async def apply(
        self, 
        job_or_func: typing.Union[Job, str],
        timeout: int = None, 
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
        """
        results = await self.map(job_or_func, timeout=timeout, iter_kwargs=[kwargs])
        return results[0]

    async def map(
        self, 
        job_or_func: typing.Union[Job, str],
        iter_kwargs: typing.Iterable[typing.Dict], 
        timeout: int = None, 
        return_exceptions: bool = False, 
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
        kwargs: Default kwargs for all jobs. These will be overridden by those in iter_kwargs.
        """
        iter_kwargs = [
            {
                "timeout": timeout,
                "key": kwargs.get("key") or get_default_job_key(),
                **kwargs,
                **kw,
            }
            for kw in iter_kwargs
        ]
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

    async def version(self):
        if not self._version:
            info = await self.ctx.async_info()
            self._version = tuple(int(i) for i in info["redis_version"].split("."))
        return self._version

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

    async def count(self, kind: str):
        if kind == "queued":
            return await self.ctx.async_llen(self.queued_key)
        if kind == "active":
            return await self.ctx.async_llen(self.active_key)
        if kind == "scheduled":
            return await self.ctx.async_zcount(self.scheduled_key, 1, "inf")
        if kind == "incomplete":
            return await self.ctx.async_zcard(self.incomplete_key)
        raise ValueError(f"Can't count unknown type {kind}")


    """
    Metrics Handler
    """
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
    def abort_id_prefix(self) -> str: return self._stats.abort_id_prefix

    def create_namespace(self, key: str) -> str: return self._stats.create_namespace(key)

    
# Job.update_forward_refs()
# Job.__try_update_forward_refs__()