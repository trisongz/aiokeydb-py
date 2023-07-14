from __future__ import annotations

import enum

import typing
import datetime
import croniter

from aiokeydb.v2.types.base import BaseModel, lazyproperty, Field, validator
from aiokeydb.v2.utils.queue import (
    get_default_job_key, 
    now, 
    seconds, 
    exponential_backoff,
    import_function,
    get_func_name,
    get_func_full_name,
    ensure_coroutine_function,
    create_background_task
)
from aiokeydb.v2.configs import settings
from aiokeydb.v2.utils.logs import logger
from aiokeydb.v2.types.static import JobStatus, TaskType, TERMINAL_STATUSES, UNSUCCESSFUL_TERMINAL_STATUSES


if typing.TYPE_CHECKING:
    from aiokeydb.v2.types.task_queue import TaskQueue



class CronJob(BaseModel):
    """
    Allows scheduling of repeated jobs with cron syntax.

    function: the async function to run
    cron: cron string for a job to be repeated, uses croniter
    unique: unique jobs only one once per queue, defaults true

    Remaining kwargs are pass through to Job
    """

    function: typing.Callable
    cron: str
    unique: bool = True
    cron_name: typing.Optional[str] = None
    timeout: typing.Optional[int] = Field(default_factory = settings.get_default_job_timeout)
    retries: typing.Optional[int] = Field(default_factory = settings.get_default_job_retries)
    ttl: typing.Optional[int] = Field(default_factory = settings.get_default_job_ttl)
    heartbeat: typing.Optional[int] = None
    default_kwargs: typing.Optional[dict] = None

    # Allow for passing in a callback function to be called after the job is run
    callback: typing.Optional[typing.Union[str, typing.Callable]] = None
    callback_kwargs: typing.Optional[dict] = Field(default_factory=dict)

    @property
    def function_name(self) -> str:
        """
        Returns the name of the function
        """
        return self.cron_name or self.function.__qualname__
    
    @validator("callback")
    def validate_callback(cls, v: typing.Optional[typing.Union[str, typing.Callable]]) -> typing.Optional[str]:
        """
        Validates the callback and returns the function name
        """
        return v if v is None else get_func_full_name(v)
    
    def next_scheduled(self) -> int:
        """
        Returns the next scheduled time for the cron job
        """
        return int(croniter.croniter(self.cron, seconds(now())).get_next())

    def to_enqueue_kwargs(self, job_key: typing.Optional[str] = None, exclude_none: typing.Optional[bool] = True, **kwargs) -> typing.Dict[str, typing.Any]:
        """
        Returns the kwargs for the job
        """
        default_kwargs = self.default_kwargs or {}
        if kwargs: default_kwargs.update(kwargs)
        default_kwargs['key'] = job_key
        enqueue_kwargs = {
            "function": self.function_name,
            **default_kwargs,
        }
        if self.callback:
            enqueue_kwargs['job_callback'] = self.callback
            enqueue_kwargs['job_callback_kwargs'] = self.callback_kwargs

        if exclude_none:
            enqueue_kwargs = {
                k: v
                for k, v in enqueue_kwargs.items()
                if v is not None
            }
        enqueue_kwargs['scheduled'] = self.next_scheduled()
        return enqueue_kwargs
    

class JobProgress(BaseModel):

    """
    Holds the progress of a job
    """

    total: int = 0
    completed: int = 0

    @property
    def progress(self) -> float:
        """
        Returns the progress of a job as a float between 0.0 and 1.0
        """
        return 0.0 if self.total == 0 else self.completed / self.total


class FunctionTracker(BaseModel):
    """
    Tracks the metrics for a job function
    """
    function: str
    completed_durations: typing.List[float] = Field(default_factory=list)
    failed_durations: typing.List[float] = Field(default_factory=list)
    last_completed: typing.Optional[datetime.datetime] = None
    last_failed: typing.Optional[datetime.datetime] = None

    @property
    def durations(self) -> typing.List[float]:
        """
        Returns the durations of a job function
        """
        return self.completed_durations + self.failed_durations
    
    @property
    def failed(self) -> int:
        """
        Returns the number of failed jobs
        """
        return len(self.failed_durations)
    
    @property
    def completed(self) -> int:
        """
        Returns the number of completed jobs
        """
        return len(self.completed_durations)

    @property
    def total_duration_ms(self) -> float:
        """
        Returns the total duration of a job function
        in milliseconds
        """
        return sum(self.durations)
    
    @property
    def total_duration(self) -> float:
        """
        Returns the total duration of a job function
        in seconds
        """
        return self.total_duration_ms / 1000.0
    
    @property
    def completed_duration_ms(self) -> float:
        """
        Returns the total duration of completed jobs
        in milliseconds
        """
        return sum(self.completed_durations)
    
    @property
    def completed_duration(self) -> float:
        """
        Returns the total duration of completed jobs
        in seconds
        """
        return self.completed_duration_ms / 1000.0
    
    @property
    def failed_duration_ms(self) -> float:
        """
        Returns the total duration of failed jobs
        in milliseconds
        """
        return sum(self.failed_durations)
    
    @property
    def failed_duration(self) -> float:
        """
        Returns the total duration of failed jobs
        in seconds
        """
        return self.failed_duration_ms / 1000.0
    
    @property
    def total(self) -> int:
        """
        Returns the total number of jobs
        """
        return self.completed + self.failed

    @property
    def average_duration_ms(self) -> float:
        """
        Returns the average duration of a job function
        in milliseconds
        """
        return 0.0 if self.completed == 0 else self.completed_duration_ms / self.completed
    
    @property
    def average_duration(self) -> float:
        """
        Returns the average duration of a job function
        in seconds
        """
        return self.average_duration_ms / 1000.0
    
    @property
    def success_rate(self) -> float:
        """
        Returns the success rate of a job function
        """
        return 0.0 if self.total == 0 else self.completed / self.total
    
    @property
    def failure_rate(self) -> float:
        """
        Returns the failure rate of a job function
        """
        return 0.0 if self.total == 0 else self.failed / self.total
    
    def track_job(self, job: 'Job'):
        """
        Tracks the job in the function tracker
        """
        if job.status in UNSUCCESSFUL_TERMINAL_STATUSES:
            self.failed_durations.append(job.job_duration)
            self.last_failed = datetime.datetime.now(tz = datetime.timezone.utc)
        elif job.status in TERMINAL_STATUSES:
            self.completed_durations.append(job.job_duration)
            self.last_completed = datetime.datetime.now(tz = datetime.timezone.utc)

    def serialize(self) -> str:
        """
        Serializes the function tracker
        """
        return self.json()
    
    @classmethod
    def deserialize(cls, data: str) -> 'FunctionTracker':
        """
        Deserializes the function tracker
        """
        return cls.parse_raw(data)
    
    @property
    def data_dict(self) -> dict:
        """
        Returns the function tracker as a dictionary
        """
        return {
            "function": self.function,
            "total": self.total,
            "completed": self.completed,
            "failed": self.failed,
            "total_duration": self.total_duration,
            "average_duration": self.average_duration,
            "success_rate": self.success_rate * 100,
            "failure_rate": self.failure_rate * 100,
            "last_completed": self.last_completed.isoformat() if self.last_completed else None,
            "last_failed": self.last_failed.isoformat() if self.last_failed else None,
        }



class Job(BaseModel):
    """
    Main job class representing a run of a function.

    User Provided Arguments
        function: the async function name to run
        kwargs: kwargs to pass to the function
        queue: the saq.Queue object associated with the job
        key: unique identifier of a job, defaults to uuid1, can be passed in to avoid duplicate jobs
        timeout: the maximum amount of time a job can run for in seconds, defaults to 600 (0 means disabled)
        heartbeat: the maximum amount of time a job can survive without a heartebat in seconds, defaults to 0 (disabled)
            a heartbeat can be triggered manually within a job by calling await job.update()
        retries: the maximum number of attempts to retry a job, defaults to 1
        ttl: the maximum time in seconds to store information about a job including results, defaults to 600 (0 means indefinitely, -1 means disabled)
        retry_delay: seconds to delay before retrying the job
        retry_backoff: If true, use exponential backoff for retry delays.
            The first retry will have whatever retry_delay is.
            The second retry will have retry_delay*2. The third retry will have retry_delay*4. And so on.
            This always includes jitter, where the final retry delay is a random number between 0 and the calculated retry delay.
            If retry_backoff is set to a number, that number is the maximum retry delay, in seconds.
        scheduled: epoch seconds for when the job should be scheduled, defaults to 0 (schedule right away)
        progress: job progress 0.0..1.0
        meta: arbitrary metadata to attach to the job
    Framework Set Properties
        attempts: number of attempts a job has had
        completed: job completion time epoch seconds
        queued: job enqueued time epoch seconds
        started: job started time epoch seconds
        touched: job touched/updated time epoch seconds
        results: payload containing the results, this is the return of the function provided, must be serializable, defaults to json
        error: stack trace if an runtime error occurs
        status: Status Enum, defaulst to Status.New
    """

    function: str
    kwargs: typing.Optional[dict] = None
    queue: typing.Optional[typing.Any] = None
    
    # queue: typing.Optional[typing.Union['TaskQueue', typing.Any]] = None
    key: typing.Optional[str] = Field(default_factory = get_default_job_key)
    timeout: typing.Optional[int] = Field(default_factory = settings.get_default_job_timeout)
    retries: typing.Optional[int] = Field(default_factory = settings.get_default_job_retries)
    ttl: typing.Optional[int] = Field(default_factory = settings.get_default_job_ttl)
    retry_delay: typing.Optional[float] = Field(default_factory = settings.get_default_job_retry_delay)

    retry_backoff: typing.Union[bool, float] = True
    
    heartbeat: int = 0
    scheduled: int = 0
    progress: float = 0.0
    attempts: int = 0
    completed: int = 0
    queued: int = 0
    started: int = 0
    touched: int = 0
    result: typing.Any = None
    error: typing.Optional[str] = None
    status: JobStatus = JobStatus.NEW
    meta: typing.Dict = Field(default_factory=dict)
    worker_id: typing.Optional[str] = None
    worker_name: typing.Optional[str] = None

    job_progress: typing.Optional[JobProgress] = Field(default_factory = JobProgress)

    # Allow for passing in a callback function to be called after the job is run
    job_callback: typing.Optional[typing.Union[str, typing.Callable]] = None
    job_callback_kwargs: typing.Optional[dict] = Field(default_factory=dict)

    if typing.TYPE_CHECKING:
        queue: typing.Optional[typing.Union['TaskQueue', typing.Any]] = None

    
    @validator("job_callback")
    def validate_job_callback(cls, v: typing.Optional[typing.Union[str, typing.Callable]]) -> typing.Optional[str]:
        """
        Validates the callback and returns the function name
        """
        return v if v is None else get_func_full_name(v)
    
    @property
    def job_callback_function(self) -> typing.Optional[typing.Callable]:
        """
        Returns the job callback function
        """
        if self.job_callback is None: return None
        func = import_function(self.job_callback)
        return ensure_coroutine_function(func)


    def __repr__(self):
        kwargs = ", ".join(
            f"{k}={v}"
            for k, v in {
                "id": self.id,
                "function": self.function,
                "has_callback": self.has_job_callback,
                "kwargs": self.kwargs,
                "queue": self.queue.queue_name,
                "scheduled": self.scheduled,
                "progress": self.progress,
                "process_ms": self.duration("process"),
                "start_ms": self.duration("start"),
                "total_ms": self.duration("total"),
                "attempts": self.attempts,
                "result": self.result,
                "error": self.error,
                "status": self.status,
                "meta": self.meta,
                "worker_id": self.worker_id,
                "worker_name": self.worker_name,
            }.items()
            if v is not None
        )
        return f"Job<{kwargs}>"
    
    @property
    def has_job_callback(self) -> bool:
        """
        Checks if the job has a callback
        """
        return self.job_callback is not None

    @property
    def short_repr(self):
        """
        Shortened representation of the job.
        """
        kwargs = ", ".join(
            f"{k}={v}"
            for k, v in {
                "id": self.id,
                "function": self.function,
                "kwargs": self.kwargs,
                "status": self.status,
                "attempts": self.attempts,
                "queue": self.queue.queue_name,
                "worker_id": self.worker_id,
                "worker_name": self.worker_name,
            }.items()
            if v is not None
        )
        return f"Job<{kwargs}>"
    

    @property
    def log_repr(self):
        """
        Shortened representation of the job.
        """
        kwargs = ", ".join(
            f"{k}={v}"
            for k, v in {
                "status": self.status,
                "attempts": self.attempts,
                "progress": self.progress,
                "kwargs": self.kwargs,
                "scheduled": self.scheduled,
                "process_ms": self.duration("process"),
                "start_ms": self.duration("start"),
                "total_ms": self.duration("total"),
                "error": self.error,
                "meta": self.meta,
            }.items()
            if v is not None
        )
        return f"{kwargs}"


    def __hash__(self):
        return hash(self.key)

    @property
    def id(self):
        """
        Returns the job id.
        """
        return self.queue.job_id(self.key)

    @classmethod
    def key_from_id(cls, job_id: str):
        """
        Returns the key from a job id.
        """
        return job_id.split(":")[-1]

    @property
    def abort_id(self):
        """
        Returns the abort id.
        """
        return f"{self.queue.abort_id_prefix}:{self.key}"

    def to_dict(self):
        """
        Serializes the job to a dictionary.
        """
        result = {}
        data = self.dict(
            exclude_none = True,
            exclude_defaults = True,
            exclude = {"kwargs"} if self.queue.serializer is not None else None,
        )
        for key, value in data.items():
            if key == "meta" and not value:
                continue
            if key == "queue" and value:
                value = value.queue_name
            result[key] = value
        if self.queue.serializer is not None: result["kwargs"] = self.kwargs
        return result


    def duration(self, kind) -> int:
        """
        Returns the duration of the job given kind.

        Kind can be process (how long it took to process),
        start (how long it took to start), or total.
        """
        if kind == "process":
            return self._duration(self.completed, self.started)
        if kind == "start":
            return self._duration(self.started, self.queued)
        if kind == "total":
            return self._duration(self.completed, self.queued)
        if kind == "running":
            return self._duration(now(), self.started)
        if kind == "queued":
            return self._duration(now(), self.queued)
        raise ValueError(f"Unknown duration type: {kind}")
    
    @property
    def job_duration(self) -> int:
        """
        Returns the duration of the job in ms.
        """
        for kind in {
            'process', 'total', 'start', 'running', 'queued'
        }:
            if duration := self.duration(kind):
                return duration
        
        # durations = []
        # for kind in {
        #     'running', 'start', 'queued'
        # }:
        #     if duration := self.duration(kind):
        #         durations.append(duration)
        #         # return duration
        # return max(durations, default=0)
        # return 0


    def _duration(self, a, b):
        #return a - b
        return a - b if a and b else None

    @property
    def stuck(self):
        """
        Checks if an active job is passed it's timeout or heartbeat.
        - if timeout is None, set timeout to 2 hrs = 7200.00
        - revised timeout to 30 mins = 1800.00
        """
        current = now()
        return (self.status == JobStatus.ACTIVE) and (
            seconds(current - self.started) > \
                # (self.timeout if self.timeout is not None else 1800.0)
                (self.timeout if self.timeout is not None else 7200.0)
            or (
                self.heartbeat and \
                    seconds(current - self.touched) > self.heartbeat
                )
        )

    def next_retry_delay(self):
        """
        Gets the next retry delay for the job.
        """
        if self.retry_backoff:
            max_delay = self.retry_delay
            if max_delay is True: max_delay = None
            return exponential_backoff(
                attempts = self.attempts,
                base_delay = self.retry_delay,
                max_delay = max_delay,
                jitter = True,
            )
        return self.retry_delay

    async def enqueue(self, queue: 'TaskQueue' = None):
        """
        Enqueues the job to it's queue or a provided one.

        A job that already has a queue cannot be re-enqueued. Job uniqueness is determined by its id.
        If a job has already been queued, it will update it's properties to match what is stored in the db.
        """
        queue = queue or self.queue
        assert queue, "Queue unspecified"
        if not await queue.enqueue(self):
            await self.refresh()

    async def abort(self, error: typing.Any, ttl: int = 5):
        """Tries to abort the job."""
        await self.queue.abort(self, error = error, ttl = ttl)

    async def finish(self, status: JobStatus, *, result: typing.Any = None, error: typing.Any = None):
        """Finishes the job with a Job.Status, result, and or error."""
        await self.queue.finish(self, status = status, result = result, error = error)

    async def retry(self, error: typing.Any):
        """Retries the job by removing it from active and requeueing it."""
        await self.queue.retry(self, error)

    async def update(self, **kwargs):
        """
        Updates the stored job in keydb.

        Set properties with passed in kwargs.
        """
        for k, v in kwargs.items():
            setattr(self, k, v)
        await self.queue.update(self)
    
    # async def reset(self):
    #     """
    #     Resets the job to it's initial state and rerun
    #     """
    #     await self.queue.reset(self)
    
    async def set_progress(self, total: typing.Optional[int] = None, completed: typing.Optional[int] = None):
        """
        Sets the job's progress.
        """
        if total is not None:
            self.job_progress.total = total
        if completed is not None:
            self.job_progress.completed = completed
        await self.queue.update(self)
    
    async def incr_progress(self, incr: typing.Optional[int] = 1):
        """
        Increments the job's progress.
        """
        self.job_progress.completed += incr
        await self.queue.update(self)

    @property
    def _progress(self) -> typing.Optional[float]:
        """
        Returns the job's progress as a float.
        """
        return self.job_progress.progress

    async def refresh(self, until_complete: int = None):
        """
        Refresh the current job with the latest data from the db.

        until_complete: None or Numeric seconds. if None (default), don't wait,
            else wait seconds until the job is complete or the interval has been reached. 0 means wait forever
        """
        job = await self.queue.job(self.key)

        if not job: raise RuntimeError(f"{self} doesn't exist")
        self.replace(job)
        if until_complete is not None and not self.completed:
            async def callback(_id, status):
                if status in TERMINAL_STATUSES:
                    return True

            await self.queue.listen([self.key], callback, until_complete)
            await self.refresh()

    @lazyproperty
    def job_fields(self) -> typing.List[str]:
        """
        Returns the fields of the job.
        """
        return [field.name for field in self.__fields__.values()]

    def replace(self, job: 'Job'):
        """
        Replace current attributes with job attributes.
        """
        for field in job.job_fields:
            setattr(self, field, getattr(job, field))

    
    @classmethod
    def get_fields(cls):
        """
        Returns the fields of the job.
        """
        return [field.name for field in cls.__fields__.values()]
    
    @classmethod
    def from_kwargs(cls, job_or_func: typing.Union[str, 'Job', typing.Callable], **kwargs) -> 'Job':
        """
        Returns a job from kwargs.
        """
        job_kwargs = {"kwargs": {}}
        job_fields = cls.get_fields()
        for k, v in kwargs.items():
            # Allow passing underscored keys
            # to prevent collision with job fields
            if k.startswith('_') and k[1:] in job_fields:
                job_kwargs[k[1:]] = v
            elif k in job_fields:
                job_kwargs[k] = v
            else:
                job_kwargs["kwargs"][k] = v
        
        if isinstance(job_or_func, type(cls)):
            job = job_or_func
            for k, v in job_kwargs.items():
                setattr(job, k, v)

        elif isinstance(job_or_func, str) or callable(job_or_func):
            job = cls(function = get_func_name(job_or_func), **job_kwargs)

        else:
            raise ValueError(f"Invalid job type {type(job_or_func)}")
    
        return job
        
    async def _run_job_callback(self):
        """
        Runs the job callback
        """
        if not self.has_job_callback: return
        if self.status not in TERMINAL_STATUSES: return
        try:
            await self.job_callback_function(
                status = self.status,
                job_id = self.id,
                function_name = self.function,
                duration = self.job_duration,
                result = self.result,
                error = self.error,
                **self.job_callback_kwargs or {},
            )
        except Exception as e:
            logger.error(f"Failed to run job callback for {self}: {e}")

    async def run_job_callback(self):
        """
        Runs the job callback in the background
        """
        create_background_task(self._run_job_callback())

    """
    Queue Keys for Job
    """

    @lazyproperty
    def queued_key(self) -> str:
        if self.worker_id:
            return f"{self.queue.queued_key}:{self.worker_id}"
        if self.worker_name:
            return f"{self.queue.queued_key}:{self.worker_name}"
        return self.queue.queued_key
    
    @lazyproperty
    def active_key(self) -> str:
        if self.worker_id:
            return f"{self.queue.active_key}:{self.worker_id}"
        if self.worker_name:
            return f"{self.queue.active_key}:{self.worker_name}"
        return self.queue.active_key
    
    @lazyproperty
    def incomplete_key(self) -> str:
        if self.worker_id:
            return f"{self.queue.incomplete_key}:{self.worker_id}"
        if self.worker_name:
            return f"{self.queue.incomplete_key}:{self.worker_name}"
        return self.queue.incomplete_key

# Job.update_forward_refs()