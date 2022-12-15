from __future__ import annotations

import enum
import typing

from aiokeydb.client.types import BaseModel, lazyproperty, Field, validator
from aiokeydb.queues.utils import (
    get_default_job_key, 
    uuid4, 
    now, 
    seconds, 
    exponential_backoff,
    get_hostname,
)

if typing.TYPE_CHECKING:
    from aiokeydb.queues.queue import TaskQueue

class JobStatus(str, enum.Enum):
    NEW = "new"
    DEFERRED = "deferred"
    QUEUED = "queued"
    ACTIVE = "active"
    ABORTED = "aborted"
    FAILED = "failed"
    COMPLETE = "complete"


TERMINAL_STATUSES = {JobStatus.COMPLETE, JobStatus.FAILED, JobStatus.ABORTED}
UNSUCCESSFUL_TERMINAL_STATUSES = TERMINAL_STATUSES - {JobStatus.COMPLETE}

class TaskType(str, enum.Enum):
    """
    The Type of Task
    for the worker
    """
    default = "default"
    function = "function"
    cronjob = "cronjob"
    dependency = "dependency"
    context = "context"
    startup = "startup"
    shutdown = "shutdown"

class QueueStats(BaseModel):
    """
    
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
    
    def create_namespace(self, namespace: str) -> str:
        return f"{self.prefix}:{self.name}:{namespace}"
    
    @lazyproperty
    def node_name(self) -> str:
        get_hostname()


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
    timeout: typing.Optional[int] = None
    heartbeat: typing.Optional[int] = None
    retries: typing.Optional[int] = None
    ttl: typing.Optional[int] = None

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
    queue: typing.Optional[typing.Union['TaskQueue', typing.Any]] = None
    key: typing.Optional[str] = Field(default_factory = get_default_job_key)
    timeout: int = 600
    heartbeat: int = 0
    retries: int = 1
    ttl: int = 600
    retry_delay: float = 2.0
    retry_backoff: typing.Union[bool, float] = True
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

    def __repr__(self):
        kwargs = ", ".join(
            f"{k}={v}"
            for k, v in {
                "function": self.function,
                "kwargs": self.kwargs,
                "queue": self.queue.queue_name,
                "id": self.id,
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
            }.items()
            if v is not None
        )
        return f"Job<{kwargs}>"
    
    @property
    def short_repr(self):
        """
        Shortened representation of the job.
        """
        return f"Job<function={self.function}, \
            kwargs={self.kwargs}, \
            status={self.status}, \
            attempts={self.attempts}, \
            queue={self.queue.queue_name}, \
            id={self.id}>"

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
        return self.queue.job_id(self.key)

    @classmethod
    def key_from_id(cls, job_id: str):
        return job_id.split(":")[-1]

    @property
    def abort_id(self):
        return f"{self.queue.abort_id_prefix}:{self.key}"

    def to_dict(self):
        result = {}
        data = self.dict(
            exclude_none = True,
            exclude_defaults = True,
        )
        for key, value in data.items():
            if key == "meta" and not value:
                continue
            if key == "queue" and value:
                value = value.queue_name
            result[key] = value
        return result


    def duration(self, kind):
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
                (self.timeout if self.timeout is not None else 1800.0)
            or (
                self.heartbeat and \
                    seconds(current - self.touched) > self.heartbeat
                )
        )

    def next_retry_delay(self):
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
        return [field.name for field in self.__fields__.values()]

    def replace(self, job: 'Job'):
        """Replace current attributes with job attributes."""
        for field in job.job_fields:
            setattr(self, field, getattr(job, field))
    
    @classmethod
    def get_fields(cls):
        return [field.name for field in cls.__fields__.values()]
