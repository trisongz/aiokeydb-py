from __future__ import absolute_import

from aiokeydb.queues.types import Job, JobStatus, TaskType
from aiokeydb.queues.queue import TaskQueue
from aiokeydb.queues.worker.base import Worker, WorkerTasks

Job.update_forward_refs(TaskQueue=TaskQueue)