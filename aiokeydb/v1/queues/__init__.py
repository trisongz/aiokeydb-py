from __future__ import absolute_import

from aiokeydb.v1.queues.types import Job, JobStatus, TaskType
from aiokeydb.v1.queues.queue import TaskQueue
from aiokeydb.v1.queues.worker.base import Worker, WorkerTasks

if hasattr(Job, 'model_rebuild'):
    Job.model_rebuild()
else:
    Job.update_forward_refs(TaskQueue=TaskQueue)