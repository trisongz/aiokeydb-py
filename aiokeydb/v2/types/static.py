
import enum

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