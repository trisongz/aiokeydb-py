from aiokeydb.queues.types import Job

class JobError(Exception):
    def __init__(self, job: 'Job'):
        super().__init__(
            f"Job {job.id} {job.status}\n\nThe above job failed with the following error:\n\n{job.error}"
        )
        self.job = job