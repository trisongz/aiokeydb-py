# KeyDB Worker Queues

KeyDB Worker Queues is a simple, fast, and reliable queue system for KeyDB. It is designed to be used in a distributed environment, where multiple KeyDB instances are used to process jobs. It is also designed to be used in a single instance environment, where a single KeyDB instance is used to process jobs.

```python
import asyncio
from aiokeydb import KeyDBClient
from aiokeydb.queues import TaskQueue, Worker
from lazyops.utils import logger


# Configure the KeyDB Client - the default keydb client will use 
# db = 0, and queue uses 2 so that it doesn't conflict with other
# by configuring it here, you can explicitly set the db to use
keydb_uri = "keydb://127.0.0.1:6379/0"

# Configure the Queue to use db = 1 instead of 2
KeyDBClient.configure(
    url = keydb_uri,
    debug_enabled = True,
    queue_db = 1,
)

@Worker.add_cronjob("*/1 * * * *")
async def test_cron_task(*args, **kwargs):
    logger.info("Cron task ran")
    await asyncio.sleep(5)

@Worker.add_function()
async def test_task(*args, **kwargs):
    logger.info("Task ran")
    await asyncio.sleep(5)

async def run_tests():
    queue = TaskQueue("test_queue")
    worker = Worker(queue)
    await worker.start()

asyncio.run(run_tests())

```