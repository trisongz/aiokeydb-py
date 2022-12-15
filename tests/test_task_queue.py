import asyncio
from aiokeydb import KeyDBClient
from aiokeydb.queues import TaskQueue, Worker
from lazyops.utils import logger

keydb_uri = "keydb://.."

KeyDBClient.configure(
    url = keydb_uri,
    debug_enabled = True,
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
