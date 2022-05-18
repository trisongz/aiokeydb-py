import logging
import os
import sys

logger = logging.getLogger("aiokeydb")
sentinel_logger = logger.getChild("sentinel")

if os.environ.get("AIOKEYDB_DEBUG"):
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(stream=sys.stderr)
    handler.setFormatter(
        logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s")
    )
    logger.addHandler(handler)
    os.environ["AIOKEYDB_DEBUG"] = ""