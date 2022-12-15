from aiokeydb import KeyDBClient
from lazyops.utils import logger

default_uri = 'keydb://public.host.com:6379/0'
keydb_dbs = {
    'cache': {
        'db_id': 1,
    },
    'db': {
        'uri': 'keydb://127.0.0.1:6379/0',
    },
}

KeyDBClient.configure(
    url = default_uri,
    debug_enabled = True,
    queue_db = 1,
)

# now any sessions that are initialized will use the global settings

sessions = {}
# these will now be initialized

# Initialize the first default session
KeyDBClient.init_session()

for name, config in keydb_dbs.items():
    sessions[name] = KeyDBClient.init_session(
        name = name,
        **config
    )
    logger.info(f'Session {name}: uri: {sessions[name].uri}')
