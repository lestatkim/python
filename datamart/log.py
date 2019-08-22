from config import *
import logging
import inspect


# Set up logger
if DEBUG:
    logging.basicConfig(
        format="[ %(levelname)s ] %(asctime)-15s %(message)s",
        level=logging.DEBUG,
        filename=DEBUG_LOG_FILE)
else:
    logging.basicConfig(
        format="[ %(levelname)s ] %(asctime)-15s %(message)s",
        level=logging.INFO,
        filename=LOG_PATH
    )
log = logging.getLogger()

log.info('APPLICATION STARTING...')
log.info('system Configuration:')
# Debug data
for name, value in sorted(globals().copy().items()):
    if name.upper() == name and not hasattr(value, '__call__') and not inspect.ismodule(value):
        log.info("system Const %s: %s" % (name, value))
log.info('/ configuration')
