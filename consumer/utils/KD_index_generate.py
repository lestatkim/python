import hnswlib
import numpy as np
import pandas as pd
import logging
from config import *
from config_default import CLIENTS_PATH

# Set up logger
if DEBUG:
    logging.basicConfig(format="[ %(levelname)s ] %(asctime)-15s %(message)s", level=logging.INFO)
else:
    logging.basicConfig(format="[ %(levelname)s ] %(asctime)-15s %(message)s", level=logging.INFO,
                    filename="./consumer.log")
log = logging.getLogger()
log.info('APPLICATION STARTING...')

data = pd.read_csv(CLIENTS_PATH)
log.info('Hashed %d faces' % len(data))
