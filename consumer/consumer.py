import paho.mqtt.client as mqtt
import json
import base64
import uuid
import logging
from reidentify import Reidentify
from db_write import DbWrite
from config import *


# Set up logger
if DEBUG:
    logging.basicConfig(format="[ %(levelname)s ] %(asctime)-15s %(message)s", level=logging.INFO)
else:
    logging.basicConfig(format="[ %(levelname)s ] %(asctime)-15s %(message)s", level=logging.INFO,
                    filename="./consumer.log")
log = logging.getLogger()
log.info('APPLICATION STARTING...')

kd_tree = Reidentify(image_file=KD_PATH + 'images.json', max_elements=10000, dim=128, kd_tree_file=KD_PATH+'fh_faces.bin')

db = DbWrite(cnxn_string)


def save_image(best_frame):
    """

    :param best_frame: base64
    :return: filename
    """
    filename = BEST_FRAME_PATH + '%s.jpg' % uuid.uuid4()
    with open(filename, 'wb') as f:
        f.write(base64.decodebytes(best_frame.encode('utf-8')))
    return filename


def on_connect(client, userdata, flags, rc):
    log.info("MQTT Connected with result code " + str(rc))
    client.subscribe("track_data")


def on_message(client, userdata, msg):
    log.info('Message received')
    data = json.loads(str(msg.payload.decode("utf-8")))
    log.info('data received')
    track = data["track"]

    best_frame_base64 = data["best_frame"]

    best_frame_filename = save_image(best_frame_base64)

    log.info('file saved: %s' % best_frame_filename)

    filename, cosine, best_index = kd_tree.re_id(track)
    log.info("Re-id data: image: %s, distance: %f " % (filename, cosine))

    db.insert_item(track, filename, cosine, best_index, best_frame_filename)
    log.info('item inserted BAZA')


client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(MQTT_SERVER_IP)
log.info('APPLICATION STARTED')
client.loop_forever()
log.info('APPICATION FINISHED')
