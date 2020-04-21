import datetime
import json
import logging
import uuid

from flask import Flask, redirect, render_template, url_for, request, Blueprint, flash

import ngsi_ld.ngsi_parser
from configuration import Config
from ngsi_ld.ngsi_parser import NGSI_Type
from other.exceptions import BrokerError
from other.logging import DequeLoggerHandler
from semanticenrichment import SemanticEnrichment

# Configure logging
logger = logging.getLogger('semanticenrichment')
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s', '%Y-%m-%dT%H:%M:%SZ')

file_handler = logging.FileHandler('semanticenrichment.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

deque_handler = DequeLoggerHandler(int(Config.get('logging', 'maxlogentries')))
deque_handler.setLevel(logging.DEBUG)
deque_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(deque_handler)
logger.info("logger ready")

bp = Blueprint('semanticenrichment', __name__, static_url_path='', static_folder='static', template_folder='html')
bp2 = Blueprint('', __name__, static_url_path='', static_folder='static',
                template_folder='html')  # second blueprint for return liveness probe for kubernets
semanticEnrichment = SemanticEnrichment()


def format_datetime(value):
    if isinstance(value, float):
        value = datetime.datetime.fromtimestamp(value)
    if value:
        return value.strftime('%Y-%m-%d %H:%M:%SZ')  # added space instead of 'T' to enable line break
    return None


@bp.route('/')
@bp.route('/index')
def index():
    return render_template("index.html")


@bp.route('/showsubscriptions', methods=['GET', 'POST'])
def showsubscriptions():
    subscriptions = semanticEnrichment.get_subscriptions()
    return render_template('subscriptions.html', subscriptions=subscriptions.values(), id=str(uuid.uuid4()),
                           endpoint=Config.get('semanticenrichment', 'callback'))


@bp.route('/log', methods=['GET'])
def showlog():
    return render_template('log.html', logmessages=deque_handler.get_entries(),
                           maxentries=int(Config.get('logging', 'maxlogentries')))


@bp.route('/configuration', methods=['GET'])
def showconfiguration():
    return render_template('configuration.html', configuration=Config.getAllOptions())


@bp.route('/changeconfiguration', methods=['POST'])
def changeconfiguration():
    section = request.form.get('section')
    key = request.form.get('key')
    value = request.form.get('value')

    # solution for checkboxes
    if value is None:
        value = "False"

    Config.update(section, key, value)
    # check if logging changed
    if section == "logging":
        deque_handler.setnrentries(int(Config.get('logging', 'maxlogentries')))
    return redirect(url_for('.showconfiguration'))


@bp.route('/addsubscription', methods=['POST'])
def addsubscription():
    subscription = request.form.get('subscription')
    try:
        semanticEnrichment.add_subscription(json.loads(subscription))
    except BrokerError as e:
        flash('Error while adding subscription:' + str(e))
    return redirect(url_for('.showsubscriptions'))


@bp.route('/getsubscriptions', methods=['POST'])
def getsubscriptions():
    semanticEnrichment.datasource_manager.get_active_subscriptions()
    logger.info("missing data for adding subscription")
    return redirect(url_for('.showsubscriptions'))


@bp.route('/deletesubscription', methods=['POST'])
def deletesubscription():
    subid = request.form.get('subid')
    if subid is not None:
        logger.info("Delete subscription: " + subid)
        semanticEnrichment.del_subscription(subid)
    return redirect(url_for('.showsubscriptions'))


@bp.route('/showdatasources', methods=['GET'])
def showdatasources():
    datasources = []
    for stream_id, stream in semanticEnrichment.get_streams().items():
        class datasource:  # local class to be returned to html page
            pass

        datasource.stream_id = stream_id

        # get sensor for stream
        sensorId = ngsi_ld.ngsi_parser.get_stream_generatedBy(stream)
        sensor = semanticEnrichment.get_sensor(sensorId)

        # get observation for sensor
        observationId = ngsi_ld.ngsi_parser.get_sensor_madeObservation(sensor)
        observation = semanticEnrichment.get_observation(observationId)

        datasource.observedat = ngsi_ld.ngsi_parser.get_observation_timestamp(observation)
        datasource.stream = json.dumps(stream, indent=2)
        datasource.qoi = json.dumps(semanticEnrichment.get_qoivector_ngsi(stream_id), indent=2)
        datasource.sensor = json.dumps(sensor, indent=2)
        datasources.append(datasource)
    return render_template('datasources.html', datasources=datasources)


@bp.route('/showmetadata', methods=['GET'])
def showmetadata():
    metadata = semanticEnrichment.get_metadata()
    return render_template('metadata.html', metadata=metadata)


@bp.route('/addmetadata', methods=['POST'])
def addmetadata():
    type = request.form.get('type')
    metadata = request.form.get('metadata')
    if None not in (type, metadata):
        try:
            semanticEnrichment.add_metadata(type, metadata)
        except Exception as e:
            flash('Error while adding metadata:' + str(e))
    else:
        logger.debug("Missing input for adding metadata")
    return redirect(url_for('.showmetadata'))


@bp.route('/deletemetadata', methods=['POST'])
def deletemetadata():
    mtype = request.form.get('mtype')
    if mtype is not None:
        logger.info("Delete metadata: " + mtype)
        semanticEnrichment.delete_metadata(mtype)
    return redirect(url_for('.showmetadata'))


@bp.route('/callback', methods=['POST'])
def callback():
    data = request.get_json()
    logger.debug("callback called" + str(data))
    print("callback called" + str(data))

    ngsi_type = ngsi_ld.ngsi_parser.get_type(data)

    # TODO check if notification which might contain other entities
    # TODO include threading for every entity to avoid blocking?
    if ngsi_type is NGSI_Type.Notification:
        data = ngsi_ld.ngsi_parser.get_notification_entities(data)
    else:
        data = [data]

    for entity in data:
        # notify about new iotstream, sensor, streamobservation, initialise qoi system if new stream
        semanticEnrichment.notify_datasource(entity)

    # TODO change return value
    return "OK"


@bp2.route('/', methods=['GET'])
def status():
    return "running"
    # return redirect(url_for('semanticenrichment.index'))


@bp.route('/status', methods=['GET'])
def status():
    return "running"


app = Flask(__name__)
app.secret_key = 'e3645c25b6d5bf67ae6da68c824e43b530e0cb43b0b9432b'
app.register_blueprint(bp, url_prefix='/semanticenrichment')
app.register_blueprint(bp2, url_prefix='/')
app.jinja_env.filters['datetime'] = format_datetime

if __name__ == "__main__":
    app.run(host=Config.get('semanticenrichment', 'host'), port=int(Config.get('semanticenrichment', 'port')),
            debug=False)
