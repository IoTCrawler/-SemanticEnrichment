import json
import uuid
import logging
import ngsi_ld.ngsi_parser
import datetime
from ngsi_ld.ngsi_parser import NGSI_Type
from flask import Flask, redirect, render_template, url_for, request, Blueprint, flash
from semanticenrichment import SemanticEnrichment
from other.exceptions import BrokerError
from other.logging import DequeLoggerHandler
from configuration import Config

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
semanticEnrichment = SemanticEnrichment()


def formate_datetime(value):
    if isinstance(value, float):
        value = datetime.datetime.fromtimestamp(value)
    if value:
        return value.strftime('%Y-%m-%dT%H:%M:%SZ')
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
    
    #solution for checkboxes
    if value is None:
        value = "False"
    else:
        value = "True"

    Config.update(section, key, value)
    # check if logging changed
    if section == "logging":
        deque_handler.setnrentries(int(Config.get('logging', 'maxlogentries')))
    return redirect(url_for('.showconfiguration'))


@bp.route('/addsubscription', methods=['POST'])
def addsubscription():
    subscription = request.form.get('subscription')
    try:
        semanticEnrichment.add_subscription(Config.get('NGSI', 'host'), Config.get('NGSI', 'port'),
                                            json.loads(subscription))
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
        class datasource:   #local class to be returned to html page
            pass

        datasource.stream_id = stream_id
        datasource.type = ngsi_ld.ngsi_parser.get_stream_observes(stream)
        # get last StreamObservation for the stream
        observation = semanticEnrichment.get_observation_for_stream(stream_id)
        datasource.observedat = ngsi_ld.ngsi_parser.get_observation_timestamp(observation)
        datasource.stream = json.dumps(stream, indent=2)
        datasource.qoi = json.dumps(semanticEnrichment.get_qoivector_ngsi(stream_id), indent=2)
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


# @cherrypy.tools.json_in()
@bp.route('/callback', methods=['POST'])
def callback():
    logger.debug("callback called" + str(request.get_json()))
    print(request.get_json())

    ngsi_id, ngsi_type = ngsi_ld.ngsi_parser.get_IDandType(request.get_json())

    # notify about new iotstream, sensor, streamobservation, initialise qoi system if new stream
    semanticEnrichment.notify_datasource(request.get_json())

    # inform about new data
    if ngsi_type is NGSI_Type.StreamObservation:
        semanticEnrichment.receive(request.get_json())
    # split to data and metadata
    # data, metadata = ngsi_ld.ngsi_parser.parse_ngsi(
    #     request.get_json())  # TODO check if metadata contains NA values, if so try to find some metadata
    # print(metadata)
    # print(data)
    # # create data source in data source manager
    # semanticEnrichment.notify_datasource(metadata)
    # semanticEnrichment.receive(data)
    # TODO change return value
    return "OK"


app = Flask(__name__)
app.secret_key = 'e3645c25b6d5bf67ae6da68c824e43b530e0cb43b0b9432b'
app.register_blueprint(bp, url_prefix='/semanticenrichment')
app.jinja_env.filters['datetime'] = formate_datetime

if __name__ == "__main__":
    app.run(host=Config.get('semanticenrichment', 'host'), port=int(Config.get('semanticenrichment', 'port')),
            debug=False)

# TODO
# which metric for whole stream, which for every single value?
# completeness for whole data
# plausibility for single value
# timeliness whole data
# concordance
# artificiality

# store for common sensor types to calc plausibility? e.g. store common min max values for temp sensors

# in which format will we receive data?
# do we need a clock for replay experiments? maybe not as we will only get live data or complete datasets
# we should have qoi for a single measurement as well as for data sources?

# # sample data
# data = {
#     "id": 1,
#     "timestamp": 121232345,
#     "values": {
#         "value1": 1,
#         "value2": -100
#     }
# }
#
# metadata = {
#     "id": 1,
#     "type": "weather",
#     "updateinterval": {
#         "frequency": "100",
#         "unit": "seconds"
#     },
#     "fields": {
#         "value1": {
#             "sensortype": "temperature",
#             "valuetype": "int",
#             "min": -20,
#             "max": 40
#         },
#         "value2": {
#             "sensortype": "humidity",
#             "valuetype": "int",
#             "min": 0,
#             "max": 100
#         }
#     }
# }
