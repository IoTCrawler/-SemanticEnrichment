import json
import uuid
import logging
import ngsi_ld.ngsi_parser
from flask import Flask, redirect, render_template, url_for, request, Blueprint, flash
from semanticenrichment import SemanticEnrichment
from other.exceptions import BrokerError
from other.logging import DequeLoggerHandler

MAX_LOG_ENTRIES = 20

# Configure logging
logger = logging.getLogger('semanticenrichment')
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

file_handler = logging.FileHandler('semanticenrichment.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

deque_handler = DequeLoggerHandler(MAX_LOG_ENTRIES)
deque_handler.setLevel(logging.DEBUG)
deque_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(deque_handler)
logger.info("logger ready")

bp = Blueprint('semanticenrichment', __name__, static_url_path='', static_folder='/static', template_folder='html')
semanticEnrichment = SemanticEnrichment()


@bp.route('/')
@bp.route('/index')
def index():
    return render_template("index.html")


@bp.route('/showsubscriptions', methods=['GET', 'POST'])
def showsubscriptions():
    subscriptions = semanticEnrichment.get_subscriptions()
    formdata = None
    with open('jsonfiles/UMU_Subscription_TemperatureSensor.json') as jFile:
        data = json.load(jFile)
        data['id'] = data['id'] + str(uuid.uuid4())
        data['notification']['endpoint']['uri'] = semanticEnrichment.callback_url
        formdata = json.dumps(data, indent=2)

    return render_template('subscriptions.html', formdata=formdata, subscriptions=subscriptions.values())

@bp.route('/log', methods=['GET'])
def showlog():
    return render_template('log.html', logmessages=deque_handler.get_entries(), maxentries=deque_handler.maxentries)

@bp.route('/addsubscription', methods=['POST'])
def addsubscription():
    host = request.form.get('host')
    port = request.form.get('port')
    subscription = request.form.get('subscription')
    if None not in (host, port, subscription):
        try:
            semanticEnrichment.add_subscription(host, port, json.loads(subscription))
        except BrokerError as e:
            flash('Error while adding subscription:' + str(e))
    else:
        logger.debug("missing data for adding subscription")
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
    datasouces = []
    for ds in semanticEnrichment.get_datasources().values():
        ds.qoi = semanticEnrichment.get_qoivector(ds.id)
        datasouces.append(ds)
    return render_template('datasources.html', datasources=datasouces)


# @cherrypy.tools.json_in()
@bp.route('/callback', methods=['POST'])
def callback():
    logger.debug("callback called" + str(request.get_json()))
    # TODO parse and add to datasource manager

    # split to data and metadata
    data, metadata = ngsi_ld.ngsi_parser.parse_ngsi(request.get_json())
    # create data source in data source manager
    semanticEnrichment.notify_datasource(metadata)
    semanticEnrichment.receive(data)
    # TODO change return value
    return "OK"


app = Flask(__name__)
app.secret_key = 'e3645c25b6d5bf67ae6da68c824e43b530e0cb43b0b9432b'
app.register_blueprint(bp, url_prefix='/semanticenrichment')


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8081, debug=False)

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
