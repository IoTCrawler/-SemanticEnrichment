import threading
import requests
import logging
import json
import uuid
from configuration import Config
from ngsi_ld.subscription import Subscription
from ngsi_ld import ngsi_parser

logger = logging.getLogger('semanticenrichment')

headers = {}
headers.update({'content-type': 'application/ld+json'})
headers.update({'accept': 'application/ld+json'})


# this method is mainly for testing etc as subscriptions are lost during restart,
# in addition ngrok won't fit for old subscriptions
def get_active_subscriptions(sublist):
    t = threading.Thread(target=_get_active_subscriptions, args=(sublist,))  # put into thread to not block server
    t.start()


def _get_active_subscriptions(subscriptions):
    # get old subscriptions for semantic enrichment (starting with 'SE_')
    host = Config.get('NGSI', 'host')
    port = Config.get('NGSI', 'port')
    server_url = "http://" + host + ":" + port + "/ngsi-ld/v1/subscriptions/"
    try:
        r = requests.get(server_url, headers=headers)
        if r.status_code == 200:
            if isinstance(r.json(), list):
                for data in r.json():
                    handlejsonsubscription(data, host, port, subscriptions)
            if isinstance(r.json(), dict):
                handlejsonsubscription(r.json(), host, port, subscriptions)
        else:
            logger.error("Error getting active subscriptions: " + r.text + str(r.status_code))
    except Exception as e:
        logger.error("Error getting active subscriptions: " + str(e))


def handlejsonsubscription(data, host, port, subscriptions):
    if data['id'].startswith('SE_', data['id'].rfind(':') + 1):
        sub = Subscription(data['id'], host, port, data)
        subscriptions[sub.id] = sub
        logger.info("Found active subscription: " + str(data))
    else:
        logger.info("not our subscription")


def add_subscription(subscription, subscriptionlist):
    t = threading.Thread(target=_add_subscription, args=(subscription, subscriptionlist))
    t.start()


def _add_subscription(subscription, subscriptions):
    host = Config.get('NGSI', 'host')
    port = Config.get('NGSI', 'port')
    # subscribe to ngsi-ld endpoint
    sub = Subscription(subscription['id'], host, port, subscription)

    if ngsi_add_subscription(subscription) is not None:
        subscriptions[sub.id] = sub


def ngsi_add_subscription(subscription):
    host = Config.get('NGSI', 'host')
    port = Config.get('NGSI', 'port')

    server_url = "http://" + host + ":" + str(port) + "/ngsi-ld/v1/subscriptions/"
    r = requests.post(server_url, json=subscription, headers=headers)
    logger.info("Adding subscription: " + str(r.status_code) + " " + r.text)
    if r.status_code != 201:
        logger.debug("error creating subscription: " + r.text)
        return None
    return r.text


def del_subscription(subscription):
    t = threading.Thread(target=_del_subscription, args=(subscription,))
    t.start()


def _del_subscription(subscription):
    server_url = "http://" + subscription.host + ":" + str(subscription.port) + "/ngsi-ld/v1/subscriptions/"
    server_url = server_url + subscription.id
    r = requests.delete(server_url, headers=headers)
    logger.debug("deleting subscription " + subscription.id + ": " + r.text)


def add_ngsi_attribute(ngsi_msg, eid):
    t = threading.Thread(target=_add_ngsi_attribute, args=(ngsi_msg, eid,))
    t.start()


def _add_ngsi_attribute(ngsi_msg, eid):
    try:
        logger.debug("Add ngsi attribute to entity " + eid + ":" + str(ngsi_msg))
        url = "http://" + Config.get('NGSI', 'host') + ":" + str(
            Config.get('NGSI', 'port')) + "/ngsi-ld/v1/entities/" + eid + "/attrs/"
        r = requests.post(url, json=ngsi_msg, headers=headers)
        if r.status_code != 204:
            logger.debug("Attribute exists, patch it")
            requests.patch(url, json=ngsi_msg, headers=headers)
    except requests.exceptions.ConnectionError as e:
        logger.error("Error while adding attribute to ngsi entity" + str(e))


def create_ngsi_entity(ngsi_msg):
    t = threading.Thread(target=_create_ngsi_entity, args=(ngsi_msg,))
    t.start()


def _create_ngsi_entity(ngsi_msg):
    try:
        logger.debug("Save entity to ngsi broker: " + str(ngsi_msg))
        url = "http://" + Config.get('NGSI', 'host') + ":" + str(
            Config.get('NGSI', 'port')) + "/ngsi-ld/v1/entities/"
        # print(url)
        r = requests.post(url, json=ngsi_msg, headers=headers)
        if r.status_code == 409:
            logger.debug("Entity exists, patch it")
            _patch_ngsi_entity(ngsi_msg)
    except requests.exceptions.ConnectionError as e:
        logger.error("Error while creating ngsi entity" + str(e))


def patch_ngsi_entity(ngsi_msg):
    t = threading.Thread(target=_patch_ngsi_entity, args=(ngsi_msg,))
    t.start()


def _patch_ngsi_entity(ngsi_msg):
    try:
        # for updating entity we have to delete id and type, first do copy if needed somewhere else
        ngsi_msg_patch = dict(ngsi_msg)
        ngsi_msg_patch.pop('id')
        ngsi_msg_patch.pop('type', None)
        url = "http://" + Config.get('NGSI', 'host') + ":" + str(
            Config.get('NGSI', 'port')) + "/ngsi-ld/v1/entities/" + ngsi_msg['id'] + "/attrs"
        r = requests.patch(url, json=ngsi_msg_patch, headers=headers)
        logger.debug("Entity patched: " + str(r.status_code))
    except requests.exceptions.ConnectionError as e:
        logger.error("Error while patching ngsi entity" + str(e))


def get_entity_updateList(entityid, entitylist):
    t = threading.Thread(target=_get_entity_updateList, args=(entityid, entitylist))
    t.start()


def _get_entity_updateList(entityid, entitylist):
    entity = get_entity(entityid)
    if entity:
        entitylist[entityid] = entity


def get_entity(entitiyid):
    try:
        url = "http://" + Config.get('NGSI', 'host') + ":" + str(
            Config.get('NGSI', 'port')) + "/ngsi-ld/v1/entities/" + entitiyid
        r = requests.get(url, headers=headers)
        if r.status_code != 200:
            logger.error("Error requesting entity " + entitiyid + ": " + r.text)
            return None
        return r.json()
    except requests.exceptions.ConnectionError as e:
        logger.error("Error while getting entity " + entitiyid + ": " + str(e))


# def _find_streamobservation(streamid):
#     try:
#         url = "http://" + Config.get('NGSI', 'host') + ":" + str(Config.get('NGSI', 'port')) + "/ngsi-ld/v1/entities/"
#         params = {'type': 'http://www.w3.org/ns/sosa/Sensor/ObservableProperty', 'q': 'http://www.w3.org/ns/sosa/Sensor/isObservedBy==' + streamid}
#         r = requests.get(url, headers=headers, params=params)
#         if r.status_code != 200:
#             logger.error("Error finding streamobservation for stream " + streamid + ": " + r.text)
#             return None
#         return r.json()
#     except requests.exceptions.ConnectionError as e:
#         logger.error("Error while finding streamobservation for stream " + streamid + ": " + str(e))


def subscribe_forTypeId(ngsi_type, entityId, sublist):
    t = threading.Thread(target=_subscribe_forTypeId, args=(ngsi_type, entityId, sublist))
    t.start()


def _subscribe_forTypeId(ngsi_type, entityId, sublist):
    logger.debug("Subscribe for " + str(ngsi_type) + " " + entityId)
    # check if subscription already in sublist
    # solution is not optimal... but no other option at the moment
    for key, value in sublist.items():
        sub = value.subscription
        try:
            tmposid = sub['entities'][0]['id']
            if tmposid == entityId:
                logger.debug("Subscription for " + tmposid + " already existing!")
                return
        except KeyError:
            pass

    # create subscription
    filename = ""
    if ngsi_type is ngsi_parser.NGSI_Type.Sensor:
        filename = 'static/json/subscription_sensor.json'
    elif ngsi_type is ngsi_parser.NGSI_Type.IoTStream:
        filename = 'static/json/subscription_iotstream.json'
    elif ngsi_type is ngsi_parser.NGSI_Type.StreamObservation:
        filename = 'static/json/subscription_streamobservation.json'

    with open(filename) as jFile:
        subscription = json.load(jFile)
        subscription['id'] = subscription['id'] + str(uuid.uuid4())
        # replace callback
        subscription['notification']['endpoint']['uri'] = Config.get('semanticenrichment', 'callback')
        # set entity to subscribe to
        subscription['entities'][0]['id'] = entityId
        _add_subscription(subscription, sublist)


def handleNewSensor(sensorId, sensors, observableproperties, subscriptions):
    # GET for sensor
    sensor = get_entity(sensorId)
    if sensor:
        sensors[sensorId] = sensor

        # GET for obsproperty(sensor)
        observablepropertyId = ngsi_parser.get_sensor_observes(sensor)
        observableproperty = get_entity(observablepropertyId)
        if observableproperty:
            observableproperties[observablepropertyId] = observableproperty

        # SUB for streamobservation(sensor)
        streamobservationId = ngsi_parser.get_sensor_madeObservation(sensor)
        _subscribe_forTypeId(ngsi_parser.NGSI_Type.StreamObservation, streamobservationId, subscriptions)

        # SUB for sensor
        _subscribe_forTypeId(ngsi_parser.NGSI_Type.Sensor, sensorId, subscriptions)


# for testing purposes
if __name__ == "__main__":
    pass
