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
    server_url = Config.getEnvironmentVariable('NGSI_ADDRESS') + "/ngsi-ld/v1/subscriptions/"
    logger.debug("Get active subscriptions from" + server_url)
    try:
        r = requests.get(server_url, headers=headers, timeout=float(Config.get('semanticenrichment', 'timeout')))
        if r.status_code == 200:
            if isinstance(r.json(), list):
                for data in r.json():
                    handlejsonsubscription(data, Config.getEnvironmentVariable('NGSI_ADDRESS'), subscriptions)
            if isinstance(r.json(), dict):
                handlejsonsubscription(r.json(), Config.getEnvironmentVariable('NGSI_ADDRESS'), subscriptions)
        else:
            logger.error("Error getting active subscriptions: " + r.text + str(r.status_code))
    except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as e:
        logger.error("Exception getting active subscriptions: " + str(e))
    except Exception as e:
        logger.error("Error getting active subscriptions: " + str(e))


def handlejsonsubscription(data, address, subscriptions):
    try:
        if data['notification']['endpoint']['uri'] == Config.getEnvironmentVariable('SE_CALLBACK'):
            # if data['id'].startswith('SE_', data['id'].rfind(':') + 1):
            sub = Subscription(data['id'], address, data)
            subscriptions[sub.id] = sub
            logger.info("Found active subscription: " + str(data))
        else:
            logger.info("not our subscription")
    except KeyError:
        return None


def initialise_subscriptions(sublist):
    t = threading.Thread(target=_initialise_subscriptions, args=(sublist,))  # put into thread to not block server
    t.start()


def _initialise_subscriptions(subscriptions):
    # first get active subscriptions
    _get_active_subscriptions(subscriptions)

    # iterate list and check for IotStream, StreamObservation, Sensor subscription, if not found subscribe
    for t in (ngsi_parser.NGSI_Type.IoTStream, ngsi_parser.NGSI_Type.Sensor, ngsi_parser.NGSI_Type.StreamObservation):
        subscribe = True
        for key, value in subscriptions.items():
            sub = value.subscription
            try:
                if ngsi_parser.get_type(sub['entities'][0]) is t:
                    # it is of type IotStream, check if it is our endpoint
                    if sub['notification']['endpoint']['uri'] == Config.getEnvironmentVariable('SE_CALLBACK'):
                        logger.debug("Subscription for " + str(t) + " already existing!")
                        subscribe = False
                        break
            except KeyError:
                pass
        if subscribe:
            logger.debug("Initialise system with subscription for " + str(t))
            _subscribe_forTypeId(t, None, subscriptions)


def add_subscription(subscription, subscriptionlist):
    t = threading.Thread(target=_add_subscription, args=(subscription, subscriptionlist))
    t.start()


def _add_subscription(subscription, subscriptions):
    # subscribe to ngsi-ld endpoint
    sub = Subscription(subscription['id'], Config.getEnvironmentVariable('NGSI_ADDRESS'), subscription)

    if ngsi_add_subscription(subscription) is not None:
        subscriptions[sub.id] = sub


def ngsi_add_subscription(subscription):
    try:
        server_url = Config.getEnvironmentVariable('NGSI_ADDRESS') + "/ngsi-ld/v1/subscriptions/"
        r = requests.post(server_url, json=subscription, headers=headers, timeout=float(Config.get('semanticenrichment', 'timeout')))
        logger.info("Adding subscription: " + str(r.status_code) + " " + r.text)
        if r.status_code != 201:
            logger.debug("error creating subscription: " + r.text)
            return None
        return r.text
    except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as e:
        logger.error("Exception while adding subscription: " + str(e))


def del_subscription(subscription):
    t = threading.Thread(target=_del_subscription, args=(subscription,))
    t.start()


def _del_subscription(subscription):
    try:
        server_url = subscription.address + "/ngsi-ld/v1/subscriptions/"
        server_url = server_url + subscription.id
        r = requests.delete(server_url, headers=headers, timeout=float(Config.get('semanticenrichment', 'timeout')))
        logger.debug("deleting subscription " + subscription.id + ": " + r.text)
    except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as e:
        logger.error("Exception while deleting subscription: " + str(e))


def add_ngsi_attribute(ngsi_msg, eid):
    t = threading.Thread(target=_add_ngsi_attribute, args=(ngsi_msg, eid,))
    t.start()


def _add_ngsi_attribute(ngsi_msg, eid):
    try:
        logger.debug("Add ngsi attribute to entity " + eid + ":" + str(ngsi_msg))
        url = Config.getEnvironmentVariable('NGSI_ADDRESS') + "/ngsi-ld/v1/entities/" + eid + "/attrs"
        r = requests.post(url, json=ngsi_msg, headers=headers, timeout=float(Config.get('semanticenrichment', 'timeout')))
        logger.debug("add_ngsi_attribute result: " + str(r.status_code))
        if r.status_code not in (204, 207):
            logger.debug("Attribute exists, patch it")
            r = requests.patch(url, json=ngsi_msg, headers=headers, timeout=float(Config.get('semanticenrichment', 'timeout')))
            logger.debug("patch result: " + str(r.status_code))
    except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as e:
        logger.error("Exception while adding attribute: " + str(e))


def create_ngsi_entity(ngsi_msg):
    t = threading.Thread(target=_create_ngsi_entity, args=(ngsi_msg,))
    t.start()


def delete_and_create_ngsi_entity(ngsi_msg):
    t = threading.Thread(target=_delete_and_create_ngsi_entity, args=(ngsi_msg,))
    t.start()


def _delete_and_create_ngsi_entity(ngsi_msg):
    _delete_ngsi_entity(ngsi_msg['id'])
    _create_ngsi_entity(ngsi_msg)


def _delete_ngsi_entity(ngsiId):
    try:
        logger.debug("Delete entity with id: " + ngsiId)
        url = Config.getEnvironmentVariable('NGSI_ADDRESS') + "/ngsi-ld/v1/entities/" + ngsiId
        r = requests.delete(url, headers=headers, timeout=float(Config.get('semanticenrichment', 'timeout')))
    except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as e:
        logger.error("Exception while deleting entity: " + str(e))


def _create_ngsi_entity(ngsi_msg):
    try:
        logger.debug("Save entity to ngsi broker: " + str(ngsi_msg))
        url = Config.getEnvironmentVariable('NGSI_ADDRESS') + "/ngsi-ld/v1/entities/"
        r = requests.post(url, json=ngsi_msg, headers=headers, timeout=float(Config.get('semanticenrichment', 'timeout')))
        if r.status_code == 409:
            logger.debug("Entity exists, try to update/add attributes")
            _add_ngsi_attribute(ngsi_msg, ngsi_msg['id'])
    except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as e:
        logger.error("Exception while creating entity: " + str(e))


# def patch_ngsi_entity(ngsi_msg):
#     t = threading.Thread(target=_patch_ngsi_entity, args=(ngsi_msg,))
#     t.start()


# def _patch_ngsi_entity(ngsi_msg):
#     try:
#         url = Config.getEnvironmentVariable('NGSI_ADDRESS') + "/ngsi-ld/v1/entities/" + ngsi_msg[
#             'id'] + "/attrs"
#         r = requests.patch(url, json=ngsi_msg, headers=headers)
#         print("hier", r)
#         logger.debug("Entity patched: " + str(r.status_code))
#     except requests.exceptions.ConnectionError as e:
#         logger.error("Error while patching ngsi entity" + str(e))


def get_entity_updateList(entityid, entitylist):
    t = threading.Thread(target=_get_entity_updateList, args=(entityid, entitylist))
    t.start()


def _get_entity_updateList(entityid, entitylist):
    entity = get_entity(entityid)
    if entity:
        entitylist[entityid] = entity


def get_entity(entitiyid):
    try:
        url = Config.getEnvironmentVariable('NGSI_ADDRESS') + "/ngsi-ld/v1/entities/" + entitiyid
        r = requests.get(url, headers=headers, timeout=float(Config.get('semanticenrichment', 'timeout')))
        if r.status_code != 200:
            logger.error("Error requesting entity " + entitiyid + ": " + r.text)
            return None
        return r.json()
    except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as e:
        logger.error("Exception while getting entity: " + str(e))


def get_entities(entitytype, limit, offset):
    try:
        url = Config.getEnvironmentVariable('NGSI_ADDRESS') + "/ngsi-ld/v1/entities/"
        params = {'type': entitytype, 'limit': limit, 'offset': offset}
        r = requests.get(url, headers=headers, params=params, timeout=float(Config.get('semanticenrichment', 'timeout')))
        if r.status_code != 200:
            logger.error("Error requesting entities of type " + entitytype + ": " + r.text)
            return None
        return r.json()
    except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as e:
        logger.error("Exception while getting entities: " + str(e))


def get_all_entities(entitytype):
    if type(entitytype) is ngsi_parser.NGSI_Type:
        entitytype = ngsi_parser.get_url(entitytype)
    limit = 50
    offset = 0
    result = []
    while True:
        tmpresult = get_entities(entitytype, limit, offset)
        if not tmpresult:
            break
        result.extend(tmpresult)
        if len(tmpresult) < limit:
            break
        offset += 50
    return result


# def _find_streamobservation(streamid):
#     try:
#         url = Config.get('NGSI', 'host') + ":" + str(Config.get('NGSI', 'port')) + "/ngsi-ld/v1/entities/"
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
    logger.debug("Subscribe for " + str(ngsi_type) + " " + str(entityId))
    # check if subscription already in sublist
    # solution is not optimal... but no other option at the moment
    if entityId:
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
        subscription['notification']['endpoint']['uri'] = Config.getEnvironmentVariable('SE_CALLBACK')
        # set entity to subscribe to
        if entityId:
            subscription['entities'][0]['id'] = entityId
        _add_subscription(subscription, sublist)


def handleNewSensor(sensorId, sensors, observableproperties, subscriptions):
    # GET for sensor
    sensor = get_entity(sensorId)
    if sensor:
        sensors[sensorId] = sensor

        # GET for obsproperty(sensor)
        observablepropertyId = ngsi_parser.get_sensor_observes(sensor)
        if observablepropertyId:
            observableproperty = get_entity(observablepropertyId)
            if observableproperty:
                observableproperties[observablepropertyId] = observableproperty

        # subscriptions disabled as we subscribe for all sensors and observations
        # SUB for streamobservation(sensor)
        # streamobservationId = ngsi_parser.get_sensor_madeObservation(sensor)
        # _subscribe_forTypeId(ngsi_parser.NGSI_Type.StreamObservation, streamobservationId, subscriptions)
        # SUB for sensor
        # _subscribe_forTypeId(ngsi_parser.NGSI_Type.Sensor, sensorId, subscriptions)


# for testing purposes
if __name__ == "__main__":
    # print(get_all_entities('http://purl.org/iot/ontology/iot-stream#IotStream'))
    #
    # id = "urn:ngsi-ld:Sensor:B4:E6:2D:8A:20:DD:Temperature"
    # ngsi = {"http://www.fault-detection.de/hasImputedResult": {"type": "Property", "value": 24, "observedAt": "2020-10-20T06:42:42Z"}}
    #
    # ngsi = {
    #     "fd:hasImputedResult": {
    #         "type": "Property",
    #         "value": 24,
    #         "observedAt": "2020-10-20T06:42:42Z"
    #     },
    #     "@context": [
    #         "http://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld", {
    #             "fd": "http://www.fault-detection.de/"
    #         }
    #     ]
    # }
    #
    # add_ngsi_attribute(ngsi, id)
    import os

    os.environ["NGSI_ADDRESS"] = "http://155.54.95.248:9090"

    test_qoi = {
        "id": "urn:ngsi-ld:QoI:test5",
        "type": "qoi:Quality",
        "test": {
            "type": "Property",
            "value": "bla",
        },
        "@context": [
            "http://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld", {
                "qoi": "https://w3id.org/iot/qoi#",
            }
        ]
    }

    # print(type(test_qoi))
    # create_ngsi_entity(test_qoi)

    test_qoi_complete = {
        "id": "urn:ngsi-ld:QoI:test6",
        "type": "qoi:Quality",
        "test": {
            "type": "Property",
            "value": "bla4",
        },
        "@context": [
            "http://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld",
            {
                "qoi": "https://w3id.org/iot/qoi#"
            }
        ],
        "qoi:plausibility": {
            "type": "Property", "value": "NA",
            "qoi:hasAbsoluteValue": {
                "type": "Property", "value": 1
            },
            "qoi:hasRatedValue": {
                "type": "Property", "value": 1.0
            }
        },
        "qoi:completeness": {
            "type": "Property",
            "value": "NA",
            "qoi:hasAbsoluteValue": {
                "type": "Property", "value": 0
            },
            "qoi:hasRatedValue": {
                "type": "Property", "value": 1.0
            }
        },
        "qoi:age": {
            "type": "Property",
            "value": "NA",
            "qoi:hasAbsoluteValue": {
                "type": "Property",
                "value": 122.907668
            }
        },
        "qoi:frequency": {
            "type": "Property",
            "value": "NA",
            "qoi:hasAbsoluteValue": {
                "type": "Property",
                "value": 118.558193
            },
            "qoi:hasRatedValue": {
                "type": "Property",
                "value": 0.7
            }
        }
    }

    # patch_ngsi_entity(test_qoi_complete)
    # print(type(test_qoi_complete))
    # add_ngsi_attribute(test_qoi_complete, "urn:ngsi-ld:QoI:test6")
    # create_ngsi_entity(test_qoi_complete)

    _get_active_subscriptions([])
