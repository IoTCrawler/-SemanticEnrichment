import dateutil.parser
import logging
from enum import Enum

logger = logging.getLogger('semanticenrichment')


class NGSI_Type(Enum):
    StreamObservation = 1
    IoTStream = 2
    Sensor = 3
    Notification = 4
    ObservableProperty = 5


def get_type(ngsi_data):
    ngsi_type = ngsi_data['type']
    if ngsi_type in ("iot-stream:IotStream", "http://purl.org/iot/ontology/iot-stream#IotStream"):
        return NGSI_Type.IoTStream
    elif ngsi_type in ("iot-stream:StreamObservation", "http://purl.org/iot/ontology/iot-stream#StreamObservation"):
        return NGSI_Type.StreamObservation
    elif ngsi_type in ("sosa:Sensor", "http://www.w3.org/ns/sosa/Sensor"):
        return NGSI_Type.Sensor
    elif ngsi_type in ("sosa:ObservableProperty", "http://www.w3.org/ns/sosa/ObservableProperty"):
        return NGSI_Type.ObservableProperty
    elif ngsi_type in "Notification":
        return NGSI_Type.Notification


def get_notification_entities(notification):
    try:
        return notification['data']
    except KeyError:
        return None


def get_observation_stream(observation):
    try:
        if 'belongsTo' in observation:
            return observation['belongsTo']['object']
        elif 'http://purl.org/iot/ontology/iot-stream#belongsTo' in observation:
            return observation['http://purl.org/iot/ontology/iot-stream#belongsTo']['object']
    except KeyError:
        return None


def get_observation_value(observation):
    try:
        return observation['hasSimpleResult']['value']
    except KeyError:
        try:
            return observation['http://www.w3.org/ns/sosa/hasSimpleResult']['value']
        except KeyError:
            return None


def get_observation_timestamp(observation):
    try:
        return dateutil.parser.parse(observation['sosa:hasSimpleResult']['observedAt']).timestamp()
    except (TypeError, KeyError, dateutil.parser.ParserError):
        try:
            return dateutil.parser.parse(
                observation['http://www.w3.org/ns/sosa/hasSimpleResult']['observedAt']).timestamp()
        except (TypeError, KeyError, dateutil.parser.ParserError):
            return None


def get_IDandType(ngsi_data):
    try:
        print("bla", ngsi_data['id'], get_type(ngsi_data))
        return ngsi_data['id'], get_type(ngsi_data)
    except KeyError as e:
        return None, None


def get_stream_min(stream):
    try:
        return stream['qoi:min']['value']
    except KeyError:
        try:
            return stream['https://w3id.org/iot/qoi#min']['value']
        except KeyError:
            return None


def get_stream_max(stream):
    try:
        return stream['qoi:max']['value']
    except KeyError:
        try:
            return stream['https://w3id.org/iot/qoi#max']['value']
        except KeyError:
            return None


def get_stream_valuetype(stream):
    try:
        return stream['qoi:valuetype']['value']
    except KeyError:
        try:
            return stream['https://w3id.org/iot/qoi#valuetype']['value']
        except KeyError:
            return None


def get_stream_updateinterval_and_unit(stream):
    try:
        return stream['qoi:updateinterval']['value'], stream['qoi:updateinterval']['qoi:unit']['value']
    except KeyError:
        try:
            return stream['https://w3id.org/iot/qoi#updateinterval']['value'], \
                   stream['https://w3id.org/iot/qoi#updateinterval']['https://w3id.org/iot/qoi#unit']['value']
        except KeyError:

            return None, None


def get_sensor_observes(sensor):
    try:
        return sensor['sosa:observes']['object']
    except KeyError:
        try:
            return sensor['http://www.w3.org/ns/sosa/observes']['object']
        except KeyError:
            return None

def get_sensor_madeObservation(sensor):
    try:
        return sensor['sosa:madeObservation']['object']
    except KeyError:
        try:
            return sensor['http://www.w3.org/ns/sosa/madeObservation']['object']
        except KeyError:
            return None

def get_stream_generatedBy(stream):
    try:
        return stream['iot-stream:generatedBy']['object']
    except KeyError:
        try:
            return stream['http://purl.org/iot/ontology/iot-stream#generatedBy']['object']
        except KeyError:
            return None


def get_obsproperty_label(obsproperty):
    try:
        return obsproperty['rdfs:label']['object']
    except KeyError:
        try:
            return obsproperty['http://www.w3.org/2000/01/rdf-schema#label']['object']
        except KeyError:
            return None


