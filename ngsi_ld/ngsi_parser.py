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
    if ngsi_type in ("iot-stream:IotStrea,", "http://purl.org/iot/ontology/iot-stream#IotStream"):
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
        return ngsi_data['id'], get_type(ngsi_data)
    except KeyError:
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


def get_stream_observes(stream):
    try:
        # TODO observes can also be a full url
        return stream['observes']['object']
    except KeyError:
        try:
            return stream['http://www.w3.org/ns/sosa/observes']['object']
        except KeyError:
            return None

# def fixurlkeys(data):
#     tmpobj = {}
#     for k, value in data.items():
#         if isinstance(value, dict):
#             value = fixurlkeys(value)
#         elif isinstance(value, str):
#             value = value.split('/')[-1]
#         tmpobj[k.split('/')[-1]] = value
#     return tmpobj


# def parse_ngsi(ngsi_data):
#     # TODO stupid fix, to be removed later when metadata has its fixed context
#     ngsi_data = fixurlkeys(ngsi_data)
#     # print("new", ngsi_data)
#
#     # parse ngsi-ld data to data and metadata
#     data = {'id': ngsi_data['id'], 'timestamp': get_ngsi_observedAt(ngsi_data), 'values': get_ngsi_values(ngsi_data)}
#     # get one observedAt as timestamp
#
#     metadata = {}
#     metadata['id'] = ngsi_data['id']
#     metadata['type'] = ngsi_data['type']
#     metadata['fields'] = get_ngsi_fields(ngsi_data)
#     try:
#         updateinterval = {}
#         updateinterval['frequency'] = ngsi_data['updateinterval']['value']
#         updateinterval['unit'] = ngsi_data['updateinterval']['unit']['value']
#         metadata['updateinterval'] = updateinterval
#     except KeyError as e:
#         logger.debug("no updateinterval found " + str(e))
#
#     try:
#         location = {}
#         if type(ngsi_data['location']['value']) is str:  # TODO workaround for ngb broker location as string
#             jsonString = json.loads(ngsi_data['location']['value'])
#             location['type'] = jsonString['type']
#             location['coordinates'] = jsonString['coordinates']
#         else:
#             location['type'] = ngsi_data['location']['value']['type']
#             location['coordinates'] = ngsi_data['location']['value']['coordinates']
#         metadata['location'] = location
#     except KeyError as e:
#         logger.debug("no location found " + str(e))
#
#     return data, metadata


# def get_ngsi_observedAt(json_object):
#     for obj in json_object:
#         obj = json_object[obj]
#         if 'type' in obj:
#             if obj['type'] == 'Property':
#                 if 'observedAt' in obj:
#                     str_date = obj['observedAt']
#                     return dateutil.parser.parse(str_date).timestamp()


# def get_ngsi_values(json_object):
#     values = {}
#     for key in json_object:
#         obj = json_object[key]
#         if 'type' in obj:
#             if obj['type'] == 'Property':
#                 if 'value' in obj:
#                     values[key] = obj['value']
#     return values


# def get_ngsi_fields(json_object):
#     fields = {}
#     for key in json_object:
#         # TODO no "nice" solution but need to filter "updateinterval"
#         if 'updateinterval' not in key:
#             obj = json_object[key]
#             if 'type' in obj:
#                 if obj['type'] == 'Property':
#                     field = {}
#                     if 'type' in obj:
#                         field['type'] = obj['type']
#                     else:
#                         field['type'] = 'NA'
#
#                     if 'min' in obj:
#                         field['min'] = obj['min']['value']
#                     else:
#                         field['min'] = 'NA'
#
#                     if 'max' in obj:
#                         field['max'] = obj['max']['value']
#                     else:
#                         field['max'] = 'NA'
#
#                     if 'valuetype' in obj:
#                         field['valuetype'] = obj['valuetype']['value']
#                     else:
#                         field['valuetype'] = 'NA'
#                     fields[key] = field
#     return fields
