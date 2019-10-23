import datetime
import logging
import json

logger = logging.getLogger('semanticenrichment')

def parse_ngsi(ngsi_data):
    print(ngsi_data)
    # parse ngsi-ld data to data and metadata
    data = {}
    data['id'] = ngsi_data['id']
    # get one observedAt as timestamp
    data['timestamp'] = get_ngsi_observedAt(ngsi_data)
    data['values'] = get_ngsi_values(ngsi_data)

    metadata = {}
    metadata['id'] = ngsi_data['id']
    metadata['type'] = ngsi_data['type']
    metadata['fields'] = get_ngsi_fields(ngsi_data)
    try:
        updateinterval = {}
        updateinterval['frequency'] = ngsi_data['updateinterval']['frequency']['value']
        updateinterval['unit'] = ngsi_data['updateinterval']['unit']['value']
        metadata['updateinterval'] = updateinterval
    except KeyError as e:
        logger.debug("no updateinterval found " + str(e))

    try:
        location = {}
        if type(ngsi_data['location']['value']) is str:     #TODO workaround for ngb broker location as string
            jsonString = json.loads(ngsi_data['location']['value'])
            location['type'] = jsonString['type']
            location['coordinates'] = jsonString['coordinates']
        else:
            location['type'] = ngsi_data['location']['value']['type']
            location['coordinates'] = ngsi_data['location']['value']['coordinates']
        metadata['location'] = location
    except KeyError as e:
        logger.debug("no location found " + str(e))

    return data, metadata


def get_ngsi_observedAt(json_object):
    for obj in json_object:
        obj = json_object[obj]
        if 'type' in obj:
            if obj['type'] == 'Property':
                if 'observedAt' in obj:
                    str_date = obj['observedAt']
                    return datetime.datetime.strptime(str_date, "%Y-%m-%dT%H:%M:%S").timestamp()


def get_ngsi_values(json_object):
    values = {}
    for key in json_object:
        obj = json_object[key]
        if 'type' in obj:
            if obj['type'] == 'Property':
                if 'value' in obj:
                    values[key] = obj['value']
    return values


def get_ngsi_fields(json_object):
    fields = {}
    for key in json_object:
        #TODO no "nice" solution but need to filter "updateinterval"
        if key != "updateinterval":
            obj = json_object[key]
            if 'type' in obj:
                if obj['type'] == 'Property':
                    field = {}
                    if 'type' in obj:
                        field['type'] = obj['type']
                    else:
                        field['type'] = 'NA'

                    if 'min' in obj:
                        field['min'] = obj['min']['value']
                    else:
                        field['min'] = 'NA'

                    if 'max' in obj:
                        field['max'] = obj['max']['value']
                    else:
                        field['max'] = 'NA'

                    if 'valuetype' in obj:
                        field['valuetype'] = obj['valuetype']['value']
                    else:
                        field['valuetype'] = 'NA'
                    fields[key] = field
    return fields
