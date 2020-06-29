import json
import statistics
import logging
from fuzzywuzzy import fuzz

logger = logging.getLogger('semanticenrichment')

metadata_example = [
    {
        'type': 'mac',
        'metadata': {
            'regexp': '^([0-9A-F]{2}[:-]){5}([0-9A-F]{2})$',
            'valuetype': 'string'
        }
    },
    {
        'type': 'color',
        'metadata': {
            'enum': ['red', 'blue'],
            'valuetype': 'enum'
        }
    },
    {
        'type': 'temperature',
        'metadata': {
            'min': -20,
            'max': 50,
            'valuetype': 'float'
        }
    },
    {
        'type': 'humidity',
        'metadata': {
            'min': 0,
            'max': 100,
            'valuetype': 'float'
        }
    },
    {
        'type': 'iaq',
        'metadata': {
            'min': 0,
            'max': 500,
            'valuetype': 'int'
        }
    }
]


class MetadataMatcher(object):

    def __init__(self):
        self.metadata = {}
        self.initialise()

    # read file, update with example data if not exist, store file
    def initialise(self):
        try:
            with open('metadata.json', mode='r+') as mFile:
                try:
                    fileMetadata = json.load(mFile)
                    for entry in fileMetadata:
                        self.metadata[entry['type']] = entry['metadata']
                except:
                    logger.debug("No metadata in file found")
        except FileNotFoundError:
            logger.debug("No metadata.json found")
        for entry in metadata_example:
            if entry['type'] not in self.metadata:
                self.metadata[entry['type']] = entry['metadata']

        self.store_file()

    def store_file(self):
        with open('metadata.json', mode='w+') as mFile:
            tmp = []
            for key, item in self.metadata.items():
                tmp.append({'type': key, 'metadata': item})
            json.dump(tmp, mFile)

    # expects metadata in the format used internally in semantic enrichment
    # splits to metadata and saves them to mongodb
    def store(self, metadata):
        if isinstance(metadata, list):
            for meta in metadata:
                self.metadata[meta['type']] = meta['metadata']
        else:
            self.metadata[metadata['type']] = metadata['metadata']
        self.store_file()

    def get_all(self):
        tmp = []
        for key, item in self.metadata.items():
            tmp.append({'type': key, 'metadata': item})
        return tmp

    def delete(self, streamtype):
        self.metadata.pop(streamtype)

    def match(self, streamtype):
        # first get types from db as matching is done locally
        streamtypes = self.metadata.keys()

        # do matching, see description here: https://www.datacamp.com/community/tutorials/fuzzy-string-python
        highestScore = 0
        highestType = None
        result = {}
        for dbtype in streamtypes:
            ratio = fuzz.ratio(dbtype.lower(), streamtype.lower())
            partial_ratio = fuzz.partial_ratio(dbtype.lower(), streamtype.lower())
            token_sort_ratio = fuzz.token_sort_ratio(dbtype, streamtype)
            token_set_ratio = fuzz.token_set_ratio(dbtype, streamtype)
            sumRatio = statistics.median([ratio, partial_ratio, token_sort_ratio, token_set_ratio])
            result[dbtype] = sumRatio

            if sumRatio > highestScore:
                highestScore = sumRatio
                highestType = dbtype
        # TODO include some limit?
        # check = highestScore * 0.7
        # print(result)
        # for key in result:
        #     if key != highestType:
        #         if result[key] > check:
        #             print("Too similar", key)
        #             return None
        return self.metadata[highestType]


if __name__ == "__main__":
    # def hello():
    #     print("hello, world")
    # timer = threading.Timer(5.0, self.initialise(True))
    # timer.start()

    matcher = MetadataMatcher()
    print("TemperatureSensor:", matcher.match("TemperatureSensor"))
    print(matcher.get_all())
    print(
        "[{'type': 'temperature', 'metadata': {'min': -20, 'max': 50, 'valuetype': 'float'}}, {'type': 'mac', 'metadata': {'regexp': '^([0-9A-F]{2}[:-]){5}([0-9A-F]{2})$', 'valuetype': 'string'}}, {'type': 'temperature1', 'fields': {'min': -20, 'max': 50, 'valuetype': 'float'}}, {'type': 'temp', 'fields': {'min': -20, 'max': 50, 'valuetype': 'float'}}, {'type': 'humidity', 'metadata': {'min': 0, 'max': 100, 'valuetype': 'float'}}, {'type': 'iaq', 'metadata': {'min': 0, 'max': 500, 'valuetype': 'int'}}, {'type': 'color', 'metadata': {'enum': ['red', 'blue'], 'valuetype': 'enum'}}]")

# while(not matcher.connected_to_db):
#     pass
# matcher.store(metadata_example)
# matcher.store(metadata_example)
# import time
# time.sleep(3)
# print('got:', matcher.get_all())
# matcher.delete("iaq")
# print(matcher.get_all())
# print("TemperatureSensor:", matcher.match("TemperatureSensor"))
# print(matcher.match("environment"))
# print(matcher.match("urn:ngsi-ld:TemperatureSensor:30:AE:A4:6E:FC:D0"))
# print("TemperatureSensor:", matcher.match("TemperatureSensor"))
# print("value:", matcher.match("value"))
# import time
# time.sleep(1)
# print("TemperatureSensor:", matcher.match("TemperatureSensor"))
# print("value:", matcher.match("value"))

# tmp = matcher.match("TemperatureSensor")
# print(tmp)
# tmp['metadata']['min'] = -200
# matcher.store(tmp)
# tmp = matcher.match("TemperatureSensor")
# print(tmp)
# mac = matcher.get_all()[0]
# print(mac)
# mac['metadata']['min'] = -200
# print(mac)
# matcher.store(mac)
# mac = matcher.get_all()[0]
# print(mac)

# m = {'id': 'urn:ngsi-ld:TemperatureSensor:30:AE:A4:6E:FC:D0', 'type': 'TemperatureSensor', 'metadata': {'Description': {'type': 'Property', 'min': 'NA', 'max': 'NA', 'valuetype': 'NA'}, 'temperature': {'type': 'Property', 'min': 'NA', 'max': 'NA', 'valuetype': 'NA'}}}
#
# matcher.check_metadata(m)
# print(m)
#
#
# import re
# p = re.compile('^([0-9A-F]{2}[:-]){5}([0-9A-F]{2})$')
# test_mac = '30:AE:A4:6E:FC:D0'
# test_mac2 = '30:AE:A4:6E:FC:D0:'
# print(re.fullmatch(p, test_mac))
# print(re.fullmatch(p, test_mac2))
# print(re.match(p, test_mac))
