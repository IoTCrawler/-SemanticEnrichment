import pymongo
from pymongo import errors
import statistics
import logging
import threading
from fuzzywuzzy import fuzz
from configuration import Config

logger = logging.getLogger('semanticenrichment')

metadata_example = [
    {
        'type': 'mac',
        'fields': {
            'min': 'NA',
            'max': 'NA',
            'valuetype': 'NA'
        }
    },
    {
        'type': 'temperature1',
        'fields': {
            'min': -20,
            'max': 50,
            'valuetype': 'float'
        }
    },
    {
        'type': 'temp',
        'fields': {
            'min': -20,
            'max': 50,
            'valuetype': 'float'
        }
    },
    {
        'type': 'humidity',
        'fields': {
            'min': 0,
            'max': 100,
            'valuetype': 'NA'
        }
    },
    {
        'type': 'iaq',
        'fields': {
            'min': 0,
            'max': 500,
            'valuetype': 'NA'
        }
    }
]


class MetadataMatcher(object):

    def __init__(self):
        self.connected_to_db = False
        self.connect_in_background()
        self.initialise()

    def connect_in_background(self):
        t = threading.Thread(target=self.connect)
        t.start()

    def connect(self):
        try:
            self.client = pymongo.MongoClient(Config.get('mongodb', 'host'), int(Config.get('mongodb', 'port')))
            self.db = self.client['se_db']
            self.metadata = self.db.metadata
            self.metadata.create_index([('type', pymongo.TEXT)])
            self.connected_to_db = True
        except errors.ServerSelectionTimeoutError as e:
            self.connected_to_db = False
            logger.debug("MetadataMatcher: error while connecting to db " + str(e))

    def connected(self):
        if not self.connected_to_db:
            self.connect_in_background()
        return self.connected_to_db

    def initialise(self):
        if self.connected():
            # TODO initialise with example data
            if bool(Config.get('mongodb', 'initialise')):
                print("hier")
                self.store(metadata_example)
        else:
            timer = threading.Timer(5, self.initialise())

    # expects metadata in the format used internally in semantic enrichment
    # splits to fields and saves them to mongodb
    def store(self, metadata):
        if self.connected():
            # logger.error("save " + metadata)
            if isinstance(metadata, list):
                for meta in metadata:
                    self.metadata.update({"type": meta['type']}, meta, upsert=True)
            else:
                self.metadata.update({"type": metadata['type']}, metadata, upsert=True)

    def get_all(self):
        if self.connected():
            return list(self.metadata.find({}, {"_id": False}))
        return None

    def delete(self, type):
        if self.connected():
            self.metadata.delete_many({"type": type})

    def match(self, type):
        if self.connected():
            # first get types from db as matching is done locally
            types = [type for type in self.metadata.distinct('type')]

            # do matching, see description here: https://www.datacamp.com/community/tutorials/fuzzy-string-python
            highestScore = 0
            highestType = None
            result = {}
            for dbtype in types:
                ratio = fuzz.ratio(dbtype.lower(), type.lower())
                partial_ratio = fuzz.partial_ratio(dbtype.lower(), type.lower())
                token_sort_ratio = fuzz.token_sort_ratio(dbtype, type)
                token_set_ratio = fuzz.token_set_ratio(dbtype, type)
                sumRatio = statistics.median([ratio, partial_ratio, token_sort_ratio, token_set_ratio])
                result[dbtype] = sumRatio

                if sumRatio > highestScore:
                    highestScore = sumRatio
                    highestType = dbtype

            check = highestScore * 0.7
            for key in result:
                if key != highestType:
                    if result[key] > check:
                        # print("Too similar", key)
                        return None
            return self.metadata.find_one({"type": highestType}, {"_id": False})
        return None

    def check_metadata(self, metadata):
        if self.connected():
            logger.debug("checking metadata " + str(metadata))
            # metadata_matcher = MetadataMatcher()
            for field in metadata['fields']:
                compatible_metadata = self.match(field)
                if compatible_metadata is not None:
                    logger.debug("Found compatible metadata " + str(compatible_metadata))
                    for f in metadata['fields'][field]:
                        if metadata['fields'][field][f] == 'NA':
                            metadata['fields'][field][f] = compatible_metadata['fields'][f]


if __name__ == "__main__":
    matcher = MetadataMatcher()
    # matcher.store(metadata_example)
    # matcher.store(metadata_example)
    print(matcher.get_all())
    # matcher.delete("iaq")
    # print(matcher.get_all())
    # print(matcher.match("temp"))
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
    # tmp['fields']['min'] = -200
    # matcher.store(tmp)
    # tmp = matcher.match("TemperatureSensor")
    # print(tmp)
    # mac = matcher.get_all()[0]
    # print(mac)
    # mac['fields']['min'] = -200
    # print(mac)
    # matcher.store(mac)
    # mac = matcher.get_all()[0]
    # print(mac)

    # m = {'id': 'urn:ngsi-ld:TemperatureSensor:30:AE:A4:6E:FC:D0', 'type': 'TemperatureSensor', 'fields': {'Description': {'type': 'Property', 'min': 'NA', 'max': 'NA', 'valuetype': 'NA'}, 'temperature': {'type': 'Property', 'min': 'NA', 'max': 'NA', 'valuetype': 'NA'}}}
    #
    # matcher.check_metadata(m)
    # print(m)
