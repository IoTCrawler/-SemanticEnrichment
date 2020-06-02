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
        self.connected_to_db = False
        self.background_thread = None
        self.retries = 5
        self.initialise()

    def create_background_thread(self):
        if self.background_thread is not None:
            if not self.background_thread.is_alive():
                self.start_background_thread()
        else:
            self.start_background_thread()

    def start_background_thread(self):
        self.background_thread = threading.Thread(target=self.connect)
        self.background_thread.start()

    def connect(self):
        try:
            self.client = pymongo.MongoClient(Config.get('mongodb', 'host'), int(Config.get('mongodb', 'port')),
                                              serverSelectionTimeoutMS=0.5)
            # self.client = pymongo.MongoClient('localhost', 27017,
            #                                   serverSelectionTimeoutMS=0.5)
            self.db = self.client['se_db']
            self.metadata = self.db.metadata
            self.metadata.create_index([('type', pymongo.TEXT)])
            self.connected_to_db = True
        except errors.ServerSelectionTimeoutError as e:
            self.connected_to_db = False
            logger.debug("MetadataMatcher: error while connecting to db " + str(e))
            self.create_background_thread()


    def connected(self):
        if not self.connected_to_db:
            self.create_background_thread()
        return self.connected_to_db

    def initialise(self):
        if self.connected():
            # TODO initialise with example data
            # if bool(Config.get('mongodb', 'initialise')):
            self.store(metadata_example)
        elif self.retries > 0:
            # database not connected yet, try again in 5s
            self.retries -= 1
            timer = threading.Timer(5.0, self.initialise)
            timer.start()

    # expects metadata in the format used internally in semantic enrichment
    # splits to metadata and saves them to mongodb
    def store(self, metadata):
        try:
            print(f'store {metadata}')
            if self.connected():
                # logger.error("save " + metadata)
                if isinstance(metadata, list):
                    for meta in metadata:
                        self.metadata.update({"type": meta['type']}, meta, upsert=True)
                else:
                    self.metadata.update({"type": metadata['type']}, metadata, upsert=True)
        except errors.ServerSelectionTimeoutError as e:
            self.connected_to_db = False
            logger.debug("MetadataMatcher: error while connecting to db " + str(e))
            self.create_background_thread()
            return None

    def get_all(self):
        try:
            if self.connected():
                return list(self.metadata.find({}, {"_id": False}))
            return None
        except errors.ServerSelectionTimeoutError as e:
            self.connected_to_db = False
            logger.debug("MetadataMatcher: error while connecting to db " + str(e))
            self.create_background_thread()
            return None

    def delete(self, streamtype):
        try:
            if self.connected():
                self.metadata.delete_many({"type": streamtype})
        except errors.ServerSelectionTimeoutError as e:
            self.connected_to_db = False
            logger.debug("MetadataMatcher: error while connecting to db " + str(e))
            self.create_background_thread()
            return None

    def match(self, streamtype):
        try:
            if self.connected():
                # first get types from db as matching is done locally
                streamtypes = [streamtype for streamtype in self.metadata.distinct('type')]

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
                return self.metadata.find_one({"type": highestType}, {"_id": False})
            return None
        except errors.ServerSelectionTimeoutError as e:
            self.connected_to_db = False
            logger.debug("MetadataMatcher: error while connecting to db " + str(e))
            self.create_background_thread()
            return None

    # def check_metadata(self, metadata):
    #     if self.connected():
    #         logger.debug("checking metadata " + str(metadata))
    #         # metadata_matcher = MetadataMatcher()
    #         for field in metadata['metadata']:
    #             compatible_metadata = self.match(field)
    #             if compatible_metadata is not None:
    #                 logger.debug("Found compatible metadata " + str(compatible_metadata))
    #                 for f in metadata['metadata'][field]:
    #                     if metadata['metadata'][field][f] == 'NA':
    #                         metadata['metadata'][field][f] = compatible_metadata['metadata'][f]


if __name__ == "__main__":
    # def hello():
    #     print("hello, world")
    # timer = threading.Timer(5.0, self.initialise(True))
    # timer.start()

    matcher = MetadataMatcher()
    while(not matcher.connected_to_db):
        pass
    # matcher.store(metadata_example)
    # matcher.store(metadata_example)
    # import time
    # time.sleep(3)
    # print('got:', matcher.get_all())
    # matcher.delete("iaq")
    # print(matcher.get_all())
    print("TemperatureSensor:", matcher.match("TemperatureSensor"))
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