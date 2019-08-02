import pymongo
import statistics
import logging
from fuzzywuzzy import fuzz

logger = logging.getLogger('semanticenrichment')

metadata_example = [
        {
            'type': 'mac',
            'fields': {
                'min':'NA',
                'max':'NA',
                'valuetype':'NA'
            }
        },
        {
            'type': 'temperature1',
            'fields': {
                'min':-20,
                'max':50,
                'valuetype':'float'
           }
        },
        {
            'type': 'temp',
            'fields': {
                'min':-20,
                'max':50,
                'valuetype':'float'
            }
        },
        {
            'type': 'humidity',
            'fields': {
                'min':0,
                'max':100,
                'valuetype':'NA'
            }
        },
        {
            'type': 'iaq',
            'fields': {
                'min':0,
                'max':500,
                'valuetype':'NA'
            }
        }
    ]




class MetadataMatcher(object):

    def __init__(self):
        self.client = pymongo.MongoClient('localhost', 27017)
        self.db = self.client['se_db']
        self.metadata = self.db.metadata
        self.metadata.create_index([('type', pymongo.TEXT)])
        # TODO initialise with example data
        self.store(metadata_example)

    # expects metadata in the format used internally in semantic enrichment
    # splits to fields and saves them to mongodb
    def store(self, metadata):
        if isinstance(metadata, list):
            for meta in metadata:
                result = self.metadata.update({"type" : meta['type']}, meta, upsert=True)
        else:
            result = self.metadata.update({"type" : metadata['type']}, metadata, upsert=True)

    def get_all(self):
        return list(self.metadata.find({}, {"_id": False}))

    def delete(self, type):
        self.metadata.delete_many( { "type": type })

    def match(self, type):
        # first get types from db as matching is done locally
        types = [type for type in self.metadata.distinct('type')]

        # do matching, see description here: https://www.datacamp.com/community/tutorials/fuzzy-string-python
        highestScore = 0
        highestType = None
        result = {}
        for dbtype in types:
            ratio = fuzz.ratio(dbtype.lower(),type.lower())
            partial_ratio = fuzz.partial_ratio(dbtype.lower(),type.lower())
            token_sort_ratio = fuzz.token_sort_ratio(dbtype,type)
            token_set_ratio = fuzz.token_set_ratio(dbtype,type)
            sumRatio = statistics.median([ratio, partial_ratio, token_sort_ratio, token_set_ratio])
            result[dbtype] = sumRatio

            if sumRatio > highestScore:
                highestScore = sumRatio
                highestType = dbtype

        check = highestScore * 0.7
        for key in result:
            if key != highestType:
                if result[key] > check:
                    print("Too similar", key)
                    return None
        return self.metadata.find_one( {"type" : highestType}, {"_id": False} )


    @staticmethod
    def check_metadata(metadata):
        logger.debug("checking metadata " + str(metadata))
        metadata_matcher = MetadataMatcher()
        for field in metadata['fields']:
            compatible_metadata = metadata_matcher.match(field)
            if compatible_metadata is not None:
                logger.debug("Found compatible metadata " + str(compatible_metadata))
                for f in metadata['fields'][field]:
                    if metadata['fields'][field][f] == 'NA':
                        metadata['fields'][field][f] = compatible_metadata['fields'][f]

if __name__ == "__main__":
    matcher = MetadataMatcher()
    # matcher.store(metadata_example)
    # matcher.store(metadata_example)
    # print(matcher.get_all())
    # matcher.delete("iaq")
    # print(matcher.get_all())
    # print(matcher.match("temp"))
    # print(matcher.match("environment"))
    # print(matcher.match("urn:ngsi-ld:TemperatureSensor:30:AE:A4:6E:FC:D0"))
    print("TemperatureSensor:", matcher.match("TemperatureSensor"))
    print("value:", matcher.match("value"))

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