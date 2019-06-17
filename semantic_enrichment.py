from qoi_system import qoi_system

class semantic_enrichment:

    def __init__(self):
        self.qoisystem_map = {}
        self.datasource_manager = ""
        self.datasource_map = ""

    def notify_datasource(self, metadata):
        #TODO call data source manager to subscribe etc.

        #store metadata in qoi_system
        self.qoisystem_map[metadata['id']] = qoi_system(metadata)


    def receive(self, data):
        self.qoisystem_map[data['id']].update(data)


    def get_qoivector(self, id):
        return self.qoisystem_map[id].get_qoivector()


#sample data
data = {
    "id":1,
    "timestamp": 121232345,
    "values":{
        "value1": 1,
        "value2": -100
    }
}

metadata = {
    "id": 1,
    "sensortype": "weather",
    "measuretime": "timestamp",
    "frequency": "100s",
    "fields": {
        "value1": {
            "sensortype": "temperature",
            "valuetype": "int",
            "min": -20,
            "max": 40
        },
        "value2": {
            "sensortype": "humidity",
            "valuetype": "int",
            "min": 0,
            "max": 100
        }
    }
}


if __name__ == "__main__":
    semantic_enrichment = semantic_enrichment()
    semantic_enrichment.notify_datasource(metadata)
    semantic_enrichment.receive(data)
    print semantic_enrichment.get_qoivector(data['id'])



#TODO
#which metric for whole stream, which for every single value?
#completeness for whole data
#plausibility for single value
#timeliness whole data
#concordance
#artificiality

#store for common sensor types to calc plausibility? e.g. store common min max values for temp sensors

#in which format will we receive data?
#do we need a clock for replay experiments? maybe not as we will only get live data or complete datasets
#we should have qoi for a single measurement as well as for data sources?



