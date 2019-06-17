import json
import cherrypy
import threading
from qoi_system import QoiSystem
from datasource_manager import DatasourceManager

class SemanticEnrichment:

    def __init__(self):
        self.qoisystem_map = {}
        self.datasource_manager = DatasourceManager()
        self.datasource_map = ""

    def notify_datasource(self, metadata):
        # TODO call data source manager to subscribe etc.

        # TODO initialise a qoi system per value of a stream?
        # store metadata in qoi_system
        self.qoisystem_map[metadata['id']] = QoiSystem(metadata)

    def receive(self, data):
        self.qoisystem_map[data['id']].update(data)

    def get_qoivector(self, sourceid):
        return self.qoisystem_map[sourceid].get_qoivector()

    @cherrypy.tools.allow(methods=['GET'])
    @cherrypy.expose
    def showsubscriptions(self):
        print("showsubscriptions called")
        print(self.datasource_manager.get_subscriptions())


# sample data
data = {
    "id": 1,
    "timestamp": 121232345,
    "values": {
        "value1": 1,
        "value2": -100
    }
}

metadata = {
    "id": 1,
    "sensortype": "weather",
    "measuretime": "timestamp",
    "updateinterval": {
        "frequency": "100",
        "unit": "seconds"
    },
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
    cherrypy.server.socket_host = '0.0.0.0'
    threading.Thread(target=cherrypy.quickstart, args=(SemanticEnrichment(),)).start()
    semantic_enrichment = SemanticEnrichment()
    semantic_enrichment.notify_datasource(metadata)
    semantic_enrichment.receive(data)
    semantic_enrichment.receive(data)
    print(semantic_enrichment.get_qoivector(data['id']))
    qoi_data = json.dumps(semantic_enrichment.get_qoivector(data['id']), indent=2)
    print(qoi_data)

# TODO
# which metric for whole stream, which for every single value?
# completeness for whole data
# plausibility for single value
# timeliness whole data
# concordance
# artificiality

# store for common sensor types to calc plausibility? e.g. store common min max values for temp sensors

# in which format will we receive data?
# do we need a clock for replay experiments? maybe not as we will only get live data or complete datasets
# we should have qoi for a single measurement as well as for data sources?
