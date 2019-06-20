import json
import cherrypy
import threading
import uuid
from qoi_system import QoiSystem
from datasource_manager import DatasourceManager

class SemanticEnrichment:

    def __init__(self):
        self.qoisystem_map = {}
        self.datasource_manager = DatasourceManager()
        self.datasource_map = ""
        self.callback_url = "http://e71f4d93.ngrok.io/callback"
        #TODO get active subscriptions from broker?


    def notify_datasource(self, metadata):
        # TODO call data source manager to subscribe etc.

        # TODO initialise a qoi system per value of a stream? per stream, metrics are split to stream or value
        # store metadata in qoi_system
        self.qoisystem_map[metadata['id']] = QoiSystem(metadata)

    def receive(self, data):
        self.qoisystem_map[data['id']].update(data)

        #Todo save qoi data to MDR
        qoidata = self.qoisystem_map[data['id']].get_qoivector()

    def get_qoivector(self, sourceid):
        return self.qoisystem_map[sourceid].get_qoivector()

    @cherrypy.tools.allow(methods=['GET', 'POST'])
    @cherrypy.expose
    def showsubscriptions(self, host=None, port=None, subscription=None):

        if None not in (host, port, subscription):
            print("new subscription")
            self.datasource_manager.add_subscription(host, port, json.loads(subscription))

        subscriptions = self.datasource_manager.get_subscriptions()
        yield '<!DOCTYPE html> <html lang="en"> <body>'
        with open('html/subscription_form.html') as formFile:
            with open('jsonfiles/UMU_Subscription_TemperatureSensor.json') as jFile:
                data = json.load(jFile)
                data['id'] = data['id'] + str(uuid.uuid4())
                data['notification']['endpoint']['uri'] = self.callback_url
                html = formFile.read()
                html = html.replace("subplaceholder", json.dumps(data, indent=2))
                yield html
        yield '<table>'
        for sub in subscriptions.values():
            yield '<tr><td><form action=\"/deletesubscription\" method=\"POST\"><button type="submit" name=\"subid\" value=\"' + str(sub.id) + '\">Delete</button></form></td><td>' + str(sub.id) + '</td><td>' + sub.host + '</td><td>' + json.dumps(sub.subscription) + '</td></tr>'
        yield '</table></body></html>'
        return

    @cherrypy.tools.allow(methods=['POST'])
    @cherrypy.expose
    def deletesubscription(self, subid):
        print("delete called", subid)
        self.datasource_manager.del_subscription(subid)

        raise cherrypy.HTTPRedirect("/showsubscriptions")

    @cherrypy.tools.allow(methods=['GET'])
    @cherrypy.expose
    def showdatasources(self):
        subscriptions = self.datasource_manager.get_subscriptions()
        yield '<!DOCTYPE html> <html lang="en"> <body><table>'
        for ds in self.datasource_manager.get_datasources().values():
            print(ds.metadata)
            yield '<tr><td></td><td>' + str(ds.id) + '</td><td>' + ds.dstype + '</td><td>' + str(ds.metadata) + '</td></tr>'
        yield '</table></body></html>'
        return

    @cherrypy.tools.allow(methods=['POST'])
    @cherrypy.expose
    @cherrypy.tools.json_in()
    def callback(self):
        print("callback called")
        print(cherrypy.request.body.read())
        #TODO parse and add to datasource manager
        print(cherrypy.request.json)
        jsonData = cherrypy.request.json
        for data in jsonData:
            self.datasource_manager.add_datasource(data)


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
    cherrypy.server.socket_port = 8081
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
