import json
import cherrypy
import threading
import uuid
import datetime
from qoi_system import QoiSystem
from datasource_manager import DatasourceManager


class SemanticEnrichment:

    def __init__(self):
        self.qoisystem_map = {}
        self.datasource_manager = DatasourceManager()
        self.datasource_map = ""
        self.callback_url = "http://454894f1.ngrok.io/callback"

    def notify_datasource(self, metadata):
        # TODO call data source manager to subscribe etc.
        self.datasource_manager.add_datasource(metadata)

        # TODO initialise a qoi system per value of a stream? per stream, metrics are split to stream or value
        # store metadata in qoi_system
        self.qoisystem_map[metadata['id']] = QoiSystem(metadata)

    def receive(self, data):
        self.qoisystem_map[data['id']].update(data)

        # Todo save qoi data to MDR
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
            yield '<tr><td><form action=\"/deletesubscription\" method=\"POST\"><button type="submit" name=\"subid\" value=\"' + str(
                sub.id) + '\">Delete</button></form></td><td>' + str(
                sub.id) + '</td><td>' + sub.host + '</td><td>' + json.dumps(sub.subscription) + '</td></tr>'
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
        yield '<!DOCTYPE html> <html lang="en"> <body><table>'
        for ds in self.datasource_manager.get_datasources().values():
            print(ds.metadata)
            yield '<tr><td></td><td>' + str(ds.id) + '</td><td>' + ds.dstype + '</td><td>' + str(
                ds.metadata) + '</td><td>' + str(self.get_qoivector(ds.id)) + '</td></tr>'
        yield '</table></body></html>'
        return

    @cherrypy.tools.allow(methods=['POST'])
    @cherrypy.expose
    @cherrypy.tools.json_in()
    def callback(self):
        print("callback called")
        print(cherrypy.request.body.read())
        # TODO parse and add to datasource manager
        print(type(cherrypy.request.json))
        # jsondata = json.loads(cherrypy.request.json)

        #split to data and metadata
        data, metadata = self.parse_ngsi(cherrypy.request.json)
        #create data source in data source manager
        self.notify_datasource(metadata)
        self.receive(data)

    def parse_ngsi(self, ngsi_data):
        #parse ngsi-ld data to data and metadata
        data = {}
        data['id'] = ngsi_data['id']
        #get one observedAt as timestamp
        data['timestamp'] = self.get_ngsi_observedAt(ngsi_data)
        data['values'] = self.get_ngsi_values(ngsi_data)

        metadata = {}
        metadata['id'] = ngsi_data['id']
        metadata['type'] = ngsi_data['type']
        metadata['fields'] = self.get_ngsi_fields(ngsi_data)

        return data, metadata


    def get_ngsi_observedAt(self, json_object):
        for obj in json_object:
            obj = json_object[obj]
            if 'type' in obj:
                if obj['type'] == 'Property':
                    if 'observedAt' in obj:
                        str_date = obj['observedAt']
                        return datetime.datetime.strptime(str_date, "%Y-%m-%dT%H:%M:%S").timestamp()

    def get_ngsi_values(self, json_object):
        values = {}
        for key in json_object:
            obj = json_object[key]
            if 'type' in obj:
                if obj['type'] == 'Property':
                    if 'value' in obj:
                        values[key] = obj['value']
        return values

    def get_ngsi_fields(self, json_object):
        fields = {}
        for key in json_object:
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

# # sample data
# data = {
#     "id": 1,
#     "timestamp": 121232345,
#     "values": {
#         "value1": 1,
#         "value2": -100
#     }
# }
#
# metadata = {
#     "id": 1,
#     "type": "weather",
#     "updateinterval": {
#         "frequency": "100",
#         "unit": "seconds"
#     },
#     "fields": {
#         "value1": {
#             "sensortype": "temperature",
#             "valuetype": "int",
#             "min": -20,
#             "max": 40
#         },
#         "value2": {
#             "sensortype": "humidity",
#             "valuetype": "int",
#             "min": 0,
#             "max": 100
#         }
#     }
# }

if __name__ == "__main__":

    # test = {'id': 'urn:ngsi-ld:Environment:B4:E6:2D:8C:30:95', 'type': 'Environment', 'mac': {'type': 'Property', 'value': 'B4:E6:2D:8C:30:95'}, 'temperature': {'type': 'Property', 'value': 27.751, 'observedAt': '2019-07-01T09:45:46', 'min': {'type': 'Property', 'value': -20}, 'max': {'type': 'Property', 'value': 50}, 'providedBy': {'type': 'Relationship', 'object': 'urn:ngsi-ld:Environment:B4:E6:2D:8C:30:95:BME680'}}, 'humidity': {'type': 'Property', 'value': 39.1929, 'observedAt': '2019-07-01T09:45:46', 'unitCode': '', 'min': {'type': 'Property', 'value': 0}, 'max': {'type': 'Property', 'value': 100}, 'providedBy': {'type': 'Relationship', 'object': 'urn:ngsi-ld:Environment:B4:E6:2D:8C:30:95:BME680'}}, 'iaq': {'type': 'Property', 'value': 25, 'observedAt': '2019-07-01T09:45:46', 'min': {'type': 'Property', 'value': 0}, 'max': {'type': 'Property', 'value': 500}, 'providedBy': {'type': 'Relationship', 'object': 'urn:ngsi-ld:Environment:B4:E6:2D:8C:30:95:BME680'}}, 'location': {'type': 'GeoProperty', 'value': {'type': 'Point', 'coordinates': [8.023865, 52.282645]}}, '@context': ['http://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld', {'Environment': 'http://example.org/environment/environment', 'iaq': 'http://example.org/environment/bme680/iaq', 'temperature': 'http://example.org/environment/bme680/temperature', 'humidity': 'http://example.org/environment/bme680/humidity', 'mac': 'http://example.org/environment/bme680/mac'}]}
    # semantic_enrichment = SemanticEnrichment()
    # data, metadata = semantic_enrichment.parse_ngsi(test)
    # semantic_enrichment.notify_datasource(metadata)
    # semantic_enrichment.receive(data)
    # semantic_enrichment.notify_datasource(metadata)
    # print(semantic_enrichment.get_qoivector('urn:ngsi-ld:Environment:B4:E6:2D:8C:30:95'))

    cherrypy.server.socket_host = '0.0.0.0'
    cherrypy.server.socket_port = 8081
    threading.Thread(target=cherrypy.quickstart, args=(SemanticEnrichment(),)).start()




    # semantic_enrichment = SemanticEnrichment()
    # semantic_enrichment.notify_datasource(metadata)
    # semantic_enrichment.receive(data)
    # semantic_enrichment.receive(data)
    # print(semantic_enrichment.get_qoivector(data['id']))
    # qoi_data = json.dumps(semantic_enrichment.get_qoivector(data['id']), indent=2)
    # print(qoi_data)

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
