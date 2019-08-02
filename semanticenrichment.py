import requests
import logging
from qoi_system import QoiSystem
from datasource_manager import DatasourceManager

logger = logging.getLogger('semanticenrichment')

# TODO shift broker, callback, etc. options into a config file
BROKER_HOST = "155.54.95.248"
BROKER_PORT = 9090


class SemanticEnrichment:

    def __init__(self):
        self.qoisystem_map = {}
        self.datasource_manager = DatasourceManager()
        self.datasource_map = ""
        self.callback_url = "https://mobcom.ecs.hs-osnabrueck.de/semanticenrichment/callback"
        logger.info("Semantic Enrichment started")

    def notify_datasource(self, metadata):
        # TODO call data source manager to subscribe etc.
        self.datasource_manager.add_datasource(metadata)

        # TODO initialise a qoi system per value of a stream? per stream, metrics are split to stream or value
        # store metadata in qoi_system
        self.qoisystem_map[metadata['id']] = QoiSystem(metadata)

    def receive(self, data):
        self.qoisystem_map[data['id']].update(data)

        # save qoi data to MDR
        qoi_ngsi = self.qoisystem_map[data['id']].get_qoivector_ngsi()
        logger.debug("Formatting qoi data as ngsi-ld: " + str(qoi_ngsi))

        #  relationship to be added to the dataset to link QoI
        ngsi = {
            "hasQuality": {
                "type": "Relationship",
                "object": qoi_ngsi['id']
            },
            "@context": [
                "http://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld", {
                    "hasQuality": "http://example.org/qoi/hasQuality"
                }
            ]
        }
        # save qoi data
        self.create_ngsi_entity(qoi_ngsi)
        # save relationship for qoi data
        self.add_ngsi_attribute(ngsi, data['id'])

    def get_qoivector(self, sourceid):
        return self.qoisystem_map[sourceid].get_qoivector()

    def get_subscriptions(self):
        return self.datasource_manager.get_subscriptions()

    def add_subscription(self, host, port, subscription):
        self.datasource_manager.add_subscription(host, port, subscription)

    def del_subscription(self, subid):
        self.datasource_manager.del_subscription(subid)

    def get_datasources(self):
        return self.datasource_manager.get_datasources()

    def get_metadata(self):
        return self.datasource_manager.matcher.get_all()

    def delete_metadata(self, mtype):
        self.datasource_manager.matcher.delete(mtype)

    def add_metadata(self, type, metadata):
        tmp = {'type': type, 'fields': metadata}
        self.datasource_manager.matcher.store(tmp)

    def add_ngsi_attribute(self, ngsi_msg, eid):
        logger.debug("Add ngsi attribute to entity " + eid + ":" + str(ngsi_msg))
        headers = {}
        headers.update({'content-type': 'application/ld+json'})
        headers.update({'accept': 'application/ld+json'})
        url = "http://" + BROKER_HOST + ":" + str(BROKER_PORT) + "/ngsi-ld/v1/entities/" + eid + "/attrs/"
        r = requests.post(url, json=ngsi_msg, headers=headers)
        if r.status_code != 204:
            logger.debug("Attribute exists, patch it")
            requests.patch(url, json=ngsi_msg, headers=headers)

    def create_ngsi_entity(self, ngsi_msg):
        logger.debug("Save entity to ngsi broker: " + str(ngsi_msg))
        headers = {}
        headers.update({'content-type': 'application/ld+json'})
        headers.update({'accept': 'application/ld+json'})
        url = "http://" + BROKER_HOST + ":" + str(BROKER_PORT) + "/ngsi-ld/v1/entities/"
        r = requests.post(url, json=ngsi_msg, headers=headers)
        if r.status_code == 409:
            logger.debug("Entity exists, patch it")
            self.patch_ngsi_entity(ngsi_msg)

    def patch_ngsi_entity(self, ngsi_msg):
        headers = {}
        headers.update({'content-type': 'application/ld+json'})
        headers.update({'accept': 'application/ld+json'})
        # for updating entity we have to delete id and type, first do copy if needed somewhere else
        ngsi_msg_patch = dict(ngsi_msg)
        ngsi_msg_patch.pop('id')
        ngsi_msg_patch.pop('type', None)
        url = "http://" + BROKER_HOST + ":" + str(BROKER_PORT) + "/ngsi-ld/v1/entities/" + ngsi_msg['id'] + "/attrs"
        r = requests.patch(url, json=ngsi_msg_patch, headers=headers)
        logger.debug("Entity patched: " + str(r.status_code))
