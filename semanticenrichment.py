import requests
import logging
import ast
import threading
from qoi_system import QoiSystem
from datasource_manager import DatasourceManager
from configuration import Config

logger = logging.getLogger('semanticenrichment')


class SemanticEnrichment:

    def __init__(self):
        self.qoisystem_map = {}
        self.datasource_manager = DatasourceManager()
        self.datasource_map = ""

        self.headers = {}
        self.headers.update({'content-type': 'application/ld+json'})
        self.headers.update({'accept': 'application/ld+json'})
        self.headers.update({'X-AUTH-TOKEN': Config.get('NGSI', 'token')})

        logger.info("Semantic Enrichment started")

    def notify_datasource(self, metadata):
        # TODO call data source manager to subscribe etc.
        self.datasource_manager.add_datasource(metadata)

        # TODO initialise a qoi system per value of a stream? per stream, metrics are split to stream or value
        # store metadata in qoi_system
        # check if system exists
        if metadata['id'] not in self.qoisystem_map:
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

    def add_metadata(self, entitytype, metadata):
        try:
            tmp = {'type': entitytype, 'fields': ast.literal_eval(metadata)}
            self.datasource_manager.matcher.store(tmp)
        except Exception as e:
            logger.debug("Error while adding metadata: " + str(e))

    def add_ngsi_attribute(self, ngsi_msg, eid):
        t = threading.Thread(target=self._add_ngsi_attribute, args=(ngsi_msg, eid,))
        t.start()

    def _add_ngsi_attribute(self, ngsi_msg, eid):
        try:
            logger.debug("Add ngsi attribute to entity " + eid + ":" + str(ngsi_msg))
            url = "http://" + Config.get('NGSI', 'host') + ":" + str(
                Config.get('NGSI', 'port')) + "/ngsi-ld/v1/entities/" + eid + "/attrs/"
            r = requests.post(url, json=ngsi_msg, headers=self.headers)
            if r.status_code != 204:
                logger.debug("Attribute exists, patch it")
                requests.patch(url, json=ngsi_msg, headers=self.headers)
        except requests.exceptions.ConnectionError as e:
            logger.error("Error while adding attribute to ngsi entity" + str(e))

    def create_ngsi_entity(self, ngsi_msg):
        t = threading.Thread(target=self._create_ngsi_entity, args=(ngsi_msg,))
        t.start()

    def _create_ngsi_entity(self, ngsi_msg):
        try:
            logger.debug("Save entity to ngsi broker: " + str(ngsi_msg))
            url = "http://" + Config.get('NGSI', 'host') + ":" + str(
                Config.get('NGSI', 'port')) + "/ngsi-ld/v1/entities/"
            print(url)
            r = requests.post(url, json=ngsi_msg, headers=self.headers)
            if r.status_code == 409:
                logger.debug("Entity exists, patch it")
                self.patch_ngsi_entity(ngsi_msg)
        except requests.exceptions.ConnectionError as e:
            logger.error("Error while creating ngsi entity" + str(e))

    def patch_ngsi_entity(self, ngsi_msg):
        t = threading.Thread(target=self._patch_ngsi_entity, args=(ngsi_msg,))
        t.start()

    def _patch_ngsi_entity(self, ngsi_msg):
        try:
            # for updating entity we have to delete id and type, first do copy if needed somewhere else
            ngsi_msg_patch = dict(ngsi_msg)
            ngsi_msg_patch.pop('id')
            ngsi_msg_patch.pop('type', None)
            url = "http://" + Config.get('NGSI', 'host') + ":" + str(
                Config.get('NGSI', 'port')) + "/ngsi-ld/v1/entities/" + ngsi_msg['id'] + "/attrs"
            r = requests.patch(url, json=ngsi_msg_patch, headers=self.headers)
            logger.debug("Entity patched: " + str(r.status_code))
        except requests.exceptions.ConnectionError as e:
            logger.error("Error while patching ngsi entity" + str(e))
