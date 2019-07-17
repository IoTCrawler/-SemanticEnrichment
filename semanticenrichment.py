import datetime
import logging
from qoi_system import QoiSystem
from datasource_manager import DatasourceManager

logger = logging.getLogger('semanticenrichment')

# TODO shift broker, callback, etc. options into a config file
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

        # Todo save qoi data to MDR
        qoidata = self.qoisystem_map[data['id']].get_qoivector()

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
