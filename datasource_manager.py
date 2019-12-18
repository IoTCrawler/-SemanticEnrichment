import logging
import threading

import requests
import datetime

from ngsi_ld import ngsi_parser
from ngsi_ld.ngsi_parser import NGSI_Type
from configuration import Config
from other.exceptions import BrokerError
from other.metadata_matcher import MetadataMatcher

logger = logging.getLogger('semanticenrichment')


class Subscription:
    def __init__(self, subid, host, port, subscription):
        self.id = subid
        self.host = host
        self.port = port
        self.subscription = subscription


class DataSource:
    def __init__(self, dsid, dstype, metadata):
        self.id = dsid
        self.dstype = dstype
        self.metadata = metadata
        self.lastupdate = datetime.datetime.now()

    def update(self, metadata):
        self.metadata.update(metadata)
        self.lastupdate = datetime.datetime.now()


class DatasourceManager:

    def __init__(self):
        self.subscriptions = {}
        self.streams = {}
        self.sensors = {}
        self.observations = {}

        self.headers = {}
        self.headers.update({'content-type': 'application/ld+json'})
        self.headers.update({'accept': 'application/ld+json'})
        self.headers.update({'X-AUTH-TOKEN': Config.get('NGSI', 'token')})
        self.matcher = MetadataMatcher()
        t = threading.Thread(target=self.get_active_subscriptions)  # put into thread to not block server
        t.start()

    def update(self, ngsi_data):
        # check type
        ngsi_id, ngsi_type = ngsi_parser.get_IDandType(ngsi_data)
        if ngsi_type is NGSI_Type.IoTStream:
            self.streams[ngsi_id] = ngsi_data
        elif ngsi_type is NGSI_Type.StreamObservation:
            self.observations[ngsi_id] = ngsi_data
        elif ngsi_type is NGSI_Type.Sensor:
            self.sensors[ngsi_id] = ngsi_data

    def get_observation_for_stream(self, stream_id):
        # TODO think about better solution than iterating all observations
        for observation in self.observations.values():
            if stream_id == ngsi_parser.get_observation_stream(observation):
                return observation

    def link_qoi(self, stream_id, qoi_id):
        try:
            self.streams[stream_id]['hasQuality']['object'] = qoi_id
        except KeyError:
            pass

    def add_subscription(self, host, port, subscription):
        # subscribe to ngsi-ld endpoint
        sub = Subscription(subscription['id'], host, port, subscription)

        server_url = "http://" + host + ":" + str(port) + "/ngsi-ld/v1/subscriptions/"
        r = requests.post(server_url, json=subscription, headers=self.headers)
        logger.info("Adding subscription: " + str(r.status_code) + " " + r.text)
        if r.status_code != 201:
            logger.debug("error creating subscription: " + r.text)
            raise BrokerError(r.text)
        else:
            self.subscriptions[sub.id] = sub
        return r.text

    def del_subscription(self, subid):
        subscription = self.subscriptions.pop(subid)

        server_url = "http://" + subscription.host + ":" + str(subscription.port) + "/ngsi-ld/v1/subscriptions/"
        server_url = server_url + subid
        r = requests.delete(server_url, headers=self.headers)
        logger.debug("deleting subscription " + subid + ": " + r.text)

    # def add_datasource(self, data):
    #     # checking metadata for 'NA' fields, if NA field try to find some metadata
    #     self.matcher.check_metadata(data)
    #
    #     # check if datasource is already registered, if so update metadata
    #     dsid = data['id']
    #     if dsid in self.datasources:
    #         self.datasources[dsid].update(data)
    #     else:
    #         datasource = DataSource(dsid, data['type'], data)
    #         self.datasources[dsid] = datasource
    #     # TODO check how to get the data
    #     # in testing we always receive data and metadata in one ngsi-ld form automatically
    #     # TODO later to send data "request" to data handler in monitoring

    def get_subscriptions(self):
        return self.subscriptions

    # def get_datasources(self):
    #     return self.datasources

    # TODO this method is mainly for testing etc as subscriptions are lost during restart,
    # in addition ngrok won't fit for old subscriptions
    def get_active_subscriptions(self):
        # get old subscriptions for semantic enrichment (starting with 'SE_')
        host = Config.get('NGSI', 'host')
        port = Config.get('NGSI', 'port')
        server_url = "http://" + host + ":" + port + "/ngsi-ld/v1/subscriptions/"
        try:
            r = requests.get(server_url, headers=self.headers)
            if r.status_code == 200:
                if isinstance(r.json(), list):
                    for data in r.json():
                        self.handlejsonsubscription(data, host, port)
                if isinstance(r.json(), dict):
                    self.handlejsonsubscription(r.json(), host, port)
            else:
                logger.error("Error getting active subscriptions: " + r.text + str(r.status_code))
        except Exception as e:
            logger.error("Error getting active subscriptions: " + str(e))

    def handlejsonsubscription(self, data, host, port):
        if data['id'].startswith('SE_', data['id'].rfind(':') + 1):
            sub = Subscription(data['id'], host, port, data)
            self.subscriptions[sub.id] = sub
            logger.info("Found active subscription: " + str(data))
        else:
            logger.info("not our subscription")

    def get_stream(self, stream_id):
        if stream_id in self.streams:
            return self.streams[stream_id]
        return None
