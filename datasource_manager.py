import datetime
import logging
import threading

import requests

from configuration import Config
from ngsi_ld import ngsi_parser
from ngsi_ld.ngsi_parser import NGSI_Type
from other.exceptions import BrokerError
from other.metadata_matcher import MetadataMatcher
from ngsi_ld.subscription import Subscription
from ngsi_ld import broker_interface

logger = logging.getLogger('semanticenrichment')





# class DataSource:
#     def __init__(self, dsid, dstype, metadata):
#         self.id = dsid
#         self.dstype = dstype
#         self.metadata = metadata
#         self.lastupdate = datetime.datetime.now()
#
#     def update(self, metadata):
#         self.metadata.update(metadata)
#         self.lastupdate = datetime.datetime.now()


class DatasourceManager:

    def __init__(self):
        self.subscriptions = {}
        self.streams = {}
        self.sensors = {}
        self.observations = {}
        self.observableproperties = {}
        self.get_active_subscriptions()

        # TODO not used anymore, update for streams without metadata?
        self.matcher = MetadataMatcher()


    def update(self, ngsi_data):
        # check type
        ngsi_id, ngsi_type = ngsi_parser.get_IDandType(ngsi_data)
        if ngsi_type is NGSI_Type.IoTStream:
            print("we are here")
            # TODO
            # stream comes in if it is a new stream or stream has been updated
            # Existing Stream: we have to check what is different... => we only have to check if there are new relations, new metadata will just be updated
            # New Stream: GET related ObservableProperty, SUB to ObservableProperty and StreamObservation

            # obspropertyId = ngsi_parser.get_stream_observes(ngsi_data)

            #get sensor id first
            sensorId = ngsi_parser.get_stream_generatedBy(ngsi_data)

            if ngsi_id in self.streams: #existing stream
                #check if sensorId changed

                oldstream = self.streams[ngsi_id]

                oldSensorId = ngsi_parser.get_stream_generatedBy(oldstream)
                if sensorId is not oldSensorId: #observable property has changed
                    #delete old sensor from dict
                    #TODO unsubscribe for old sensor?
                    self.sensors.pop(oldSensorId, None)

                    #reqeuest new sensor (in new tread to avoid blocking) and subscribe to obsproperties and streamobservations
                    broker_interface.handleNewSensor(sensorId, self.sensors, self.observableproperties, self.subscriptions)

                #finally just update the stream, metrics will request new metadata from store automatically
                self.streams[ngsi_id] = ngsi_data
            else:                       #new stream
                #check if there is already data for sensor locally available
                if sensorId in self.sensors:
                    #if sensor found we do nothing
                    #TODO we could check if subscriptions still running
                    pass
                else:
                    print("new sensor")
                    #TODO if no sensor found (do all in a threaded method to avoid blocking here)
                    broker_interface.handleNewSensor(sensorId, self.sensors, self.observableproperties, self.subscriptions)
                    #GET for sensor
                    #GET for obsproperty(sensor)
                    #SUB for streamobservation(sensor)
                    #SUB for sensor




                #update the stream in local list
                self.streams[ngsi_id] = ngsi_data


            # self.streams[ngsi_id] = ngsi_data
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

    def get_active_subscriptions(self):
        broker_interface.get_active_subscriptions(self.subscriptions)

    def add_subscription(self, host, port, subscription):
        broker_interface.add_subscription(host, port, subscription, self.subscriptions)

    def del_subscription(self, subid):
        subscription = self.subscriptions.pop(subid)
        broker_interface.del_subscription(subscription)




    def get_subscriptions(self):
        return self.subscriptions





    def get_stream(self, stream_id):
        if stream_id in self.streams:
            return self.streams[stream_id]
        return None
