import logging
import json
from ngsi_ld import broker_interface
from ngsi_ld import ngsi_parser
from ngsi_ld.ngsi_parser import NGSI_Type
from other.metadata_matcher_filebased import MetadataMatcher

logger = logging.getLogger('semanticenrichment')


class DatasourceManager:

    def __init__(self):
        self.subscriptions = {}
        self.streams = {}
        self.sensors = {}
        self.observations = {}
        self.observableproperties = {}

        self.matcher = MetadataMatcher()

    def update(self, ngsi_type, ngsi_id, ngsi_data):
        # check type
        if ngsi_type is NGSI_Type.IoTStream:
            # stream comes in if it is a new stream or stream has been updated
            # Existing Stream: we have to check what is different... => we only have to check if there are new relations, new metadata will just be updated
            # New Stream: GET related ObservableProperty, SUB to ObservableProperty and StreamObservation

            # get sensor id first
            sensorId = ngsi_parser.get_stream_generatedBy(ngsi_data)

            #check if iotstream contains sensorId, if yes check sensor details
            if sensorId:
                if ngsi_id in self.streams:  # existing stream
                    # check if sensorId changed
                    oldstream = self.streams[ngsi_id]
                    oldSensorId = ngsi_parser.get_stream_generatedBy(oldstream)
                    if sensorId != oldSensorId:  # observable property has changed
                        # delete old sensor from dict
                        self.sensors.pop(oldSensorId, None)

                # reqeuest new sensor (in new tread to avoid blocking) and subscribe to obsproperties and streamobservations
                broker_interface.handleNewSensor(sensorId, self.sensors, self.observableproperties, self.subscriptions)
            else:
                logger.error("SensorId for IotStream missing:" + json.dumps(ngsi_data))

            # finally just update the stream, metrics will request new metadata from store automatically
            self.streams[ngsi_id] = ngsi_data

        elif ngsi_type is NGSI_Type.StreamObservation:
            self.observations[ngsi_id] = ngsi_data
        elif ngsi_type is NGSI_Type.Sensor:
            self.sensors[ngsi_id] = ngsi_data
        elif ngsi_type is NGSI_Type.ObservableProperty:
            self.observableproperties[ngsi_id] = ngsi_data

    def get_sensor(self, sensor_id):
        try:
            return self.sensors[sensor_id]
        except KeyError:
            return None

    def get_observation(self, observation_id):
        try:
            return self.observations[observation_id]
        except KeyError:
            return None

    def get_observableproperty(self, observableproperty_id):
        try:
            return self.observableproperties[observableproperty_id]
        except KeyError:
            return None

    def link_qoi(self, stream_id, qoi_id):
        try:
            stream = self.streams[stream_id]
            ngsi_parser.update_stream_hasQuality(stream, qoi_id)
        except KeyError:
            pass

    def initialise_subscriptions(self):
        broker_interface.initialise_subscriptions(self.subscriptions)

    def get_active_subscriptions(self):
        broker_interface.get_active_subscriptions(self.subscriptions)

    def add_subscription(self, subscription):
        broker_interface.add_subscription(subscription, self.subscriptions)

    def del_subscription(self, subid):
        subscription = self.subscriptions.pop(subid)
        broker_interface.del_subscription(subscription)

    def del_all_subscriptions(self):
        for subid in list(self.subscriptions.keys()):
            self.del_subscription(subid)

    def get_subscriptions(self):
        return self.subscriptions

    def get_stream(self, stream_id):
        if stream_id in self.streams:
            return self.streams[stream_id]
        return None

    def getStoredMetadata(self, sensor, field):
        #first get observed property, TODO move this to initilisation?
        # sensor = self.get_sensor()

        observablePropertyId = ngsi_parser.get_sensor_observes(sensor)
        if observablePropertyId:
            observableProperty = self.get_observableproperty(observablePropertyId)
            if observableProperty:
                observedType = ngsi_parser.get_obsproperty_label(observableProperty)

                if observedType:
                    metadata = self.matcher.match(observedType)
                    if metadata:
                        try:
                            return metadata['metadata'][field]
                        except KeyError:
                            return None
        return None