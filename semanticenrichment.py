import ast
import logging
import threading

from configuration import Config
from datasource_manager import DatasourceManager
from ngsi_ld import broker_interface
from ngsi_ld import ngsi_parser
from ngsi_ld.ngsi_parser import NGSI_Type
from qoi_system import QoiSystem

logger = logging.getLogger('semanticenrichment')


class SemanticEnrichment:

    def __init__(self):
        self.qoisystem_map = {}
        self.datasource_manager = DatasourceManager()
        self.initialise() #TODO enable!
        logger.info("Semantic Enrichment started")

    def initialise(self):
        self.datasource_manager.initialise_subscriptions()

        # get and notify for existing streams in a separate thread as this is blocking
        t = threading.Thread(target=self.initialise_existing_streams)
        t.start()

    def initialise_existing_streams(self):
        streams = broker_interface.get_all_entities(NGSI_Type.IoTStream)
        logger.debug("There are " + str(len(streams)) + " existing streams")
        for stream in streams:
            logger.debug("Notifiy existing stream " + ngsi_parser.get_id(stream))
            self.notify_datasource(stream)

    def clear(self):
        self.datasource_manager.clear()
        self.qoisystem_map.clear()

    def clearAndInitalise(self):
        self.clear()
        self.initialise()

    def del_stream(self, streamId):
        del self.qoisystem_map[streamId]
        self.datasource_manager.delete_stream(streamId)


    def notify_datasource(self, ngsi_data):
        ngsi_id, ngsi_type = ngsi_parser.get_IDandType(ngsi_data)
        # Save data locally, instantiate subscriptions
        self.datasource_manager.update(ngsi_type, ngsi_id, ngsi_data)

        # check if type is stream, if yes we have to initialise/update qoi
        if ngsi_type is NGSI_Type.IoTStream:
            if ngsi_id not in self.qoisystem_map:
                self.qoisystem_map[ngsi_id] = QoiSystem(ngsi_id, self.datasource_manager)

            if ngsi_parser.get_stream_hasQuality(ngsi_data) is None:

                qoi_ngsi = self.qoisystem_map[ngsi_id].get_qoivector_ngsi()
                logger.debug("Formatting qoi data as ngsi-ld: " + str(qoi_ngsi))

                #  relationship to be added to the dataset to link QoI
                ngsi = {
                    "qoi:hasQuality": {
                        "type": "Relationship",
                        "object": qoi_ngsi['id']
                    },
                    "@context": [
                        "http://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld", {
                            "qoi": "https://w3id.org/iot/qoi#"
                        }
                    ]
                }

                # update locally
                self.datasource_manager.link_qoi(ngsi_id, qoi_ngsi['id'])

                # save qoi data
                broker_interface.create_ngsi_entity(qoi_ngsi)
                # save relationship for qoi data
                broker_interface.add_ngsi_attribute(ngsi, ngsi_id)

        # if incoming data is observation we have to update QoI
        elif ngsi_type is NGSI_Type.StreamObservation:
            #TODO check if observation has imputed flag, if yes discard it
            if not ngsi_parser.is_imputedObservation(ngsi_data):
                self.receive(ngsi_data)

    def receive(self, observation):
        # get stream id from observation
        stream_id = ngsi_parser.get_observation_stream(observation)

        if stream_id not in self.qoisystem_map:
            logger.debug("Stream " + stream_id + " not found, requesting it")
            stream = broker_interface.get_entity(stream_id)
            self.notify_datasource(stream)

        try:
            self.qoisystem_map[stream_id].update(observation)

            # get current qoi data
            qoi_ngsi = self.qoisystem_map[stream_id].get_qoivector_ngsi()
            logger.debug("Formatting qoi data as ngsi-ld: " + str(qoi_ngsi))

            # save qoi data
            #TODO delete the delete workaround
            deleteqoi = Config.get('workaround', 'deleteqoi')
            if deleteqoi == "True":
                logger.debug("incoming observation for " + observation['id'] + " called, delete before updating/creating it")
                broker_interface.delete_and_create_ngsi_entity(qoi_ngsi)
            else:
                logger.debug("incoming observation for " + observation['id'] + " called, dont delete before updating/creating it")
                broker_interface.create_ngsi_entity(qoi_ngsi)
        except KeyError:
            logger.error("There is no stream " + str(stream_id) + " found for this observation!")

    def get_qoivector_ngsi(self, sourceid):
        return self.qoisystem_map[sourceid].get_qoivector_ngsi()

    def get_subscriptions(self):
        return self.datasource_manager.get_subscriptions()

    def add_subscription(self, subscription):
        self.datasource_manager.add_subscription(subscription)

    def del_subscription(self, subid):
        self.datasource_manager.del_subscription(subid)

    def get_streams(self):
        return self.datasource_manager.streams

    def get_sensor(self, sensor_id):
        return self.datasource_manager.get_sensor(sensor_id)

    def get_observation(self, observation_id):
        return self.datasource_manager.get_observation(observation_id)

    def get_observableproperty(self, obsproperty_id):
        return self.datasource_manager.get_observableproperty(obsproperty_id)

    def get_metadata(self):
        return self.datasource_manager.matcher.get_all()

    def delete_metadata(self, mtype):
        self.datasource_manager.matcher.delete(mtype)

    def add_metadata(self, entitytype, metadata):
        try:
            tmp = {'type': entitytype, 'metadata': ast.literal_eval(metadata)}
            self.datasource_manager.matcher.store(tmp)
        except Exception as e:
            logger.debug("Error while adding metadata: " + str(e))
