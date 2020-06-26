import threading
from ngsi_ld import ngsi_parser
from metrics.completenessmetric import CompletenessMetric
from metrics.plausibilitymetric import PlausibilityMetric
from metrics.timelinessagemmetric import TimelinessAgeMetric
from metrics.timelinessfrequencymetric import TimelinessFrequencyMetric
from metrics.artificiality import ArtificialityMetric
from metrics.concordancemetric import ConcordanceMetric
from other.metadata_matcher import MetadataMatcher


class QoiSystem:

    def __init__(self, streamid, ds_manager):
        print("init qoi system with", streamid)
        self.streamid = streamid
        self.ds_manager = ds_manager
        self.metrics = []
        self.add_metric(PlausibilityMetric(self))
        self.add_metric(ConcordanceMetric(self))
        self.add_metric(CompletenessMetric(self))
        self.add_metric(TimelinessAgeMetric(self))
        self.add_metric(TimelinessFrequencyMetric(self))
        self.add_metric(ArtificialityMetric(self))
        self.timer = None

    def cancel_timer(self):
        if isinstance(self.timer, threading.Timer):
            self.timer.cancel()
            self.timer = None

    def start_timer(self):
        # start timer for update interval + 10%
        sensor = self.get_sensor()
        if sensor:
            updateinterval, unit = ngsi_parser.get_sensor_updateinterval_and_unit(sensor)
            if updateinterval:
                if self.is_number(updateinterval):
                    updateinterval = float(updateinterval)
                    self.timer = threading.Timer(updateinterval * 1.1, self.timer_update)
                    self.timer.start()

    def add_metric(self, metric):
        self.metrics.append(metric)

    def get_stream(self):
        return self.ds_manager.get_stream(self.streamid)

    def get_sensor(self):
        stream = self.get_stream()
        if stream:
            sensor_id = ngsi_parser.get_stream_generatedBy(stream)
            if sensor_id:
                return self.ds_manager.get_sensor(sensor_id)
        return None

    def update(self, data):
        self.cancel_timer()

        for m in self.metrics:
            m.update(data)

        self.start_timer()

    def timer_update(self):
        for m in self.metrics:
            m.timer_update_metric()
        self.start_timer()

    # iterate through all metrics to get qoi vector
    def get_qoivector(self):
        qoilist = []
        for m in self.metrics:
            qoilist.append(m.get_qoivalue())
        return qoilist

    def get_qoivector_ngsi(self):
        qoi_ngsi = {
            "id": "urn:ngsi-ld:QoI:" + self.streamid,
            "type": "qoi:Quality",
            "@context": [
                "http://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld", {
                    "qoi": "https://w3id.org/iot/qoi#",
                }
            ]
        }
        for m in self.metrics:
            if m.get_ngsi():
                qoi_ngsi['qoi:' + m.name] = m.get_ngsi()

        return qoi_ngsi

    def getStoredMetadata(self, field):
        return self.ds_manager.getStoredMetadata(self.get_sensor(), field)

    @staticmethod
    def is_number(s):
        try:
            float(s)
            return True
        except ValueError:
            return False