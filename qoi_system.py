from metrics.completenessmetric import CompletenessMetric
from metrics.plausibilitymetric import PlausibilityMetric
from metrics.timelinessagemmetric import TimelinessAgeMetric
from metrics.timelinessfrequencymetric import TimelinessFrequencyMetric
from metrics.timelinessmetric import TimelinessMetric
from metrics.concordancemetric import ConcordanceMetric


class QoiSystem:

    def __init__(self, streamid, ds_manager):
        print("init qoi system with", streamid)
        self.streamid = streamid
        self.ds_manager = ds_manager
        self.metrics = []
        self.add_metric(PlausibilityMetric(self))
        self.add_metric(ConcordanceMetric(self))
        self.add_metric(CompletenessMetric(self))
        timeliness = TimelinessMetric(self)
        timeliness.add_submetric(TimelinessAgeMetric(self))
        timeliness.add_submetric(TimelinessFrequencyMetric(self))
        self.add_metric(timeliness)

    def add_metrics(self, metrics):
        pass

    def add_metric(self, metric):
        self.metrics.append(metric)

    # def change_metadata(self, metadata):
    #     self.metadata = metadata

    def get_stream(self, stream_id):
        return self.ds_manager.get_stream(stream_id)

    def update(self, data):
        for m in self.metrics:
            m.update(data)

    # iterate through all metrics to get qoi vector
    def get_qoivector(self):
        qoilist = []
        for m in self.metrics:
            qoilist.append(m.get_qoivalue())
        return qoilist

    def get_qoivector_ngsi(self):
        qoi_ngsi = {
            "id": "urn:ngsi-ld:QoI:" + self.streamid,
            "type": "Quality",
            "@context": [
                "http://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld", {
                    "Quality": "https://w3id.org/iot/qoi#Quality",
                    "hasRatedValue": "https://w3id.org/iot/qoi#hasRatedValue",
                    "hasAbsoluteValue": "https://w3id.org/iot/qoi#hasAbsoluteValue"
                }
            ]
        }
        i = 0
        for m in self.metrics:
            if m.name in qoi_ngsi.keys():
                qoi_ngsi[m.name + str(i)] = m.get_ngsi()    # TODO this is a workaround as one metric can only be used once as a key, this should be change as soon as one stream contains only one sensor!
                i += 1
            else:
                qoi_ngsi[m.name] = m.get_ngsi()
            qoi_ngsi['@context'][1][m.name] = "w3id.org/iot/qoi#" + m.name

        return qoi_ngsi
