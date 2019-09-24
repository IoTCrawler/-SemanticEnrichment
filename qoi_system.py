from metrics.completenessmetric import CompletenessMetric
from metrics.plausibilitymetric import PlausibilityMetric
from metrics.timelinessagemmetric import TimelinessAgeMetric
from metrics.timelinessfrequencymetric import TimelinessFrequencyMetric
from metrics.timelinessmetric import TimelinessMetric
from metrics.concordancemetric import ConcordanceMetric


class QoiSystem:

    def __init__(self, metadata):
        self.metadata = metadata
        self.metrics = []
        # check values in metadata
        for field in metadata['fields']:
            self.add_metric(PlausibilityMetric(self, field))
            self.add_metric(ConcordanceMetric(self, field))
            self.add_metric(CompletenessMetric(self, field))

        self.add_metric(CompletenessMetric(self))
        timeliness = TimelinessMetric(self)
        timeliness.add_submetric(TimelinessAgeMetric(self))
        timeliness.add_submetric(TimelinessFrequencyMetric(self))
        self.add_metric(timeliness)

    def add_metrics(self, metrics):
        pass

    def add_metric(self, metric):
        self.metrics.append(metric)

    def change_metadata(self, metadata):
        self.metadata = metadata

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
            "id": "urn:ngsi-ld:QoI:" + self.metadata['id'],
            "type": "QoI",
            "@context": [
                "http://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld", {
                    "QoI": "http://example.org/qoi/QoI",
                    "continuous": "http://example.org/qoi/continous",
                    "last": "http://example.org/qoi/last",
                    "for": "http://example.org/qoi/for"
                }
            ]
        }
        for m in self.metrics:
            qoi_ngsi[m.name] = m.get_ngsi()
            qoi_ngsi['@context'][1][m.name] = "http://example.org/qoi/" + m.name

        return qoi_ngsi
