from metrics.plausibilitymetric import PlausibilityMetric
from metrics.completenessmetric import CompletenessMetric
from metrics.timelinessmetric import TimelinessMetric
from metrics.timelinessagemmetric import TimelinessAgeMetric
from metrics.timelinessfrequencymetric import TimelinessFrequencyMetric


class QoiSystem:

    def __init__(self, metadata):
        self.metadata = metadata
        self.metrics = []
        # check values in metadata
        for field in metadata['fields']:
            self.add_metric(PlausibilityMetric(self, field))

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
        # qoivector = {}
        # for m in self.metrics:
        #     qoivector[m.get_metricname()] = m.get_qoivalue()
        qoilist = []
        for m in self.metrics:
            qoilist.append(m.get_qoivalue())
        return qoilist
