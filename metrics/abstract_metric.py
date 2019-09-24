import abc
import logging
from other.rewardpunishment import RewardAndPunishment


class AbstractMetric(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, qoisystem, field=None):
        self.name = "abstract_metric"
        self.qoi_system = qoisystem
        self.rp = RewardAndPunishment(5)
        self.lastValue = 'NA'
        self.submetrics = []
        if not field:
            self.field = 'sensor'
        else:
            self.field = field
        self.logger = logging.getLogger('semanticenrichment')

    def add_submetric(self, submetric):
        self.submetrics.append(submetric)

    def update_submetrics(self, data):
        for metric in self.submetrics:
            metric.update(data)

    @abc.abstractmethod
    def update_metric(self, data):
        pass

    def update(self, data):
        self.update_submetrics(data)
        self.update_metric(data)

    def get_qoivalue(self):
        qoi_values = {'metric': self.name, 'for': self.field, 'last': self.lastValue,
                      'continuous': 'NA' if self.rp.value() is 'NA' else '{:.2f}'.format(self.rp.value())}
        if len(self.submetrics) > 0:
            subvalues = []
            for submetric in self.submetrics:
                subvalues.append(submetric.get_qoivalue())
            qoi_values['submetrics'] = subvalues

        return qoi_values

    def get_metricname(self):
        return self.name

    def get_ngsi(self):
        ngsi = {
            "type": "Property",
            "value": "NA",  # TODO value set to NA as it cannot be null
            "for": {
                "type": "Relationship",
                "object": self.qoi_system.metadata['id'] + [":" + self.field if self.field is not "sensor" else ""][0]
            },
            "last": {
                "type": "Property",
                "value": self.lastValue
            },
            "continuous": {
                "type": "Property",
                "value": self.rp.value()
            }
        }
        for submetric in self.submetrics:
            ngsi[submetric.name] = submetric.get_ngsi()
        return ngsi
