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
        qoi_values = {'metric': self.name, 'for': self.field, 'last': self.lastValue, 'running': 'NA' if self.rp.value() is 'NA' else '{:.2f}'.format(self.rp.value())}
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
            self.name: {
                "type": "Property",
                "for":{
                    "type": "Relationship",
                    "object": "urn:ngsi-ld:" + self.qoi_system.metadata['id'] + ":" + self.field if self.field is not "sensor" else ""
                },
                "last": {
                    "type": "Property",
                    "value": self.lastValue
                },
                "running": {
                    "type": "Property",
                    "value": self.rp.value()
                }
            }
        }
        return ngsi
        # TODO submetrics missing

        # "plausibility": {
        #     "type": "Property",
        #     "for":{
        #         "type": "Relationship",
        #         "object": "urn:ngsi-ld:"
        #     },
        #     "last": {
        #         "type": "Property",
        #         "value": ""
        #     },
        #     "running": {
        #         "type": "Property",
        #         "value": ""
        #     }
        # }