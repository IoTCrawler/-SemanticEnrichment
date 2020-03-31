import abc
import logging
from other.rewardpunishment import RewardAndPunishment
from configuration import Config


class AbstractMetric(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, qoisystem):
        self.name = "abstract_metric"
        self.qoi_system = qoisystem
        self.rp = RewardAndPunishment(5)
        self.lastValue = 'NA'
        self.unit = 'NA'
        self.submetrics = []
        self.logger = logging.getLogger('semanticenrichment')

    def add_submetric(self, submetric):
        self.submetrics.append(submetric)

    def update_submetrics(self, data):
        for metric in self.submetrics:
            metric.update(data)

    @abc.abstractmethod
    def update_metric(self, data):
        pass

    @abc.abstractmethod
    def timer_update_metric(self):
        pass

    def update(self, data):
        self.update_submetrics(data)
        self.update_metric(data)

    def get_qoivalue(self):
        qoi_values = {'metric': self.name,
                      # 'for': self.field,
                      'qoi:hasAbsoluteValue': self.lastValue,
                      'qoi:hasRatedValue': 'NA' if self.rp.value() == 'NA' else '{:.2f}'.format(self.rp.value())}
        if self.unit != 'NA':
            qoi_values['qoi:unit'] = self.unit

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
        }
        enable_na = Config.get('semanticenrichment', 'enablena')
        if enable_na == "False":
            if self.lastValue != 'NA':
                ngsi['qoi:hasAbsoluteValue'] = {"type": "Property", "value": self.lastValue}
            if self.rp.value() != 'NA':
                ngsi['qoi:hasRatedValue'] = {"type": "Property", "value": self.rp.value()}
        else:
            ngsi['qoi:hasAbsoluteValue'] = {"type": "Property", "value": self.lastValue}
            ngsi['qoi:hasRatedValue'] = {"type": "Property", "value": self.rp.value()}

        for submetric in self.submetrics:
            ngsi[submetric.name] = submetric.get_ngsi()

        # check if metric contains entries if na values are not enables
        if enable_na == "False":
            if len(ngsi) <= 2:
                return None

        return ngsi
