import abc
from other.rewardpunishment import RewardAndPunishment


class AbstractMetric(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, qoisystem):
        self.name = "abstract_metric"
        self.qoi_system = qoisystem
        self.rp = RewardAndPunishment(5)
        self.lastValue = 'NA'
        self.submetrics = []

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
        qoi_values = {'metric': self.name, 'last': self.lastValue, 'running': self.rp.value()}
        if len(self.submetrics) > 0:
            subvalues = []
            for submetric in self.submetrics:
                subvalues.append(submetric.get_qoivalue())
            qoi_values['submetrics'] = subvalues

        return qoi_values

    def get_metricname(self):
        return self.name
