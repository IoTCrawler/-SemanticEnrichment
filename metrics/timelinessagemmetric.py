import datetime
from metrics.abstract_metric import AbstractMetric


class TimelinessAgeMetric(AbstractMetric):

    def __init__(self, qoisystem):
        super(TimelinessAgeMetric, self).__init__(qoisystem)
        self.qoisystem = qoisystem
        self.name = "age"

    def update_metric(self, data):
        if data['timestamp']:
            age = (datetime.datetime.now() - datetime.datetime.fromtimestamp(data['timestamp'])).total_seconds()
            self.lastValue = age
        else:
            self.lastValue = 'NA'

    # def get_qoivalue(self):
    #     qoi_values = {'metric': self.name, 'last': self.lastValue, 'running': 'NA'}
    #     if len(self.submetrics) > 0:
    #         subvalues = []
    #         for submetric in self.submetrics:
    #             subvalues.append(submetric.get_qoivalue())
    #         qoi_values['submetrics'] = subvalues
    #
    #     return qoi_values
