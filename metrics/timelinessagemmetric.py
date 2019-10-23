import datetime
from metrics.abstract_metric import AbstractMetric


class TimelinessAgeMetric(AbstractMetric):

    def __init__(self, qoisystem):
        super(TimelinessAgeMetric, self).__init__(qoisystem)
        self.qoisystem = qoisystem
        self.name = "age"
        self.unit = "seconds"

    def update_metric(self, data):
        if data['timestamp']:
            age = (datetime.datetime.now() - datetime.datetime.fromtimestamp(data['timestamp'])).total_seconds()
            self.lastValue = age
        else:
            self.lastValue = 'NA'


