import datetime
from metrics.abstract_metric import AbstractMetric


class TimelinessFrequencyMetric(AbstractMetric):

    def __init__(self, qoisystem):
        super(TimelinessFrequencyMetric, self).__init__(qoisystem)
        self.qoisystem = qoisystem
        self.name = "frequency"
        self.lastUpdate = 'NA'

    def update_metric(self, data):
        current = datetime.datetime.now()
        if self.lastUpdate is 'NA':
            self.lastUpdate = current
        else:
            update_interval = self.qoisystem.metadata['updateinterval']['frequency']
            unit = self.qoisystem.metadata['updateinterval']['unit']
            if unit is "seconds":
                diff = (current - self.lastUpdate).total_seconds()
                if diff > float(update_interval):
                    self.rp.update(1)
                else:
                    self.rp.update(0)
                self.lastValue = diff
            else:
                self.logger.debug("Unit not supported for frequency metric:" + update_interval)
                self.lastValue = "NA"
