from metrics.abstract_metric import AbstractMetric


class TimelinessMetric(AbstractMetric):

    def __init__(self, qoisystem):
        super(TimelinessMetric, self).__init__(qoisystem)
        self.qoisystem = qoisystem
        self.name = "timeliness"

    def update_metric(self, data):
        pass


