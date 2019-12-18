from metrics.abstract_metric import AbstractMetric


class ArtificialityMetric(AbstractMetric):

    def __init__(self, qoisystem):
        super(ArtificialityMetric, self).__init__(qoisystem)
        self.qoisystem = qoisystem
        self.name = "artificiality"

    def update_metric(self, data):
        # TODO implement!
        self.lastValue = 'NA'
