from metrics.abstract_metric import AbstractMetric


class ConcordanceMetric(AbstractMetric):

    def __init__(self, qoisystem):
        super(ConcordanceMetric, self).__init__(qoisystem)
        self.qoisystem = qoisystem
        self.name = "concordance"

    def update_metric(self, data):
        # TODO implement!
        self.lastValue = 'NA'
