from metrics.abstract_metric import AbstractMetric


class CompletenessMetric(AbstractMetric):

    def __init__(self, qoisystem):
        super(CompletenessMetric, self).__init__(qoisystem)
        self.qoisystem = qoisystem
        self.name = "completeness_metric"

    def update_metric(self, data):
        nr = len(self.qoisystem.metadata['fields'])
        missing = 0
        for field in self.qoisystem.metadata['fields']:
            if field not in data['values']:
                missing += 1
        self.lastValue = nr / missing if missing != 0 else 1
        self.rp.update(0) if self.lastValue != 1 else self.rp.update(1)
