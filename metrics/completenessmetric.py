from metrics.abstract_metric import AbstractMetric


class CompletenessMetric(AbstractMetric):

    def __init__(self, qoisystem):
        super(CompletenessMetric, self).__init__(qoisystem)
        self.qoisystem = qoisystem
        self.name = "completeness"

    def update_metric(self, observation):
        self.lastValue = 'NA'
        # if self.field is "sensor":
        #     nr = len(self.qoisystem.metadata['fields'])
        #     missing = 0
        #     for field in self.qoisystem.metadata['fields']:
        #         if field not in data['values']:
        #             missing += 1
        #     self.lastValue = nr / missing if missing != 0 else 1
        #     self.rp.update(0) if self.lastValue != 1 else self.rp.update(1)
        # else:
        #     if self.field not in data['values']:
        #         self.lastValue = 0
        #     else:
        #         self.lastValue = 1

    def timer_update_metric(self):
        pass
