from ngsi_ld import ngsi_parser
from metrics.abstract_metric import AbstractMetric


class CompletenessMetric(AbstractMetric):

    def __init__(self, qoisystem):
        super(CompletenessMetric, self).__init__(qoisystem)
        self.qoisystem = qoisystem
        self.name = "completeness"
        self.missingValues = 0

    def update_metric(self, observation):
        self.missingValues = 0
        value = ngsi_parser.get_observation_value(observation)
        if value:
            self.missingValues = 0
            self.lastValue = self.missingValues
            self.rp.update(1)
        else:
            self.missingValues += 1
            self.lastValue = self.missingValues
            self.rp.update(0)

    def timer_update_metric(self):
        # timer update means we did not receive any data for the last planned update time
        # so we have to punish
        self.rp.update(0)
        self.missingValues += 1
        self.lastValue = self.missingValues
