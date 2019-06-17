from metrics.abstract_metric import AbstractMetric


class TimelinessMetric(AbstractMetric):

    def __init__(self, qoisystem):
        super(TimelinessMetric, self).__init__(qoisystem)
        self.qoisystem = qoisystem
        self.name = "timeliness_metric"

    def update_metric(self, data):
        # TODO
        pass

    def get_qoivalue(self):
        qoi_values = {'metric': self.name, 'last': 'NA', 'running': 'NA'}
        if len(self.submetrics) > 0:
            subvalues = []
            for submetric in self.submetrics:
                subvalues.append(submetric.get_qoivalue())
            qoi_values['submetrics'] = subvalues

        return qoi_values
