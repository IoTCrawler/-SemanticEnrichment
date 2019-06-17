from metrics.abstract_metric import AbstractMetric


class PlausibilityMetric(AbstractMetric):

    def __init__(self, qoisystem, field):
        super(PlausibilityMetric, self).__init__(qoisystem)
        self.qoisystem = qoisystem
        self.name = "plausibility_metric" + "_" + field
        self.field = field

    def update_metric(self, data):
        # for d in data['values']:
        value = data['values'][self.field]
        # get metadata
        metadata = self.qoisystem.metadata['fields'][self.field]
        # get type
        datatype = metadata['valuetype']
        if datatype in ['int', 'integer', 'double', 'float']:
            self.handle_number(value, metadata)

    def handle_number(self, value, metadata):
        if metadata['min'] <= value <= metadata['max']:
            self.lastValue = 1
            self.rp.update(1)
        else:
            self.lastValue = 0
            self.rp.update(0)
