from metrics.abstract_metric import AbstractMetric


class PlausibilityMetric(AbstractMetric):

    def __init__(self, qoisystem, field=None):
        super(PlausibilityMetric, self).__init__(qoisystem, field)
        self.qoisystem = qoisystem
        self.name = "plausibility"

    def update_metric(self, data):
        # for d in data['values']:
        value = data['values'][self.field]
        # get metadata
        metadata = self.qoisystem.metadata['fields'][self.field]
        # get type
        datatype = metadata['valuetype']
        if datatype is not 'NA':
            if datatype in ['int', 'integer', 'double', 'float']:
                self.handle_number(value, metadata)
        elif self.is_number(value):
            self.handle_number(value, metadata)
        else:
            self.lastValue = 'NA'

    def handle_number(self, value, metadata):
        if (metadata['min'] is not 'NA') & (metadata['max'] is not 'NA'):
            if metadata['min'] <= value <= metadata['max']:
                self.lastValue = 1
                self.rp.update(1)
            else:
                self.lastValue = 0
                self.rp.update(0)
        else:
            self.lastValue = 'NA'

    @staticmethod
    def is_number(s):
        try:
            float(s)
            return True
        except ValueError:
            return False
