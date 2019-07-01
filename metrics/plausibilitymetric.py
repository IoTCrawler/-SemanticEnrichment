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
        if datatype is not 'NA':
            if datatype in ['int', 'integer', 'double', 'float']:
                self.handle_number(value, metadata)
        elif self.is_number(value):
            self.handle_number(value, metadata)
        else:
            self.rp.update(0)
            self.lastValue = 'NA'

    def handle_number(self, value, metadata):
        #TODO check if min/max in metadata
        if metadata['min'] <= value <= metadata['max']:
            self.lastValue = 1
            self.rp.update(1)
        else:
            self.lastValue = 0
            self.rp.update(0)


    def is_number(self, s):
        try:
            float(s)
            return True
        except ValueError:
            return False