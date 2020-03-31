from metrics.abstract_metric import AbstractMetric
from ngsi_ld import ngsi_parser


class PlausibilityMetric(AbstractMetric):

    def __init__(self, qoisystem):
        super(PlausibilityMetric, self).__init__(qoisystem)
        self.qoisystem = qoisystem
        self.name = "plausibility"

    def update_metric(self, observation):
        # for d in data['values']:
        value = ngsi_parser.get_observation_value(observation)  # data['values'][self.field]
        # get metadata
        # metadata = self.qoisystem.metadata['fields'][self.field]
        stream_id = ngsi_parser.get_observation_stream(observation)
        if stream_id:
            stream = self.qoisystem.get_stream(stream_id)
            if stream:
                # get type
                datatype = ngsi_parser.get_stream_valuetype(stream)
                if datatype:
                    datatype = ngsi_parser.get_stream_valuetype(stream)
                    # datatype = stream['valuetype']['value']
                    if datatype != 'NA':
                        if datatype in ['int', 'integer', 'double', 'float']:
                            self.handle_number(value, stream)
                    elif self.is_number(value):
                        self.handle_number(value, stream)
                    else:
                        self.lastValue = 'NA'

    def handle_number(self, value, stream):
        # TODO add error handling if min/max are not in stream
        min = ngsi_parser.get_stream_min(stream)
        max = ngsi_parser.get_stream_max(stream)
        # min = stream['min']['value']
        # max = stream['max']['value']
        if (min != 'NA') & (max != 'NA'):
            if min <= value <= max:
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

    def timer_update_metric(self):
        pass
