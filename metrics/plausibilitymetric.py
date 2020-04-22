from metrics.abstract_metric import AbstractMetric
from ngsi_ld import ngsi_parser


class PlausibilityMetric(AbstractMetric):

    def __init__(self, qoisystem):
        super(PlausibilityMetric, self).__init__(qoisystem)
        self.qoisystem = qoisystem
        self.name = "plausibility"

    def update_metric(self, observation):
        value = ngsi_parser.get_observation_value(observation)
        # get sensor for accessing metadata
        sensor = self.qoisystem.get_sensor()
        if sensor:
            # get type
            datatype = ngsi_parser.get_sensor_valuetype(sensor)
            if datatype:
                if datatype != 'NA':
                    if datatype in ['int', 'integer', 'double', 'float']:
                        self.handle_number(value, sensor)
                elif self.is_number(value):
                    self.handle_number(value, sensor)
                else:
                    self.lastValue = 'NA'

    def handle_number(self, value, sensor):
        # add error handling if min/max are not in stream
        minvalue = ngsi_parser.get_sensor_min(sensor)
        maxvalue = ngsi_parser.get_sensor_max(sensor)

        if (minvalue != 'NA') & (maxvalue != 'NA'):
            if minvalue <= value <= maxvalue:
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
