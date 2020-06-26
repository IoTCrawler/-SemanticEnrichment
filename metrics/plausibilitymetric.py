import re
from metrics.abstract_metric import AbstractMetric
from ngsi_ld import ngsi_parser


class PlausibilityMetric(AbstractMetric):

    def __init__(self, qoisystem):
        super(PlausibilityMetric, self).__init__(qoisystem)
        self.qoisystem = qoisystem
        self.name = "plausibility"

    def update_metric(self, observation):
        value = ngsi_parser.get_observation_value(observation)

        # TODO parse values like 44^^http://www.w3.org/2001/XMLSchema#integer
        if isinstance(value, str):
            m = re.search("[-+]?\\d*\\.\\d+|\\d+", value)
            value = m.group()

        # get sensor for accessing metadata
        sensor = self.qoisystem.get_sensor()
        if sensor:
            # get type
            datatype = ngsi_parser.get_sensor_valuetype(sensor)
            if not datatype:
                datatype = self.qoisystem.getStoredMetadata('valuetype')
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
        if not minvalue:
            minvalue = self.qoisystem.getStoredMetadata('min')
        maxvalue = ngsi_parser.get_sensor_max(sensor)
        if not maxvalue:
            maxvalue = self.qoisystem.getStoredMetadata('max')

        # check annotated dummy min/max value
        if ((minvalue == 'NA') & (maxvalue == 'NA') or (not minvalue and not maxvalue)):
            self.lastValue = 'NA'
            return

        # check min
        if (minvalue != 'NA') and (minvalue != None):
            if value < minvalue:
                self.lastValue = 0
                self.rp.update(0)
                return

        # check max
        if (maxvalue != 'NA') and (maxvalue != None):
            if value > maxvalue:
                self.lastValue = 0
                self.rp.update(0)
                return

        self.lastValue = 1
        self.rp.update(1)

    @staticmethod
    def is_number(s):
        try:
            float(s)
            return True
        except ValueError:
            return False

    def timer_update_metric(self):
        pass
