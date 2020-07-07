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

        # parse values like 44^^http://www.w3.org/2001/XMLSchema#integer, only if contains '^^'
        if isinstance(value, str):
            if '^^' in value:
                m = re.search("[-+]?\\d*\\.?\\d+|\\d+", value)
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
                    if datatype.lower() in ['int', 'integer', 'double', 'float']:
                        self.handle_number(value, sensor)
                    elif datatype.lower() in ['str', 'string']:
                        self.handle_string(value, sensor)
                elif self.is_number(value):
                    self.handle_number(value, sensor)
                else:
                    self.lastValue = 'NA'

    def handle_string(self, value, sensor):
        regexp = ngsi_parser.get_sensor_regexp(sensor)
        if not regexp or (regexp == 'NA'):
            regexp = self.qoisystem.getStoredMetadata('regexp')
        if regexp and (regexp != 'NA'):
            if re.findall(regexp, value)[0] == value:
                self.lastValue = 1
                self.rp.update(1)
                return
        self.lastValue = 0
        self.rp.update(0)


    def handle_number(self, value, sensor):
        # add error handling if min/max are not in stream
        minvalue = ngsi_parser.get_sensor_min(sensor)
        if not minvalue  or (minvalue == 'NA'):
            minvalue = self.qoisystem.getStoredMetadata('min')
        maxvalue = ngsi_parser.get_sensor_max(sensor)
        if not maxvalue  or (maxvalue == 'NA'):
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
