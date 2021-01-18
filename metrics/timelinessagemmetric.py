import datetime
from ngsi_ld import ngsi_parser
from metrics.abstract_metric import AbstractMetric


class TimelinessAgeMetric(AbstractMetric):

    def __init__(self, qoisystem):
        super(TimelinessAgeMetric, self).__init__(qoisystem)
        self.qoisystem = qoisystem
        self.name = "age"
        self.unit = "SEC"   #NGSI-LD unitCode expects unitCodes from table: http://www.unece.org/fileadmin/DAM/cefact/recommendations/rec20/rec20_Rev9e_2014.xls, so SEC for seconds

    def update_metric(self, observation):
        time = ngsi_parser.get_observation_timestamp(observation)
        if time:
            age = (datetime.datetime.now() - datetime.datetime.fromtimestamp(time)).total_seconds()
            self.lastValue = age
        else:
            self.lastValue = 'NA'

    def timer_update_metric(self):
        pass
