import datetime
from ngsi_ld import ngsi_parser
from metrics.abstract_metric import AbstractMetric


class TimelinessFrequencyMetric(AbstractMetric):

    def __init__(self, qoisystem):
        super(TimelinessFrequencyMetric, self).__init__(qoisystem)
        self.qoisystem = qoisystem
        self.name = "frequency"
        self.lastUpdate = datetime.datetime.now()   #'NA' # TODO NA has been replaced to enable Frequency also for data sources that have not sent any Observation yet
        self.unit = "HTZ"   #NGSI-LD unitCode expects unitCodes from table: http://www.unece.org/fileadmin/DAM/cefact/recommendations/rec20/rec20_Rev9e_2014.xls, so HTZ for hertz

    def update_metric(self, observation):
        current = datetime.datetime.now()
        if self.lastUpdate == 'NA':
            self.lastUpdate = current
        else:
            sensor = self.qoi_system.get_sensor()
            if sensor:

                updateinterval, unit = ngsi_parser.get_sensor_updateinterval_and_unit(sensor)
                if updateinterval:
                    if unit:
                        if unit in ("s", "seconds"):
                            diff = (current - self.lastUpdate).total_seconds()
                            self.lastUpdate = current
                            if diff > float(updateinterval):
                                self.rp.update(0)
                            else:
                                self.rp.update(1)
                            self.lastValue = diff
                        else:
                            self.logger.debug("Unit not supported for frequency metric:" + updateinterval)
                            self.lastValue = "NA"
                    else:       #assume seconds as defalt if no unit is set
                        diff = (current - self.lastUpdate).total_seconds()
                        self.lastUpdate = current
                        if diff > float(updateinterval):
                            self.rp.update(0)
                        else:
                            self.rp.update(1)
                        self.lastValue = diff

    def timer_update_metric(self):
        if self.lastUpdate != 'NA':
            # do an update without any observation
            current = datetime.datetime.now()
            diff = (current - self.lastUpdate).total_seconds()
            # as this was timer based diff is bigger than updateinverval, therefore punish
            self.lastValue = diff
            self.rp.update(0)

