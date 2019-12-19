import datetime
from ngsi_ld import ngsi_parser
from metrics.abstract_metric import AbstractMetric


class TimelinessFrequencyMetric(AbstractMetric):

    def __init__(self, qoisystem):
        super(TimelinessFrequencyMetric, self).__init__(qoisystem)
        self.qoisystem = qoisystem
        self.name = "frequency"
        self.lastUpdate = 'NA'
        self.unit = "seconds"

    def update_metric(self, observation):
        current = datetime.datetime.now()
        if self.lastUpdate == 'NA':
            self.lastUpdate = current
        else:
            stream_id = self.qoi_system.streamid
            if stream_id:
                stream = self.qoisystem.get_stream(stream_id)
                updateinterval, unit = ngsi_parser.get_stream_updateinterval_and_unit(stream)
                if updateinterval and unit:
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

    def timer_update_metric(self):
        if self.lastUpdate != 'NA':
            # do an update without any observation
            current = datetime.datetime.now()
            diff = (current - self.lastUpdate).total_seconds()
            # as this was timer based diff is bigger than updateinverval, therefore punish
            self.lastValue = diff
            self.rp.update(0)

