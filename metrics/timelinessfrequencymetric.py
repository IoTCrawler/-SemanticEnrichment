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
        if self.lastUpdate is 'NA':
            self.lastUpdate = current
        else:
            stream_id = ngsi_parser.get_observation_stream(observation)
            if stream_id:
                stream = self.qoisystem.get_stream(stream_id)
                updateinterval, unit = ngsi_parser.get_stream_updateinterval_and_unit(stream)
                if updateinterval and unit:
                    # update_interval = self.qoisystem.metadata['updateinterval']['frequency']
                    # unit = self.qoisystem.metadata['updateinterval']['unit']
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
