from datetime import datetime

import luigi

from global_utilities.luigi import StandardTask
from global_utilities.log import log


class MoneyLoverTask(StandardTask):
    module = "money_lover"
    priority = 100


class FlightsTask(StandardTask):
    module = "flights"
    priority = 50


class ReportsTask(StandardTask):
    module = "reports"
    priority = 80

    def requires(self):
        yield MoneyLoverTask(self.mdate)


class DoAllTask(luigi.WrapperTask):
    mdate = luigi.DateParameter(default=datetime.now())

    def requires(self):
        yield ReportsTask(self.mdate)
        yield FlightsTask(self.mdate)


if __name__ == "__main__":

    log.info("Starting vtasks")
    luigi.build([DoAllTask()])

    log.info("End of vtasks")
