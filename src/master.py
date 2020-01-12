from datetime import date

import luigi

from global_utilities.luigi import StandardTask


class ExpensorTask(StandardTask):
    module = "expensor"


class FlightsTask(StandardTask):
    module = "flights"


class DoAllTask(luigi.WrapperTask):
    mdate = luigi.DateParameter(default=date.today())

    def requires(self):
        yield ExpensorTask(self.mdate)
        yield FlightsTask(self.mdate)


if __name__ == "__main__":
    luigi.build([DoAllTask()])
