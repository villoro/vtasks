import luigi

from luigi_utils import StandardTask, date


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
