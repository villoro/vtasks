import luigi

from luigi_utils import StandardTask, date


class ExpensorTask(StandardTask):
    module = "expensor"


class DoAllTask(luigi.WrapperTask):
    mdate = luigi.DateParameter(default=date.today())

    def requires(self):
        yield ExpensorTask(self.mdate)


if __name__ == "__main__":
    luigi.build([DoAllTask()])
