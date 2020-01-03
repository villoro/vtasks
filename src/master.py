import luigi

from luigi_utils import Task, date


class ExpensorTask(Task):
    module = "expensor"


class DoAllTask(luigi.WrapperTask):
    mdate = luigi.DateParameter(default=date.today())

    def requires(self):
        yield ExpensorTask(self.mdate)


if __name__ == "__main__":
    luigi.build([DoAllTask()])
