import os
import time
from datetime import date, datetime
from subprocess import check_output

import luigi
import oyaml as yaml
from v_time import time_human

from config import PATH_ROOT

PATH_LUIGI_YAML = f"{PATH_ROOT}runs/"


class StandardTask(luigi.Task):
    """
        Extends luigi task, instead of calling run, one must call run_std

        Params:
            mdate:          date of execution
            t_data:         is a dictionary with instance data
            worker_timeout: maximum time allowed for a task to run in seconds
    """

    mdate = luigi.DateParameter(default=date.today())
    worker_timeout = 1 * 3600  # Default timeout is 1h per task
    t_data = {}

    # This is meant to be overwritten
    module = "change_this_to_module_name"

    def __init__(self, *args, **kwargs):
        """ Extends init in order to store task name before task init """

        self.name = self.__class__.__name__
        super().__init__(*args, **kwargs)

    def output_filename(self, success=True):
        """ Get output filename """

        # output will be a yaml file inside a folder with date
        uri = f"{PATH_LUIGI_YAML}{self.mdate:%Y%m%d}/"

        # make sure folder exists
        os.makedirs(uri, exist_ok=True)

        # add task name
        uri += self.name

        # If task fails write a file with different name
        # This allows re-runs to retry the failed task while keeping info about fails
        if not success:
            uri += datetime.now().strftime("_fail_%Y%m%d_%H%M%S")

        return f"{uri}.yaml"

    def output(self, success=True):
        return luigi.LocalTarget(self.output_filename())

    def save_result(self, success=True, **kwa):
        """ Stores result as a yaml file """

        # Store basic execution info
        self.t_data["end_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.t_data["duration"] = time.time() - self.start_time
        self.t_data["duration_human"] = time_human(self.t_data["duration"])
        self.t_data["success"] = success

        # Allow extra params like 'exception'
        self.t_data.update(**kwa)

        # Export them as an ordered yaml
        with open(self.output_filename(success), "w") as stream:
            yaml.dump(self.t_data, stream)

    def on_failure(self, exception):

        # If there is an error store it anyway
        self.save_result(success=False, exception=repr(exception))
        self.disabled = True

        # If needed, do extra stuff (like log.error)

        # End up raising the error to Luigi
        super().on_failure(exception)

    def run_std(self):
        """
            This is what the task will actually do.

            If it is not overwritten it will 'import module' and then run:

                module.main(mdate)
        """

        # By default run the 'main' function of the asked module
        module = __import__(self.module)
        module.main(self.mdate.strftime("%Y_%m_%d"))

    def run(self):
        # Store start time and task name
        self.t_data["name"] = self.name
        self.start_time = time.time()
        self.t_data["start_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Run the task and store the resutls
        self.run_std()
        self.save_result()


class GitFetchAndPull(StandardTask):
    """ Install python requirements task """

    sentences = ["git fetch", "git checkout master", "git pull origin master"]

    def run_std(self):
        for x in self.sentences:
            check_output(x)


class InstallRequirementsTask(StandardTask):
    """ Install python requirements task """

    def run_std(self):
        check_output("pip install -r requirements.txt")

    def requires(self):
        yield GitFetchAndPull(self.mdate)


class Task(StandardTask):
    """ Standard tasks required pre-tasks to be run """

    def requires(self):
        yield InstallRequirementsTask(self.mdate)
