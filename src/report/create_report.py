"""
    Create the raw data for the reprot
"""

from datetime import date

import oyaml as yaml
import jinja2

from config import PATH_ROOT

import global_utilities as gu
from global_utilities import log
from . import constants as c


def main(mdate=date.today()):
    """ Creates the report """

    dbx = gu.dropbox.get_dbx_connector(c.VAR_DROPBOX_TOKEN)

    # Set up jinja to render parent templates and retrive template
    template = jinja2.Environment(
        loader=jinja2.FileSystemLoader(f"{PATH_ROOT}src/report/templates")
    ).get_template("template.html")

    # Read data
    log.info("Reading report_data from dropbox")
    data = gu.dropbox.read_yaml(dbx, f"/report_data/{mdate:%Y_%m}.yaml")

    # Create report
    gu.dropbox.write_textfile(dbx, template.render(**data), f"/reports/{mdate:%Y_%m}.html")


if __name__ == "__main__":
    main()
