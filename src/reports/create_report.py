"""
    Create the raw data for the reprot
"""

from datetime import datetime

import oyaml as yaml
import jinja2

from config import PATH_ROOT

import global_utilities as gu
from global_utilities import log
from . import constants as c


def main(mdate=datetime.now()):
    """ Creates the report """

    dbx = gu.dropbox.get_dbx_connector(c.VAR_DROPBOX_TOKEN)

    # Read data
    log.debug("Reading report_data from dropbox")
    data = gu.dropbox.read_yaml(dbx, f"/report_data/{mdate.year}/{mdate:%Y_%m}.yaml")

    # Add title
    data["mdate"] = f"{mdate:%Y_%m}"
    data["title"] = f"{mdate:%Y_%m} Expensor"

    # Set up jinja to render parent templates and retrive template
    template = jinja2.Environment(
        loader=jinja2.FileSystemLoader(f"{PATH_ROOT}src/reports/templates")
    ).get_template("template.html")

    # Create report
    report = template.render(**data)
    gu.dropbox.write_textfile(dbx, report, f"/reports/{mdate.year}/{mdate:%Y_%m}.html")
