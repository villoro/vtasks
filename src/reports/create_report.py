"""
    Create the raw data for the reprot
"""

from datetime import datetime

import jinja2
import oyaml as yaml

import global_utilities as gu

from . import constants as c
from global_utilities import log


def main(mdate=datetime.now(), data=None):
    """ Creates the report """

    mdate = mdate.replace(day=1)

    dbx = gu.dropbox.get_dbx_connector(c.VAR_DROPBOX_TOKEN)

    # Read data
    if data is None:
        log.debug("Reading report_data from dropbox")
        data = gu.dropbox.read_yaml(dbx, f"/report_data/{mdate.year}/{mdate:%Y_%m}.yaml")

    # Add title
    data["mdate"] = f"{mdate:%Y_%m}"
    data["title"] = f"{mdate:%Y_%m} Expensor"

    # Set up jinja to render parent templates and retrive template
    template = jinja2.Environment(
        loader=jinja2.FileSystemLoader(gu.get_path("src/reports/templates"))
    ).get_template("template.html")

    # Create report
    report = template.render(**data)
    gu.dropbox.write_textfile(dbx, report, f"/reports/{mdate.year}/{mdate:%Y_%m}.html")
