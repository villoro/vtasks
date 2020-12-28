"""
    Create the raw data for the reprot
"""

from datetime import datetime

import jinja2
import oyaml as yaml

from utils import get_path
from utils import get_secret

from . import constants as c
from utils import get_vdropbox
from utils import log


def main(mdate=datetime.now(), data=None):
    """ Creates the report """

    mdate = mdate.replace(day=1)

    vdp = get_vdropbox()

    # Read data
    if data is None:
        log.debug("Reading report_data from dropbox")
        data = vdp.read_yaml(f"/report_data/{mdate.year}/{mdate:%Y_%m}.yaml")

    # Add title
    data["mdate"] = f"{mdate:%Y_%m}"
    data["title"] = f"{mdate:%Y_%m} Expensor"

    # Set up jinja to render parent templates and retrive template
    template = jinja2.Environment(
        loader=jinja2.FileSystemLoader(get_path("templates"))
    ).get_template("expensor.html")

    # Create report
    report = template.render(**data)
    vdp.write_file(report, f"/reports/{mdate.year}/{mdate:%Y_%m}.html")
