"""
    Create the raw data for the reprot
"""

from datetime import datetime

import utils as u

from . import constants as c
from utils import log


def main(mdate=datetime.now(), data=None):
    """ Creates the report """

    mdate = mdate.replace(day=1)

    vdp = u.get_vdropbox()

    # Read data
    if data is None:
        log.debug("Reading report_data from dropbox")
        data = vdp.read_yaml(f"{c.PATH_EXPENSOR}/report_data/{mdate.year}/{mdate:%Y_%m}.yaml")

    # Add title
    data["mdate"] = f"{mdate:%Y_%m}"
    data["title"] = f"{mdate:%Y_%m} Expensor"
    data["sections"] = {
        "evolution": "fa-chart-line",
        "comparison": "fa-poll",
        "pies": "fa-chart-pie",
        "liquid": "fa-tint",
        "investments": "fa-wallet",
        "fire": "fa-fire-alt",
        "sankey": "fa-stream",
    }

    # Create report
    report = u.render_jinja_template("expensor.html", data)
    vdp.write_file(report, f"{c.PATH_EXPENSOR}/reports/{mdate.year}/{mdate:%Y_%m}.html")
