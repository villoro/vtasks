"""
    Create the raw data for the reprot
"""

import oyaml as yaml
import jinja2

from config import PATH_ROOT

import global_utilities as gu


def create_report(filename=f"{PATH_ROOT}data.yaml"):
    """ Creates the report """

    dbx = gu.dropbox.get_dbx_connector(c.VAR_DROPBOX_TOKEN)

    # Set up jinja to render parent templates and retrive template
    template = jinja2.Environment(
        loader=jinja2.FileSystemLoader(f"{PATH_ROOT}report/templates")
    ).get_template("template.html")

    # Read data
    data = gu.dropbox.read_yaml(dbx, "/data.yaml")

    # Create report
    gu.dropbox.write_textfile(dbx, template.render(**data), "/report.html")
