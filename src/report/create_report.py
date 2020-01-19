"""
    Create the raw data for the reprot
"""

import oyaml as yaml
import jinja2

from config import PATH_ROOT

from .data_loader import read_yaml, upload_text_file


def create_report(filename=f"{PATH_ROOT}data.yaml"):
    """ Creates the report """

    # Set up jinja to render parent templates and retrive template
    template = jinja2.Environment(
        loader=jinja2.FileSystemLoader(f"{PATH_ROOT}report/templates")
    ).get_template("template.html")

    # Read data
    data = read_yaml("/data.yaml")

    # Create report
    upload_text_file(template.render(**data), "/report.html")
