"""
    Create the raw data for the reprot
"""

import oyaml as yaml
import jinja2

from config import PATH_ROOT


def create_report(filename=f"{PATH_ROOT}data.yaml"):
    """ Creates the report """

    # Set up jinja to render parent templates and retrive template
    template = jinja2.Environment(
        loader=jinja2.FileSystemLoader(f"{PATH_ROOT}report/templates")
    ).get_template("template.html")

    # Read data
    with open(filename, "r") as file:
        data = yaml.safe_load(file)

    # Render template and save file
    with open(f"{PATH_ROOT}report.html", "w") as file:
        file.write(template.render(**data))
