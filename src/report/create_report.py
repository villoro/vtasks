"""
    Create the raw data for the reprot
"""

import oyaml as yaml
import jinja2

from config import PATH_ROOT


def create_report(filename=f"{PATH_ROOT}data.yaml"):
    """ Creates the report """

    # Read data
    with open(filename, "r") as file:
        data = yaml.safe_load(file)

    # Get template
    with open(f"{PATH_ROOT}report/template/template.html", "r") as file:
        template = jinja2.Template(file.read())

    # Render template and save file
    with open(f"{PATH_ROOT}report.html", "w") as file:
        file.write(template.render(**data))
