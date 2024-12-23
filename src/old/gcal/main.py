from datetime import date

from prefect import flow

import utils as u

from .confusions import extract_gcal_confusions
from .export import export_calendar_events
from .report import gcal_report
from .summary import needs_summary
from .summary import process_summary


@flow(**u.get_prefect_args("vtasks.gcal"))
def gcal(mdate: date):
    _export_cal = export_calendar_events(mdate)
    extract_gcal_confusions(wait_for=[_export_cal])
    gcal_report(mdate, wait_for=[_export_cal])
    if needs_summary(mdate, wait_for=[_export_cal]):
        process_summary(mdate)
