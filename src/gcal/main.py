from datetime import date

from prefect import flow

from .confusions import extract_gcal_confusions
from .export import export_calendar_events
from .report import gcal_report
from .summary import do_summary


@flow(name="vtasks.gcal")
def gcal(mdate: date):
    _export_cal = export_calendar_events(mdate)
    extract_gcal_confusions(wait_for=[_export_cal])
    gcal_report(mdate, wait_for=[_export_cal])
    do_summary(mdate, wait_for=[_export_cal])
