COL_EXPORTED_AT = "exported_at"
COL_CREATED = "created"
COL_NAME = "name"
COL_FLOW_ID = "flow_id"
COL_FLOW_NAME = "flow_name"
COL_STATE = "state_name"
COL_START = "start_time"
COL_END = "end_time"
COL_DAY = "day"
COL_TAGS = "tags"
COL_ENV = "environment"

STATE_COMPLETED = "Completed"
STATE_RUNNING = "Running"

PATH_VTASKS = "/Aplicaciones/vtasks"
PATH_FLOW_RUNS = f"{PATH_VTASKS}/flows.parquet"
PATH_TASK_RUNS = f"{PATH_VTASKS}/tasks.parquet"
PATH_REPORT = f"{PATH_VTASKS}/vtasks.html"

COLORS = {
    "vtasks": ("black", 500),
    "vtasks.money_lover": ("yellow", 500),
    "vtasks.crypto": ("orange", 300),
    "vtasks.indexa": ("orange", 500),
    "vtasks.expensor": ("red", 500),
    "vtasks.flights": ("blue", 500),
    "vtasks.flights_history": ("blue", 200),
    "vtasks.vbooks": ("green", 300),
    "vtasks.archive": ("grey", 500),
    "vtasks.gcal": ("green", 500),
    "vtasks.backup": ("purple", 500),
    "vtasks.vprefect": ("brown", 500),
}
