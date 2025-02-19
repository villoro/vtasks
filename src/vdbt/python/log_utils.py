# This is needed to use `prefect.logger` inside a `DBT.callback`
# DO NOT USE FOR ANYTHING OTHER THAN A DBT.CALLBACK!
LOGGER = None

EVENTS_TO_LOG = [
    "AdapterRegistered",
    "CommandCompleted",
    "ConcurrencyLine",
    "EndOfRunSummary",
    "FinishedRunningStats",
    "FoundStats",
    "HooksRunning",
    "LogHookEndLine",
    "LogHookStartLine",
    "LogModelResult",
    "LogStartLine",
    "LogTestResult",
    "MainReportVersion",
    "NodeStart",
    "RunResultError",
    "SkippingDetails",
    "StatsLine",
    "UnusedResourceConfigPath",
    "LogFreshnessResult",
]


def log_callback(event_raw):
    # In order to use 'prefect.logger' here we need it declared it as global (LOGGER)

    event = event_raw.info

    if (name := event.name) not in EVENTS_TO_LOG:
        return None

    level = event.level
    msg = f"{event.msg} [{name}]"

    if level == "error":
        LOGGER.error(msg)
    elif level == "warn":
        LOGGER.warning(msg)
    elif (level == "info") or (name == "CommandCompleted"):
        LOGGER.info(msg)
    else:
        LOGGER.debug(msg)

    return None
