from utils import get_vdropbox

from .gcal import PATH_GCAL_DATA


def get_confusion_matrix(df_in, exclude_other=True):
    df = df_in.copy()
    df["aux"] = 1

    if exclude_other:
        df = df[df["calendar"] != "71_Other"]

    # Count by comment and calendar
    df_agg = df.pivot_table(
        index="summary", columns="calendar", values="aux", aggfunc="sum"
    ).fillna(0)
    df_agg.columns.name = ""
    df_agg.index.name = ""

    # Get percentage of appearance by calendar
    return df_agg.div(df_agg.sum(axis=1), axis=0)


def filter_confusions(df, min_alpha=0.1):
    """Get comments that are repeated in different calendars"""

    df = df[df > 0]
    df_confusions = df[df.count(axis=1) > 1]

    confusions = df_confusions[df_confusions < min_alpha].count(axis=1)
    return df_confusions[confusions > 0].dropna(axis=1, how="all")


def get_confusions(min_alpha=0.1):
    vdp = get_vdropbox()

    dfg = vdp.read_parquet(PATH_GCAL_DATA)

    df_matrix = get_confusion_matrix(dfg)
    return filter_confusions(df_matrix, min_alpha)
