from utils import get_vdropbox
from utils import log

from .gcal import PATH_GCAL_DATA

from prefect_task import vtask

PATH_GCAL = "/Aplicaciones/gcalendar"
PATH_CONFUSIONS = f"{PATH_GCAL}/confusions.xlsx"


def clear_potential_confusions(df_in, exclude_other=True, merge_study=True):
    """Clear some potential confusions that are not"""

    df = df_in.copy()

    if exclude_other:
        df = df[df["calendar"] != "71_Other"]

    if merge_study:
        study_related = ["21_Class", "22_Practices", "23_Study", "51_Exams"]
        df.loc[df["calendar"].isin(study_related), "calendar"] = "21-23/51 study_related"

    return df


def get_confusion_matrix(df_in, col_text, col_category):
    df = df_in.copy()
    df["aux"] = 1

    # Count by comment and calendar
    df_agg = df.pivot_table(
        index=col_text, columns=col_category, values="aux", aggfunc="sum"
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


@vtask
def extract_gcal_confusions(exclude_other=True, merge_study=True, min_alpha=0.1):
    vdp = get_vdropbox()

    dfg = vdp.read_parquet(PATH_GCAL_DATA)

    df_aux = clear_potential_confusions(dfg, exclude_other, merge_study)
    df_matrix = get_confusion_matrix(df_aux, col_text="summary", col_category="calendar")
    df_confusions = filter_confusions(df_matrix, min_alpha)

    num_confusions = df_confusions.shape[0]

    if num_confusions > 0:
        log.warning(f"There are {num_confusions} in google calendar. Exporting them")
        vdp.write_excel(df_confusions, PATH_CONFUSIONS)
    else:
        log.success("There are no confusions in google calendar")
