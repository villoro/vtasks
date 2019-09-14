"""
    Create the raw data for the reprot
"""

from collections import OrderedDict
import oyaml as yaml
import pandas as pd

from . import constants as c
from . import utilities as u
from .data_loader import get_dfs, get_config, upload_yaml


def get_raw_data(dfs, col_period):

    series = {
        name: df.groupby(col_period)[c.COL_AMOUNT].sum()
        for name, df in dfs[c.DF_TRANS].groupby(c.COL_TYPE)
    }

    series[c.EXPENSES], series[c.INCOMES] = u.normalize_index(series[c.EXPENSES], series[c.INCOMES])
    series[c.EBIT] = series[c.INCOMES] - series[c.EXPENSES]

    series = u.series_to_dicts(series)

    # Get by groups
    for name, dfg in dfs[c.DF_TRANS].groupby(c.COL_TYPE):

        df = dfg.pivot_table(c.COL_AMOUNT, col_period, c.COL_CATEGORY, "sum").fillna(0)

        df_categ = dfs[c.DF_CATEG]
        df_categ = df_categ[df_categ[c.COL_TYPE] == name]

        aux = OrderedDict()
        for x in reversed(df_categ[c.COL_NAME].to_list()):
            aux[x] = df[x]

        series[f"{name}_by_groups"] = u.series_to_dicts(aux)

    return series


def get_investment_or_liquid(dfs, yml, entity):
    """
        Retrives investment or liquid data

        Args:
            dfs:    dict with dataframes
            yml:    dict with config info
            entity: entity to process
    """

    entity_name = entity.split("_")[0]

    dfg = dfs[entity].copy()

    series = {entity_name: u.serie_to_dict(dfg["Total"])}

    aux = OrderedDict()
    for name, config in yml[c.INVEST].items():

        # Check that accounts are in the config
        mlist = [x for x in config[c.ACCOUNTS] if x in dfg.columns]

        aux[name] = dfg.sum(axis=1)

    series[f"{entity_name}_by_groups"] = u.series_to_dicts(aux)

    return series


def get_colors(dfs, yml):
    """ Get colors from config file """

    out = {name: u.get_colors(data) for name, data in c.DEFAULT_COLORS.items()}

    for entity in [c.LIQUID, c.INVEST]:
        out[entity] = OrderedDict()
        for name, config in yml[entity].items():
            out[entity][name] = u.get_colors((config[c.COLOR_NAME], config[c.COLOR_INDEX]))

    for entity, df in dfs["trans_categ"].set_index("Name").groupby("Type"):
        out[f"{entity}_categ"] = OrderedDict()
        for name, row in df.iterrows():
            out[f"{entity}_categ"][name] = u.get_colors((row["Color Name"], row["Color Index"]))

    return out


def get_report_data():
    """ Create the report """

    dfs = get_dfs()
    yml = get_config()

    out = {}

    # Expenses, incomes and EBIT
    for period, col_period in {"month": c.COL_MONTH_DATE, "year": c.COL_YEAR}.items():
        out[period] = get_raw_data(dfs, col_period)

    # Liquid, worth and invested
    for name in [c.DF_LIQUID, c.DF_WORTH, c.DF_INVEST]:
        dfs[name] = dfs[name].set_index(c.COL_DATE)

        out["month"].update(get_investment_or_liquid(dfs, yml, name))

    out["colors"] = get_colors(dfs, yml)

    upload_yaml(out, "/data.yaml")  # TODO: change that name
