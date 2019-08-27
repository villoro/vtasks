"""
    Create the raw data for the reprot
"""

import oyaml as yaml
import pandas as pd

from . import data_loader as dl
from . import constants as c
from . import utilities as u


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
        series[f"{name}_by_groups"] = u.series_to_dicts({x: df[x] for x in df.columns})

    return series


def get_investment_or_liquid(dfs, entity):
    """ Retrives investment or liquid data """

    entity_name = entity.split("_")[0]

    dfg = dfs[entity].copy()

    series = {entity_name: u.serie_to_dict(dfg["Total"])}

    aux = {}
    for name, config in YML[c.INVEST].items():

        # Check that accounts are in the config
        mlist = [x for x in config[c.ACCOUNTS] if x in dfg.columns]

        aux[name] = dfg.sum(axis=1)

    series[f"{entity_name}_by_groups"] = u.series_to_dicts(aux)

    return series


def get_report_data():
    """ Create the report """

    dfs = dl.get_dfs()

    out = {}

    # Expenses, incomes and EBIT
    for period, col_period in {"Month": c.COL_MONTH_DATE, "Year": c.COL_YEAR}.items():
        out[period] = get_raw_data(dfs, col_period)

    # Liquid, worth and invested
    for name in [c.DF_LIQUID, c.DF_WORTH, c.DF_INVEST]:
        dfs[name] = dfs[name].set_index(c.COL_DATE)

        out["Month"].update(get_investment_or_liquid(dfs, name))

    return out
