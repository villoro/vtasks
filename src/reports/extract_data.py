"""
    Create the raw data for the reprot
"""

from datetime import date
from collections import OrderedDict

import global_utilities as gu
from global_utilities import log
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

    # Append time averaged data
    for name in [c.EXPENSES, c.INCOMES, c.EBIT]:
        aux = u.time_average(series[name], months=12)["Total"]
        series[f"{name}_12m"] = u.serie_to_dict(aux)

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

    name = entity.split("_")[0]

    dfg = dfs[entity].copy()

    series = {
        name: u.serie_to_dict(dfg["Total"]),
        f"{name}_12m": u.serie_to_dict(u.time_average(dfg, months=12)["Total"]),
    }

    aux = OrderedDict()
    for name in reversed(list(yml.keys())):

        # Check that accounts are in the yml
        mlist = [x for x in yml[name][c.ACCOUNTS] if x in dfg.columns]

        aux[name] = dfg[mlist].sum(axis=1)

    series[f"{name}_by_groups"] = u.series_to_dicts(aux)

    return series


def get_colors(dfs, yml):
    """ Get colors from config file """

    out = {name: u.get_colors(data) for name, data in c.DEFAULT_COLORS.items()}

    for entity in [c.LIQUID, c.INVEST]:
        out[f"{entity}_categ"] = OrderedDict()
        for name, config in yml[entity].items():
            out[f"{entity}_categ"][name] = u.get_colors(
                (config[c.COLOR_NAME], config[c.COLOR_INDEX])
            )

    for entity, df in dfs["trans_categ"].set_index("Name").groupby("Type"):
        out[f"{entity}_categ"] = OrderedDict()
        for name, row in df.iterrows():
            out[f"{entity}_categ"][name] = u.get_colors((row["Color Name"], row["Color Index"]))

    return out


def main(mdate=date.today()):
    """ Create the report """

    dbx = gu.dropbox.get_dbx_connector(c.VAR_DROPBOX_TOKEN)

    # Get dfs
    log.info("Reading excels from dropbox")
    dfs = gu.dropbox.read_excel(dbx, c.FILE_DATA, c.DFS_ALL_FROM_DATA)
    dfs[c.DF_TRANS] = gu.dropbox.read_excel(dbx, c.FILE_TRANSACTIONS)

    yml = gu.dropbox.read_yaml(dbx, c.FILE_CONFIG)

    out = {}

    # Expenses, incomes and EBIT
    log.info("Extracting expenses, incomes and EBIT")
    for period, col_period in {"month": c.COL_MONTH_DATE, "year": c.COL_YEAR}.items():
        out[period] = get_raw_data(dfs, col_period)

    # Liquid, worth and invested
    log.info("Adding liquid, worth and invested")
    for name, yml_name in [
        (c.DF_LIQUID, c.LIQUID),
        (c.DF_WORTH, c.INVEST),
        (c.DF_INVEST, c.INVEST),
    ]:
        dfs[name] = dfs[name].set_index(c.COL_DATE)

        out["month"].update(get_investment_or_liquid(dfs, yml[yml_name], name))

    log.info("Appending colors")
    out["colors"] = get_colors(dfs, yml)

    gu.dropbox.write_yaml(dbx, out, f"/report_data/{mdate:%Y_%m}.yaml")


if __name__ == "__main__":
    main()
