"""
    Create the raw data for the reprot
"""

import pandas as pd
from datetime import date
from collections import OrderedDict

import global_utilities as gu
from global_utilities import log
from . import constants as c
from . import utilities as u


def get_basic_traces(dfs, col_period):
    """
        Extract Incomes, Expenses, EBIT and savings traces

        Args:
            dfs:        dict with dataframes
            col_period: month or year
    """

    series = {
        name: df.groupby(col_period)[c.COL_AMOUNT].sum()
        for name, df in dfs[c.DF_TRANS].groupby(c.COL_TYPE)
    }

    # Extract expenses and incomes
    series[c.EXPENSES], series[c.INCOMES] = u.normalize_index(series[c.EXPENSES], series[c.INCOMES])
    series[c.EBIT] = series[c.INCOMES] - series[c.EXPENSES]

    # Add savings ratio
    series[c.SAVINGS] = (series[c.EBIT] / series[c.INCOMES]).apply(lambda x: max(0, x))

    out = u.series_to_dicts(series)

    # Append time averaged data
    if col_period == c.COL_MONTH_DATE:
        for name, serie in series.items():
            out[f"{name}_12m"] = u.serie_to_dict(u.time_average(serie, months=12))
            out[f"{name}_6m_e"] = u.serie_to_dict(u.time_average(serie, months=6, exponential=True))

    # Get by groups
    for name, dfg in dfs[c.DF_TRANS].groupby(c.COL_TYPE):

        df = dfg.pivot_table(c.COL_AMOUNT, col_period, c.COL_CATEGORY, "sum").fillna(0)

        df_categ = dfs[c.DF_CATEG]
        df_categ = df_categ[df_categ[c.COL_TYPE] == name]

        aux = OrderedDict()
        for x in reversed(df_categ[c.COL_NAME].to_list()):
            aux[x] = df[x]

        out[f"{name}_by_groups"] = u.series_to_dicts(aux)

    return out


def get_investment_or_liquid(dfs, yml, entity):
    """
        Retrives investment or liquid data

        Args:
            dfs:    dict with dataframes
            yml:    dict with config info
            entity: entity to process
    """

    dfg = dfs[entity].copy()

    entity = entity.split("_")[0].title()

    out = {
        entity: u.serie_to_dict(dfg["Total"]),
        f"{entity}_12m": u.serie_to_dict(u.time_average(dfg, months=12)["Total"]),
        f"{entity}_6m_e": u.serie_to_dict(u.time_average(dfg, months=6, exponential=True)["Total"]),
    }

    aux = OrderedDict()
    for name in reversed(list(yml.keys())):

        # Check that accounts are in the yml
        mlist = [x for x in yml[name][c.ACCOUNTS] if x in dfg.columns]

        aux[name] = dfg[mlist].sum(axis=1)

    out[f"{entity}_by_groups"] = u.series_to_dicts(aux)

    return out


def get_comparison_traces(dfs):
    """
        Add traces for comparison plots

        Args:
            dfs:    dict with dataframes
    """

    out = {}

    get_traces = (
        lambda df: df.reset_index()
        .pivot_table(c.COL_AMOUNT, c.COL_MONTH, c.COL_YEAR, "sum")
        .apply(lambda x: round(x, 2))
        .fillna("null")
        .to_dict()
    )

    # Expenses and incomes
    for name, dfg in dfs[c.DF_TRANS].groupby(c.COL_TYPE):
        df = dfg.groupby([c.COL_YEAR, c.COL_MONTH]).agg({c.COL_AMOUNT: "sum"})
        out[name] = get_traces(u.time_average(df))

    # Prepare transactions for EBIT
    dfg = dfs[c.DF_TRANS].copy()
    mfilter = dfg[c.COL_TYPE] == c.EXPENSES
    dfg.loc[mfilter, c.COL_AMOUNT] = -dfg.loc[mfilter, c.COL_AMOUNT]

    # Add EBIT
    df = dfg.groupby([c.COL_YEAR, c.COL_MONTH]).agg({c.COL_AMOUNT: "sum"})
    out[c.EBIT] = get_traces(u.time_average(df))

    # Add liquid
    dfg = dfs[c.DF_LIQUID].reset_index().copy()
    dfg[c.COL_MONTH] = pd.to_datetime(dfg[c.COL_DATE]).dt.month
    dfg[c.COL_YEAR] = pd.to_datetime(dfg[c.COL_DATE]).dt.year
    dfg[c.COL_AMOUNT] = dfg["Total"]
    df = dfg.groupby([c.COL_YEAR, c.COL_MONTH]).agg({c.COL_AMOUNT: "sum"})
    out[c.LIQUID] = get_traces(u.time_average(df, months=3, exponential=True))

    return out


def get_pie_traces(dfs):
    """
        Add traces for pie plots

        Args:
            dfs:    dict with dataframes
    """

    out = {}
    for name, dfg in dfs[c.DF_TRANS].groupby(c.COL_TYPE):

        df_cat = dfs[c.DF_CATEG]
        categories = sorted(df_cat[df_cat[c.COL_TYPE] == name][c.COL_NAME].tolist(), reverse=True)

        df = dfg.pivot_table(c.COL_AMOUNT, c.COL_MONTH_DATE, c.COL_CATEGORY, "sum").fillna(0)

        export_trace = lambda serie: u.serie_to_dict(serie[categories])

        out[name] = {
            "last_1m": export_trace(df.iloc[-1, :]),
            "last_12m": export_trace(df.iloc[-12:, :].sum()),
            "all": export_trace(df.sum()),
        }

    return out


def get_colors_comparisons(dfs):
    """
        Get colors for comparison plots
        
        Args:
            dfs:    dict with dataframes
    """

    def extract_colors_from_years(years, color_name):
        """
            Extract colors from a list of years
            
            Args:
                years:      series with years
                color_name: name of the color
        """

        out = {}
        for year in sorted(years.unique().tolist(), reverse=True):
            color_index = max(100, 900 - 200 * (max(years) - year))
            size = max(6 - (max(years) - year), 1)

            out[year] = {"color": u.get_colors((color_name, color_index)), "size": size}

        return out

    # Incomes, Expenses and EBIT
    out = {}
    for name, color_name in [(c.INCOMES, "green"), (c.EXPENSES, "red"), (c.EBIT, "amber")]:
        out[name] = extract_colors_from_years(dfs[c.DF_TRANS][c.COL_YEAR], color_name)

    # Liquid
    years = pd.to_datetime(dfs[c.DF_LIQUID].reset_index()[c.COL_DATE]).dt.year
    out[c.LIQUID] = extract_colors_from_years(years, "blue")

    return out


def get_colors(dfs, yml):
    """ Get colors from config file """

    out = {name: u.get_colors(data) for name, data in c.DEFAULT_COLORS.items()}

    # Liquid and investments colors
    for entity in [c.LIQUID, c.INVEST]:
        out[f"{entity}_categ"] = OrderedDict()
        for name, config in yml[entity].items():
            out[f"{entity}_categ"][name] = u.get_colors(
                (config[c.COLOR_NAME], config[c.COLOR_INDEX])
            )

    # Expenses and incomes colors
    for entity, df in dfs["trans_categ"].set_index("Name").groupby("Type"):
        out[f"{entity}_categ"] = OrderedDict()
        for name, row in df.iterrows():
            out[f"{entity}_categ"][name] = u.get_colors((row["Color Name"], row["Color Index"]))

    # Colors comparison plot
    out["comp"] = get_colors_comparisons(dfs)

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

    # Expenses, incomes, EBIT and Savings ratio
    log.info("Extracting expenses, incomes, EBIT and savings ratio")
    for period, col_period in {"month": c.COL_MONTH_DATE, "year": c.COL_YEAR}.items():
        out[period] = get_basic_traces(dfs, col_period)

    # Liquid, worth and invested
    log.info("Adding liquid, worth and invested")
    for name, yml_name in [
        (c.DF_LIQUID, c.LIQUID),
        (c.DF_WORTH, c.INVEST),
        (c.DF_INVEST, c.INVEST),
    ]:
        dfs[name] = dfs[name].set_index(c.COL_DATE)

        out["month"].update(get_investment_or_liquid(dfs, yml[yml_name], name))

    # Comparison traces
    log.info("Adding comparison traces")
    out["comp"] = get_comparison_traces(dfs)

    # Pie traces
    log.info("Adding pie traces")
    out["pies"] = get_pie_traces(dfs)

    # Add colors
    log.info("Appending colors")
    out["colors"] = get_colors(dfs, yml)

    gu.dropbox.write_yaml(dbx, out, f"/report_data/{mdate:%Y_%m}.yaml")


if __name__ == "__main__":
    main()
