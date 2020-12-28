"""
    Create the raw data for the reprot
"""

import numpy as np
import pandas as pd

from collections import OrderedDict
from datetime import datetime
from vpalette import get_colors

from . import constants as c
from .functions import add_missing_months
from .functions import filter_by_date
from .functions import serie_to_dict
from .functions import series_to_dicts
from .functions import time_average
from utils import get_vdropbox
from utils import log
from utils import read_df_gdrive


def get_basic_traces(dfs, col_period, mdate):
    """
        Extract Incomes, Expenses, EBIT and savings traces

        Args:
            dfs:        dict with dataframes
            col_period: month or year
            mdate:      date of the report
    """

    series = {}
    for name, df in dfs[c.DF_TRANS].groupby(c.COL_TYPE):

        aux = df.groupby(col_period)[c.COL_AMOUNT].sum()

        # Add missing months for month grouping
        if col_period == c.COL_MONTH_DATE:
            aux = add_missing_months(aux, mdate).fillna(0)

        series[name] = aux

    # Extract expenses and incomes
    series[c.EBIT] = series[c.INCOMES] - series[c.EXPENSES]

    # Add savings ratio
    series[c.SAVINGS] = (100 * series[c.EBIT] / series[c.INCOMES]).apply(lambda x: max(0, x))

    out = series_to_dicts(series)

    # Append time averaged data
    if col_period == c.COL_MONTH_DATE:
        for name, serie in series.items():
            out[f"{name}_12m"] = serie_to_dict(time_average(serie, months=12))
            out[f"{name}_6m_e"] = serie_to_dict(time_average(serie, months=6, exponential=True))

    # Get by groups
    for name, dfg in dfs[c.DF_TRANS].groupby(c.COL_TYPE):

        df = dfg.pivot_table(c.COL_AMOUNT, col_period, c.COL_CATEGORY, "sum").fillna(0)

        df_categ = dfs[c.DF_CATEG]
        df_categ = df_categ[df_categ[c.COL_TYPE] == name]

        aux = OrderedDict()
        for x in reversed(df_categ.index.to_list()):
            if x in df.columns:
                aux[x] = df[x]

        out[f"{name}_by_groups"] = series_to_dicts(aux)

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
        entity: serie_to_dict(dfg["Total"]),
        f"{entity}_12m": serie_to_dict(time_average(dfg, months=12)["Total"]),
        f"{entity}_6m_e": serie_to_dict(time_average(dfg, months=6, exponential=True)["Total"]),
    }

    aux = OrderedDict()
    for name in reversed(list(yml.keys())):

        # Check that accounts are in the yml
        mlist = [x for x in yml[name][c.ACCOUNTS] if x in dfg.columns]

        aux[name] = dfg[mlist].sum(axis=1)

    out[f"{entity}_by_groups"] = series_to_dicts(aux)

    return out


def get_total_investments(data):
    """
        Extract data for dashboard cards
        
        Args:
            data:   dict with data
    """

    liquid = pd.Series(data["month"][c.LIQUID])
    worth = pd.Series(data["month"]["Worth"])
    invest = pd.Series(data["month"]["Invest"])

    return {
        "Total_Worth": serie_to_dict((liquid + worth).dropna()),
        "Total_Invest": serie_to_dict((liquid + invest).dropna()),
    }


def get_salaries(dfs, mdate):
    """
        Extract salaries

        Args:
            data:   dict with data
    """

    df = dfs[c.DF_SALARY].copy()

    # First complete data from previous months then with 0
    df = add_missing_months(df, mdate)
    df = df.fillna(method="ffill").fillna(0)

    return {
        "salary": {
            "real": serie_to_dict(df["Total"]),
            "full_time": serie_to_dict(df["EAGI"]),
            "fixed": serie_to_dict(df["Fixed"]),
        }
    }


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
        out[name] = get_traces(time_average(df))

    # Prepare transactions for EBIT
    dfg = dfs[c.DF_TRANS].copy()
    mfilter = dfg[c.COL_TYPE] == c.EXPENSES
    dfg.loc[mfilter, c.COL_AMOUNT] = -dfg.loc[mfilter, c.COL_AMOUNT]

    # Add EBIT
    df = dfg.groupby([c.COL_YEAR, c.COL_MONTH]).agg({c.COL_AMOUNT: "sum"})
    out[c.EBIT] = get_traces(time_average(df))

    # Add liquid
    dfg = dfs[c.DF_LIQUID].reset_index().copy()
    dfg[c.COL_MONTH] = pd.to_datetime(dfg[c.COL_DATE]).dt.month
    dfg[c.COL_YEAR] = pd.to_datetime(dfg[c.COL_DATE]).dt.year
    dfg[c.COL_AMOUNT] = dfg["Total"]
    df = dfg.groupby([c.COL_YEAR, c.COL_MONTH]).agg({c.COL_AMOUNT: "sum"})
    out[c.LIQUID] = get_traces(time_average(df, months=3, exponential=True))

    log.debug("Comparison traces added")

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
        categories = df_cat[df_cat[c.COL_TYPE] == name].index.tolist()

        df = dfg.pivot_table(c.COL_AMOUNT, c.COL_MONTH_DATE, c.COL_CATEGORY, "sum").fillna(0)

        def export_trace(serie):
            """ Extract all possible categories """

            # Keep only present categories
            indexs = [x for x in categories if x in serie.index]

            # Reverse categories order
            return serie_to_dict(serie[indexs][::-1])

        out[name] = {
            "month": export_trace(df.iloc[-1, :]),
            "year": export_trace(df[df.index.year == df.index.year[-1]].sum()),
            "all": export_trace(df.sum()),
        }

    log.debug("Pie traces added")

    return out


def get_dashboard(data, mdate):
    """
        Extract data for dashboard cards
        
        Args:
            data:   dict with data
            mdate:  date of the report
    """

    traces = [c.EXPENSES, c.INCOMES, c.EBIT, c.LIQUID]
    traces += [x + "_12m" for x in traces] + ["Worth", "Invest"]

    out = {}

    for tw in ["month", "year"]:

        if tw == "month":
            date_index = mdate.strftime("%Y-%m-%d")
        else:
            date_index = mdate.year

        out[tw] = {}

        # Basic traces
        for name in traces:
            mdict = data[tw].get(name, None)

            if mdict is not None:
                out[tw][name] = mdict[date_index]

        # Traces by groups
        for name in [f"{c.EXPENSES}_by_groups", f"{c.INCOMES}_by_groups"]:

            out[tw][name] = {}

            for categ, mdict in data[tw][name].items():
                value = mdict.get(date_index, 0)
                if value > 0:
                    out[tw][name][categ] = value

    # Add value of end of year before worth, invested and liquid
    for name in [c.LIQUID, "Worth", "Invest"]:
        mdict = data["month"][name]

        dates = [x for x in mdict.keys() if x.startswith(str(mdate.year - 1))]

        if dates:
            value = mdict.get(max(dates), 0)
        else:
            value = 0

        out["month"][f"{name}_1y"] = value

    # Invest last month
    mdict = data["month"]["Invest"]
    out["month"]["Invest_1m"] = mdict[list(mdict.keys())[-2]]

    # Add totals
    for name in ["Worth", "Invest"]:
        # Values for actual year
        out["month"][f"Total_{name}"] = round(out["month"][c.LIQUID] + out["month"][name], 2)

        # Values for a year before
        aux = out["month"][f"{c.LIQUID}_1y"] + out["month"][f"{name}_1y"]
        out["month"][f"Total_{name}_1y"] = round(aux, 2)

    log.debug("Dashboard info added")

    return out


def get_ratios(data):
    """ Calculate ratios """

    aux = {}
    names = [
        c.INCOMES,
        c.EXPENSES,
        c.LIQUID,
        "Worth",
        "Invest",
        f"{c.EXPENSES}_12m",
        f"{c.EXPENSES}_6m_e",
        f"{c.LIQUID}_6m_e",
    ]
    for name in names:
        aux[name] = pd.Series(data["month"][name])
    liquid = pd.Series(data["month"][c.LIQUID])

    out = {
        f"{c.LIQUID}/{c.EXPENSES}": aux[c.LIQUID] / aux[c.EXPENSES],
        f"{c.LIQUID}_6m_e/{c.EXPENSES}_12m": aux[f"{c.LIQUID}_6m_e"] / aux[f"{c.EXPENSES}_12m"],
        f"Total_Worth/{c.EXPENSES}": (aux["Worth"] + aux[c.LIQUID]) / (12 * aux[c.EXPENSES]),
        f"Total_Worth_6m_e/{c.EXPENSES}_6m_e": (aux["Worth"] + aux[f"{c.LIQUID}_6m_e"])
        / (12 * aux[f"{c.EXPENSES}_12m"]),
        "Total_worth_performance": 100 * (aux["Worth"] + aux[c.LIQUID]) / aux[c.INCOMES].cumsum(),
        "Total_invest_performance": 100 * (aux["Invest"] + aux[c.LIQUID]) / aux[c.INCOMES].cumsum(),
    }

    # Drop nans and round values
    for name, serie in out.items():
        serie = serie.replace([np.inf, -np.inf], np.nan)
        out[name] = serie_to_dict(serie.dropna())

    out["Worth_by_groups"] = {}
    for name, values in data["month"]["Worth_by_groups"].items():
        out["Worth_by_groups"][name] = serie_to_dict(
            100 * pd.Series(values) / pd.Series(data["month"]["Worth"])
        )

    log.debug("Ratios info added")

    return out


def get_bubbles(dfs, mdate, min_year=2011):
    """ Get info for bubbles plot """

    # Get expenses/incomes and extrapolate for last year if necessary
    aux = {}
    for name, df in dfs[c.DF_TRANS].groupby(c.COL_TYPE):
        dfa = df.groupby(c.COL_YEAR).agg({c.COL_AMOUNT: "sum", c.COL_MONTH: "nunique"})
        aux[name] = dfa[c.COL_AMOUNT] * 12 / dfa[c.COL_MONTH]

    def get_year(dfi):
        """ Get year data """
        df = dfi.copy()
        df["Year"] = df.index.year
        return df.drop_duplicates("Year", keep="last").set_index("Year")["Total"]

    aux["Liquid"] = get_year(dfs[c.DF_LIQUID])
    aux["Worth"] = get_year(dfs[c.DF_WORTH])

    # Create dataframe
    df = pd.DataFrame(aux).fillna(0)

    df["Total_Worth"] = df["Worth"] + df["Liquid"]
    df["savings"] = 100 * (df["Incomes"] - df["Expenses"]) / df["Incomes"]
    df["doomsday"] = df["Total_Worth"] / df["Expenses"]

    # First year has strange values
    df = df[df.index > min_year]

    df = df.apply(lambda x: round(x, 2))

    out = []
    for i, row in df.iterrows():
        out.append(
            f'x: {row["doomsday"]}, y: {row["savings"]}, z: {row["Expenses"]}, name: {i}, '
            f'incomes: {row["Incomes"]}, total_worth: {row["Total_Worth"]}'
        )

    return out


def extract_sankey(data):
    """ Calculate Sankey flows """

    out = {}
    for tw in ["month", "year"]:
        mdict = data["dash"][tw].copy()

        incomes = mdict[c.INCOMES]
        expenses = mdict[c.EXPENSES]
        ebit = incomes - expenses

        if tw == "month":
            invest = mdict["Invest"] - mdict["Invest_1m"]
        else:
            invest = data["dash"]["month"]["Invest"] - data["dash"]["month"]["Invest_1y"]

        # Values for traces
        incomes_to_invest = max(min(ebit, invest), 0)
        incomes_to_savings = max(ebit - max(invest, 0), 0)
        invest_to_expenses = max(-max(invest, ebit), 0)
        invest_to_savings = -min(invest + invest_to_expenses, 0)
        savings_to_expenses = -min(ebit + invest_to_expenses, 0)
        savings_to_invest = max(invest - incomes_to_invest, 0)

        # Create flows
        aux = [
            [c.INCOMES, c.EXPENSES, min(incomes, expenses)],
            [c.INCOMES, "Investments", incomes_to_invest],
            [c.INCOMES, "Savings", incomes_to_savings],
            ["Investments", c.EXPENSES, invest_to_expenses],
            ["Investments", "Savings", invest_to_savings],
            ["Savings", c.EXPENSES, savings_to_expenses],
            ["Savings", "Investments", savings_to_invest],
        ]

        # Add Incomes
        for name, value in mdict[f"{c.INCOMES}_by_groups"].items():
            aux.append([name, c.INCOMES, value])

        # Add Expenes
        for name, value in mdict[f"{c.EXPENSES}_by_groups"].items():
            aux.append([c.EXPENSES, name, value])

        # Prune flows = 0 and round the others
        out[tw] = [f'"{x}", "{y}", {value:.2f}' for x, y, value in aux if value > 0]

    log.debug("Sankey info added")
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

            out[year] = {"color": get_colors((color_name, color_index)), "size": size}

        return out

    # Incomes, Expenses and EBIT
    out = {}
    for name, color_name in [(c.INCOMES, "green"), (c.EXPENSES, "red"), (c.EBIT, "amber")]:
        out[name] = extract_colors_from_years(dfs[c.DF_TRANS][c.COL_YEAR], color_name)

    # Liquid
    years = pd.to_datetime(dfs[c.DF_LIQUID].reset_index()[c.COL_DATE]).dt.year
    out[c.LIQUID] = extract_colors_from_years(years, "blue")

    return out


def add_colors(dfs, yml):
    """
        Get colors from config file.
        It can't be named get_colors since that function already exists
    """

    out = {name: get_colors(data) for name, data in c.DEFAULT_COLORS.items()}

    # Liquid and investments colors
    for entity in [c.LIQUID, c.INVEST]:
        out[f"{entity}_categ"] = OrderedDict()
        for name, config in yml[entity].items():
            out[f"{entity}_categ"][name] = get_colors((config[c.COLOR_NAME], config[c.COLOR_INDEX]))

    # Expenses and incomes colors
    for entity, df in dfs[c.DF_CATEG].groupby("Type"):
        out[f"{entity}_categ"] = OrderedDict()
        for name, row in df.iterrows():
            out[f"{entity}_categ"][name] = get_colors((row["Color Name"], row["Color Index"]))

    # Colors comparison plot
    out["comp"] = get_colors_comparisons(dfs)

    log.debug("Colors added")

    return out


def main(mdate=datetime.now(), export_data=False):
    """ Create the report """

    mdate = mdate.replace(day=1)

    # Get dfs
    log.debug("Reading excels from gdrive")
    dfs = {x: read_df_gdrive(c.FILE_DATA, x, cols) for x, cols in c.DFS_ALL_FROM_DATA.items()}

    # Add transactions
    log.debug("Reading data from dropbox")
    vdp = get_vdropbox(c.VAR_DROPBOX_TOKEN)
    dfs[c.DF_TRANS] = vdp.read_excel(c.FILE_TRANSACTIONS)

    # Filter dates
    dfs = filter_by_date(dfs, mdate)

    yml = vdp.read_yaml(c.FILE_CONFIG)

    out = {}

    # Expenses, incomes, EBIT and Savings ratio
    log.debug("Extracting expenses, incomes, EBIT and savings ratio")
    for period, col_period in {"month": c.COL_MONTH_DATE, "year": c.COL_YEAR}.items():
        out[period] = get_basic_traces(dfs, col_period, mdate)

    # Liquid, worth and invested
    log.debug("Adding liquid, worth and invested")
    data = [(c.DF_LIQUID, c.LIQUID), (c.DF_WORTH, c.INVEST), (c.DF_INVEST, c.INVEST)]
    for name, yml_name in data:
        out["month"].update(get_investment_or_liquid(dfs, yml[yml_name], name))

    out["month"].update(get_total_investments(out))
    out["month"].update(get_salaries(dfs, mdate))

    out["comp"] = get_comparison_traces(dfs)
    out["pies"] = get_pie_traces(dfs)
    out["dash"] = get_dashboard(out, mdate)
    out["ratios"] = get_ratios(out)
    out["bubbles"] = get_bubbles(dfs, mdate)
    out["sankey"] = extract_sankey(out)

    out["colors"] = add_colors(dfs, yml)

    if export_data:
        vdp.write_yaml(out, f"/report_data/{mdate.year}/{mdate:%Y_%m}.yaml")

    return out
