"""
    Create the raw data for the reprot
"""

from datetime import date

import numpy as np
import pandas as pd

from collections import OrderedDict
from datetime import datetime
from vpalette import get_colors

from . import constants as c
from .functions import filter_by_date
from .functions import serie_to_dict
from .functions import series_to_dicts
from .functions import smooth_serie
from utils import get_vdropbox
from utils import log


def get_categories(dfs, mtype):
    """
        Gets a list of categories

        Args:
            dfs:    dict with dataframes
            mtype:  [Incomes/Expenes]
    """

    df = dfs[c.DF_CATEG]

    return reversed(df[df[c.COL_TYPE] == mtype].index.to_list())


def resample(df, period, mdate):
    """ Resample and fill missing periods """

    index = pd.date_range(df.index.min(), mdate, freq=period)
    df = df.resample(period).sum().reindex(index).fillna(0)

    # If working with years, cast the index to integer
    if period == "YS":
        df.index = df.index.year

    return df


def get_basic_traces(dfs, period, mdate):
    """
        Extract Incomes, Expenses, Result and savings traces

        Args:
            dfs:        dict with dataframes
            period:     month or year [MS/YS]
            mdate:      date of the report
    """

    series = {}
    for name, df in dfs[c.DF_TRANS].groupby(c.COL_TYPE):
        series[name] = resample(df[c.COL_AMOUNT], period, mdate)

    # Extract expenses and incomes
    series[c.RESULT] = (series[c.INCOMES] - series[c.EXPENSES]).dropna()

    # Add savings ratio
    series[c.SAVINGS] = (100 * series[c.RESULT] / series[c.INCOMES]).apply(lambda x: max(0, x))

    out = series_to_dicts(series)

    # Append time averaged data
    if period == "MS":
        for name, serie in series.items():
            out[f"{name}_trend"] = serie_to_dict(smooth_serie(serie))

    # Get by groups
    for name, dfg in dfs[c.DF_TRANS].groupby(c.COL_TYPE):

        df = dfg.pivot_table(c.COL_AMOUNT, c.COL_DATE, c.COL_CATEGORY, "sum")
        df = resample(df, "YS", mdate)

        aux = OrderedDict()
        for x in get_categories(dfs, name):
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
        f"{entity}_trend": serie_to_dict(smooth_serie(dfg)["Total"]),
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
    index = pd.date_range(df.index.min(), mdate, freq="MS")
    df = df.reindex(index).fillna(method="ffill").fillna(0)

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

    def get_one_trace(df, col=c.COL_AMOUNT):
        """ Create the comparison trace """

        df = smooth_serie(df[[col]])
        df["Month"] = df.index.month
        df["Year"] = df.index.year

        return (
            df.pivot_table(col, "Month", "Year", "sum")
            .apply(lambda x: round(x, 2))
            .fillna("null")
            .to_dict()
        )

    # Expenses and incomes
    for name, df in dfs[c.DF_TRANS].groupby(c.COL_TYPE):
        out[name] = get_one_trace(df)

    # Prepare transactions for Result
    df = dfs[c.DF_TRANS].copy()
    mfilter = df[c.COL_TYPE] == c.EXPENSES
    df.loc[mfilter, c.COL_AMOUNT] = -df.loc[mfilter, c.COL_AMOUNT]

    # Add Result
    out[c.RESULT] = get_one_trace(df)

    # Add liquid
    out[c.LIQUID] = get_one_trace(dfs[c.DF_LIQUID], "Total")

    log.debug("Comparison traces added")

    return out


def get_pie_traces(dfs, mdate):
    """
        Add traces for pie plots

        Args:
            dfs:    dict with dataframes
    """

    out = {}
    for name, df in dfs[c.DF_TRANS].groupby(c.COL_TYPE):

        df = df.pivot_table(c.COL_AMOUNT, c.COL_DATE, c.COL_CATEGORY, "sum").fillna(0)

        def export_trace(serie):
            """ Extract all possible categories """

            # Keep only present categories
            indexs = [x for x in get_categories(dfs, name) if x in serie.index]
            return serie_to_dict(serie[indexs])

        out[name] = {
            "month": export_trace(resample(df, "MS", mdate).iloc[-1, :]),
            "year": export_trace(resample(df, "YS", mdate).iloc[-1, :]),
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

    traces = [c.EXPENSES, c.INCOMES, c.RESULT, c.LIQUID]
    traces += [x + "_trend" for x in traces] + ["Worth", "Invest"]

    out = {}

    for tw, date_index in [("month", f"{mdate:%Y-%m-01}"), ("year", mdate.year)]:

        out[tw] = {}

        # Basic traces
        for name in traces:
            mdict = data[tw].get(name, None)

            # This is intentional so that we don't include some years for the year tw
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
        # MS of the last month of last year
        last_year = date(year=mdate.year - 1, month=12, day=1).isoformat()

        out["month"][f"{name}_1y"] = data["month"][name].get(last_year, 0)

    # Invest last month (for the Sankey)
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
        f"{c.EXPENSES}_trend",
        f"{c.LIQUID}_trend",
    ]
    for name in names:
        aux[name] = pd.Series(data["month"][name])
    liquid = pd.Series(data["month"][c.LIQUID])

    out = {
        f"{c.LIQUID}/{c.EXPENSES}": aux[c.LIQUID] / aux[c.EXPENSES],
        f"{c.LIQUID}_trend/{c.EXPENSES}_trend": aux[f"{c.LIQUID}_trend"]
        / aux[f"{c.EXPENSES}_trend"],
        f"Total_Worth/{c.EXPENSES}": (aux["Worth"] + aux[c.LIQUID]) / (12 * aux[c.EXPENSES]),
        f"Total_Worth_trend/{c.EXPENSES}_trend": (aux["Worth"] + aux[f"{c.LIQUID}_trend"])
        / (12 * aux[f"{c.EXPENSES}_trend"]),
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
        dfa = df.resample("YS").agg({c.COL_AMOUNT: "sum", c.COL_MONTH: "nunique"})
        dfa.index = dfa.index.year
        aux[name] = dfa[c.COL_AMOUNT] * 12 / dfa[c.COL_MONTH]

    def get_year(dfi):
        """ Get last value of each year """
        df = dfi.resample("YS").last()["Total"]
        df.index = df.index.year
        return df

    aux["Liquid"] = get_year(dfs[c.DF_LIQUID])
    aux["Worth"] = get_year(dfs[c.DF_WORTH])

    # Create dataframe
    df = pd.DataFrame(aux).fillna(0)

    df["Total_Worth"] = df["Worth"] + df["Liquid"]
    df["savings"] = 100 * (df["Incomes"] - df["Expenses"]) / df["Incomes"]
    df["doomsday"] = df["Total_Worth"] / df["Expenses"]

    # First year has strange values
    df = df[df.index > min_year].apply(lambda x: round(x, 2))

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
        result = incomes - expenses

        if tw == "month":
            invest = mdict["Invest"] - mdict["Invest_1m"]
        else:
            invest = data["dash"]["month"]["Invest"] - data["dash"]["month"]["Invest_1y"]

        # Values for traces
        incomes_to_invest = max(min(result, invest), 0)
        incomes_to_savings = max(result - max(invest, 0), 0)
        invest_to_expenses = max(-max(invest, result), 0)
        invest_to_savings = -min(invest + invest_to_expenses, 0)
        savings_to_expenses = -min(result + invest_to_expenses, 0)
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

    # Incomes, Expenses and result
    out = {}
    for name, color_name in [(c.INCOMES, "green"), (c.EXPENSES, "red"), (c.RESULT, "amber")]:
        out[name] = extract_colors_from_years(dfs[c.DF_TRANS][c.COL_YEAR], color_name)

    # Liquid
    years = dfs[c.DF_LIQUID].index.year
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


def main(dfs, mdate=datetime.now(), export_data=False):
    """ Create the report """

    mdate = mdate.replace(day=1)

    # Filter dates
    dfs = filter_by_date(dfs, mdate)

    # Get config info
    vdp = get_vdropbox()
    yml = vdp.read_yaml(c.FILE_CONFIG)

    out = {}

    # Expenses, incomes, result and savings ratio
    log.debug("Extracting expenses, incomes, result and savings ratio")
    for period in ["month", "year"]:
        out[period] = get_basic_traces(dfs, period[0].upper() + "S", mdate)

    # Liquid, worth and invested
    log.debug("Adding liquid, worth and invested")
    data = [(c.DF_LIQUID, c.LIQUID), (c.DF_WORTH, c.INVEST), (c.DF_INVEST, c.INVEST)]
    for name, yml_name in data:
        out["month"].update(get_investment_or_liquid(dfs, yml[yml_name], name))

    out["month"].update(get_total_investments(out))
    out["month"].update(get_salaries(dfs, mdate))

    out["comp"] = get_comparison_traces(dfs)
    out["pies"] = get_pie_traces(dfs, mdate)
    out["dash"] = get_dashboard(out, mdate)
    out["ratios"] = get_ratios(out)
    out["bubbles"] = get_bubbles(dfs, mdate)
    out["sankey"] = extract_sankey(out)

    out["colors"] = add_colors(dfs, yml)

    if export_data:
        vdp.write_yaml(out, f"{c.PATH_EXPENSOR}/report_data/{mdate.year}/{mdate:%Y_%m}.yaml")

    return out
