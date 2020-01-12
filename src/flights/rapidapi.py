import requests
from time import sleep
from datetime import date, timedelta

import pandas as pd
from tqdm import tqdm

from global_utilities import get_secret
from global_utilities.log import log
from . import constants as c

BASE_URL = (
    "https://skyscanner-skyscanner-flight-search-v1.p.rapidapi.com/apiservices/browseroutes/v1.0/"
)

HEADERS = {
    "x-rapidapi-host": "skyscanner-skyscanner-flight-search-v1.p.rapidapi.com",
    "x-rapidapi-key": get_secret("RAPIDAPI_KEY"),
}


def query_flights(
    origin,
    destination,
    day,
    max_attempts=20,
    seconds_sleep=1,
    country="ES",
    currency="EUR",
    locale="en-US",
):
    """
        Query flights iterating until there is a result

        Args:
            origin:         code for origin airport
            destination:    code for destination airport
            day:            day for the flights [date]
            max_attempts:   number of retries
            seconds_sleep:  seconds to sleep before returning a result
            country:        code for country (default: ES)
            currency:       code for currency (default: EUR)
            locale:         code for output info (default: en-US)
    """

    url = f"{BASE_URL}{country}/{currency}/{locale}/{origin}/{destination}/{day:%Y-%m-%d}"

    for attemp_num in range(max_attempts):
        response = requests.get(url, headers=HEADERS)

        if response.status_code == 200:
            sleep(seconds_sleep)
            return response

        # If there are 'Too many requests' sleep a little
        elif response.status_code == 429:
            log.warning(f"API limit reached at attempt {attemp_num + 1}")
            sleep(2 * attemp_num + 1)

        # Raise unknown cases
        else:
            response.raise_for_status()

    log.error(f"Number max of attempts reached ({max_attempts})")
    raise TimeoutError("TimeOut")


def fix_places(df, data):
    """
        Map Places id to actual airport code
        
        Args:
            df:     dataframe with flights
            data:   output of the request as dict containg 'Places' info
    """

    # Get places
    df_places = pd.DataFrame(data["Places"])
    places = df_places.set_index("PlaceId")["IataCode"].to_dict()

    # Rename places for origin and destination
    for x in [c.COL_ORIGIN, c.COL_DESTINATION]:
        df[x] = df[f"{x}Id"].replace(places)
        df = df.drop(f"{x}Id", axis=1)

    return df


def fix_carriers(df, data):
    """
        Map Carriers id to actual carrier name
        
        Args:
            df:     dataframe with flights
            data:   output of the request as dict containg 'Carriers' info
    """
    # Get carriers
    df_carriers = pd.DataFrame(data["Carriers"])
    carriers = df_carriers.set_index("CarrierId")["Name"].to_dict()

    # Rename carriers
    df[c.COL_CARRIER] = df[c.COL_CARRIER].replace(carriers)

    return df


def retrive_quotes(data):
    """
        Get info from quotes as a pandas dataframe
    """

    out = []
    for quote in data["Quotes"]:
        # Extract data from "OutboundLeg" nested dict
        quote.update(quote.pop("OutboundLeg"))

        # For all possible Carrier, create an entry
        for carrier in quote.pop("CarrierIds"):
            aux = quote.copy()
            aux.update({c.COL_CARRIER: carrier})

            out.append(aux)

    # Create pandas dataframe
    df = pd.DataFrame(out).set_index("QuoteId")
    df.index.name = ""

    return df


def parse_data(data):
    """
        Parse all data from the request and create a pandas dataframe with fligths
    """

    df = retrive_quotes(data)

    # Rename columns
    df = df.rename(columns=c.COL_RENAMES)

    # Fix dates
    for x in [c.COL_QUOTE_DATE, c.COL_DATE]:
        df[x] = pd.to_datetime(df[x])

    df = fix_places(df, data)
    df = fix_carriers(df, data)

    df[c.COL_INSTERTED] = pd.to_datetime(date.today())

    return df


def query_pair(origin, destination, n_days=366):
    """
        Query all flights between 2 airports

        Args:
            origin:         code for origin airport
            destination:    code for destination airport
            n_days:         max days of history
    """

    # Start at day 1 since it will only query when day==1
    start_day = date.today()

    log.info(f"Quering flights from '{origin}' to '{destination}'")

    dfs = []
    for x in tqdm(range(n_days)):
        query_day = start_day + timedelta(x)

        # Only do first day of month
        if (query_day.day != 1) and (query_day != start_day):
            log.debug(f"Skiping day '{query_day}'")
            continue

        response = query_flights(origin, destination, query_day)
        data = response.json()

        if data["Quotes"]:
            dfs.append(parse_data(data))

    if dfs:
        return pd.concat(dfs).reset_index(drop=True)
    else:
        log.warning(f"No flights from '{origin}' to '{destination}'")
