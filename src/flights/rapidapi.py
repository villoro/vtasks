import requests
from time import sleep
from datetime import date, timedelta

import pandas as pd
from tqdm import tqdm

from global_utilities import get_secret


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
    country="ES",
    currency="EUR",
    locale="en-US",
    max_attempts=10,
    seconds_sleep=1,
):

    url = f"{BASE_URL}{country}/{currency}/{locale}/{origin}/{destination}/{day:%Y-%m-%d}"

    for attemp_num in range(max_attempts):
        response = requests.get(url, headers=HEADERS)

        if response.status_code == 200:
            sleep(seconds_sleep)
            return response

        # If there are 'Too many requests' sleep a little
        elif response.status_code == 429:
            sleep(attemp_num + 1)

        # Raise unknown cases
        else:
            response.raise_for_status()

    raise TimeoutError("TimeOut")


def fix_places(df, data):

    # Get places
    df_places = pd.DataFrame(data["Places"])
    places = df_places.set_index("PlaceId")["IataCode"].to_dict()

    # Rename places for origin and destination
    for x in ["OriginId", "DestinationId"]:
        df[x.replace("Id", "")] = df[x].replace(places)
        df = df.drop(x, axis=1)

    return df


def fix_carriers(df, data):

    # Get carriers
    df_carriers = pd.DataFrame(data["Carriers"])
    carriers = df_carriers.set_index("CarrierId")["Name"].to_dict()

    # Rename carriers
    df["Carrier"] = df["Carrier"].replace(carriers)

    return df


def retrive_quotes(data):

    out = []
    for quote in data["Quotes"]:
        # Extract data from "OutboundLeg" nested dict
        quote.update(quote.pop("OutboundLeg"))

        # For all possible Carrier, create an entry
        for carrier in quote.pop("CarrierIds"):
            aux = quote.copy()
            aux.update({"Carrier": carrier})

            out.append(aux)

    # Create pandas dataframe
    df = pd.DataFrame(out).set_index("QuoteId")
    df.index.name = ""

    return df


def parse_data(data):

    df = retrive_quotes(data)

    # Fix dates
    for x in ["QuoteDateTime", "DepartureDate"]:
        df[x] = pd.to_datetime(df[x])

    df = fix_places(df, data)
    df = fix_carriers(df, data)

    df["Inserted"] = date.today()

    return df


def query_pair(origin, destination, n_days=366):

    # Start at day 1 since it will only query when day==1
    start_day = date.today().replace(day=1)

    dfs = []
    for x in tqdm(range(n_days)):
        query_day = start_day + timedelta(x)

        # Only do first day of month
        if (query_day.day != 1) and (query_day != start_day):
            continue

        response = query_flights("BCN", "CAG", query_day)
        data = response.json()

        if data["Quotes"]:
            dfs.append(parse_data(data))

    return pd.concat(dfs).reset_index(drop=True)
