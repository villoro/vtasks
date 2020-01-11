import requests
from datetime import date

import pandas as pd


from global_utilities import get_secret


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
