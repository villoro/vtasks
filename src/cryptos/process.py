import cryptocompare

import utils as u


def get_crypto_prices(cryptos):
    """ Get latest prices of a list of cryptos """

    # Query cryptos
    data = cryptocompare.get_price([*cryptos])

    # Return a dict with prices
    return {i: x["EUR"] for i, x in data.items()}


def get_coordinates(df):
    """ Get gdrive coordinates as a pandas dataframe """

    df_index = df.copy()

    # Get column letter (Chr(65) = 'A')
    index_to_letter = lambda x: chr(65 + x + 1)

    numbers = pd.Series([str(x + 2) for x in range(df_index.shape[0])], index=df_index.index)

    for i, col in enumerate(df_index.columns):
        df_index[col] = index_to_letter(i) + numbers

    return df_index
