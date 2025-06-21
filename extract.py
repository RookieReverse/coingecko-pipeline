# extract.py

import requests
import pandas as pd

BASE_URL = "https://api.coingecko.com/api/v3"

def fetch_data(endpoint, data_field=None, params=None, headers=None, as_dataframe=False, record_path=None, meta=None):
    """
    Realiza una solicitud GET a la CoinGecko API y devuelve los datos.

    Parámetros:
    - endpoint (str): ruta del endpoint (ej: 'coins/list').
    - data_field (str): clave dentro del JSON (opcional).
    - params (dict): parámetros de URL (opcional).
    - headers (dict): encabezados HTTP (opcional).
    - as_dataframe (bool): si True, intenta convertir a DataFrame (opcional).
    - record_path (str | list): ruta a la lista anidada si se usa json_normalize.
    - meta (list): claves para incluir como columnas auxiliares.

    Retorna:
    - dict | list | DataFrame | None
    """
    try:
        url = f"{BASE_URL}/{endpoint}"
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()

        try:
            data = response.json()
            if data_field:
                data = data[data_field]

            if as_dataframe:
                # Si se especifica una ruta, normalizamos el JSON
                if record_path:
                    return pd.json_normalize(data, record_path=record_path, meta=meta)
                return pd.DataFrame(data)

            return data

        except Exception as e:
            print(f" Failed to parse JSON: {e}")
            return None

    except requests.exceptions.RequestException as e:
        print(f" HTTP request failed for endpoint '{endpoint}': {e}")
        return None

def extract_coin_list():
    """
    Extrae la lista completa de monedas desde CoinGecko.

    Retorna:
    - list: Lista de diccionarios con 'id', 'symbol' y 'name' de cada moneda.
    """
    return fetch_data("coins/list")

def extract_prices(coin_ids, vs_currency="usd"):
    """
    Extrae los precios actuales de una lista de monedas.

    Parámetros:
    - coin_ids (list): IDs de las monedas a consultar.
    - vs_currency (str): Moneda de referencia (por defecto 'usd').

    Retorna:
    - dict: Diccionario con precios por moneda.
    """
    params = {
        "ids": ",".join(coin_ids),
        "vs_currencies": vs_currency
    }
    return fetch_data("simple/price", params=params)

def extract_market_data(coins: list, vs_currency="usd", as_dataframe=True):
    """
    Extrae datos de mercado para una lista de monedas desde CoinGecko.

    Parámetros:
    - coins (list): lista de monedas (ej: ['bitcoin', 'ethereum']).
    - vs_currency (str): moneda de referencia (ej: 'usd').
    - as_dataframe (bool): si True, devuelve un pandas.DataFrame.

    Retorna:
    - DataFrame o lista de dicts con datos como market cap, precios, etc.
    """
    if not coins:
        print("⚠️ Lista de monedas vacía.")
        return pd.DataFrame() if as_dataframe else []

    ids_param = ",".join(coins)

    endpoint = "coins/markets"
    params = {
        "vs_currency": vs_currency,
        "ids": ids_param,
        "order": "market_cap_desc",
        "sparkline": "false",
        "price_change_percentage": "24h"
    }

    return fetch_data(endpoint=endpoint, params=params, as_dataframe=as_dataframe)
