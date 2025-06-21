# schema.py

"""
Definición centralizada del esquema para el dataset de mercados de CoinGecko.
Incluye columnas requeridas y tipos de datos esperados para procesamiento y almacenamiento.
"""

#  Columnas mínimas obligatorias para un registro válido
REQUIRED_COLUMNS = {
    "id",
    "coin",
    "symbol",
    "name",
    "current_price",
    "market_cap",
    "total_volume",
    "last_updated"
}

#  Mapeo de tipos de datos óptimos por columna
TYPE_MAP = {
    "id": "category",
    "symbol": "category",
    "name": "string",
    "image": "string",
    "current_price": "float64",
    "market_cap": "float64",
    "market_cap_rank": "int16",
    "fully_diluted_valuation": "float64",
    "total_volume": "float64",
    "high_24h": "float64",
    "low_24h": "float64",
    "price_change_24h": "float64",
    "price_change_percentage_24h": "float32",
    "market_cap_change_24h": "float64",
    "market_cap_change_percentage_24h": "float32",
    "circulating_supply": "float64",
    "total_supply": "float64",
    "max_supply": "float64",
    "ath": "float64",
    "ath_change_percentage": "float32",
    "ath_date": "datetime64[ns]",
    "atl": "float64",
    "atl_change_percentage": "float32",
    "atl_date": "datetime64[ns]",
    "roi": "object",  # Puede ser un dict o null
    "last_updated": "datetime64[ns]",
    
    #  Columnas adicionales para particionamiento en el Delta Lake
    "coin": "string",
    "date": "string",
    "day": "string",
    "hour": "string"
}
# fill NaN
IMPUTATION_MAP = {
    "market_cap": -1,
    "total_volume": -1,
    "current_price": -1
}

# === Esquema para datos estáticos (/coins/list) ===
REQUIRED_COINLIST_COLUMNS = {
    "id",
    "symbol",
    "name"
}

COINLIST_TYPE_MAP = {
    "id": "string",
    "symbol": "string",
    "name": "string"
}