from schema import TYPE_MAP, REQUIRED_COLUMNS, COINLIST_TYPE_MAP, REQUIRED_COINLIST_COLUMNS
import pandas as pd  



def apply_column_types(df):
    """
    Aplica los tipos de datos definidos en TYPE_MAP a las columnas del DataFrame si están presentes.
    """
    for col, dtype in TYPE_MAP.items():
        if col in df.columns:
            try:
                if dtype == "datetime64[ns]":
                    df[col] = pd.to_datetime(df[col], utc=True)  

                else:
                    df[col] = df[col].astype(dtype)
            except Exception as e:
                print(f"⚠️ No se pudo convertir '{col}' a {dtype}: {e}")
    return df

def validate_required_columns(df, required_columns ):
    """
    Verifica que todas las columnas obligatorias estén presentes en el DataFrame.
    Retorna True si están todas; False en caso contrario.
    """
    missing = required_columns - set(df.columns)
    if missing:
        print(f"❌ ERROR: Faltan columnas requeridas: {missing}")
        return False
    return True

def clean_coin_list(df):
    """
    Limpieza básica y validación del DataFrame obtenido desde /coins/list.
    """
    missing = REQUIRED_COINLIST_COLUMNS - set(df.columns)
    if missing:
        print(f"❌ ERROR: Faltan columnas requeridas en coin_list: {missing}")
        return None

    df = df.drop_duplicates(subset=["id"])
    df = df.dropna(subset=REQUIRED_COINLIST_COLUMNS)

    for col, dtype in COINLIST_TYPE_MAP.items():
        if col in df.columns:
            try:
                df[col] = df[col].astype(dtype)
            except Exception as e:
                print(f"⚠️ No se pudo convertir '{col}' a {dtype}: {e}")

    return df