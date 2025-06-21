import os
import json
import logging
import pandas as pd
from datetime import datetime, timedelta

from extract import extract_market_data, extract_coin_list
from delta_utils import save_data_as_delta, upsert_data_as_delta
from process_coinlist import process_and_save_coinlist
from process_markets import process_and_save_markets

# === CONFIGURACIÓN ===
COINS = ["bitcoin", "ethereum", "mochicat", "dogecoin"]
STATE_FILE = "state/last_extraction.json"
BRONZE_PATH = "datalake/bronze/coingecko/markets"
SILVER_PATH = "datalake/silver/coingecko/markets"
COINS_SILVER_PATH = "datalake/silver/coingecko/coins"
COINS_BRONZE_PATH = "datalake/bronze/coingecko/coins"
PARTITION_COLS = ["coin", "date", "day", "hour"]
os.makedirs("state", exist_ok=True)
os.makedirs(COINS_BRONZE_PATH, exist_ok=True)

# === LOGGING ===
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

# === FUNCIONES DE CONTROL DE ESTADO ===
def read_last_extraction():
    if not os.path.exists(STATE_FILE):
        return None
    with open(STATE_FILE, "r") as f:
        state = json.load(f)
        return datetime.strptime(state["last_extraction"], "%Y-%m-%dT%H:%M:%S")

def save_current_extraction(current_time):
    with open(STATE_FILE, "w") as f:
        json.dump({"last_extraction": current_time.strftime("%Y-%m-%dT%H:%M:%S")}, f)

def should_extract(now, last_time):
    if last_time is None:
        return True
    elapsed = now - last_time
    if elapsed < timedelta(hours=1):
        logger.info("⏩ Skipping extracción: no ha pasado una hora desde la última ejecución.")
        return False
    elif elapsed > timedelta(hours=2):
        logger.warning(f"⏱️ Última extracción fue hace más de 2 horas ({elapsed}). Puede haberse perdido una ejecución intermedia.")
        logger.info("⚙️ Procediendo con la extracción de todos modos...")
    return True

# === FLUJO FUNCIONAL ===
def run_coin_list_extraction(now):
    logger.info("🚀 Extracción FULL desde /coins/list...")
    coin_list = extract_coin_list()
    if coin_list:
        logger.info(f"🪙 Extraído {len(coin_list)} monedas.")
        logger.info(f"{coin_list[:3]}")
        df_coins = pd.DataFrame(coin_list)
        df_coins['timestamp'] = now.strftime("%Y-%m-%d %H:%M:%S")
        df_coins["date"] = now.strftime("%Y-%m-%d")
        df_coins["hour"] = now.strftime("%H")
        save_data_as_delta(df_coins, path=COINS_BRONZE_PATH, mode="overwrite", partition_cols=["date", "hour"])
        logger.info(f"✅ Datos de monedas guardados en {COINS_BRONZE_PATH}.")
    else:
        logger.error("❌ Error al extraer la lista de monedas.")

def run_market_extraction(now):
    logger.info("📈 Extracción INCREMENTAL de datos crudos del mercado...")
    df_raw = extract_market_data(coins=COINS, vs_currency="usd", as_dataframe=True)
    if df_raw is not None and not df_raw.empty:
        df_raw["coin"] = df_raw["id"]
        df_raw["date"] = now.strftime("%Y-%m-%d")
        df_raw["day"] = now.strftime("%d")
        df_raw["hour"] = now.strftime("%H")

        predicate = "target.id = source.id AND target.last_updated = source.last_updated"

        try:
            upsert_data_as_delta(df_raw, BRONZE_PATH, predicate)
            logger.info("✅ Upsert realizado con éxito en Delta Lake.")
        except Exception as e:
            logger.warning(f"⚠️ Falló el upsert. Se realizará inserción inicial: {e}")
            save_data_as_delta(df_raw, BRONZE_PATH, mode="overwrite", partition_cols=PARTITION_COLS)
            logger.info("✅ Datos insertados por primera vez con 'overwrite'.")

        return df_raw
    else:
        logger.error("❌ No se extrajo información desde CoinGecko.")
        return None

def main():
    """
     Ejecuta el pipeline completo:
    - Verifica si debe correr (por frecuencia horaria)
    - Extrae y guarda coin list (full)
    - Extrae y guarda market data (incremental)
    - Procesa ambos hacia Silver
    - Actualiza estado de ejecución
    """
    
     # Timestamp actual
    now = datetime.now()
    date_str = now.strftime("%Y-%m-%d")
    hour_str = now.strftime("%H")
    logger.info(f"🕐 Ejecutando extracción {date_str} a las {hour_str}hs")

    # Verificación de estado
    last_extraction = read_last_extraction()
    if not should_extract(now, last_extraction):
        return

    # Ejecución del flujo de trabajo
    run_coin_list_extraction(now)

    # Limpieza y guardado de la coin list en Silver
    process_and_save_coinlist(COINS_BRONZE_PATH,COINS_SILVER_PATH)


    df_raw = run_market_extraction(now)
    if df_raw is not None:
        process_and_save_markets(BRONZE_PATH, SILVER_PATH, day=now.day, hour=now.hour)
        save_current_extraction(now)


if __name__ == "__main__":
    main()
