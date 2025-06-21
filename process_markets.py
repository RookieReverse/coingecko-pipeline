# process_markets.py
import pandas as pd
from deltalake import DeltaTable
from utils.data_validation import apply_column_types, validate_required_columns
from delta_utils import save_data_as_delta, upsert_data_as_delta, verify_delta_write
from schema import IMPUTATION_MAP,REQUIRED_COLUMNS
from datetime import datetime
import logging

# Configuración de logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

def process_and_save_markets(bronze_path, silver_path, day=None, hour=None):
    """
    Procesa datos crudos desde Bronze y los guarda en Silver.

    Args:
        bronze_path (str): Ruta al Delta Lake (bronze).
        silver_path (str): Ruta al Delta Lake (silver).
        day (int, optional): Día a procesar (por defecto, día actual).
        hour (int, optional): Hora a procesar (por defecto, hora actual).
    """

    logger.info("🔧 Iniciando procesamiento de datos crudos desde Bronze...")

    # Obtener fecha y hora actuales para filtrar los datos recién ingresados
    now = datetime.now()
    date_str = now.strftime("%Y-%m-%d")
    hour_int = int(now.strftime("%H"))
    if day is None:
        day = now.day
    if hour is None:
        hour = now.hour
    date_str = now.strftime("%Y-%m-%d")

    # 1. Leer todos los datos desde Delta Lake (filtro posterior en pandas)
    try:
        dt = DeltaTable(bronze_path)
        df = dt.to_pandas()
    except Exception as e:
        logger.error(f"❌ ERROR al leer datos desde bronze: {e}")
        return

    # Convertir day y hour a int8 explícitamente
    df["day"] = df["day"].astype("int8")
    df["hour"] = df["hour"].astype("int8")

    # Filtramos para la fecha y hora actuales, esto es para la ejecución incremental correspondiente a la hora XX
    df = df[(df["date"] == date_str) & (df["hour"] == hour_int)]

    # Validar que el DataFrame contiene las columnas MANDATORIAS, en caso de que la API cambie
    if not validate_required_columns(df, REQUIRED_COLUMNS):
        return

    logger.info(f"📥 Registros crudos extraídos de bronze: {len(df)}")

    if df.empty:
        logger.warning("⚠️ No hay registros nuevos para procesar en esta ejecución. Finalizando...")
        return

    logger.info("\n📊 ANTES DE TRANSFORMAR:")
    logger.info(f"\nTipos de datos:\n{df.dtypes}")
    df.info(memory_usage="deep")
    logger.info(f"\nTamaño por columna (bytes):\n{df.memory_usage(deep=True)}")

    # 2. Eliminar duplicados donde la llave primaria es id y last_updated
    df = df.drop_duplicates(subset=["id", "last_updated"])

    # 3. Reemplazo de nulos
    df = df.fillna(IMPUTATION_MAP)

    # 4. Conversión de tipos
    df = apply_column_types(df)

    # Calcular la media de cotización por moneda y agregarla al DataFrame
    avg_prices = df.groupby("coin")["current_price"].mean().reset_index()
    avg_prices.rename(columns={"current_price": "avg_price"}, inplace=True)
    df = df.merge(avg_prices, on="coin", how="left")

    logger.info("📈 Media de cotización por moneda:")
    logger.info(f"\n{avg_prices}")

    # Validar que todas las columnas necesarias para particionar existen y no tienen nulos
    
    
    # Nueva columna para análisis
    df["is_high_value"] = df["current_price"] > 50000

    logger.info("\n✅ TRANSFORMACIONES APLICADAS.")

    logger.info("\n📊 DESPUÉS DE TRANSFORMAR:")
    logger.info(f"\nTipos de datos:\n{df.dtypes}")
    df.info(memory_usage="deep")
    logger.info(f"\nTamaño por columna (bytes):\n{df.memory_usage(deep=True)}")
    logger.info(f"📦 Registros a guardar en Silver: {len(df)}")

    # 6. Guardar en Silver con upsert si ya existe la tabla, o save si es la primera vez
    partition_cols = ["coin", "date", "day", "hour"]
    predicate = "target.id = source.id AND target.last_updated = source.last_updated"

    try:
        # Conversión a string para particiones
        for col in partition_cols:
            df[col] = df[col].astype(str)

        upsert_data_as_delta(
            df,
            data_path=silver_path,
            predicate=predicate,
            partition_cols=partition_cols
        )
        logger.info(f"✅ Upsert realizado con éxito en Silver: {silver_path}")
    except Exception:
        save_data_as_delta(
            df,
            path=silver_path,
            mode="overwrite",
            partition_cols=partition_cols
        )
        logger.info(f"✅ Tabla no encontrada. Datos guardados por primera vez en Silver: {silver_path}")

    # Verificación post guardado
    verify_delta_write(silver_path, date_str, hour)
