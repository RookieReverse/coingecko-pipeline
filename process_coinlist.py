import logging
from deltalake import DeltaTable
from delta_utils import save_data_as_delta
from utils.data_validation import apply_column_types, validate_required_columns
from schema import REQUIRED_COINLIST_COLUMNS
from datetime import datetime

# Configuración de logging
logger = logging.getLogger(__name__)

def process_and_save_coinlist(bronze_path, silver_path, day=None, hour=None):
    """
    Procesa los datos crudos de coinlist desde Bronze y los guarda en Silver.

    Aplica:
    - Filtrado por fecha/hora actuales
    - Validación de columnas requeridas
    - Conversión de tipos
    - Validación de columnas de partición

    Args:
        bronze_path (str): Ruta a la tabla en la capa Bronze.
        silver_path (str): Ruta de destino para la capa Silver.
        day (int, optional): Día de ejecución.
        hour (int, optional): Hora de ejecución.
    """
    logger.info("🔧 Iniciando procesamiento (Limpieza) de datos crudos de coinlist desde Bronze a Silver...")

    now = datetime.now()
    if day is None:
        day = now.day
    if hour is None:
        hour = now.hour

    date_str = now.strftime("%Y-%m-%d")

    try:
        dt = DeltaTable(bronze_path)
        df = dt.to_pandas()
    except Exception as e:
        logger.error(f"❌ ERROR al leer datos desde bronze (coinlist): {e}")
        return

    # Filtrar por fecha y hora actuales
    df = df[(df["date"] == date_str) & (df["hour"] == now.strftime("%H"))]

    if df.empty:
        logger.warning("⚠️ No hay registros nuevos para procesar (coinlist).")
        return

    if not validate_required_columns(df, REQUIRED_COINLIST_COLUMNS):
        return

    df = apply_column_types(df)

    for col in ["date", "hour"]:
        if col not in df.columns or df[col].isnull().any():
            logger.error(f"❌ ERROR: Columna de partición '{col}' está ausente o contiene nulos.")
            return

    logger.info("✅ Transformaciones de coinlist completadas. Guardando en Silver (overwrite)...")

    save_data_as_delta(
        df,
        path=silver_path,
        mode="overwrite",
        partition_cols=["date", "hour"]
    )
    logger.info(f"✅ Coinlist guardado en Silver: {silver_path}")
