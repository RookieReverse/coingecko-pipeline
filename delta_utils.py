# delta_utils.py

import pyarrow as pa
from deltalake import write_deltalake, DeltaTable
from deltalake.exceptions import TableNotFoundError
import logging

logger = logging.getLogger(__name__)

def save_data_as_delta(df, path, mode="overwrite", partition_cols=None):
    """
    Guarda un DataFrame como tabla Delta Lake en la ruta especificada.
    """
    write_deltalake(path, df, mode=mode, partition_by=partition_cols)

def save_new_data_as_delta(new_data, data_path, predicate, partition_cols=None):
    """
    Inserta nuevos datos si no existen previamente en la tabla Delta.
    """
    try:
        dt = DeltaTable(data_path)
        new_data_pa = pa.Table.from_pandas(new_data)
        dt.merge(
            source=new_data_pa,
            source_alias="source",
            target_alias="target",
            predicate=predicate
        ).when_not_matched_insert_all().execute()
    except TableNotFoundError:
        save_data_as_delta(new_data, data_path, partition_cols=partition_cols)

def upsert_data_as_delta(data, data_path, predicate, partition_cols=None):
    """
    Realiza un upsert (insertar o actualizar) en la tabla Delta Lake.
    """
    try:
        dt = DeltaTable(data_path)
        data_pa = pa.Table.from_pandas(data)
        dt.merge(
            source=data_pa,
            source_alias="source",
            target_alias="target",
            predicate=predicate
        ).when_matched_update_all().when_not_matched_insert_all().execute()
    except TableNotFoundError:
        save_data_as_delta(data, data_path, mode="overwrite", partition_cols=partition_cols)

def verify_delta_write(path, date_str, hour):
    """
    Verifica que los datos fueron escritos correctamente en la tabla Delta Lake.

    Args:
        path (str): Ruta del Delta Lake (ej: silver_path).
        date_str (str): Fecha de partición (formato YYYY-MM-DD).
        hour (int): Hora de partición (0–23).
    """
    try:
        dt = DeltaTable(path)
        df = dt.to_pandas()

        logger.info(f"📅 Fechas disponibles: {df['date'].unique()}")
        logger.info(f"🕐 Horas disponibles: {df['hour'].unique()}")

        df["hour"] = df["hour"].astype(int)
        df = df[(df["date"] == date_str) & (df["hour"] == hour)]

        logger.info(f"✅ Verificación de registros para {date_str} {hour}hs:\n{df.head()}")

    except Exception as e:
        logger.warning(f"⚠️ No se pudo leer/verificar datos en Delta: {e}")
