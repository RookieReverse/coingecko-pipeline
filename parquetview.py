import pandas as pd
import glob
import os

# Ruta específica del archivo parquet que querés revisar
ruta = r"C:\Users\rodri\OneDrive\Documents\DataCampAI\DataEngineer\elt_coingecko\datalake\silver\coingecko\markets\coin=bitcoin\date=2025-06-18\day=18\hour=22\*.parquet"

# Usamos glob para encontrar el archivo si tiene nombre dinámico
files = glob.glob(ruta)

# 🔍 Mostrar lo que encuentra
print(f"🔍 Buscando archivos en: {ruta}")
print(f"🔍 Archivos encontrados: {files}")


for file in files:
    print(f"📄 Archivo encontrado: {file}")
    df = pd.read_parquet(file)
    print("🧾 Columnas disponibles:", df.columns.tolist())
    print("📋 Primeras filas del archivo:")
    print(df.head())