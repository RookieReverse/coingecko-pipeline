Proyecto: ETL de CoinGecko con Delta Lake

Este proyecto realiza un pipeline de extracción, transformación y carga (ELT) sobre datos de criptomonedas obtenidos desde la API pública de CoinGecko, almacenando los datos en formato Delta Lake en una estructura particionada.

📂 Estructura del Proyecto

elt_coingecko/
│
├── main.py                       # Script principal de orquestación del pipeline
├── extract.py                    # Extracción de datos desde la API de CoinGecko
├── process_markets.py           # Transformación y carga de datos de mercado (markets)
├── process_coinlist.py          # Transformación y carga de datos de monedas (coin list)
├── schema.py                    # Esquema y definiciones de columnas y tipos
├── delta_utils.py               # Funciones auxiliares para guardar/upsert en Delta Lake
├── utils/
│   └── data_validation.py       # Validación y conversión de columnas para los datasets
├── datalake/
│   ├── bronze/                  # Datos crudos particionados
│   └── silver/                  # Datos transformados para análisis
└── state/                       # Control de última ejecución
    └── last_extraction.json

⚙️ Funcionalidades

Extracción

/coins/list: Datos de referencia estáticos de criptomonedas (FULL extraction)

/markets: Datos de mercado (precio, volumen, etc) por moneda (INCREMENTAL extraction)

Transformación (Silver Layer)

Validación de columnas requeridas (REQUIRED_COLUMNS)

Tipado de columnas (TYPE_MAP)

Imputación de valores nulos (IMPUTATION_MAP)

Eliminacion de duplicados usando como llave primaria : id, last_updated

Agregado de métricas (ej: promedio de precios por moneda)

Flags derivados (ej: is_high_value si el precio supera los $50,000)

🧪 Ejecución del Pipeline

python main.py

El script realiza:

Verificación del tiempo desde la última ejecución (cada 1 hora)

Extracción y almacenamiento en bronze

Procesamiento y guardado en silver

🪙 Particiones del Delta Lake

Markets: coin, date, day, hour

Coinlist: date, hour

📌 Requisitos (requirements.txt)

Asegurate de tener los siguientes paquetes instalados:

pandas
pyarrow
deltalake
requests

✨ Mejoras posibles

Tests unitarios para funciones de procesamiento

Scheduling con Airflow

Enriquecimiento con más APIs

Visualización con dashboards

📅 Última ejecución

El estado de la última ejecución está en state/last_extraction.json, usado para controlar las extracciones incrementales.

Implementacion: El proyecto debe implementarse de forma tal que su ejecucion sea periodicamente cada una hora o  menos.

Autor: German Rodriguez. Proyecto academico para Certificacion de Data Engineer