import functions_framework
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timezone

# Configuración de BigQuery (¡Buena práctica usar variables de entorno!)
PROJECT_ID = "[ID_DE_TU_PROYECTO]"
DATASET_ID = "[ID_DE_TU_DATASET_BQ]"
TABLE_ID = "[ID_DE_TU_TABLA_BQ]"

TARGET_FOLDER = "reports_to_load/" #AGREGADO

@functions_framework.cloud_event
def load_csv_to_bigquery(cloud_event):
    data = cloud_event.data
    bucket_name = data["bucket"]
    file_name = data["name"]

    # 1. VALIDACIÓN DE CARPETA Y TIPO DE ARCHIVO
    if not file_name.startswith(TARGET_FOLDER):
        print(f"ℹ️ Archivo '{file_name}' ignorado: no está en la carpeta '{TARGET_FOLDER}'.")
        return
    if not file_name.lower().endswith('.csv'):
        print(f"ℹ️ Archivo '{file_name}' ignorado: no es un archivo CSV.")
        return

    print(f"✅ Archivo detectado: '{file_name}'. Procediendo con la carga.")

    # 2. PROCESAMIENTO DEL ARCHIVO CON PANDAS
    uri = f"gs://{bucket_name}/{file_name}"
    df = pd.read_csv(uri)

    # --- NUEVO: 3. LIMPIEZA Y TIPADO DE DATOS ---
    # Esta sección es CRUCIAL para evitar errores de tipo al cargar.
    print("Ajustando tipos de datos del DataFrame...")

    # Convertimos la columna 'reporting_date' de texto a un objeto de fecha/hora.
    # Pandas es lo suficientemente inteligente para entender el formato YYYY-MM-DD.
    df['reporting_date'] = pd.to_datetime(df['reporting_date'])

    # Aseguramos que las columnas numéricas sean del tipo correcto (entero o flotante).
    # Esto también ayuda a limpiar datos si vinieran como texto "123".
    df['impressions'] = pd.to_numeric(df['impressions'])
    df['clicks'] = pd.to_numeric(df['clicks'])
    df['amount_spent'] = pd.to_numeric(df['amount_spent'])
    df['leads'] = pd.to_numeric(df['leads'])
    
    # 4. AÑADIR METADATOS
    load_time = datetime.now(timezone.utc)
    df['load_timestamp'] = load_time
    df['source_filename'] = uri
    
    print(f"Tipos ajustados y metadatos añadidos: {len(df)} filas listas.")

    # 5. CARGA DEL DATAFRAME A BIGQUERY
    client = bigquery.Client()
    table_ref = client.dataset(DATASET_ID, project=PROJECT_ID).table(TABLE_ID)
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    load_job = client.load_table_from_dataframe(
        df, table_ref, job_config=job_config
    )
    print(f"Iniciando trabajo de carga desde DataFrame. Job ID: {load_job.job_id}")

    load_job.result()
    print(f"🎉 ¡Éxito! El archivo '{file_name}' se cargó y enriqueció en la tabla '{TABLE_ID}'.")