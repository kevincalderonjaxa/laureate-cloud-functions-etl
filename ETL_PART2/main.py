import functions_framework
from google.cloud import bigquery

# Configuración de BigQuery (¡Buena práctica usar variables de entorno!)
PROJECT_ID = "[ID_DE_TU_PROYECTO]"
DATASET_ID = "[ID_DE_TU_DATASET_BQ]"
TABLE_ID = "[ID_DE_TU_TABLA_BQ]"

@functions_framework.cloud_event
def load_csv_to_bigquery(cloud_event):
    """Función que se activa con un archivo en GCS y lo carga a BigQuery."""
    data = cloud_event.data
    bucket_name = data["bucket"]
    file_name = data["name"]

    print(f"Nuevo archivo detectado: {file_name} en el bucket: {bucket_name}.")

    # Construir el URI del archivo en Cloud Storage
    uri = f"gs://{bucket_name}/{file_name}"

    # Inicializar cliente de BigQuery
    client = bigquery.Client()
    table_ref = client.dataset(DATASET_ID).table(TABLE_ID)

    # Configurar el trabajo de carga
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Saltar la fila del encabezado
        autodetect=False, # Ya definimos el schema en el paso 1
    )

    # Iniciar el trabajo de carga
    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    print(f"Iniciando trabajo de carga de BigQuery: {load_job.job_id}")

    load_job.result()  # Espera a que el trabajo termine

    print(f"¡Éxito! El archivo {file_name} se cargó en la tabla {TABLE_ID}.")