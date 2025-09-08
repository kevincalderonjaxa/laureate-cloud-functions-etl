import functions_framework
from google.cloud import bigquery

# --- Configuración ---
PROJECT_ID = "[ID_DE_TU_PROYECTO]"
DATASET_ID = "[ID_DE_TU_DATASET]"
TABLE_ID = "reportes_meta"


@functions_framework.cloud_event
def process_auditlog_event(cloud_event):
    """
    Función que se activa con un evento de AUDITORÍA (storage.object.create)
    y carga el archivo CSV correspondiente en una tabla de BigQuery.
    """
    # 1. EXTRACCIÓN DE DATOS DEL PAYLOAD DE AUDITORÍA
    # En un evento de auditoría, la información clave está anidada.
    payload = cloud_event.data.get("protoPayload", {})
    resource_name = payload.get("resourceName", "")

    # El 'resourceName' viene en un formato largo, por ejemplo:
    # "projects/_/buckets/mi-bucket/objects/mi-archivo.csv"
    print(f"✅ Evento de Auditoría recibido. Recurso afectado: '{resource_name}'.")

    # Filtramos para actuar solo sobre archivos CSV y evitar errores.
    if not resource_name or not resource_name.lower().endswith('.csv'):
        print(f"ℹ️ El recurso no es un archivo CSV válido o está vacío. Se ignora.")
        return

    # 2. PARSEO DEL 'resourceName' PARA OBTENER BUCKET Y ARCHIVO
    # Dividimos la cadena de texto para extraer las partes que nos interesan.
    try:
        parts = resource_name.split("/")
        bucket_name = parts[3]
        # Unimos el resto por si el archivo está en una subcarpeta
        file_name = "/".join(parts[5:])
    except IndexError:
        print(f"❌ Error: No se pudo analizar el recurso '{resource_name}'.")
        return

    print(f"-> Archivo extraído: '{file_name}' en el bucket: '{bucket_name}'.")

    # 3. CARGA DE DATOS A BIGQUERY (Esta lógica no cambia)
    uri = f"gs://{bucket_name}/{file_name}"
    client = bigquery.Client()
    table_ref = client.dataset(DATASET_ID, project=PROJECT_ID).table(TABLE_ID)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=False,
    )

    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    print(f"🚀 Iniciando trabajo de carga en BigQuery. Job ID: {load_job.job_id}")

    load_job.result()
    print(f"🎉 ¡Éxito! El archivo '{file_name}' se cargó correctamente en la tabla '{TABLE_ID}'.")