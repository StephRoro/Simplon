import os
import io
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
import logging

def csv_to_parquet():
    logging.info("Début de la fonction csv_to_parquet")
    
    # Initialiser le client Cloud Storage
    try:
        storage_client = storage.Client()
        logging.info("Client Storage initialisé avec succès")
    except Exception as e:
        logging.error(f"Erreur lors de l'initialisation du client Storage: {e}")
        raise

    bucket_name = 'simplon-dev-ingestion-sr'
    source_prefix = 'raw_csv/'
    destination_prefix = 'raw_parquet/'
    
    try:
        bucket = storage_client.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=source_prefix)
        logging.info(f"Bucket {bucket_name} récupéré avec succès")
    except Exception as e:
        logging.error(f"Erreur lors de la récupération du bucket: {e}")
        raise

    # Parcourir les fichiers
    for blob in blobs:
        if blob.name.endswith('.csv'):
            logging.info(f"Traitement du fichier CSV: {blob.name}")
            try:
                csv_data = blob.download_as_text()
                df = pd.read_csv(io.StringIO(csv_data))
                logging.info(f"Fichier CSV {blob.name} chargé dans un DataFrame")
            except Exception as e:
                logging.error(f"Erreur lors de la lecture du fichier CSV {blob.name}: {e}")
                continue  # Continue to next file in case of error

            try:
                # Convertir en Parquet
                table = pa.Table.from_pandas(df)
                destination_blob_name = blob.name.replace(source_prefix, destination_prefix).replace('.csv', '.parquet')
                parquet_temp_file = f'/tmp/{os.path.basename(destination_blob_name)}'
                pq.write_table(table, parquet_temp_file)
                logging.info(f"Fichier CSV {blob.name} converti en Parquet avec succès")

                # Uploader dans le bucket
                destination_blob = bucket.blob(destination_blob_name)
                destination_blob.upload_from_filename(parquet_temp_file)
                os.remove(parquet_temp_file)
                logging.info(f"Fichier Parquet uploadé avec succès: {destination_blob_name}")
            except Exception as e:
                logging.error(f"Erreur lors de la conversion ou upload du fichier {blob.name}: {e}")
        else:
            logging.info(f"Fichier ignoré: {blob.name} n'est pas un fichier CSV.")
    
    return "Transformation terminée avec succès", 200
