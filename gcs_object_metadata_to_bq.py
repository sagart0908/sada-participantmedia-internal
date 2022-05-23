from google.cloud import storage, bigquery
import pandas as pd
from hurry.filesize import size


def blob_metadata(storage_object, bucket_name, blob_name):
    """Gets  blob's metadata information."""
    bucket = storage_object.bucket(bucket_name)

    blob = bucket.get_blob(blob_name)
    blob_info = {"bucket_name": blob.bucket.name,
                 "blob_name": blob.name,
                 "gcs_object_name": blob.name.split("/")[-1],
                 "gcs_path": "gs://" + blob.bucket.name + "/" + blob.name,
                 "storage_class": blob.storage_class,
                 "id": blob.id.split("/")[-1],
                 "size": size(blob.size),
                 "updated": blob.updated,
                 "generation": blob.generation,
                 "metageneration": blob.metageneration,
                 "etag": blob.etag,
                 "owner": blob.owner,
                 "component_count": blob.component_count,
                 "crc32c": blob.crc32c,
                 "md5_hash": blob.md5_hash,
                 "cache_control": blob.cache_control,
                 "content_type": blob.content_type,
                 "content_disposition": blob.content_disposition,
                 "content_encoding": blob.content_encoding,
                 "content_language": blob.content_language,
                 "metadata": blob.metadata,
                 "media_link": blob.media_link,
                 "custom_time": blob.custom_time,
                 "temporary_hold": "enabled" if blob.temporary_hold else "disabled",
                 "event_based_hold": "enabled" if blob.event_based_hold else "disabled",
                 "retention_expiration_time": blob.retention_expiration_time if blob.retention_expiration_time else None
                 }
    return blob_info


def list_blobs(storage_object, bucket_name, dataframe):
    """Lists all the blobs in the bucket."""

    blobs = storage_object.list_blobs(bucket_name)

    for blob in blobs:
        print("------------Start---------------")
        print(blob.name)
        if blob.name[-1] != '/':
            print(blob)
            print(blob.name)
            dataframe.append(blob_metadata(storage_object, bucket_name, blob.name))
        print("----------------------------")


def upload_blob(storage_object, bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    bucket = storage_object.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)


def load_to_bq(dataset_name, table_name, gcs_filename):
    client = bigquery.Client()
    dataset_ref = client.dataset(dataset_name)
    schema = [
        bigquery.SchemaField(name='bucket_name', field_type='STRING'),
        bigquery.SchemaField(name='blob_name', field_type='STRING'),
        bigquery.SchemaField(name='gcs_object_name', field_type='STRING'),
        bigquery.SchemaField(name='gcs_path', field_type='STRING'),
        bigquery.SchemaField(name='storage_class', field_type='STRING'),
        bigquery.SchemaField(name='id', field_type='STRING'),
        bigquery.SchemaField(name='size', field_type='STRING'),
        bigquery.SchemaField(name='updated', field_type='TIMESTAMP'),
        bigquery.SchemaField(name='generation', field_type='INTEGER'),
        bigquery.SchemaField(name='metageneration', field_type='INTEGER'),
        bigquery.SchemaField(name='etag', field_type='STRING'),
        bigquery.SchemaField(name='owner', field_type='STRING'),
        bigquery.SchemaField(name='component_count', field_type='STRING'),
        bigquery.SchemaField(name='crc32c', field_type='STRING'),
        bigquery.SchemaField(name='md5_hash', field_type='STRING'),
        bigquery.SchemaField(name='cache_control', field_type='STRING'),
        bigquery.SchemaField(name='content_type', field_type='STRING'),
        bigquery.SchemaField(name='content_disposition', field_type='STRING'),
        bigquery.SchemaField(name='content_encoding', field_type='STRING'),
        bigquery.SchemaField(name='content_language', field_type='STRING'),
        bigquery.SchemaField(name='metadata', field_type='STRING'),
        bigquery.SchemaField(name='media_link', field_type='STRING'),
        bigquery.SchemaField(name='custom_time', field_type='STRING'),
        bigquery.SchemaField(name='temporary_hold', field_type='STRING'),
        bigquery.SchemaField(name='event_based_hold', field_type='STRING'),
        bigquery.SchemaField(name='retention_expiration_time', field_type='STRING')
    ]
    table = bigquery.Table(dataset_ref.table(table_name), schema=schema)
    external_config = bigquery.ExternalConfig('CSV')
    external_config.options.skip_leading_rows = 1
    external_config.source_uris = [gcs_filename]
    table.external_data_configuration = external_config
    client.create_table(table)
    return


if __name__ == '__main__':
    gcs_bucket_name = "data-platform-foundation-001"
    dataset = "gcs_duplication_test"
    table = "test_obj"
    storage_client = storage.Client()
    dataframe_rows = []
    list_blobs(storage_client, gcs_bucket_name, dataframe_rows)
    df = pd.DataFrame(dataframe_rows)
    df.to_csv("all_object_info.csv", index=False)
    upload_blob(storage_client, gcs_bucket_name, "all_object_info.csv", "all_obj_info/all_object_info.csv")
    load_to_bq(dataset, table, "gs://data-platform-foundation-001/all_obj_info/all_object_info.csv")

