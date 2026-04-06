import json
import os
import librosa
import tempfile
from google.cloud import storage
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter
from datetime import datetime


PROJECT = "voice-data-pipeline"
BUCKET = "voice_data_bucket_demo"
DATASET_ID = "voice_data"
TABLE_ID = "audio_metadata"


def get_unprocessed_files():
    bq_client = bigquery.Client()
    query = """
        SELECT audio_name, transcript_path, normalized_path
        FROM voice_data.pipeline_status
        WHERE is_transcribed = TRUE
        AND is_metadata_loaded = FALSE
        AND failed_at_stage IS NULL
    """
    results = bq_client.query(query).result()
    return [(row.audio_name, row.transcript_path, row.normalized_path) for row in results]


def extract_metadata(audio_name, transcript_path, normalized_path):
    bq_client = bigquery.Client()
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET)

    tmp_path = None

    try:
        print(f"\n{'='*50}")
        print(f"Extracting metadata: {audio_name}")
        print(f"{'='*50}")

        print(f"  📄 Reading transcript...")
        transcript_blob_name = transcript_path.replace(f"gs://{BUCKET}/", "")
        transcript_text = bucket.blob(transcript_blob_name).download_as_string().decode("utf-8").strip()
        total_words = len(transcript_text.split())
        print(f"  ✅ Transcript read — {total_words} words")

        audio_blob_name = normalized_path.replace(f"gs://{BUCKET}/", "")
        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tmp:
            tmp_path = tmp.name

        print(f"  ⬇️  Downloading audio for duration...")
        bucket.blob(audio_blob_name).download_to_filename(tmp_path)
        duration = librosa.get_duration(path=tmp_path)
        print(f"  ✅ Duration: {round(duration, 2)}s ({round(duration/60, 1)} mins)")

        record = {
            "audio_file": normalized_path,
            "transcript_file": transcript_path,
            "audio_name": audio_name,
            "transcript": transcript_text,
            "total_words": total_words,
            "duration_seconds": round(duration, 2),
            "language": "multilingual",
            "processed_at": datetime.now().isoformat()
        }

        jsonl_content = json.dumps(record)
        jsonl_blob_name = f"metadata/{audio_name}.jsonl"
        bucket.blob(jsonl_blob_name).upload_from_string(jsonl_content)
        print(f"  💾 Saved JSONL to gs://{BUCKET}/{jsonl_blob_name}")

        table_ref = f"{PROJECT}.{DATASET_ID}.{TABLE_ID}"
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("audio_file", "STRING"),
                bigquery.SchemaField("transcript_file", "STRING"),
                bigquery.SchemaField("audio_name", "STRING"),
                bigquery.SchemaField("transcript", "STRING"),
                bigquery.SchemaField("total_words", "INTEGER"),
                bigquery.SchemaField("duration_seconds", "FLOAT"),
                bigquery.SchemaField("language", "STRING"),
                bigquery.SchemaField("processed_at", "TIMESTAMP"),
            ],
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
        load_job = bq_client.load_table_from_uri(
            f"gs://{BUCKET}/{jsonl_blob_name}",
            table_ref,
            job_config=job_config
        )
        load_job.result()
        print(f"  ✅ Loaded into BigQuery → {table_ref}")

        job_config_bq = QueryJobConfig(
            query_parameters=[
                ScalarQueryParameter("audio_name", "STRING", audio_name),
                ScalarQueryParameter("updated_at", "STRING", datetime.now().isoformat()),
            ]
        )
        bq_client.query("""
            MERGE voice_data.pipeline_status T
            USING (SELECT @audio_name as audio_name) S
            ON T.audio_name = S.audio_name
            WHEN MATCHED THEN
                UPDATE SET
                    is_metadata_loaded = TRUE,
                    updated_at = @updated_at
        """, job_config=job_config_bq).result()
        print(f"  ✅ pipeline_status updated for {audio_name}")
        return True

    except Exception as e:
        print(f"  ❌ Error: {e}")
        job_config_err = QueryJobConfig(
            query_parameters=[
                ScalarQueryParameter("audio_name", "STRING", audio_name),
                ScalarQueryParameter("error_message", "STRING", str(e)),
                ScalarQueryParameter("updated_at", "STRING", datetime.now().isoformat()),
            ]
        )
        bq_client.query("""
            MERGE voice_data.pipeline_status T
            USING (SELECT @audio_name as audio_name) S
            ON T.audio_name = S.audio_name
            WHEN MATCHED THEN
                UPDATE SET
                    failed_at_stage = 'metadata',
                    error_message = @error_message,
                    updated_at = @updated_at
        """, job_config=job_config_err).result()
        return False

    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except:
                pass


def run():
    print(f"\n{'='*50}")
    print(f"Starting metadata extraction at {datetime.now()}")
    print(f"{'='*50}\n")

    unprocessed = get_unprocessed_files()

    if not unprocessed:
        print("No files to extract metadata!")
        return

    print(f"Found {len(unprocessed)} files to process")

    for audio_name, transcript_path, normalized_path in unprocessed:
        extract_metadata(audio_name, transcript_path, normalized_path)

    print(f"\n{'='*50}")
    print(f"🎉 Metadata extraction complete!")
    print(f"{'='*50}\n")


if __name__ == "__main__":
    run()






