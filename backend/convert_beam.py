import os
import time
import tempfile
import subprocess
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions
from google.cloud import storage
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter
from datetime import datetime


PROJECT = "voice-data-pipeline"
BUCKET = "voice_data_bucket_demo"
REGION = "europe-west1"


def get_unprocessed_files():
    bq_client = bigquery.Client()
    query = """
        SELECT audio_name, raw_audio_path
        FROM voice_data.pipeline_status
        WHERE is_converted = FALSE
    """
    results = bq_client.query(query).result()
    return [(row.audio_name, row.raw_audio_path) for row in results]


def register_new_files():
    storage_client = storage.Client()
    bq_client = bigquery.Client()
    bucket = storage_client.get_bucket(BUCKET)

    query = "SELECT audio_name FROM voice_data.pipeline_status"
    registered = {row.audio_name for row in bq_client.query(query).result()}

    blobs = bucket.list_blobs(prefix="raw_audio/")

    for blob in blobs:
        if blob.name.endswith((".wav", ".mp3", ".m4a")):
            filename = blob.name.split("/")[-1]
            audio_name = filename.rsplit(".", 1)[0]

            if audio_name not in registered:
                now = datetime.now().isoformat()
                raw_audio_path = f"gs://{BUCKET}/{blob.name}"

                # use MERGE to insert new file — no streaming buffer issue
                job_config = QueryJobConfig(
                    query_parameters=[
                        ScalarQueryParameter("audio_name", "STRING", audio_name),
                        ScalarQueryParameter("raw_audio_path", "STRING", raw_audio_path),
                        ScalarQueryParameter("created_at", "STRING", now),
                        ScalarQueryParameter("updated_at", "STRING", now),
                    ]
                )
                bq_client.query("""
                    MERGE voice_data.pipeline_status T
                    USING (SELECT @audio_name as audio_name) S
                    ON T.audio_name = S.audio_name
                    WHEN NOT MATCHED THEN
                        INSERT (
                            audio_name, raw_audio_path,
                            converted_path, normalized_path, transcript_path,
                            is_converted, is_normalized, is_transcribed, is_metadata_loaded,
                            failed_at_stage, error_message,
                            created_at, updated_at
                        )
                        VALUES (
                            @audio_name, @raw_audio_path,
                            NULL, NULL, NULL,
                            FALSE, FALSE, FALSE, FALSE,
                            NULL, NULL,
                            @created_at, @updated_at
                        )
                """, job_config=job_config).result()

                print(f"  ✅ Registered: {audio_name}")

    print("✅ Registration complete")


class ConvertToWavFn(beam.DoFn):

    def process(self, element):
        audio_name, raw_audio_path = element
        bq_client = bigquery.Client()
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(BUCKET)

        tmp_path = None
        wav_path = None

        try:
            blob_name = raw_audio_path.replace(f"gs://{BUCKET}/", "")
            filename = blob_name.split("/")[-1]
            suffix = "." + filename.rsplit(".", 1)[1]

            print(f"\n{'='*50}")
            print(f"Converting: {audio_name}")
            print(f"{'='*50}")

            # download
            with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
                tmp_path = tmp.name

            print(f"  ⬇️  Downloading {filename}...")
            bucket.blob(blob_name).download_to_filename(tmp_path)
            print(f"  ✅ Downloaded ({suffix.upper()} detected)")

            # convert to WAV or use as-is if already WAV
            if suffix.lower() == ".wav":
                print(f"  ✅ Already WAV — skipping conversion")
                wav_path = tmp_path
            else:
                wav_path = tmp_path.replace(suffix, ".wav")
                print(f"  🔄 Converting to WAV (16kHz mono 16-bit)...")
                subprocess.run([
                    "ffmpeg", "-i", tmp_path,
                    "-ar", "16000",
                    "-ac", "1",
                    "-sample_fmt", "s16",
                    "-y",
                    wav_path
                ], check=True, capture_output=True)
                print(f"  ✅ Converted to WAV")

            # upload to wav_convert/
            converted_filename = f"wav_convert/{audio_name}.wav"
            bucket.blob(converted_filename).upload_from_filename(wav_path)
            print(f"  💾 Saved to gs://{BUCKET}/{converted_filename}")

            # MERGE to update pipeline_status
            job_config = QueryJobConfig(
                query_parameters=[
                    ScalarQueryParameter("audio_name", "STRING", audio_name),
                    ScalarQueryParameter("converted_path", "STRING", f"gs://{BUCKET}/{converted_filename}"),
                    ScalarQueryParameter("updated_at", "STRING", datetime.now().isoformat()),
                ]
            )
            bq_client.query("""
                MERGE voice_data.pipeline_status T
                USING (SELECT @audio_name as audio_name) S
                ON T.audio_name = S.audio_name
                WHEN MATCHED THEN
                    UPDATE SET
                        is_converted = TRUE,
                        converted_path = @converted_path,
                        updated_at = @updated_at
            """, job_config=job_config).result()
            print(f"  ✅ pipeline_status updated for {audio_name}")

            yield audio_name

        except Exception as e:
            print(f"  ❌ Error: {e}")
            job_config = QueryJobConfig(
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
                        failed_at_stage = 'convert',
                        error_message = @error_message,
                        updated_at = @updated_at
            """, job_config=job_config).result()

        finally:
            for path in [tmp_path, wav_path]:
                if path and os.path.exists(path):
                    os.remove(path)


def run():
    print(f"\n{'='*50}")
    print(f"Starting Beam conversion at {datetime.now()}")
    print(f"{'='*50}\n")

    register_new_files()
    unprocessed = get_unprocessed_files()

    if not unprocessed:
        print("No files to convert!")
        return

    print(f"Found {len(unprocessed)} files to convert")

    options = PipelineOptions()
    options.view_as(StandardOptions).runner = "DirectRunner"

    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = PROJECT
    google_cloud_options.region = REGION

    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | "Create file list" >> beam.Create(unprocessed)
            | "Convert to WAV" >> beam.ParDo(ConvertToWavFn())
            | "Print results" >> beam.Map(lambda x: print(f"✅ Completed: {x}"))
        )

    print(f"\n{'='*50}")
    print(f"🎉 Conversion pipeline complete!")
    print(f"{'='*50}\n")


if __name__ == "__main__":
    run()


    



