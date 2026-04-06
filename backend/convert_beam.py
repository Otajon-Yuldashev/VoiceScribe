import os
import tempfile
import subprocess
from google.cloud import storage
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter
from datetime import datetime


PROJECT = "voice-data-pipeline"
BUCKET = "voice_data_bucket_demo"


def get_unprocessed_files():
    bq_client = bigquery.Client()
    query = """
        SELECT audio_name, raw_audio_path
        FROM voice_data.pipeline_status
        WHERE is_converted = FALSE
        AND failed_at_stage IS NULL
    """
    results = bq_client.query(query).result()
    return [(row.audio_name, row.raw_audio_path) for row in results]


def convert_file(audio_name, raw_audio_path):
    bq_client = bigquery.Client()
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET)

    tmp_path = None
    wav_path = None

    try:
        blob_name = raw_audio_path.replace(f"gs://{BUCKET}/", "")
        filename = blob_name.split("/")[-1]
        suffix = "." + filename.rsplit(".", 1)[1]

        print(f"\n{'='*50}")
        print(f"Converting: {audio_name}")
        print(f"{'='*50}")

        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
            tmp_path = tmp.name

        print(f"  ⬇️  Downloading {filename}...")
        bucket.blob(blob_name).download_to_filename(tmp_path)
        print(f"  ✅ Downloaded ({suffix.upper()} detected)")

        if suffix.lower() == ".wav":
            print(f"  ✅ Already WAV — skipping conversion")
            wav_path = tmp_path
        else:
            wav_path = tmp_path.replace(suffix, ".wav")
            print(f"  🔄 Converting to WAV (16kHz mono 16-bit)...")
            subprocess.run([
                "ffmpeg", "-i", tmp_path,
                "-ar", "16000", "-ac", "1", "-sample_fmt", "s16",
                "-y", wav_path
            ], check=True, capture_output=True)
            print(f"  ✅ Converted to WAV")

        converted_filename = f"wav_convert/{audio_name}.wav"
        bucket.blob(converted_filename).upload_from_filename(wav_path)
        print(f"  💾 Saved to gs://{BUCKET}/{converted_filename}")

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
        return True

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
        return False

    finally:
        for path in [tmp_path, wav_path]:
            if path and os.path.exists(path):
                try:
                    os.remove(path)
                except:
                    pass


def run():
    print(f"\n{'='*50}")
    print(f"Starting conversion at {datetime.now()}")
    print(f"{'='*50}\n")

    unprocessed = get_unprocessed_files()

    if not unprocessed:
        print("No files to convert!")
        return

    print(f"Found {len(unprocessed)} files to convert")

    for audio_name, raw_audio_path in unprocessed:
        convert_file(audio_name, raw_audio_path)

    print(f"\n{'='*50}")
    print(f"🎉 Conversion complete!")
    print(f"{'='*50}\n")


if __name__ == "__main__":
    run()


