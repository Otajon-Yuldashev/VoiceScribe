import os
import json
import base64
import threading
import subprocess
from flask import Flask, request
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter
from datetime import datetime

app = Flask(__name__)

PROJECT = "voice-data-pipeline"
BUCKET = "voice_data_bucket_demo"


def register_file(audio_name, raw_audio_path):
    """Register file into pipeline_status before pipeline runs."""
    bq_client = bigquery.Client()
    now = datetime.now().isoformat()
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


def run_script(script_name):
    print(f"🚀 Running: {script_name}")
    result = subprocess.run(
        ["python", f"/app/{script_name}"],
        capture_output=False,
        text=True
    )
    if result.returncode != 0:
        print(f"❌ {script_name} failed")
        return False
    print(f"✅ {script_name} completed")
    return True


def process_pipeline(file_name, audio_name, raw_audio_path):
    print(f"🎙️  Processing: {file_name}")

    # register file into BigQuery first
    try:
        register_file(audio_name, raw_audio_path)
    except Exception as e:
        print(f"❌ Registration failed: {e}")
        return

    for script in [
        "convert_beam.py",
        "normalize_beam.py",
        "transcript_beam.py",
        "metadata_beam.py",
        "features_beam.py",
    ]:
        if not run_script(script):
            print(f"❌ Pipeline failed at {script}")
            return

    print("🎉 Full pipeline completed!")


@app.route("/", methods=["POST"])
def handle_pubsub():
    envelope = request.get_json()

    if not envelope or "message" not in envelope:
        print("❌ Invalid Pub/Sub message")
        return "Bad Request", 400

    message = envelope["message"]
    data = base64.b64decode(message["data"]).decode("utf-8")
    event = json.loads(data)

    file_name = event.get("name", "")
    print(f"📁 New file detected: {file_name}")

    if not file_name.startswith("raw_audio/"):
        print("⏭️  Not a raw_audio file — skipping")
        return "OK", 200

    if not file_name.endswith((".mp3", ".m4a", ".wav")):
        print("⏭️  Not an audio file — skipping")
        return "OK", 200

    # extract audio_name and raw_audio_path
    filename = file_name.split("/")[-1]
    audio_name = filename.rsplit(".", 1)[0]
    raw_audio_path = f"gs://{BUCKET}/{file_name}"

    # run pipeline in background thread — ack Pub/Sub immediately
    thread = threading.Thread(
        target=process_pipeline,
        args=(file_name, audio_name, raw_audio_path)
    )
    thread.start()

    return "OK", 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)

    