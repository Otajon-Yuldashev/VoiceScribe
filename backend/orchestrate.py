import os
import json
import base64
import threading
import subprocess
from flask import Flask, request

app = Flask(__name__)


def run_script(script_name):
    print(f"🚀 Running: {script_name}")
    result = subprocess.run(
        ["python", f"/app/{script_name}"],
        capture_output=True,
        text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        print(f"❌ {script_name} failed: {result.stderr}")
        return False
    print(f"✅ {script_name} completed")
    return True


def process_pipeline(file_name):
    print(f"🎙️  Processing: {file_name}")

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

    # run pipeline in background thread — ack Pub/Sub immediately
    thread = threading.Thread(target=process_pipeline, args=(file_name,))
    thread.start()

    return "OK", 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)

