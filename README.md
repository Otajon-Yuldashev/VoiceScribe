# VoiceScribe 🎙️

An end-to-end AI voice data pipeline built on Google Cloud Platform. Converts uploaded audio files into transcripts with metadata and audio feature extraction — designed as a data engineering portfolio project and practical tool for AI interview data collection.

---

## Architecture Overview

```
User uploads audio (mp3 / m4a / wav)
→ Streamlit UI (Cloud Run)
→ GCS raw_audio/
→ OBJECT_FINALIZE → Pub/Sub → HTTP push → Cloud Run backend
→ Background thread runs 5 pipeline scripts sequentially:
    1. convert_beam.py     — ffmpeg → 16kHz mono 16-bit WAV
    2. normalize_beam.py   — VAD + noise reduction + format validation
    3. transcript_beam.py  — Google Speech-to-Text (en-US / uz-UZ / it-IT)
    4. metadata_beam.py    — duration, word count, WPM → BigQuery
    5. features_beam.py    — MFCCs, mel spectrogram, pitch, energy → BigQuery
→ BigQuery MERGE updates pipeline_status at each stage
→ UI polls pipeline_status → live progress → transcript + metrics
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Frontend | Streamlit (Python) |
| Backend | Flask + Gunicorn (Python) |
| Containerisation | Docker |
| CI/CD | Google Cloud Build |
| Container Registry | Google Artifact Registry |
| Hosting | Google Cloud Run |
| Storage | Google Cloud Storage |
| Messaging | Google Pub/Sub |
| Transcription | Google Speech-to-Text v1 |
| Data Warehouse | Google BigQuery |
| Audio Processing | ffmpeg, librosa, webrtcvad, noisereduce |

---

## Repository Structure

```
VoiceScribe/
├── backend/
|   └── bigquery_tables.sql  # BigQuery table definitions
│   ├── orchestrate.py       # Flask app — receives Pub/Sub, runs pipeline
│   ├── convert_beam.py      # Audio format conversion
│   ├── normalize_beam.py    # VAD + noise reduction
│   ├── transcript_beam.py   # Speech-to-Text transcription
│   ├── metadata_beam.py     # Duration, word count, WPM extraction
│   ├── features_beam.py     # MFCC, mel spectrogram, pitch, energy extraction
│   ├── requirements.txt
│   └── Dockerfile
├── frontend/
│   ├── web.py               # Streamlit UI
│   ├── requirements.txt
│   └── Dockerfile
├── cloudbuild.yaml           # CI/CD pipeline
├── .gitignore
└── README.md
```

---

## GCP Infrastructure

- **Project:** voice-data-pipeline
- **Region:** europe-west1
- **GCS Bucket:** voice_data_bucket_demo
  - `raw_audio/` → `wav_convert/` → `wav_normalized/` → `audio_transcript/` → `metadata/`
- **BigQuery Dataset:** voice_data
  - `pipeline_status` — tracks each file through pipeline stages
  - `audio_metadata` — transcript, word count, duration
  - `audio_features` — MFCCs, mel spectrogram, pitch, energy
- **Pub/Sub:** voice-pipeline-topic → voice-pipeline-subscription (push to Cloud Run)
- **GCS Notification:** OBJECT_FINALIZE on `raw_audio/` prefix

---

## Pipeline Scripts

### convert_beam.py
Downloads raw audio from GCS and converts to 16kHz mono 16-bit WAV using ffmpeg. Handles mp3, m4a, and wav inputs. Updates `pipeline_status` on completion.

### normalize_beam.py
Applies three processing steps to the converted WAV:
1. Format validation — ensures 16kHz mono 16-bit (re-encodes with ffmpeg if not)
2. Voice Activity Detection (webrtcvad) — removes silence frames
3. Noise reduction (noisereduce) — cleans background noise

### transcript_beam.py
Sends normalized audio to Google Speech-to-Text. Selects model based on language and duration — uses `latest_short` / `latest_long` for English, `default` for Uzbek and Italian. Saves transcript as `.txt` to GCS.

### metadata_beam.py
Reads transcript and audio to extract: total word count, duration in seconds, words per minute. Stores as JSONL in GCS and loads into BigQuery `audio_metadata` table.

### features_beam.py
Extracts audio features from normalized files created on or after 2026-04-05:
- **MFCCs** — 13 coefficients (mean + std) capturing vocal tract characteristics
- **Mel Spectrogram** — 128 frequency band means representing energy distribution
- **Pitch (F0)** — mean fundamental frequency in Hz using pyin algorithm
- **Energy (RMS)** — mean amplitude/loudness

Stores results in BigQuery `audio_features` table for ML use.

---

## BigQuery Table Definitions

See `sql/bigquery_tables.sql` for full schema definitions.

---

## CI/CD

Cloud Build trigger watches the `master` branch. On push:
- Detects whether `backend/` or `frontend/` changed
- Builds Docker image for changed service only
- Pushes to Artifact Registry
- Deploys to Cloud Run

---

## Supported Languages

| Language | Code |
|---|---|
| English | en-US |
| Uzbek | uz-UZ |
| Italian | it-IT |

---

## Audio Constraints

- **Formats:** mp3, m4a, wav
- **Max size:** 30MB
- **Processing:** All audio converted to 16kHz mono 16-bit WAV internally

---

## Local Development

```bash
# Backend
cd backend
pip install -r requirements.txt
python orchestrate.py

# Frontend
cd frontend
pip install -r requirements.txt
streamlit run web.py
```

Requires GCP credentials with access to Cloud Storage, BigQuery, and Speech-to-Text.
