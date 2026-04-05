import os
import json
import tempfile
import numpy as np
import librosa
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
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
        SELECT ps.audio_name, ps.normalized_path
        FROM voice_data.pipeline_status ps
        LEFT JOIN voice_data.audio_features af
            ON ps.audio_name = af.audio_name
        WHERE ps.is_normalized = TRUE
        AND ps.failed_at_stage IS NULL
        AND DATE(ps.created_at) >= '2026-04-05'
        AND af.audio_name IS NULL
    """
    results = bq_client.query(query).result()
    return [(row.audio_name, row.normalized_path) for row in results]


class ExtractFeaturesFn(beam.DoFn):

    def process(self, element):
        audio_name, normalized_path = element
        bq_client = bigquery.Client()
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(BUCKET)

        tmp_path = None

        try:
            blob_name = normalized_path.replace(f"gs://{BUCKET}/", "")
            filename = blob_name.split("/")[-1]

            print(f"\n{'='*50}")
            print(f"Feature Extraction: {audio_name}")
            print(f"{'='*50}")

            # download from wav_normalized/
            with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tmp:
                tmp_path = tmp.name

            print(f"  ⬇️  Downloading {filename}...")
            bucket.blob(blob_name).download_to_filename(tmp_path)

            # load audio
            audio, sr = librosa.load(tmp_path, sr=16000)
            duration_seconds = librosa.get_duration(y=audio, sr=sr)
            print(f"  ✅ Loaded — duration: {round(duration_seconds, 2)}s")

            # ── MFCC ──────────────────────────────────────────
            print(f"  🎵 Extracting MFCCs...")
            mfcc = librosa.feature.mfcc(y=audio, sr=sr, n_mfcc=13)
            mfcc_mean = mfcc.mean(axis=1).tolist()
            mfcc_std = mfcc.std(axis=1).tolist()
            print(f"  ✅ MFCCs extracted — shape: {mfcc.shape}")

            # ── MEL SPECTROGRAM ───────────────────────────────
            print(f"  🎼 Extracting Mel Spectrogram...")
            mel_spec = librosa.feature.melspectrogram(y=audio, sr=sr, n_mels=128)
            mel_spec_db = librosa.power_to_db(mel_spec, ref=np.max)
            mel_mean = mel_spec_db.mean(axis=1).tolist()
            print(f"  ✅ Mel spectrogram extracted — shape: {mel_spec.shape}")

            # ── PITCH (F0) ────────────────────────────────────
            print(f"  🎤 Extracting Pitch (F0)...")
            f0, voiced_flag, voiced_probs = librosa.pyin(
                audio,
                fmin=librosa.note_to_hz('C2'),
                fmax=librosa.note_to_hz('C7')
            )
            pitch_mean = float(np.nanmean(f0)) if not np.all(np.isnan(f0)) else 0.0
            print(f"  ✅ Pitch extracted — mean: {round(pitch_mean, 2)}Hz")

            # ── ENERGY (RMS) ──────────────────────────────────
            print(f"  ⚡ Extracting Energy (RMS)...")
            rms = librosa.feature.rms(y=audio)
            energy_mean = float(rms.mean())
            print(f"  ✅ Energy extracted — mean: {round(energy_mean, 6)}")

            # ── STORE IN BIGQUERY ─────────────────────────────
            job_config = QueryJobConfig(
                query_parameters=[
                    ScalarQueryParameter("audio_name", "STRING", audio_name),
                    ScalarQueryParameter("mfcc_mean", "JSON", json.dumps(mfcc_mean)),
                    ScalarQueryParameter("mfcc_std", "JSON", json.dumps(mfcc_std)),
                    ScalarQueryParameter("mel_mean", "JSON", json.dumps(mel_mean)),
                    ScalarQueryParameter("pitch_mean", "FLOAT64", pitch_mean),
                    ScalarQueryParameter("energy_mean", "FLOAT64", energy_mean),
                    ScalarQueryParameter("duration_seconds", "FLOAT64", duration_seconds),
                    ScalarQueryParameter("extracted_at", "STRING", datetime.now().isoformat()),
                ]
            )
            bq_client.query("""
                MERGE voice_data.audio_features T
                USING (SELECT @audio_name as audio_name) S
                ON T.audio_name = S.audio_name
                WHEN NOT MATCHED THEN
                    INSERT (
                        audio_name, mfcc_mean, mfcc_std, mel_mean,
                        pitch_mean, energy_mean, duration_seconds, extracted_at
                    )
                    VALUES (
                        @audio_name, @mfcc_mean, @mfcc_std, @mel_mean,
                        @pitch_mean, @energy_mean, @duration_seconds, @extracted_at
                    )
            """, job_config=job_config).result()
            print(f"  ✅ audio_features updated in BigQuery")

            yield audio_name

        except Exception as e:
            print(f"  ❌ Error: {e}")

        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)


def run():
    print(f"\n{'='*50}")
    print(f"Starting Feature Extraction at {datetime.now()}")
    print(f"{'='*50}\n")

    unprocessed = get_unprocessed_files()

    if not unprocessed:
        print("No files for feature extraction!")
        return

    print(f"Found {len(unprocessed)} files for feature extraction")

    options = PipelineOptions([
        '--runner=DirectRunner',
        '--direct_running_mode=multi_threading',
        '--direct_num_workers=1',
        f'--project={PROJECT}',
        f'--region={REGION}',
    ])

    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | "Create file list" >> beam.Create(unprocessed)
            | "Extract features" >> beam.ParDo(ExtractFeaturesFn())
            | "Print results" >> beam.Map(lambda x: print(f"✅ Completed: {x}"))
        )

    print(f"\n{'='*50}")
    print(f"🎉 Feature Extraction pipeline complete!")
    print(f"{'='*50}\n")


if __name__ == "__main__":
    run()




    