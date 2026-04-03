import os
import wave
import tempfile
import subprocess
import librosa
import noisereduce as nr
import soundfile as sf
import webrtcvad
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions
from google.cloud import storage
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter
from datetime import datetime


PROJECT = "voice-data-pipeline"
BUCKET = "voice_data_bucket_demo"
REGION = "europe-west1"


def read_wave(path):
    with wave.open(path, 'rb') as wf:
        sample_rate = wf.getframerate()
        pcm_data = wf.readframes(wf.getnframes())
    return pcm_data, sample_rate


def ensure_wav_format(tmp_path):
    """Ensure WAV is exactly 16kHz mono 16-bit — re-encode if not."""
    with wave.open(tmp_path, 'rb') as wf:
        sr = wf.getframerate()
        ch = wf.getnchannels()
        sw = wf.getsampwidth()
    print(f"  🔍 WAV info: {sr}Hz, {ch}ch, {sw*8}bit")
    if sr != 16000 or ch != 1 or sw != 2:
        print(f"  ⚠️  WAV format mismatch — re-encoding to 16kHz mono 16-bit...")
        fixed_path = tmp_path.replace(".wav", "_fixed.wav")
        subprocess.run([
            "ffmpeg", "-i", tmp_path,
            "-ar", "16000", "-ac", "1", "-sample_fmt", "s16",
            "-y", fixed_path
        ], check=True, capture_output=True)
        os.replace(fixed_path, tmp_path)
        print(f"  ✅ Re-encoded successfully")
    else:
        print(f"  ✅ WAV format OK")


def vad_filter(wav_path, aggressiveness=2):
    vad = webrtcvad.Vad(aggressiveness)
    pcm_data, sample_rate = read_wave(wav_path)

    frame_duration = 30
    frame_size = int(sample_rate * frame_duration / 1000) * 2

    speech_frames = []
    total_frames = 0
    speech_count = 0

    for i in range(0, len(pcm_data) - frame_size, frame_size):
        frame = pcm_data[i:i + frame_size]
        if len(frame) < frame_size:
            break
        total_frames += 1
        if vad.is_speech(frame, sample_rate):
            speech_frames.append(frame)
            speech_count += 1

    speech_ratio = speech_count / total_frames if total_frames > 0 else 0
    speech_pcm = b"".join(speech_frames)

    return speech_pcm, speech_ratio, sample_rate


def get_unprocessed_files():
    bq_client = bigquery.Client()
    query = """
        SELECT audio_name, converted_path
        FROM voice_data.pipeline_status
        WHERE is_converted = TRUE
        AND is_normalized = FALSE
    """
    results = bq_client.query(query).result()
    return [(row.audio_name, row.converted_path) for row in results]


class NormalizeAudioFn(beam.DoFn):

    def process(self, element):
        audio_name, converted_path = element
        bq_client = bigquery.Client()
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(BUCKET)

        tmp_path = None
        speech_path = None
        clean_path = None

        try:
            blob_name = converted_path.replace(f"gs://{BUCKET}/", "")
            filename = blob_name.split("/")[-1]

            print(f"\n{'='*50}")
            print(f"Normalizing: {audio_name}")
            print(f"{'='*50}")

            # step 1 - download from wav_convert/
            with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tmp:
                tmp_path = tmp.name

            print(f"  ⬇️  Downloading {filename}...")
            bucket.blob(blob_name).download_to_filename(tmp_path)
            duration_before = librosa.get_duration(path=tmp_path)
            print(f"  ✅ Downloaded — duration: {round(duration_before, 2)}s ({round(duration_before/60, 1)} mins)")

            # step 2 - ensure correct WAV format before VAD
            ensure_wav_format(tmp_path)

            # step 3 - webrtcvad
            print(f"  🎙️  Running VAD...")
            speech_pcm, speech_ratio, sample_rate = vad_filter(tmp_path, aggressiveness=2)
            print(f"  ✅ VAD complete — {round(speech_ratio * 100, 1)}% speech")

            if speech_ratio < 0.1:
                print(f"  ⚠️  Warning: less than 10% speech detected — continuing anyway")

            # save speech only
            speech_path = tmp_path.replace(".wav", "_speech.wav")
            with wave.open(speech_path, 'wb') as wf:
                wf.setnchannels(1)
                wf.setsampwidth(2)
                wf.setframerate(sample_rate)
                wf.writeframes(speech_pcm)

            # step 4 - noisereduce
            print(f"  🔇 Running noise reduction...")
            audio, sr = librosa.load(speech_path, sr=16000)

            if len(audio) > sr * 0.5:
                noise_sample = audio[:int(sr * 0.5)]
                cleaned_audio = nr.reduce_noise(y=audio, sr=sr, y_noise=noise_sample)
            else:
                cleaned_audio = nr.reduce_noise(y=audio, sr=sr)

            clean_path = tmp_path.replace(".wav", "_clean.wav")
            sf.write(clean_path, cleaned_audio, sr)
            print(f"  ✅ Noise reduction complete")

            # check duration
            duration_after = librosa.get_duration(path=clean_path)
            print(f"  ⏱️  Before: {round(duration_before, 2)}s → After: {round(duration_after, 2)}s")

            if duration_after < 10:
                print(f"  ⚠️  Warning: short audio after processing — continuing anyway")

            # step 5 - upload to wav_normalized/
            normalized_filename = f"wav_normalized/{audio_name}.wav"
            bucket.blob(normalized_filename).upload_from_filename(clean_path)
            print(f"  💾 Saved to gs://{BUCKET}/{normalized_filename}")

            # update pipeline_status
            job_config = QueryJobConfig(
                query_parameters=[
                    ScalarQueryParameter("audio_name", "STRING", audio_name),
                    ScalarQueryParameter("normalized_path", "STRING", f"gs://{BUCKET}/{normalized_filename}"),
                    ScalarQueryParameter("updated_at", "STRING", datetime.now().isoformat()),
                ]
            )
            bq_client.query("""
                MERGE voice_data.pipeline_status T
                USING (SELECT @audio_name as audio_name) S
                ON T.audio_name = S.audio_name
                WHEN MATCHED THEN
                    UPDATE SET
                        is_normalized = TRUE,
                        normalized_path = @normalized_path,
                        updated_at = @updated_at
            """, job_config=job_config).result()
            print(f"  ✅ pipeline_status updated for {audio_name}")

            yield audio_name

        except Exception as e:
            print(f"  ❌ Error: {e}")
            self._update_failed(bq_client, audio_name, str(e))

        finally:
            for path in [tmp_path, speech_path, clean_path]:
                if path and os.path.exists(path):
                    os.remove(path)

    def _update_failed(self, bq_client, audio_name, error_message):
        job_config = QueryJobConfig(
            query_parameters=[
                ScalarQueryParameter("audio_name", "STRING", audio_name),
                ScalarQueryParameter("error_message", "STRING", error_message),
                ScalarQueryParameter("updated_at", "STRING", datetime.now().isoformat()),
            ]
        )
        bq_client.query("""
            MERGE voice_data.pipeline_status T
            USING (SELECT @audio_name as audio_name) S
            ON T.audio_name = S.audio_name
            WHEN MATCHED THEN
                UPDATE SET
                    failed_at_stage = 'normalize',
                    error_message = @error_message,
                    updated_at = @updated_at
        """, job_config=job_config).result()


def run():
    print(f"\n{'='*50}")
    print(f"Starting Beam normalization at {datetime.now()}")
    print(f"{'='*50}\n")

    unprocessed = get_unprocessed_files()

    if not unprocessed:
        print("No files to normalize!")
        return

    print(f"Found {len(unprocessed)} files to normalize")

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
            | "Normalize audio" >> beam.ParDo(NormalizeAudioFn())
            | "Print results" >> beam.Map(lambda x: print(f"✅ Completed: {x}"))
        )

    print(f"\n{'='*50}")
    print(f"🎉 Normalization pipeline complete!")
    print(f"{'='*50}\n")


if __name__ == "__main__":
    run()





