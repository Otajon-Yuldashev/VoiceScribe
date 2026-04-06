import os
import tempfile
import librosa
from google.cloud import storage
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter
from google.cloud import speech_v1
from datetime import datetime


PROJECT = "voice-data-pipeline"
BUCKET = "voice_data_bucket_demo"


def get_unprocessed_files():
    bq_client = bigquery.Client()
    query = """
        SELECT audio_name, normalized_path
        FROM voice_data.pipeline_status
        WHERE is_normalized = TRUE
        AND is_transcribed = FALSE
        AND failed_at_stage IS NULL
    """
    results = bq_client.query(query).result()
    return [(row.audio_name, row.normalized_path) for row in results]


def transcribe_file(audio_name, normalized_path):
    bq_client = bigquery.Client()
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET)

    tmp_path = None

    try:
        print(f"\n{'='*50}")
        print(f"Transcribing: {audio_name}")
        print(f"{'='*50}")

        language_code = audio_name.split("_")[-1]
        print(f"  🌐 Language: {language_code}")

        audio_blob_name = normalized_path.replace(f"gs://{BUCKET}/", "")
        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tmp:
            tmp_path = tmp.name
        bucket.blob(audio_blob_name).download_to_filename(tmp_path)
        duration = librosa.get_duration(path=tmp_path)
        os.remove(tmp_path)
        tmp_path = None
        print(f"  ⏱️  Duration: {round(duration, 2)}s")

        if language_code == "en-US":
            model = "latest_short" if duration < 55 else "latest_long"
        else:
            model = "default"
        print(f"  🤖 Using model: {model}")

        speech_client = speech_v1.SpeechClient()
        config = speech_v1.RecognitionConfig(
            encoding=speech_v1.RecognitionConfig.AudioEncoding.LINEAR16,
            sample_rate_hertz=16000,
            audio_channel_count=1,
            enable_word_confidence=True,
            enable_automatic_punctuation=True,
            model=model,
            language_code=language_code,
        )
        audio = speech_v1.RecognitionAudio(uri=normalized_path)

        print(f"  🎙️  Transcribing...")
        if duration < 55:
            response = speech_client.recognize(config=config, audio=audio)
        else:
            operation = speech_client.long_running_recognize(config=config, audio=audio)
            response = operation.result(timeout=600)

        if not response.results:
            print(f"  ⚠️  No speech detected")
            _update_failed(bq_client, audio_name, "no speech detected")
            return False

        full_transcript = ""
        total_words = 0

        print(f"\n  --- Chunks for {audio_name} ---")
        for chunk_index, result in enumerate(response.results):
            best = max(result.alternatives, key=lambda a: a.confidence)
            chunk_words = len(best.transcript.split())
            total_words += chunk_words
            full_transcript += best.transcript + " "
            print(f"  Chunk {chunk_index+1:02d} [{round(best.confidence, 2)}] ({chunk_words} words): {best.transcript}")

        full_transcript = full_transcript.strip()
        avg_confidence = round(
            sum(max(r.alternatives, key=lambda a: a.confidence).confidence
                for r in response.results) / len(response.results), 2
        )
        print(f"  --- Total chunks: {chunk_index+1} | Total words: {total_words} | Avg confidence: {avg_confidence} ---")

        transcript_filename = f"audio_transcript/{audio_name}.txt"
        bucket.blob(transcript_filename).upload_from_string(full_transcript)
        print(f"  💾 Saved to gs://{BUCKET}/{transcript_filename}")

        job_config = QueryJobConfig(
            query_parameters=[
                ScalarQueryParameter("audio_name", "STRING", audio_name),
                ScalarQueryParameter("transcript_path", "STRING", f"gs://{BUCKET}/{transcript_filename}"),
                ScalarQueryParameter("updated_at", "STRING", datetime.now().isoformat()),
            ]
        )
        bq_client.query("""
            MERGE voice_data.pipeline_status T
            USING (SELECT @audio_name as audio_name) S
            ON T.audio_name = S.audio_name
            WHEN MATCHED THEN
                UPDATE SET
                    is_transcribed = TRUE,
                    transcript_path = @transcript_path,
                    updated_at = @updated_at
        """, job_config=job_config).result()
        print(f"  ✅ pipeline_status updated for {audio_name}")
        return True

    except Exception as e:
        print(f"  ❌ Error: {e}")
        _update_failed(bq_client, audio_name, str(e))
        return False

    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except:
                pass


def _update_failed(bq_client, audio_name, error_message):
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
                failed_at_stage = 'transcribe',
                error_message = @error_message,
                updated_at = @updated_at
    """, job_config=job_config).result()


def run():
    print(f"\n{'='*50}")
    print(f"Starting transcription at {datetime.now()}")
    print(f"{'='*50}\n")

    unprocessed = get_unprocessed_files()

    if not unprocessed:
        print("No files to transcribe!")
        return

    print(f"Found {len(unprocessed)} files to transcribe")

    for audio_name, normalized_path in unprocessed:
        transcribe_file(audio_name, normalized_path)

    print(f"\n{'='*50}")
    print(f"🎉 Transcription complete!")
    print(f"{'='*50}\n")


if __name__ == "__main__":
    run()





    