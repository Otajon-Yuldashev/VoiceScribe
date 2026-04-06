-- ============================================================
-- VoiceScribe BigQuery Table Definitions
-- Dataset: voice_data
-- Project: voice-data-pipeline
-- ============================================================


-- ── 1. PIPELINE STATUS ───────────────────────────────────────
-- Tracks each audio file through the pipeline stages

CREATE TABLE IF NOT EXISTS `voice-data-pipeline.voice_data.pipeline_status` (
    audio_name          STRING,
    raw_audio_path      STRING,
    converted_path      STRING,
    normalized_path     STRING,
    transcript_path     STRING,
    is_converted        BOOL,
    is_normalized       BOOL,
    is_transcribed      BOOL,
    is_metadata_loaded  BOOL,
    failed_at_stage     STRING,
    error_message       STRING,
    created_at          STRING,
    updated_at          STRING
);


-- ── 2. AUDIO METADATA ────────────────────────────────────────
-- Stores transcript text, word count, duration per audio file

CREATE TABLE IF NOT EXISTS `voice-data-pipeline.voice_data.audio_metadata` (
    audio_file      STRING,
    transcript_file STRING,
    audio_name      STRING,
    transcript      STRING,
    total_words     INTEGER,
    duration_seconds FLOAT64,
    language        STRING,
    processed_at    TIMESTAMP
);


-- ── 3. AUDIO FEATURES ────────────────────────────────────────
-- Stores extracted audio features for ML purposes
-- Only processes files created on or after 2026-04-05
-- Uses LEFT JOIN with pipeline_status to find unprocessed files

CREATE TABLE IF NOT EXISTS `voice-data-pipeline.voice_data.audio_features` (
    audio_name          STRING,
    mfcc_mean           JSON,       -- 13 MFCC coefficient means
    mfcc_std            JSON,       -- 13 MFCC coefficient standard deviations
    mel_mean            JSON,       -- 128 mel spectrogram band means (dB)
    pitch_mean          FLOAT64,    -- mean fundamental frequency in Hz
    energy_mean         FLOAT64,    -- mean RMS energy
    duration_seconds    FLOAT64,
    extracted_at        STRING
);




