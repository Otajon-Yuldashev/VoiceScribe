import uuid
import requests
import streamlit as st
import time
from google.cloud import storage
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter
from datetime import datetime, timedelta
import io

# page config
st.set_page_config(
    page_title="VoiceScribe",
    page_icon="🎙️",
    layout="centered"
)

# custom styling
st.markdown("""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@300;400;500&family=DM+Serif+Display&family=Syne:wght@400;600;700;800&display=swap');

        body, .stApp {
            background-color: #F5F5F0;
            font-family: 'DM Mono', monospace;
        }

        h1, h2, h3 {
            font-family: 'DM Serif Display', serif;
            color: #1A3A5C;
        }

        /* upload area */
        [data-testid="stFileUploader"] {
            background: #EEEEE8;
            border: 2px dashed #BBBBB0;
            border-radius: 16px;
            padding: 1rem;
            transition: border-color 0.2s;
        }
        [data-testid="stFileUploader"]:hover {
            border-color: #2B5EA7;
        }

        /* buttons */
        .stButton > button {
            background-color: #1A3A5C;
            color: #F5F5F0;
            border-radius: 6px;
            border: none;
            padding: 0.6rem 2rem;
            font-family: 'DM Mono', monospace;
            font-size: 0.85rem;
            font-weight: 500;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            transition: all 0.2s ease;
            width: fit-content;
        }
        .stButton > button:hover {
            background-color: #2B5EA7;
            color: #F5F5F0;
            transform: translateY(-1px);
        }

        /* language cards */
        .lang-card {
            background: #EEEEE8;
            border: 1.5px solid #BBBBB0;
            border-radius: 10px;
            padding: 0.8rem 0.5rem;
            text-align: center;
            font-family: 'DM Mono', monospace;
            font-size: 0.78rem;
            letter-spacing: 0.06em;
            color: #3A3A35;
            cursor: pointer;
            transition: all 0.2s ease;
        }
        .lang-card.selected {
            background: #EAF4EC;
            border-color: #2B8A3E;
            color: #1A4A25;
            transform: scale(1.04);
            font-weight: 500;
        }

        /* language card buttons — override default button style */
        div[data-testid="column"] .stButton > button {
            background-color: #EEEEE8;
            color: #3A3A35;
            border: 1.5px solid #BBBBB0;
            border-radius: 10px;
            padding: 0.8rem 0.5rem;
            font-family: 'DM Mono', monospace;
            font-size: 0.78rem;
            font-weight: 400;
            letter-spacing: 0.06em;
            text-transform: lowercase;
            width: 100%;
            transition: all 0.2s ease;
            transform: none;
        }
        div[data-testid="column"] .stButton > button:hover {
            background-color: #E0F0E3;
            border-color: #2B8A3E;
            color: #1A4A25;
            transform: scale(1.02);
        }

        /* status cards */
        .stage-card {
            background: #EEEEE8;
            border-left: 3px solid #BBBBB0;
            border-radius: 4px;
            padding: 0.85rem 1.2rem;
            margin: 0.4rem 0;
            font-family: 'DM Mono', monospace;
            font-size: 0.82rem;
            color: #3A3A35;
            display: flex;
            align-items: center;
            gap: 0.6rem;
            opacity: 0;
            animation: fadeSlideIn 0.4s ease forwards;
        }
        .stage-card.done {
            border-left-color: #2B8A3E;
            background: #EAF4EC;
            color: #1A4A25;
        }
        .stage-card.active {
            border-left-color: #2B5EA7;
            background: #EAF0FB;
            color: #1A3A5C;
        }
        .stage-card.failed {
            border-left-color: #C0392B;
            background: #FDECEA;
            color: #7A1A10;
        }

        /* metric cards */
        .metric-card {
            background: #EEEEE8;
            border-radius: 12px;
            padding: 1.4rem 1rem;
            text-align: center;
            border: 1px solid #DDDDD8;
            opacity: 0;
            animation: fadeSlideIn 0.5s ease forwards;
        }
        .metric-label {
            font-family: 'DM Mono', monospace;
            font-size: 0.7rem;
            letter-spacing: 0.12em;
            text-transform: uppercase;
            color: #888880;
            margin-bottom: 0.4rem;
        }
        .metric-value {
            font-family: 'DM Serif Display', serif;
            font-size: 2rem;
            font-weight: 400;
            color: #1A3A5C;
            line-height: 1;
        }
        .metric-unit {
            font-family: 'DM Mono', monospace;
            font-size: 0.72rem;
            color: #888880;
            margin-top: 0.3rem;
        }

        /* transcript card */
        .transcript-card {
            background: #EEEEE8;
            border-radius: 12px;
            padding: 1.8rem;
            margin: 1rem 0;
            font-family: 'DM Mono', monospace;
            font-size: 0.88rem;
            line-height: 1.8;
            color: #2A2A25;
            border: 1px solid #DDDDD8;
            white-space: pre-wrap;
            opacity: 0;
            animation: fadeSlideIn 0.5s ease forwards;
        }

        /* divider */
        .custom-divider {
            border: none;
            border-top: 1px solid #DDDDD8;
            margin: 1.5rem 0;
        }

        /* section label */
        .section-label {
            font-family: 'DM Mono', monospace;
            font-size: 0.7rem;
            letter-spacing: 0.15em;
            text-transform: uppercase;
            color: #888880;
            margin-bottom: 0.8rem;
        }

        /* status header */
        .status-header {
            font-family: 'DM Serif Display', serif;
            font-size: 0.95rem;
            font-weight: 400;
            color: #1A3A5C;
            letter-spacing: 0.05em;
            margin-bottom: 0.8rem;
        }

        /* info box override */
        [data-testid="stAlert"] {
            background: #EAF0FB;
            border: 1px solid #C5D8F5;
            border-radius: 8px;
            font-family: 'DM Mono', monospace;
            font-size: 0.82rem;
        }

        /* fade + slide animation */
        @keyframes fadeSlideIn {
            from { opacity: 0; transform: translateY(10px); }
            to   { opacity: 1; transform: translateY(0); }
        }

        /* header — full width centered */
        .header-wrap {
            width: 100%;
            display: flex;
            flex-direction: column;
            align-items: center;
            padding: 2rem 0 0.5rem 0;
            text-align: center;
        }

        /* typewriter title */
        .typewriter {
            font-family: 'DM Serif Display', serif;
            font-size: 2.8rem;
            font-weight: 400;
            color: #1A3A5C;
            letter-spacing: 0.01em;
            overflow: hidden;
            white-space: nowrap;
            border-right: 2px solid #1A3A5C;
            width: 0;
            display: inline-block;
            animation:
                typing 1.2s steps(11, end) forwards,
                blink 0.6s step-end 1.2s 4,
                hideCursor 0s 3.6s forwards;
        }
        @keyframes typing {
            from { width: 0 }
            to   { width: 11ch }
        }
        @keyframes blink {
            0%, 100% { border-color: #1A3A5C; }
            50%       { border-color: transparent; }
        }
        @keyframes hideCursor {
            to { border-color: transparent; border-width: 0; }
        }

        /* subtitle */
        .subtitle-fade {
            font-family: 'DM Mono', monospace;
            font-size: 0.72rem;
            color: #4A90D9;
            text-align: center;
            letter-spacing: 0.08em;
            margin-top: 0.6rem;
            width: 100%;
            white-space: nowrap;
            opacity: 0;
            animation: fadeSlideIn 0.8s ease 1.5s forwards;
        }

        /* processing message */
        .processing-msg {
            font-family: 'DM Mono', monospace;
            font-size: 0.78rem;
            color: #2B5EA7;
            margin-bottom: 1.2rem;
            letter-spacing: 0.05em;
            opacity: 0;
            animation: fadeSlideIn 0.5s ease forwards;
        }

        /* selected language card highlight */
        .lang-selected-indicator {
            display: inline-block;
            width: 6px;
            height: 6px;
            border-radius: 50%;
            background: #2B8A3E;
            margin-right: 6px;
            vertical-align: middle;
        }

        /* hide streamlit branding */
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
    </style>
""", unsafe_allow_html=True)

BUCKET = "voice_data_bucket_demo"
MAX_FILE_SIZE_MB = 30
MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024

LANGUAGES = {
    "English":  "en-US",
    "Uzbek":    "uz-UZ",
    "Italian":  "it-IT",
}

# session state init
if "stage" not in st.session_state:
    st.session_state.stage = "idle"
if "audio_name" not in st.session_state:
    st.session_state.audio_name = None
if "result" not in st.session_state:
    st.session_state.result = None
if "selected_language" not in st.session_state:
    st.session_state.selected_language = "English"


def upload_to_gcs(file_bytes, filename):
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET)
    blob = bucket.blob(f"raw_audio/{filename}")
    blob.upload_from_file(io.BytesIO(file_bytes))
    return f"gs://{BUCKET}/raw_audio/{filename}"


def get_pipeline_status(audio_name):
    bq_client = bigquery.Client()
    query = """
        SELECT *
        FROM voice_data.pipeline_status
        WHERE audio_name = @audio_name
        LIMIT 1
    """
    job_config = QueryJobConfig(
        query_parameters=[
            ScalarQueryParameter("audio_name", "STRING", audio_name)
        ]
    )
    results = list(bq_client.query(query, job_config=job_config).result())
    if results:
        return results[0]
    return None


def get_transcript(audio_name):
    bq_client = bigquery.Client()
    query = """
        SELECT transcript, total_words, duration_seconds
        FROM voice_data.audio_metadata
        WHERE audio_name = @audio_name
        LIMIT 1
    """
    job_config = QueryJobConfig(
        query_parameters=[
            ScalarQueryParameter("audio_name", "STRING", audio_name)
        ]
    )
    results = list(bq_client.query(query, job_config=job_config).result())
    if results:
        return results[0]
    return None


def poll_pipeline(audio_name, status_placeholder):
    stages = [
        ("is_converted",       "▶  format conversion"),
        ("is_normalized",      "◈  noise reduction"),
        ("is_transcribed",     "◎  speech recognition"),
        ("is_metadata_loaded", "◷  metadata extraction"),
    ]

    while True:
        status = get_pipeline_status(audio_name)

        if not status:
            with status_placeholder.container():
                st.markdown('<div class="status-header">processing audio</div>', unsafe_allow_html=True)
                st.markdown('<div class="stage-card active">⌛&nbsp; starting process...</div>', unsafe_allow_html=True)
            time.sleep(3)
            continue

        with status_placeholder.container():
            st.markdown('<div class="status-header">processing audio</div>', unsafe_allow_html=True)

            all_done = True
            for field, label in stages:
                value = getattr(status, field, False)
                if value:
                    st.markdown(f"""
                        <div class="stage-card done">
                            ✓ &nbsp;<span>{label}</span>
                        </div>
                    """, unsafe_allow_html=True)
                else:
                    all_done = False
                    if status.failed_at_stage:
                        st.markdown(f"""
                            <div class="stage-card failed">
                                ✗ &nbsp;<span>{label}</span>
                                <span style='margin-left:auto;font-size:0.7rem;opacity:0.8'>{status.error_message}</span>
                            </div>
                        """, unsafe_allow_html=True)
                    else:
                        st.markdown(f"""
                            <div class="stage-card active">
                                ◌ &nbsp;<span>{label}</span>
                            </div>
                        """, unsafe_allow_html=True)

            if all_done:
                return True

            if status.failed_at_stage:
                return False

        time.sleep(5)


# ── UI ──────────────────────────────────────────────────────────────

st.markdown("""
    <div class="header-wrap">
        <div class="typewriter">VoiceScribe</div>
        <div class="subtitle-fade">speak slightly louder and clear for better transcript</div>
    </div>
    <hr style='border:none; border-top: 1px solid #DDDDD8; margin: 1rem 0 2rem 0;'>
""", unsafe_allow_html=True)


# ── IDLE STATE ──────────────────────────────────────────────────────
if st.session_state.stage == "idle":

    # upload section
    st.markdown(f'<div class="section-label">upload audio file — max {MAX_FILE_SIZE_MB}mb</div>', unsafe_allow_html=True)
    uploaded_file = st.file_uploader(
        "drag & drop or browse — mp3, m4a, wav",
        type=["mp3", "m4a", "wav"],
        label_visibility="visible"
    )

    if uploaded_file:
        ext = uploaded_file.name.rsplit(".", 1)[-1].lower()
        if ext not in ["mp3", "m4a", "wav"]:
            st.markdown("""
                <div style='font-family: DM Mono, monospace; font-size:0.78rem;
                            color:#C0392B; margin-top:0.5rem; letter-spacing:0.04em;'>
                    ✗ &nbsp; unsupported file type — use mp3, m4a, or wav
                </div>
            """, unsafe_allow_html=True)
            uploaded_file = None
        elif uploaded_file.size > MAX_FILE_SIZE_BYTES:
            st.markdown(f"""
                <div style='font-family: DM Mono, monospace; font-size:0.78rem;
                            color:#C0392B; margin-top:0.5rem; letter-spacing:0.04em;'>
                    ✗ &nbsp; file too large — maximum size is {MAX_FILE_SIZE_MB}mb
                </div>
            """, unsafe_allow_html=True)
            uploaded_file = None

    # language selector
    st.markdown('<div style="height:1.2rem"></div>', unsafe_allow_html=True)
    st.markdown('<div class="section-label">select language</div>', unsafe_allow_html=True)

    col_en, col_uz, col_it = st.columns(3)

    with col_en:
        selected = st.session_state.selected_language == "English"
        label = "✓  english" if selected else "english"
        if st.button(label, key="lang_en"):
            st.session_state.selected_language = "English"
            st.rerun()
        if selected:
            st.markdown("""
                <div style='height:3px; background:#2B8A3E; border-radius:2px; margin-top:-8px;'></div>
            """, unsafe_allow_html=True)

    with col_uz:
        selected = st.session_state.selected_language == "Uzbek"
        label = "✓  uzbek" if selected else "uzbek"
        if st.button(label, key="lang_uz"):
            st.session_state.selected_language = "Uzbek"
            st.rerun()
        if selected:
            st.markdown("""
                <div style='height:3px; background:#2B8A3E; border-radius:2px; margin-top:-8px;'></div>
            """, unsafe_allow_html=True)

    with col_it:
        selected = st.session_state.selected_language == "Italian"
        label = "✓  italian" if selected else "italian"
        if st.button(label, key="lang_it"):
            st.session_state.selected_language = "Italian"
            st.rerun()
        if selected:
            st.markdown("""
                <div style='height:3px; background:#2B8A3E; border-radius:2px; margin-top:-8px;'></div>
            """, unsafe_allow_html=True)

    # record live
    st.markdown('<div style="height:1.2rem"></div>', unsafe_allow_html=True)
    st.markdown('<div class="section-label">or record live</div>', unsafe_allow_html=True)

    try:
        from streamlit_audiorec import st_audiorec
        audio_bytes = st_audiorec()
    except ImportError:
        st.markdown("""
            <div style='font-family: DM Mono, monospace; font-size:0.78rem;
                        color:#888880; letter-spacing:0.04em;'>
                ◌ &nbsp; audio recording unavailable
            </div>
        """, unsafe_allow_html=True)
        audio_bytes = None

    if uploaded_file or audio_bytes:
        st.markdown('<div style="height:0.8rem"></div>', unsafe_allow_html=True)
        if st.button("→  run transcription"):

            language_code = LANGUAGES[st.session_state.selected_language]

            if uploaded_file:
                file_bytes = uploaded_file.read()
                original_name = uploaded_file.name.rsplit(".", 1)[0]
                ext = uploaded_file.name.rsplit(".", 1)[1]
                filename = f"{uuid.uuid4()}_{original_name}_{language_code}.{ext}"
            else:
                file_bytes = audio_bytes
                filename = f"{uuid.uuid4()}_recording_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{language_code}.wav"

            audio_name = filename.rsplit(".", 1)[0]
            st.session_state.audio_name = audio_name
            st.session_state.stage = "processing"

            with st.spinner("uploading..."):
                upload_to_gcs(file_bytes, filename)

            st.rerun()


# ── PROCESSING STATE ─────────────────────────────────────────────────
elif st.session_state.stage == "processing":

    st.markdown("""
        <div class="processing-msg">
            ◌ &nbsp; processing — this usually takes 2–5 minutes
        </div>
    """, unsafe_allow_html=True)

    status_placeholder = st.empty()
    success = poll_pipeline(st.session_state.audio_name, status_placeholder)

    if success:
        st.session_state.result = get_transcript(st.session_state.audio_name)
        st.session_state.stage = "done"
        st.rerun()
    else:
        st.markdown("""
            <div style='font-family: DM Mono, monospace; font-size:0.82rem;
                        color:#C0392B; margin-top:1rem;
                        opacity:0; animation: fadeSlideIn 0.5s ease forwards;'>
                ✗ &nbsp; pipeline failed — check cloud run logs
            </div>
        """, unsafe_allow_html=True)


# ── DONE STATE ───────────────────────────────────────────────────────
elif st.session_state.stage == "done":

    result = st.session_state.result

    if result:
        st.markdown('<div class="section-label">transcript</div>', unsafe_allow_html=True)
        st.markdown(f"""
            <div class="transcript-card">{result.transcript}</div>
        """, unsafe_allow_html=True)

        st.markdown('<div style="height:1rem"></div>', unsafe_allow_html=True)

        st.markdown('<div class="section-label">audio metrics</div>', unsafe_allow_html=True)

        wpm = round(result.total_words / (result.duration_seconds / 60)) if result.duration_seconds > 0 else 0

        col1, col2, col3 = st.columns(3)

        with col1:
            st.markdown(f"""
                <div class="metric-card" style="animation-delay:0.1s">
                    <div class="metric-label">duration</div>
                    <div class="metric-value">{round(result.duration_seconds, 1)}</div>
                    <div class="metric-unit">seconds</div>
                </div>
            """, unsafe_allow_html=True)

        with col2:
            st.markdown(f"""
                <div class="metric-card" style="animation-delay:0.2s">
                    <div class="metric-label">word count</div>
                    <div class="metric-value">{result.total_words}</div>
                    <div class="metric-unit">words</div>
                </div>
            """, unsafe_allow_html=True)

        with col3:
            st.markdown(f"""
                <div class="metric-card" style="animation-delay:0.3s">
                    <div class="metric-label">speaking pace</div>
                    <div class="metric-value">{wpm}</div>
                    <div class="metric-unit">words / min</div>
                </div>
            """, unsafe_allow_html=True)

    st.markdown('<div style="height:2rem"></div>', unsafe_allow_html=True)
    st.markdown('<hr class="custom-divider">', unsafe_allow_html=True)

    _, col, _ = st.columns([2, 1, 2])
    with col:
        if st.button("↑  upload again"):
            st.session_state.stage = "idle"
            st.session_state.audio_name = None
            st.session_state.result = None
            st.rerun()


            


