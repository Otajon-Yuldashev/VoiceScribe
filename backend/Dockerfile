FROM python:3.12-slim

RUN apt-get update && apt-get install -y \
    ffmpeg gcc g++ libsndfile1 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY orchestrate.py .
COPY convert_beam.py .
COPY normalize_beam.py .
COPY transcript_beam.py .
COPY metadata_beam.py .
COPY features_beam.py .

ENV PYTHONUNBUFFERED=1
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "1", "--timeout", "0", "orchestrate:app"]


