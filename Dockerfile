FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY web.py .

RUN mkdir -p /root/.streamlit
RUN printf '[server]\nenableCORS = false\nenableXsrfProtection = false\n[browser]\ngatherUsageStats = false\n[client]\ntoolbarMode = "minimal"\n' > /root/.streamlit/config.toml

EXPOSE 8080

CMD ["streamlit", "run", "web.py", \
     "--server.port=8080", \
     "--server.address=0.0.0.0", \
     "--server.headless=true", \
     "--server.enableXsrfProtection=false", \
     "--server.enableCORS=false"]
