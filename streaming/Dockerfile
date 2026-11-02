FROM python:3.11-slim-bookworm

# stdout tanpa buffer: tabel running_total (print Python) tampil langsung di `logs`
ENV PYTHONUNBUFFERED=1

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py ./
CMD ["python", "spark_streaming_job.py"]
