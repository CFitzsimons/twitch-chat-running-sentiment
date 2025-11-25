FROM python:3.11-slim

# Stop hf and python from dumping data to the wrong spots
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    HF_HOME=/hf_cache \
    TRANSFORMERS_CACHE=/hf_cache

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt /app/requirements.txt

# Have to use cpu only since it's running in docker
RUN pip install --no-cache-dir \
    torch --index-url https://download.pytorch.org/whl/cpu
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py /app/app.py
EXPOSE 5500

# Muti-thread it
CMD ["gunicorn", "-b", "0.0.0.0:5500", "app:app", "--workers", "2", "--threads", "4"]