# E.D.I.T.H's brain + relay (edith_app.py).
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    DATA_DIR=/data

# libgomp1: OpenMP for llama.cpp, faster-whisper (CTranslate2) and onnxruntime
RUN apt-get update \
    && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY edith_app.py edith_brain.py edith_barrier.py edith_mind.py edith_relay.py ./
COPY edith_library ./edith_library

EXPOSE 8000
CMD ["uvicorn", "edith_app:app", "--host", "0.0.0.0", "--port", "8000", "--ws-ping-interval", "20"]
