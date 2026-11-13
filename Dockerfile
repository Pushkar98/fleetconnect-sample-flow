# One small Python image used by two containers: `api` (API + web map + agent)
# and `sink` (Kafka -> PostGIS). Code is mounted from the repo, not copied,
# so edits on your laptop are picked up on restart.
FROM python:3.12.8-slim
WORKDIR /workspace
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
ENV PYTHONPATH=/workspace PYTHONUNBUFFERED=1
