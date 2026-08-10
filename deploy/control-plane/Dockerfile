FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app

RUN useradd --create-home --uid 10001 mdh \
    && pip install 'fastapi>=0.115,<1' 'uvicorn>=0.34,<1'

WORKDIR /app
COPY src/my_data_hub/__init__.py ./my_data_hub/__init__.py
COPY src/my_data_hub/control_plane ./my_data_hub/control_plane

USER mdh
ENTRYPOINT ["python", "-m", "my_data_hub.control_plane"]
