FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app

RUN useradd --create-home --uid 10001 mdh \
    && install -d -o mdh -g mdh -m 0700 /state

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
COPY sql ./sql
COPY schemas ./schemas
RUN pip install '.[kaggle]'

USER mdh
ENTRYPOINT ["python", "-m", "my_data_hub.control_plane"]
