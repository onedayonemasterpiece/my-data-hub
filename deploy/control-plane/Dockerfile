FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/* \
    && command -v ffmpeg >/dev/null \
    && command -v ffprobe >/dev/null \
    && useradd --create-home --uid 10001 mdh \
    # Production Compose deliberately replaces the image user with the
    # owning host UID and mounts private 0700 state directories below this
    # path.  The mount-point parent must therefore be traversable without
    # making any mounted state listable or readable.
    && install -d -o mdh -g mdh -m 0711 /state

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
COPY sql ./sql
COPY schemas ./schemas
RUN pip install '.[kaggle]'

USER mdh
ENTRYPOINT ["python", "-m", "my_data_hub.control_plane"]
