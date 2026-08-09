FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN useradd --create-home --uid 10001 mdh
WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --upgrade pip && pip install .

COPY sql ./sql
COPY config ./config
COPY schemas ./schemas
COPY scripts ./scripts

RUN mkdir -p /var/lib/my-data-hub/artifacts \
    && chown -R mdh:mdh /var/lib/my-data-hub /app

USER mdh
ENTRYPOINT ["python", "-m", "my_data_hub.cli"]
CMD ["api", "serve"]
