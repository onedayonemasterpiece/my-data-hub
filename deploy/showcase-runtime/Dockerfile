# syntax=docker/dockerfile:1.7
FROM node:22-bookworm-slim AS node-runtime

FROM node:22-bookworm-slim AS renderer-deps
WORKDIR /opt/showcase-site
COPY showcase-site/package.json showcase-site/package-lock.json ./
RUN npm ci --omit=dev --no-audit --no-fund

FROM python:3.12-slim-bookworm AS runtime
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    MY_DATA_HUB_SHOWCASE_SITE_TEMPLATE_DIR=/opt/showcase-site \
    MY_DATA_HUB_SHOWCASE_SITE_DIR=/work/showcase-site \
    MY_DATA_HUB_ARTIFACT_ROOT=/var/lib/my-data-hub-showcase

COPY --from=node-runtime /usr/local/bin/node /usr/local/bin/node
COPY --from=node-runtime /usr/local/lib/node_modules /usr/local/lib/node_modules
RUN ln -s /usr/local/lib/node_modules/npm/bin/npm-cli.js /usr/local/bin/npm \
    && ln -s /usr/local/lib/node_modules/npm/bin/npx-cli.js /usr/local/bin/npx

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN python -m pip install .

COPY showcase-site /opt/showcase-site
COPY --from=renderer-deps /opt/showcase-site/node_modules /opt/showcase-site/node_modules

RUN groupadd --system --gid 65532 showcase \
    && useradd --system --uid 65532 --gid 65532 --home-dir /nonexistent --shell /usr/sbin/nologin showcase \
    && mkdir -p /work /var/lib/my-data-hub-showcase \
    && chown -R 65532:65532 /work /var/lib/my-data-hub-showcase \
    && chmod -R a-w /app /opt/showcase-site

USER 65532:65532
EXPOSE 8790
ENTRYPOINT ["my-data-hub-showcase-runtime"]
