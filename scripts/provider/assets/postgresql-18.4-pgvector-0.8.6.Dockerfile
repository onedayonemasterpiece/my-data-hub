FROM ubuntu:22.04@sha256:3b06811b2afd352be909dd088a004166d665dc76d38b13eada33522a9d915c6f AS build
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential bison flex ca-certificates curl bzip2 pkg-config libssl-dev zlib1g-dev && \
    rm -rf /var/lib/apt/lists/*
WORKDIR /build
ARG PG_VERSION=18.4
ARG PG_SHA256=81a81ec695fb0c7901407defaa1d2f7973617154cf27ba74e3a7ab8e64436094
ARG PGVECTOR_VERSION=0.8.6
ARG PGVECTOR_SHA256=10bf9938906e5d643bbc4a7eea104b6f57ba4898e5b76b20e60484ea1d5a7f8f
RUN curl -fsSLo postgresql.tar.bz2 "https://ftp.postgresql.org/pub/source/v${PG_VERSION}/postgresql-${PG_VERSION}.tar.bz2" && \
    echo "${PG_SHA256}  postgresql.tar.bz2" | sha256sum -c - && \
    tar -xjf postgresql.tar.bz2 && \
    curl -fsSLo pgvector.tar.gz "https://github.com/pgvector/pgvector/archive/refs/tags/v${PGVECTOR_VERSION}.tar.gz" && \
    echo "${PGVECTOR_SHA256}  pgvector.tar.gz" | sha256sum -c - && \
    tar -xzf pgvector.tar.gz
WORKDIR /build/postgresql-18.4
RUN ./configure --prefix=/opt/pgsql --with-openssl --with-zlib --without-readline --without-icu \
    --without-libxml --without-libxslt --without-ldap --without-pam --without-gssapi \
    --without-systemd --without-lz4 --without-zstd && \
    make -j2 && make install-strip && \
    make -C contrib/pgcrypto -j2 && make -C contrib/pgcrypto install-strip && \
    make -C contrib/citext -j2 && make -C contrib/citext install-strip && \
    make -C contrib/pg_trgm -j2 && make -C contrib/pg_trgm install-strip
WORKDIR /build/pgvector-0.8.6
RUN make -j2 OPTFLAGS="" PG_CONFIG=/opt/pgsql/bin/pg_config && make install-strip OPTFLAGS="" PG_CONFIG=/opt/pgsql/bin/pg_config
RUN mkdir -p /opt/pgsql/lib/runtime-deps && \
    cp -L /usr/lib/x86_64-linux-gnu/libssl.so.3 /opt/pgsql/lib/runtime-deps/ && \
    cp -L /usr/lib/x86_64-linux-gnu/libcrypto.so.3 /opt/pgsql/lib/runtime-deps/ && \
    cp -L /usr/lib/x86_64-linux-gnu/libz.so.1 /opt/pgsql/lib/runtime-deps/ && \
    /opt/pgsql/bin/postgres --version | grep -F "18.4" && \
    test -f /opt/pgsql/lib/vector.so && test -f /opt/pgsql/share/extension/vector.control && \
    find /opt/pgsql -type f -exec chmod 0644 {} + && find /opt/pgsql/bin -type f -exec chmod 0755 {} + && \
    find /opt/pgsql/lib -type f -name '*.so*' -exec chmod 0755 {} + && \
    tar --sort=name --mtime='UTC 2026-08-11' --owner=0 --group=0 --numeric-owner -C /opt -cf - pgsql | gzip -n -9 > /postgresql-18.4-pgvector-0.8.6-ubuntu22.04-x86_64.tar.gz && \
    sha256sum /postgresql-18.4-pgvector-0.8.6-ubuntu22.04-x86_64.tar.gz > /postgresql-runtime.sha256 && \
    printf '{"schema_version":"my-data-hub-postgresql-runtime-build.v1","postgresql_version":"18.4","postgresql_source_sha256":"%s","pgvector_version":"0.8.6","pgvector_source_sha256":"%s","build_base":"ubuntu:22.04","architecture":"x86_64"}\n' \
      '81a81ec695fb0c7901407defaa1d2f7973617154cf27ba74e3a7ab8e64436094' \
      '10bf9938906e5d643bbc4a7eea104b6f57ba4898e5b76b20e60484ea1d5a7f8f' > /postgresql-runtime-build.json
FROM scratch
COPY --from=build /postgresql-18.4-pgvector-0.8.6-ubuntu22.04-x86_64.tar.gz /
COPY --from=build /postgresql-runtime.sha256 /
COPY --from=build /postgresql-runtime-build.json /
