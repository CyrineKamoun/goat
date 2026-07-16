# ==== STAGE 1: General debian environment ====
FROM debian:bookworm-slim AS linux-base

LABEL org.opencontainers.image.source=https://github.com/plan4better/goat
LABEL org.opencontainers.image.description="Goat Processes API"
LABEL org.opencontainers.image.licenses="GPL-3.0"

ARG PYTHON_VERSION=3.11


ENV LC_CTYPE=C.utf8 \
    UV_PROJECT_ENVIRONMENT="/venv" \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_INSTALL_DIR="/python" \
    UV_PYTHON=python$PYTHON_VERSION

ENV PATH="$UV_PROJECT_ENVIRONMENT/bin:$PATH"

RUN apt-get update
RUN apt-get upgrade -y

# Install general required dependencies
RUN apt-get install --no-install-recommends -y tzdata

# ===== STAGE 2: Python environment ====
# processes needs no GDAL/routing and no build toolchain — it imports the
# analytics stack (all manylinux wheels) for OGC schema generation but never
# GDAL/routing (those run only on the Windmill worker image).
FROM linux-base AS python-base
# Install uv
COPY --from=ghcr.io/astral-sh/uv:0.9.2 /uv /usr/local/bin/uv

# Create virtual environment and install dependencies
COPY apps/processes /app/apps/processes
COPY packages/python/goatlib /app/packages/python/goatlib
COPY packages/python/goatobs /app/packages/python/goatobs
COPY pyproject.toml uv.lock /app/

WORKDIR /app

RUN uv sync --frozen --no-dev --no-editable --package=processes

# Bake DuckDB REPOSITORY extensions (spatial/httpfs/postgres/ducklake) into the
# image so the runtime never fetches them from extensions.duckdb.org — removes
# the egress/TLS dependency and speeds cold start. Needs ca-certificates+egress,
# available only in this build stage.
RUN apt-get update && apt-get install --no-install-recommends -y ca-certificates \
    && python -c "import duckdb; from goatlib.storage.ducklake import REQUIRED_DUCKDB_EXTENSIONS as X; con=duckdb.connect(config={'extension_directory':'/opt/duckdb-extensions'}); [con.execute(f'INSTALL {e}') for e in X]" \
    && rm -rf /var/lib/apt/lists/*

# ==== STAGE 3: Webapp environment ====
FROM linux-base AS webapp

# ca-certificates for TLS (DuckDB extension downloads via httpfs verify against
# the system CA store; also needed for any HTTPS the service makes).
RUN apt-get update && apt-get install --no-install-recommends -y ca-certificates && rm -rf /var/lib/apt/lists/*

RUN useradd -m appuser

# Copy python, virtual env and application
COPY --from=python-base $UV_PYTHON_INSTALL_DIR $UV_PYTHON_INSTALL_DIR
COPY --from=python-base $UV_PROJECT_ENVIRONMENT $UV_PROJECT_ENVIRONMENT
COPY --from=python-base /app /app

# DuckDB extensions baked at build time; goatlib loads them from here at runtime
# (DUCKDB_EXTENSION_DIRECTORY), so no extension is ever downloaded.
COPY --from=python-base /opt/duckdb-extensions /opt/duckdb-extensions
ENV DUCKDB_EXTENSION_DIRECTORY=/opt/duckdb-extensions

RUN chown -R appuser:appuser /app $UV_PROJECT_ENVIRONMENT $UV_PYTHON_INSTALL_DIR /opt/duckdb-extensions
USER appuser

WORKDIR /app

EXPOSE 8000

CMD ["fastapi", "run", "apps/processes/src/processes/main.py"]