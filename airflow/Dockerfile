FROM ghcr.io/astral-sh/uv:0.11.26 AS uv
FROM apache/airflow:2.10.5-python3.12

COPY --from=uv /uv /uvx /bin/

USER root
WORKDIR /opt/airflow/project
COPY pyproject.toml uv.lock ./
RUN chown -R airflow:root /opt/airflow/project

USER airflow
RUN uv sync --locked --no-dev --no-install-project

ENV PATH=/opt/airflow/project/.venv/bin:/usr/local/sbin:/usr/local/bin:/usr/bin:/var/lib/flatpak/exports/bin:/usr/lib/jvm/default/bin:/usr/bin/site_perl:/usr/bin/vendor_perl:/usr/bin/core_perl
