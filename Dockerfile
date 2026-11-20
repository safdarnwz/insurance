# syntax=docker/dockerfile:1
# =============================================================================
# Ek image, saari services: Airflow (api-server/scheduler/dag-processor),
# PySpark + Delta (local mode), dbt (DuckDB + Databricks), Databricks CLI.
#
# KYUN do Python environments:
#   * Airflow apne "constraints file" ke exact versions maangta hai.
#   * Pipeline (pyspark, dbt, deltalake) ko alag versions chahiye.
#   Ek hi env me dono = dependency conflict. Isliye pipeline /opt/venvs/pipeline
#   me, aur Airflow tasks use BashOperator se CLI ki tarah chalate hain.
# KYA NAHI KARNA: `_PIP_ADDITIONAL_REQUIREMENTS` se container start par packages
#   install - har restart slow + non-reproducible. Sab kuch image build me.
# =============================================================================
ARG AIRFLOW_VERSION=3.1.3
ARG PYTHON_VERSION=3.11
# Company me aksar Docker Hub ki jagah internal registry mirror hota hai -> ARG se badlo
ARG AIRFLOW_BASE_IMAGE=apache/airflow

# Java (Spark ko JVM chahiye) official Temurin image se copy -> apt-get ki zaroorat nahi
FROM eclipse-temurin:17-jre AS jre

FROM ${AIRFLOW_BASE_IMAGE}:${AIRFLOW_VERSION}-python${PYTHON_VERSION}
ARG AIRFLOW_VERSION
ARG PYTHON_VERSION
ARG TARGETARCH
ARG DATABRICKS_CLI_VERSION=0.278.0

USER root
COPY --from=jre /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk \
    PATH="/opt/java/openjdk/bin:${PATH}"

# Databricks CLI (asset bundles deploy). Version pinned.
RUN set -eux; \
    arch="$( [ "${TARGETARCH:-amd64}" = "arm64" ] && echo arm64 || echo amd64 )"; \
    curl -fsSL -o /tmp/dbcli.zip \
      "https://github.com/databricks/cli/releases/download/v${DATABRICKS_CLI_VERSION}/databricks_cli_${DATABRICKS_CLI_VERSION}_linux_${arch}.zip"; \
    python -m zipfile -e /tmp/dbcli.zip /tmp/dbcli; \
    install -m 0755 /tmp/dbcli/databricks /usr/local/bin/databricks; \
    rm -rf /tmp/dbcli /tmp/dbcli.zip

RUN python -m venv /opt/venvs/pipeline \
 && chown -R airflow:0 /opt/venvs && chmod -R g+rwX /opt/venvs

USER airflow

# 1) Airflow + Databricks provider, Airflow ke tested constraints ke saath
RUN pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      "apache-airflow-providers-databricks" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# 2) Pipeline venv (requirements pehle copy: code badalne par ye layer cache se aati hai)
COPY --chown=airflow:0 requirements.txt /tmp/requirements.txt
RUN /opt/venvs/pipeline/bin/pip install --no-cache-dir -r /tmp/requirements.txt

# 3) Delta Lake jars pyspark ke andar - runtime par Maven download nahi
#    (offline bhi chale, har container start fast, reproducible)
ARG DELTA_VERSION=3.2.1
ADD --chown=airflow:0 \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/${DELTA_VERSION}/delta-spark_2.12-${DELTA_VERSION}.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar \
    /opt/venvs/pipeline/lib/python${PYTHON_VERSION}/site-packages/pyspark/jars/

# 4) Project code (editable install: docker-compose isi path par repo mount karta
#    hai, to VS Code me code badlo -> container me turant reflect, rebuild nahi)
COPY --chown=airflow:0 . /opt/project
RUN /opt/venvs/pipeline/bin/pip install --no-cache-dir --no-deps -e /opt/project

ENV PIPELINE_BIN=/opt/venvs/pipeline/bin/insurance-pipeline \
    DBT_EXECUTABLE=/opt/venvs/pipeline/bin/dbt \
    PYTHONUNBUFFERED=1
WORKDIR /opt/project
