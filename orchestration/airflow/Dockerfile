FROM apache/airflow:3.1.7

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

COPY requirements.txt /tmp/banking-requirements.txt
RUN pip install --no-cache-dir -r /tmp/banking-requirements.txt \
    && pip install --no-cache-dir pyspark==3.5.7

COPY --chown=50000:0 . /opt/banking-platform
COPY --chown=50000:0 orchestration/airflow/dags/ /opt/airflow/dags/

USER root
RUN chmod -R g+rwX /opt/banking-platform /opt/airflow/dags
USER airflow