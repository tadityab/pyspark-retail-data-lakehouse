FROM python:3.11-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PYSPARK_PYTHON=python3

WORKDIR /app

COPY pyproject.toml .
RUN pip install --no-cache-dir pyspark==3.5.* delta-spark==3.2.* pyyaml pytest

COPY . .

ENTRYPOINT ["python"]
CMD ["jobs/run_full_pipeline.py"]
