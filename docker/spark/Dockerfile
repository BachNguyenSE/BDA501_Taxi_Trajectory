# Spark runtime for the whole team: identical Java/Python/PySpark everywhere.
# Spark 4.0.x supports Java 17/21 only, so the JVM is pinned here instead of
# relying on whatever JDK each laptop has installed.
FROM python:3.12-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive \
    JAVA_HOME=/opt/java \
    PYTHONHASHSEED=42 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYSPARK_PYTHON=python3 \
    TZ=UTC

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl \
 && rm -rf /var/lib/apt/lists/* \
 # JVM path differs between arm64 and amd64; expose it at a fixed /opt/java.
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java \
 && /opt/java/bin/java -version

WORKDIR /workspace
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

EXPOSE 8888 4040
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", \
     "--ServerApp.token=", "--ServerApp.root_dir=/workspace"]
