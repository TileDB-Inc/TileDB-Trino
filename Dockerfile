FROM ubuntu:20.04
MAINTAINER help@tiledb.io

ENV TRINO_VERSION=433
ENV TRINO_HOME=/opt/trino
ENV TRINO_CONF_DIR=${TRINO_HOME}/etc

# Install necessary packages including curl, ca-certificates, wget, Python 3, and Java 17
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl ca-certificates wget python-is-python3 openjdk-17-jdk less && \
    rm -rf /var/lib/apt/lists/*


# Download trino cluster
RUN curl -L https://repo1.maven.org/maven2/io/trino/trino-server/${TRINO_VERSION}/trino-server-${TRINO_VERSION}.tar.gz -o /tmp/trino-server.tgz && \
    tar -xzf /tmp/trino-server.tgz -C /opt && \
    ln -s /opt/trino-server-${TRINO_VERSION} ${TRINO_HOME} && \
    mkdir -p ${TRINO_HOME}/data && \
    rm -f /tmp/trino-server.tgz

# Download trino CLI
ADD https://repo1.maven.org/maven2/io/trino/trino-cli/${TRINO_VERSION}/trino-cli-${TRINO_VERSION}-executable.jar ${TRINO_HOME}/bin/

RUN chmod +x ${TRINO_HOME}/bin/trino-cli-${TRINO_VERSION}-executable.jar

ARG TRINO_TILEDB_VERSION=1.17.2

# Download latest trino release
RUN mkdir ${TRINO_HOME}/plugin/tiledb && \
    cd ${TRINO_HOME}/plugin/tiledb && \
    curl -s https://api.github.com/repos/TileDB-Inc/TileDB-Trino/releases/tags/${TRINO_TILEDB_VERSION} \
    | grep "browser_download_url.*jar" \
    | cut -d : -f 2,3 \
    | tr -d \" \
    | wget -i -

# Add entry script to start trino server and cli
ADD docker/entrypoint.sh ${TRINO_HOME}/bin/

RUN chmod +x ${TRINO_HOME}/bin/entrypoint.sh

# Add example arrays
ADD src/test/resources/tiledb_arrays /opt/tiledb_example_arrays

WORKDIR ${TRINO_HOME}

# Add configuration parameters
COPY docker/etc ${TRINO_HOME}/etc

# Expose port for trino ui
EXPOSE 8080

ENV PATH=${PATH}:"${TRINO_HOME}/bin"

# Volumes for config and data (used for stats)
VOLUME ["${TRINO_HOME}/etc", "${TRINO_HOME}/data"]

# Set default command to entry point script
CMD ["./bin/entrypoint.sh"]

