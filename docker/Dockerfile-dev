FROM openjdk:8
MAINTAINER help@tiledb.io

ENV PRESTO_VERSION=315
ENV PRESTO_HOME=/opt/presto
ENV PRESTO_CONF_DIR=${PRESTO_HOME}/etc

# Add less for pagenation
RUN apt-get update && apt-get install -y --no-install-recommends \
		less && \
    rm -rf /var/lib/apt/lists/

# Download presto cluster
RUN curl -L https://repo1.maven.org/maven2/io/prestosql/presto-server/${PRESTO_VERSION}/presto-server-${PRESTO_VERSION}.tar.gz -o /tmp/presto-server.tgz && \
    tar -xzf /tmp/presto-server.tgz -C /opt && \
    ln -s /opt/presto-server-${PRESTO_VERSION} ${PRESTO_HOME} && \
    mkdir -p ${PRESTO_HOME}/data && \
    rm -f /tmp/presto-server.tgz

# Download presto CLI
ADD https://repo1.maven.org/maven2/io/prestosql/presto-cli/${PRESTO_VERSION}/presto-cli-${PRESTO_VERSION}-executable.jar ${PRESTO_HOME}/bin/

RUN chmod +x ${PRESTO_HOME}/bin/presto-cli-${PRESTO_VERSION}-executable.jar

# Copy plugin to build
COPY . /tmp/presto-build

WORKDIR /tmp/presto-build

# Add entry script to start presto server and cli
ADD docker/entrypoint.sh ${PRESTO_HOME}/bin/

RUN chmod +x ${PRESTO_HOME}/bin/entrypoint.sh

# Add example arrays
ADD src/test/resources/tiledb_arrays /opt/tiledb_example_arrays

# Build presto and copy package to PRESTO_HOME and purge build
RUN ./mvnw package -DskipTests && \
    mkdir ${PRESTO_HOME}/plugin/tiledb && \
    cp target/presto-tiledb-${PRESTO_VERSION}.jar ${PRESTO_HOME}/plugin/tiledb/presto-tiledb-${PRESTO_VERSION}.jar && \
    ./mvnw clean && \
    rm -rf ${HOME}/.m2

WORKDIR ${PRESTO_HOME}

# Remove plugin sources
RUN rm -r /tmp/presto-build

# Add configuration parameters
COPY docker/etc ${PRESTO_HOME}/etc

# Expose port for presto ui
EXPOSE 8080

ENV PATH=${PATH}:"${PRESTO_HOME}/bin"

# Volumes for config and data (used for stats)
VOLUME ["${PRESTO_HOME}/etc", "${PRESTO_HOME}/data"]

# Set default command to entry point script
CMD ["./bin/entrypoint.sh"]

