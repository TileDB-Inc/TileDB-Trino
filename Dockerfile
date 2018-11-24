FROM openjdk:8
MAINTAINER support@tiledb.io

ENV PRESTO_VERSION=0.211
ENV PRESTO_HOME=/opt/presto

RUN apt-get update && apt-get install -y --no-install-recommends \
		less && \
    rm -rf /var/lib/apt/lists/

RUN curl -L https://repo1.maven.org/maven2/com/facebook/presto/presto-server/${PRESTO_VERSION}/presto-server-${PRESTO_VERSION}.tar.gz -o /tmp/presto-server.tgz && \
    tar -xzf /tmp/presto-server.tgz -C /opt && \
    ln -s /opt/presto-server-${PRESTO_VERSION} ${PRESTO_HOME} && \
    mkdir -p ${PRESTO_HOME}/data && \
    rm -f /tmp/presto-server.tgz

ADD https://repo1.maven.org/maven2/com/facebook/presto/presto-cli/${PRESTO_VERSION}/presto-cli-${PRESTO_VERSION}-executable.jar ${PRESTO_HOME}/bin/

RUN chmod +x ${PRESTO_HOME}/bin/presto-cli-${PRESTO_VERSION}-executable.jar

COPY . /tmp/presto-build

WORKDIR /tmp/presto-build

ADD docker/entrypoint.sh ${PRESTO_HOME}/bin/

RUN chmod +x ${PRESTO_HOME}/bin/entrypoint.sh

# Build presto and copy package to PRESTO_HOME and purge build
RUN ./mvnw package -DskipTests && \
    mkdir ${PRESTO_HOME}/plugin/tiledb && \
    cp target/presto-tiledb-${PRESTO_VERSION}.jar ${PRESTO_HOME}/plugin/tiledb/presto-tiledb-${PRESTO_VERSION}.jar && \
    ./mvnw clean && \
    rm -rf ${HOME}/.m2

WORKDIR ${PRESTO_HOME}

RUN rm -r /tmp/presto-build

COPY docker/etc ${PRESTO_HOME}/etc
EXPOSE 8080

VOLUME ["${PRESTO_HOME}/etc", "${PRESTO_HOME}/data"]

WORKDIR ${PRESTO_HOME}

CMD ["./bin/entrypoint.sh"]

