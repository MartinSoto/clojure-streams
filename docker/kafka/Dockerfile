FROM openjdk:8-jre-alpine

ARG kafka_version=0.11.0.0
ARG scala_version=2.12

RUN apk add --no-cache \
    unzip \
    wget \
    coreutils \
    bash

ENV KAFKA_VERSION=$kafka_version SCALA_VERSION=$scala_version
RUN wget "http://www.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" -O "/tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" \
    && mkdir /opt \
    && tar xfz /tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz -C /opt \
    && rm /tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz \
    && ln -s /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION} /opt/kafka

VOLUME ["/kafka"]

ENV KAFKA_HOME /opt/kafka
ENV PATH ${PATH}:${KAFKA_HOME}/bin

# Location for the Kafka data files.
VOLUME ["/var/lib/kafka"]
ENV KAFKA_LOG_DIRS=/var/lib/kafka/kafka-logs-

ADD entrypoint.sh /bin/entrypoint.sh
ENTRYPOINT ["/bin/entrypoint.sh"]

EXPOSE 9092

CMD ["kafka-server-start.sh", "/opt/kafka/config/server.properties"]
