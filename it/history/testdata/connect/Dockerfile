# Kafka Connect with the Debezium PostgreSQL connector, for the pipeline tests.
#
# The cluster's worker (cluster/docker-compose.yml) installs the same connector, 2.5.4, from Confluent Hub
# every time it starts. A test cannot depend on that: it needs the network at each start and the CDN is not
# always reachable. This image installs the connector once, from the artifact Debezium publishes on Maven
# Central, checked against its published SHA-1, and Docker keeps it.
FROM confluentinc/cp-kafka-connect:7.5.3

USER root
RUN set -eux; \
    version=2.5.4.Final; \
    url="https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/${version}/debezium-connector-postgres-${version}-plugin.tar.gz"; \
    curl -fsSL -o /tmp/debezium.tgz "${url}"; \
    echo "d79664ac279ae9e6d35253ff15db8e03efd4f10b  /tmp/debezium.tgz" | sha1sum -c -; \
    mkdir -p /usr/share/java/debezium-connector-postgres; \
    tar -xzf /tmp/debezium.tgz -C /usr/share/java/debezium-connector-postgres --strip-components=1; \
    rm /tmp/debezium.tgz
USER appuser
