#!/bin/bash

export CID=$(hostname)

if [[ -z "$KAFKA_ADVERTISED_PORT" ]]; then
    export KAFKA_ADVERTISED_PORT=9092
fi
if [[ -z "$KAFKA_BROKER_ID" ]]; then
    export KAFKA_BROKER_ID=1
fi

# This block adapted from the wurstmeister/kafka image:
# https://github.com/wurstmeister/kafka-docker/blob/378d047b612df19a1f9b6e9bfb060e6507279148/start-kafka.sh#L35
echo >> $KAFKA_HOME/config/server.properties
for VAR in $(env); do
    if [[ $VAR =~ ^KAFKA_ && ! $VAR =~ ^KAFKA_HOME ]]; then
        kafka_name=$(echo "$VAR" | sed -r "s/KAFKA_(.*)=.*/\1/g" | tr '[:upper:]' '[:lower:]' | tr _ .)
        env_var=$(echo "$VAR" | sed -r "s/(.*)=.*/\1/g")
        if egrep -q "(^|^#)$kafka_name=" $KAFKA_HOME/config/server.properties; then
            sed -r -i "s@(^|^#)($kafka_name)=(.*)@\2=${!env_var}@g" $KAFKA_HOME/config/server.properties #note that no config values may contain an '@' char
        else
            echo "$kafka_name=${!env_var}" >> $KAFKA_HOME/config/server.properties
        fi
    fi
done

exec "$@"
