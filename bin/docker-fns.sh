# Docker-related Bash functions.

if [ -z $PROJECT_DIR ]; then
    echo "$(basename $0): This script requires the PROJECT_DIR variable to be defined" 1>&2
    exit 1
fi
if [ -z $COMPOSE_PROJECT_NAME ]; then
    echo "$(basename $0): This script requires the COMPOSE_PROJECT_NAME variable to be defined" 1>&2
    exit 1
fi

COMPOSE_FILE=${PROJECT_DIR:-.}/docker-compose.yml
COMPOSE_PROJECT_NAME=clojurestreams

start_compose_project() {
    echo "Starting services"
    docker-compose --file $COMPOSE_FILE --project-name $COMPOSE_PROJECT_NAME up -d
}

test_for_service() {
    service_name=$1

    docker ps \
           --format "yeah" \
           --filter "label=com.docker.compose.service=$service_name" \
           --filter="label=com.docker.compose.project=$COMPOSE_PROJECT_NAME" \
           --filter="status=running" \
        | grep -q yeah

    return $?
}

ensure_service() {
    service_name=$1

    if test_for_service $service_name; then
        return 0
    fi

    start_compose_project

    for wait_time in 0.5 0.5 0.5 0.5 0.5 0.5 0.5 0.5 0.5 0.5 1 1 1 1 1; do
        sleep $wait_time
        if test_for_service $service_name; then
            return 0
        fi
    done

    echo "$(basename $0): Failed to start service \"$service_name\", giving up" 1>&2
    return 1
}
