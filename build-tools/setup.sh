#!/usr/bin/env bash
set -euo pipefail

CONTAINER_NAME="${CONTAINER_NAME:-clickhouse-rs-sea-tests}"
CLICKHOUSE_IMAGE="${CLICKHOUSE_IMAGE:-docker.io/clickhouse/clickhouse-server:latest-alpine}"
HOST_HTTP_PORT="${HOST_HTTP_PORT:-18123}"
HOST_NATIVE_PORT="${HOST_NATIVE_PORT:-19000}"
CH_URL="${CH_URL:-http://localhost:${HOST_HTTP_PORT}}"
QUERY_URL="${CH_URL%/}/?query=SELECT%201"

if ! command -v docker >/dev/null 2>&1; then
    echo "docker is required to start the ClickHouse test database" >&2
    exit 1
fi

if ! command -v curl >/dev/null 2>&1; then
    echo "curl is required to wait for the ClickHouse test database" >&2
    exit 1
fi

if docker ps -a --filter "name=^/${CONTAINER_NAME}$" --format '{{.Names}}' | grep -qx "${CONTAINER_NAME}"; then
    if [ "$(docker inspect -f '{{.State.Running}}' "${CONTAINER_NAME}")" = "true" ]; then
        echo "ClickHouse container is already running: ${CONTAINER_NAME}"
    else
        echo "Starting existing ClickHouse container: ${CONTAINER_NAME}"
        docker start "${CONTAINER_NAME}" >/dev/null
    fi
else
    echo "Creating ClickHouse container: ${CONTAINER_NAME}"
    docker run -d \
        --name "${CONTAINER_NAME}" \
        --ulimit nofile=262144:262144 \
        -p "${HOST_HTTP_PORT}:8123" \
        -p "${HOST_NATIVE_PORT}:9000" \
        -e CLICKHOUSE_SKIP_USER_SETUP=1 \
        "${CLICKHOUSE_IMAGE}" >/dev/null
fi

echo "Waiting for ClickHouse at ${CH_URL}"
for _ in {1..60}; do
    if curl --silent --show-error --fail "${QUERY_URL}" >/dev/null 2>&1; then
        echo "ClickHouse is ready."
        echo "Run: bash build-tools/sea-tests.sh"
        echo "Stop: docker rm -f ${CONTAINER_NAME}"
        exit 0
    fi
    sleep 1
done

echo "ClickHouse did not become ready within 60 seconds." >&2
docker logs --tail 50 "${CONTAINER_NAME}" >&2 || true
exit 1
