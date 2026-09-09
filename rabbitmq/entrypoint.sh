#!/usr/bin/env bash
set -euo pipefail

USER="${RABBITMQ_USER:-admin}"
PASS="${RABBITMQ_PASS:-admin123}"

_term() {
    echo "==> Deteniendo RabbitMQ limpiamente..."
    rabbitmqctl stop
    wait "$RABBITMQ_PID"
}

trap _term SIGTERM SIGINT

echo "==> Iniciando RabbitMQ en segundo plano..."
/usr/local/bin/docker-entrypoint.sh rabbitmq-server &
RABBITMQ_PID=$!

echo "==> Esperando a que el broker RabbitMQ esté completamente operativo..."
until rabbitmq-diagnostics -q check_running > /dev/null 2>&1; do
    sleep 1
done

echo "==> Configurando usuario '$USER' desde variables de entorno..."
for i in $(seq 1 15); do
    if rabbitmqctl add_user "$USER" "$PASS" 2>/dev/null || rabbitmqctl change_password "$USER" "$PASS" 2>/dev/null; then
        rabbitmqctl set_user_tags "$USER" administrator 2>/dev/null || true
        rabbitmqctl set_permissions -p / "$USER" ".*" ".*" ".*" 2>/dev/null || true
        echo "==> Credenciales de '$USER' configuradas exitosamente con rol administrator."
        break
    fi
    sleep 1
done

wait "$RABBITMQ_PID"
