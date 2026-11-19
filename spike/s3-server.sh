#!/usr/bin/env bash
# Starts or stops RustFS (an S3-compatible server) for the spike tests, on port 19000, and
# creates the test bucket. Credentials are throwaway values for a local container.
#
#   spike/s3-server.sh start   # then: source <(spike/s3-server.sh env)
#   spike/s3-server.sh env     # prints the variables the tests read
#   spike/s3-server.sh stop    # removes the container and its anonymous data volume
set -euo pipefail
NAME=moraine-spike-s3
PORT=${MORAINE_SPIKE_S3_PORT:-19000}
BUCKET=${MORAINE_SPIKE_S3_BUCKET:-moraine-spike}
KEY=minioadmin
SECRET=minioadmin

case "${1:-}" in
  start)
    docker run -d --rm --name "$NAME" -p "127.0.0.1:${PORT}:9000" \
      -e RUSTFS_ACCESS_KEY=$KEY -e RUSTFS_SECRET_KEY=$SECRET rustfs/rustfs:latest >/dev/null
    for _ in $(seq 1 30); do
      curl -fsS "http://127.0.0.1:${PORT}/health" >/dev/null 2>&1 && break
      sleep 1
    done
    AWS_ACCESS_KEY_ID=$KEY AWS_SECRET_ACCESS_KEY=$SECRET AWS_REGION=us-east-1 \
      aws --endpoint-url "http://127.0.0.1:${PORT}" s3 mb "s3://${BUCKET}" >/dev/null
    docker inspect --format '{{.Config.Image}} {{.Image}}' "$NAME"
    ;;
  env)
    echo "export MORAINE_SPIKE_S3_ENDPOINT=http://127.0.0.1:${PORT}"
    echo "export MORAINE_SPIKE_S3_BUCKET=${BUCKET}"
    echo "export AWS_ACCESS_KEY_ID=${KEY} AWS_SECRET_ACCESS_KEY=${SECRET} AWS_REGION=us-east-1"
    ;;
  stop)
    docker rm -f -v "$NAME" >/dev/null 2>&1 || true
    ;;
  *)
    echo "usage: $0 start|env|stop" >&2
    exit 2
    ;;
esac
