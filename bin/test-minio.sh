#!/bin/bash
set -e

echo "Starting Minio..."
docker-compose up -d

echo "Waiting for Minio to be ready..."
timeout 30 bash -c 'until curl -f http://localhost:9000/minio/health/live > /dev/null 2>&1; do sleep 1; done'

echo "Minio is ready!"
echo "Console: http://localhost:9001 (minioadmin/minioadmin)"
echo "API: http://localhost:9000"
echo ""
echo "Running tests..."

# Run only minio tests
clojure -X:test :patterns '["konserve-s3.minio-test"]'

echo ""
echo "Tests complete. To stop Minio: docker-compose down"
