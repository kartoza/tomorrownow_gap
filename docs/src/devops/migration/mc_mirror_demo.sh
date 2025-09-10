#!/bin/bash

# Exit on any failure
set -e

# Configurable variables
MINIO_ALIAS="minio-src"
MINIO_URL="https://api.minio.do.kartoza.com"
MINIO_ACCESS_KEY="MINIO_ACCESS_KEY"
MINIO_SECRET_KEY="MINIO_SECRET_KEY"

DO_ALIAS="do-dest"
DO_URL="https://fra1.digitaloceanspaces.com"
DO_ACCESS_KEY="DO_ACCESS_KEY"
DO_SECRET_KEY="DO_SECRET_KEY"

SRC_BUCKET="mergin-bucket"
DEST_BUCKET="snap1"
LOG_FILE="mc-mirror.log"

# Initialize mc alias for MinIO (source)
mc alias set "$MINIO_ALIAS" "$MINIO_URL" "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY"

# Initialize mc alias for DigitalOcean Spaces (destination)
mc alias set "$DO_ALIAS" "$DO_URL" "$DO_ACCESS_KEY" "$DO_SECRET_KEY"

# Optional: Check bucket existence
echo "Checking source and destination buckets..."
mc ls "${MINIO_ALIAS}/${SRC_BUCKET}" > /dev/null
mc ls "${DO_ALIAS}/${DEST_BUCKET}" > /dev/null || mc mb "${DO_ALIAS}/${DEST_BUCKET}"

# Dry-run mode to preview what will be mirrored
echo "Starting dry-run mirror from $SRC_BUCKET to $DEST_BUCKET..."
mc mirror --dry-run --overwrite --remove "${MINIO_ALIAS}/${SRC_BUCKET}" "${DO_ALIAS}/${DEST_BUCKET}" | tee "$LOG_FILE"

# Perform the actual mirror
echo "Starting actual mirror..."
mc mirror --overwrite --remove "${MINIO_ALIAS}/${SRC_BUCKET}" "${DO_ALIAS}/${DEST_BUCKET}" | tee -a "$LOG_FILE"

echo "✅ Mirror complete. Logs saved to $LOG_FILE"
