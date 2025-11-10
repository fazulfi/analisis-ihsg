#!/usr/bin/env bash
set -euo pipefail

# config
DB_PATH="db/historical.db"
BACKUP_DIR="backups"
TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"   # UTC timestamp
TMP_FILE="${BACKUP_DIR}/historical.db.${TIMESTAMP}.tmp"
FINAL_FILE="${BACKUP_DIR}/historical.db.${TIMESTAMP}.db"
GZ_FILE="${FINAL_FILE}.gz"
LOG_FILE="logs/backup.log"
KEEP_DAYS=30   # retention (optional) - adjust

mkdir -p "${BACKUP_DIR}"
mkdir -p "$(dirname "${LOG_FILE}")"

if [ ! -f "${DB_PATH}" ]; then
  echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) ERROR DB not found: ${DB_PATH}" | tee -a "${LOG_FILE}"
  exit 1
fi

# copy atomically (avoid partial file)
cp --preserve=mode,timestamps "${DB_PATH}" "${TMP_FILE}"
mv "${TMP_FILE}" "${FINAL_FILE}"

# compress (optional)
gzip -c "${FINAL_FILE}" > "${GZ_FILE}" && rm -f "${FINAL_FILE}"

# cleanup old backups (optional)
find "${BACKUP_DIR}" -type f -mtime +"${KEEP_DAYS}" -name "historical.db.*.gz" -delete

echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) INFO backup created: ${GZ_FILE}" | tee -a "${LOG_FILE}"
