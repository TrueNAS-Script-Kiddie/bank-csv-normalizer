#!/bin/bash

shopt -s nullglob

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
IN_DIR="${BASE_DIR}/data/incoming"
FAILED_DIR="${BASE_DIR}/data/failed"
LOG_DIR="${BASE_DIR}/data/logs"

PYTHONSCRIPT_FILENAME="process_csv.py"
PYTHONSCRIPT_PATH="${BASE_DIR}/engine/${PYTHONSCRIPT_FILENAME}"

LOCKFILE_PATH="${BASE_DIR}/.process.lock"

# Always remove lockfile, even on crash
cleanup() {
    rm -f "${LOCKFILE_PATH}"
}
trap cleanup INT TERM EXIT

# Prevent double runs
[[ -e "${LOCKFILE_PATH}" ]] && exit 0
touch "${LOCKFILE_PATH}"

for FILE_PATH in "${IN_DIR}"/*.csv; do
    FILENAME="$(basename "${FILE_PATH}")"

    # One timestamp/logfile per csv
    RUN_TIMESTAMP="$(date '+%Y%m%d-%H%M%S')"
    LOGFILE_PATH="${LOG_DIR}/${RUN_TIMESTAMP}-${FILENAME}.log"

    echo -n "$(date '+%F %T') Processing file ${FILENAME}... " >> "${LOGFILE_PATH}"

    # Avoid processing files still being uploaded
    # --- NEW: size-stabilisatie ---
    SIZE1=$(stat -c%s "${FILE_PATH}")
    sleep 2
    SIZE2=$(stat -c%s "${FILE_PATH}")

    if [[ "${SIZE1}" -ne "${SIZE2}" ]]; then
        echo "Skipped (file still growing)" >> "${LOGFILE_PATH}"
        continue
    fi
    # --- END NEW ---

    python3 "${PYTHONSCRIPT_PATH}" "${FILE_PATH}" "${RUN_TIMESTAMP}" "${LOGFILE_PATH}"
    EXIT_CODE="${?}"

    case "${EXIT_CODE}" in
        0|65|75)
            # Python handled everything → bash does absolutely nothing
            ;;

        1)
            # Python crashed before cleanup → bash must move the file
            [[ -f "${FILE_PATH}" ]] && mv "${FILE_PATH}" "${FAILED_DIR}/${RUN_TIMESTAMP}-${FILENAME}-failed.csv"
            echo "${PYTHONSCRIPT_FILENAME} crashed before cleanup (exit code 1)." >> "${LOGFILE_PATH}"
            ;;

        *)
            # Unknown exit code → treat as Python crash
            [[ -f "${FILE_PATH}" ]] && mv "${FILE_PATH}" "${FAILED_DIR}/${RUN_TIMESTAMP}-${FILENAME}-failed.csv"
            echo "${PYTHONSCRIPT_FILENAME} exited with unknown code ${EXIT_CODE}." >> "${LOGFILE_PATH}"
            ;;
    esac
done
