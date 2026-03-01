#!/bin/bash

shopt -s nullglob

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
IN_DIR="${BASE_DIR}/data/incoming"
FAILED_DIR="${BASE_DIR}/data/failed"
LOG_DIR="${BASE_DIR}/data/logs"

PYTHON_MODULE="engine.process_csv"

LOCKFILE_PATH="${BASE_DIR}/.process.lock"

# Ensure Python can import the engine/ package (cron has no PYTHONPATH)
export PYTHONPATH="${BASE_DIR}"

# Ensure working directory is the project root (cron starts in /)
cd "${BASE_DIR}" || exit 1

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

    echo "$(date '+%F %T') Processing file ${FILENAME}... " >> "${LOGFILE_PATH}"

    # Avoid processing files still being uploaded
    # --- NEW: size-stabilisatie ---
    SIZE1=$(stat -c%s "${FILE_PATH}")
    sleep 2
    SIZE2=$(stat -c%s "${FILE_PATH}")

    if [[ "${SIZE1}" -ne "${SIZE2}" ]]; then
        echo "$(date '+%F %T') Skipped (file still growing)" >> "${LOGFILE_PATH}"
        continue
    fi
    # --- END NEW ---

    python3 -m "${PYTHON_MODULE}" "${FILE_PATH}" "${RUN_TIMESTAMP}" "${LOGFILE_PATH}"
    EXIT_CODE="${?}"

    case "${EXIT_CODE}" in
        0|65|75|99)
            # Python handled everything → bash does absolutely nothing
            ;;

        1)
            # Python crashed before cleanup → bash must move the file
            [[ -f "${FILE_PATH}" ]] && mv "${FILE_PATH}" "${FAILED_DIR}/${RUN_TIMESTAMP}-${FILENAME}-failed.csv"
            echo "$(date '+%F %T') ${PYTHONSCRIPT_FILENAME} crashed before cleanup (exit code 1)." >> "${LOGFILE_PATH}"
            ;;

        *)
            # Unknown exit code → treat as Python crash
            [[ -f "${FILE_PATH}" ]] && mv "${FILE_PATH}" "${FAILED_DIR}/${RUN_TIMESTAMP}-${FILENAME}-failed.csv"
            echo "$(date '+%F %T') ${PYTHONSCRIPT_FILENAME} exited with unknown code ${EXIT_CODE}." >> "${LOGFILE_PATH}"
            ;;
    esac
done
