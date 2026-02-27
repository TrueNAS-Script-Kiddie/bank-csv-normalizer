"""
Generic runtime utilities used across the CSV pipeline.

This module contains only small, infrastructure-level helpers that:
- do NOT belong to CSV logic
- do NOT belong to duplicate-index logic
- do NOT belong to normalization logic
- do NOT belong to completion logic

Functions included:
- log_event: append timestamped log entries
- load_env: minimal .env key=value loader
- send_email: send notifications via system sendmail (TrueNAS compatible)
"""

import os
import subprocess
from datetime import datetime
from typing import Dict, List


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def log_event(logfile_path: str, message: str) -> None:
    """
    Append a timestamped log message to the logfile.

    Logging must never interrupt the pipeline. Any failure is silently ignored.
    """
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(logfile_path, "a", encoding="utf-8") as f:
            f.write(f"{timestamp} {message}\n")
    except Exception:
        # Logging failures must never break the pipeline
        pass


# ---------------------------------------------------------------------------
# Minimal .env loader
# ---------------------------------------------------------------------------
def load_env(path: str) -> Dict[str, str]:
    """
    Load a minimal .env file containing simple KEY=VALUE pairs.

    - Lines starting with '#' are ignored.
    - Empty lines are ignored.
    - No quoting, no type conversion, no nesting.
    - Returns a dict with string keys and string values.

    This loader is intentionally minimalistic to avoid dependencies.
    """
    config: Dict[str, str] = {}

    if not os.path.exists(path):
        return config

    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, value = line.split("=", 1)
                    config[key.strip()] = value.strip()
    except Exception:
        # Config loading must never break the pipeline
        pass

    return config


# ---------------------------------------------------------------------------
# Email via system sendmail (TrueNAS compatible)
# ---------------------------------------------------------------------------
def send_email(
    subject: str,
    body: str,
    log_event,
    logfile_path: str,
) -> None:
    """
    Send an email using the system's sendmail binary.

    - Compatible with TrueNAS system email configuration.
    - Recipients are loaded from CONFIG["EMAIL_TO"].
    - Logging is used only on error.
    - Never interrupts the pipeline.
    """
    try:
        email_to = CONFIG.get("EMAIL_TO", "").split(",")

        process = subprocess.Popen(
            ["/usr/sbin/sendmail", "-t"],
            stdin=subprocess.PIPE,
        )

        msg = (
            f"Subject: {subject}\n"
            f"To: {', '.join(email_to)}\n"
            f"\n"
            f"{body}"
        )

        process.communicate(msg.encode("utf-8"))

    except Exception as exc:
        log_event(logfile_path, f"[EMAIL ERROR] {exc}")


# ---------------------------------------------------------------------------
# Global config (loaded once)
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG = load_env(os.path.join(BASE_DIR, "config", "app.env"))
