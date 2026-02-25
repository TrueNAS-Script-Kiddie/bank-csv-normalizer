import smtplib
from email.message import EmailMessage
from typing import Callable


# ---------------------------------------------------------------------------
# Email configuration (later verplaatsbaar naar config.py)
# ---------------------------------------------------------------------------
SMTP_SERVER = "smtp.example.com"
SMTP_PORT = 587
SMTP_USERNAME = "your_username"
SMTP_PASSWORD = "your_password"

EMAIL_FROM = "noreply@example.com"
EMAIL_TO = ["recipient@example.com"]

EMAIL_SUBJECT_PREFIX = "[CSV PIPELINE] "


# ---------------------------------------------------------------------------
# Send email (plain text)
# ---------------------------------------------------------------------------
def send_email(
    subject: str,
    body: str,
    log_event: Callable[[str, str], None],
    logfile_path: str,
) -> None:
    """
    Send a plain text email.
    Logs only on error. Never interrupts the pipeline.
    """

    try:
        msg = EmailMessage()
        msg["From"] = EMAIL_FROM
        msg["To"] = ", ".join(EMAIL_TO)
        msg["Subject"] = EMAIL_SUBJECT_PREFIX + subject
        msg.set_content(body)

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as smtp:
            smtp.starttls()
            smtp.login(SMTP_USERNAME, SMTP_PASSWORD)
            smtp.send_message(msg)

    except Exception as exception:
        log_event(logfile_path, f"[EMAIL ERROR] {exception}")
