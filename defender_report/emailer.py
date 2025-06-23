# defender_report/emailer.py

import os
import smtplib
import json
from email.message import EmailMessage
from typing import List


def send_email(
    smtp_server: str,
    smtp_port: int,
    smtp_user: str,
    smtp_password: str,
    from_addr: str,
    to_addrs: List[str],
    subject: str,
    body: str,
    attachments: List[str],
) -> None:
    """
    Send an email with the given attachments via an SMTP server.
    """
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = ", ".join(to_addrs)
    msg.set_content(body)

    for file_path in attachments:
        with open(file_path, "rb") as f:
            data = f.read()
        maintype, subtype = ("application", "octet-stream")
        filename = os.path.basename(file_path)
        msg.add_attachment(data, maintype=maintype, subtype=subtype, filename=filename)

    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.send_message(msg)
