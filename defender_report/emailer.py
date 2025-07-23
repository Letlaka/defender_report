import os
import smtplib
from email.message import EmailMessage
from typing import List, Optional


def send_email(
    smtp_server: str,
    smtp_port: int,
    from_addr: str,
    to_addrs: List[str],
    subject: str,
    body: str,
    attachments: Optional[List[str]] = None,
    smtp_user: Optional[str] = None,
    smtp_password: Optional[str] = None,
) -> None:
    """
    Send an email with the given attachments via an SMTP server.
    If smtp_user and smtp_password are provided, use TLS and authenticate.
    Otherwise, send mail anonymously (like SCCM relay).
    """
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_addr
    # Always ensure to_addrs is a list of strings
    if isinstance(to_addrs, str):
        to_addrs = [to_addrs]
    msg["To"] = ", ".join(to_addrs)
    msg.set_content(body)

    # Attach files if any
    if attachments:
        for file_path in attachments:
            with open(file_path, "rb") as f:
                data = f.read()
            maintype, subtype = ("application", "octet-stream")
            filename = os.path.basename(file_path)
            msg.add_attachment(
                data, maintype=maintype, subtype=subtype, filename=filename
            )

    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            if smtp_user and smtp_password:
                server.ehlo()
                server.starttls()
                server.ehlo()
                server.login(smtp_user, smtp_password)
            server.send_message(msg)
    except Exception as exc:
        raise RuntimeError(
            f"Failed to send email to {to_addrs} via {smtp_server}:{smtp_port}: {exc}"
        ) from exc
