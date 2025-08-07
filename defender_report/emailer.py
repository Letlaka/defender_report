# emailer.py
import mimetypes
import os
import smtplib
from email.message import EmailMessage
from typing import List, Optional, Union


def send_email(
    smtp_server: str,
    smtp_port: int,
    from_addr: str,
    to_addrs: Union[str, List[str]],
    cc_addrs: Union[str, List[str]],
    subject: str,
    body: str,
    attachments: Optional[List[str]] = None,
    smtp_user: Optional[str] = None,
    smtp_password: Optional[str] = None,
) -> None:
    """
    Send an email (optionally with attachments) via an SMTP server.
    Uses TLS and authentication if credentials are provided.
    Falls back to anonymous SMTP relay if not.
    Raises RuntimeError on failure.

    Args:
        smtp_server: SMTP host (e.g. 'smtp.example.com')
        smtp_port: SMTP port (usually 587)
        from_addr: The "From" email address
        to_addrs: Recipient address(es) as list or comma-separated string
        subject: Email subject
        body: Email body (plain text)
        attachments: List of file paths to attach (optional)
        smtp_user: SMTP username (optional)
        smtp_password: SMTP password (optional)
    """
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_addr
    if isinstance(to_addrs, str):
        to_addrs = [addr.strip() for addr in to_addrs.split(",") if addr.strip()]
    msg["To"] = ", ".join(to_addrs)

    if cc_addrs:
        if isinstance(cc_addrs, str):
            cc_addrs = [addr.strip() for addr in cc_addrs.split(",") if addr.strip()]
        msg["Cc"] = ", ".join(cc_addrs)
    else:
        cc_addrs = []

    all_recipients = to_addrs + cc_addrs
    msg.set_content(body)

    if attachments:
        for file_path in attachments:
            try:
                with open(file_path, "rb") as f:
                    data = f.read()
                ctype, encoding = mimetypes.guess_type(file_path)
                if ctype is None or encoding is not None:
                    ctype = "application/octet-stream"
                maintype, subtype = ctype.split("/", 1)
                filename = os.path.basename(file_path)
                msg.add_attachment(
                    data, maintype=maintype, subtype=subtype, filename=filename
                )
            except Exception as e:
                raise RuntimeError(f"Failed to attach file '{file_path}': {e}") from e

    try:
        with smtplib.SMTP(smtp_server, smtp_port, timeout=30) as server:
            server.ehlo()
            if smtp_user and smtp_password:
                server.starttls()
                server.ehlo()
                server.login(smtp_user, smtp_password)
            server.send_message(msg, to_addrs=all_recipients)
    except Exception as exc:
        raise RuntimeError(
            f"Failed to send email to {to_addrs} via {smtp_server}:{smtp_port}: {exc}"
        ) from exc
