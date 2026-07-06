import logging
from typing import Any, Dict

import emails

from core.core.config import settings
from core.utils.i18n import jinja_env


def send_email(
    email_to: str,
    subject: str = "",
    html_template: str = "email/template.html",
    environment: Dict[str, Any] = {},
) -> None:
    if not settings.SMTP_USER:
        logging.info(f"SMTP not configured, skipping email to {email_to}: {subject}")
        return
    template = jinja_env.get_template(html_template)
    message = emails.Message(
        subject=subject,
        html=template,
        mail_from=(settings.EMAILS_FROM_NAME, settings.SMTP_USER),
    )
    smtp_options = {"host": settings.SMTP_HOST, "port": settings.SMTP_PORT}
    if settings.SMTP_TLS:
        smtp_options["tls"] = True
    if settings.SMTP_USER:
        smtp_options["user"] = settings.SMTP_USER
    if settings.SMTP_PASSWORD:
        smtp_options["password"] = settings.SMTP_PASSWORD
    response = message.send(to=email_to, render=environment, smtp=smtp_options)
    logging.info(f"send email result: {response}")


email_content_config = {
    "activate_new_organization": {
        "url": f"{settings.API_URL}/organizations/activate?token=",
        "subject": {
            "en": "Activate your GOAT demo",
            "de": "Demo aktivieren",
        },
        "template_name": "activate_new_organization",
    },
    "subscription_trial_started": {
        "url": "",
        "subject": {
            "en": "Your GOAT demo is ready to use",
            "de": "Ihre GOAT Demo steht bereit",
        },
        "template_name": "subscription_trial_started",
    },
    "subscription_trial_expiring": {
        "url": "",
        "subject": {"en": "Demo expiring soon", "de": "Demo bald ablaufen"},
        "template_name": "subscription_trial_expiring",
    },
    "subscription_trial_expired": {
        "url": "",
        "subject": {"en": "Demo expired", "de": "Demo abgelaufen"},
        "template_name": "subscription_trial_expired",
    },
}
