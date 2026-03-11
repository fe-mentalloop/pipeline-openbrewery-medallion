"""
Utilitários de monitoramento e alertas.
Suporta Slack webhook e e-mail via SMTP, configuráveis por variáveis de ambiente.
"""

import logging
import os
import smtplib
from email.mime.text import MIMEText

logger = logging.getLogger(__name__)

# Emojis por nível de alerta para facilitar triagem visual no Slack
LEVEL_EMOJI = {"INFO": "ℹ️", "WARNING": "⚠️", "CRITICAL": "🚨"}


def send_slack_alert(message: str, level: str = "INFO") -> bool:
    """Envia alerta para o Slack via webhook. Retorna True em caso de sucesso."""
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        logger.debug("SLACK_WEBHOOK_URL não configurado – alerta Slack ignorado.")
        return False

    try:
        import urllib.request, json as _json
        emoji = LEVEL_EMOJI.get(level, "📢")
        payload = {"text": f"{emoji} *[PIPELINE CERVEJARIAS – {level}]*\n{message}"}
        data = _json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(webhook_url, data=data, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status == 200
    except Exception as exc:
        logger.error(f"Falha ao enviar alerta Slack: {exc}")
        return False


def send_email_alert(message: str, level: str = "INFO") -> bool:
    """Envia alerta por e-mail via SMTP. Retorna True em caso de sucesso."""
    smtp_host = os.getenv("SMTP_HOST")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER")
    smtp_password = os.getenv("SMTP_PASSWORD")
    alert_recipients = os.getenv("ALERT_EMAIL_RECIPIENTS", "")

    # Ignora silenciosamente se as credenciais SMTP não estiverem configuradas
    if not all([smtp_host, smtp_user, smtp_password, alert_recipients]):
        logger.debug("Configuração SMTP incompleta – alerta por e-mail ignorado.")
        return False

    try:
        recipients = [r.strip() for r in alert_recipients.split(",")]
        msg = MIMEText(message)
        msg["Subject"] = f"[PIPELINE CERVEJARIAS – {level}] Alerta"
        msg["From"] = smtp_user
        msg["To"] = ", ".join(recipients)

        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.sendmail(smtp_user, recipients, msg.as_string())
        return True
    except Exception as exc:
        logger.error(f"Falha ao enviar alerta por e-mail: {exc}")
        return False


def send_alert(message: str, level: str = "INFO") -> None:
    """Envia alerta por todos os canais configurados (Slack e e-mail)."""
    # Usa o nível de log apropriado para cada severidade
    log_fn = logger.critical if level == "CRITICAL" else logger.warning if level == "WARNING" else logger.info
    log_fn(f"ALERTA [{level}]: {message}")
    send_slack_alert(message, level)
    send_email_alert(message, level)
