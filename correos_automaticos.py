import os
import json
import smtplib
import requests
from datetime import datetime, timedelta, timezone
from email.mime.text import MIMEText
from dotenv import load_dotenv

load_dotenv()

# ── Configuración ─────────────────────────────────────────────────────────────
BREVO_API_KEY = os.environ.get("BREVO_API_KEY", "")
BREVO_API_URL = "https://api.brevo.com/v3"
BREVO_HEADERS = {
    "api-key": BREVO_API_KEY,
    "Content-Type": "application/json",
    "Accept": "application/json",
}
BREVO_TAG = os.environ["BREVO_TAG"]   # debe coincidir con el de automatizacion.py

MONDAY_API_TOKEN  = os.environ["MONDAY_API_TOKEN"]
MONDAY_BOARD_ID   = os.environ.get("MONDAY_BOARD_ID", "")
MONDAY_COLUMN_ID  = os.environ.get("MONDAY_COLUMN_ID", "")
MONDAY_COLUMN_ID_ABIERTOS = os.environ.get("MONDAY_COLUMN_ID_ABIERTOS", "")

MONDAY_STATUS_LABEL          = "Listo"
MONDAY_STATUS_LABEL_ABIERTOS = "Sin respuesta"
MONDAY_STATUS_LABEL_REBOTADOS = "Bounce"
MONDAY_STATUS_LABEL_OMITIDOS  = "No abierto"

MONDAY_API_URL = "https://api.monday.com/v2"
MONDAY_HEADERS = {
    "Authorization": MONDAY_API_TOKEN,
    "Content-Type": "application/json",
}

SMTP_USER     = os.environ.get("SMTP_USER", "")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "")
NOTIFY_TO     = os.environ.get("NOTIFY_TO", "")

# ── Notificaciones ────────────────────────────────────────────────────────────

def enviar_notificacion(asunto: str, cuerpo: str):
    if not SMTP_USER or not SMTP_PASSWORD:
        print(">> Notificación: SMTP no configurado, saltando.")
        return
    try:
        msg = MIMEText(cuerpo, "plain", "utf-8")
        msg["Subject"] = asunto
        msg["From"] = SMTP_USER
        msg["To"] = NOTIFY_TO
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SMTP_USER, NOTIFY_TO, msg.as_string())
        print(f">> Notificación enviada a {NOTIFY_TO}")
    except Exception as e:
        print(f">> Error enviando notificación: {e}")

# ── Brevo: obtener eventos de correo ─────────────────────────────────────────

def _obtener_eventos_brevo(event: str, dias: int = 7) -> list[str]:
    """
    Consulta la API de Brevo y retorna lista de emails que tuvieron
    el evento indicado en los últimos `dias` días, filtrado por el tag IMPI-oposicion.

    Eventos válidos: opened, clicks, hardBounces, softBounces, unsubscribed, delivered
    """
    if not BREVO_API_KEY:
        print(">> BREVO_API_KEY no configurado, saltando.")
        return []

    start_date = (datetime.now(timezone.utc) - timedelta(days=dias)).strftime("%Y-%m-%d")
    end_date   = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    emails_encontrados = []
    offset = 0
    limit  = 100

    while True:
        params = {
            "event":     event,
            "startDate": start_date,
            "endDate":   end_date,
            "tags":      BREVO_TAG,
            "limit":     limit,
            "offset":    offset,
        }
        r = requests.get(f"{BREVO_API_URL}/smtp/statistics/events", headers=BREVO_HEADERS, params=params)
        if r.status_code != 200:
            print(f">> Error Brevo [{r.status_code}] al consultar evento '{event}': {r.text}")
            break

        data = r.json()
        eventos = data.get("events", [])
        for ev in eventos:
            email = ev.get("email", "")
            if email and email not in emails_encontrados:
                emails_encontrados.append(email)
                print(f"   >> [{event}] {email}")

        if len(eventos) < limit:
            break
        offset += limit

    return emails_encontrados


def obtener_emails_abiertos() -> list[str]:
    print(">> Consultando correos 'Abiertos' en Brevo...")
    emails = _obtener_eventos_brevo("opened")
    print(f">> {len(emails)} abierto(s) encontrado(s).")
    return emails


def obtener_emails_rebotados() -> list[str]:
    print(">> Consultando correos 'Rebotados' en Brevo...")
    hard   = _obtener_eventos_brevo("hardBounces")
    soft   = _obtener_eventos_brevo("softBounces")
    emails = list({*hard, *soft})   # unión sin duplicados
    print(f">> {len(emails)} rebotado(s) encontrado(s).")
    return emails


def obtener_emails_omitidos() -> list[str]:
    """
    'Omitidos' = correos entregados que NO fueron abiertos.
    """
    print(">> Consultando correos 'No abiertos' en Brevo...")
    entregados = set(_obtener_eventos_brevo("delivered"))
    abiertos   = set(_obtener_eventos_brevo("opened"))
    emails     = list(entregados - abiertos)
    print(f">> {len(emails)} no abierto(s) encontrado(s).")
    return emails

# ── Monday.com ────────────────────────────────────────────────────────────────

def buscar_items_por_emails(emails: list[str]) -> list[dict]:
    """Busca en Monday los items cuya columna Email coincida con los emails dados (con paginación)."""
    query = """
    query ($boardId: ID!, $cursor: String) {
        boards(ids: [$boardId]) {
            items_page(limit: 100, cursor: $cursor) {
                cursor
                items {
                    id
                    name
                    column_values { id text }
                }
            }
        }
    }
    """
    emails_lower = [e.lower() for e in emails]
    encontrados  = []
    cursor       = None

    while True:
        r = requests.post(
            MONDAY_API_URL,
            json={"query": query, "variables": {"boardId": MONDAY_BOARD_ID, "cursor": cursor}},
            headers=MONDAY_HEADERS,
        )
        r.raise_for_status()
        page   = r.json()["data"]["boards"][0]["items_page"]
        items  = page["items"]
        cursor = page.get("cursor")

        for item in items:
            for col in item["column_values"]:
                if col["text"] and col["text"].lower() in emails_lower:
                    encontrados.append({"id": item["id"], "name": item["name"]})
                    break

        if not cursor:
            break

    return encontrados


def cambiar_status(item_id, column_id, label):
    mutation = """
    mutation ($boardId: ID!, $itemId: ID!, $columnId: String!, $value: JSON!) {
        change_column_value(board_id: $boardId, item_id: $itemId, column_id: $columnId, value: $value) {
            id
        }
    }
    """
    r = requests.post(
        MONDAY_API_URL,
        json={
            "query": mutation,
            "variables": {
                "boardId": MONDAY_BOARD_ID,
                "itemId": item_id,
                "columnId": column_id,
                "value": json.dumps({"label": label}),
            },
        },
        headers=MONDAY_HEADERS,
    )
    r.raise_for_status()


def monday_actualizar(emails: list[str], column_id: str, label: str, descripcion: str):
    if not MONDAY_BOARD_ID or not column_id:
        print(f">> Monday: columna para '{descripcion}' no configurada. Saltando.")
        return
    print(f">> Buscando items en Monday para actualizar a '{label}' ({descripcion})...")
    items = buscar_items_por_emails(emails)
    if not items:
        print(f">> No hay items en Monday que coincidan ({descripcion}).")
        return
    for item in items:
        print(f"   >> Actualizando: {item['name']} → {label}")
        cambiar_status(item["id"], column_id, label)
    print(f">> Monday: {len(items)} item(s) actualizados a '{label}'.")

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 50)
    print("AUTOMATIZACIÓN: Brevo + Monday.com")
    print("=" * 50)

    try:
        abiertos  = obtener_emails_abiertos()
        rebotados = obtener_emails_rebotados()
        omitidos  = obtener_emails_omitidos()
    except Exception as e:
        enviar_notificacion(
            "❌ Error en automatización Brevo + Monday",
            f"La automatización falló con el siguiente error:\n\n{e}\n\n"
            "Revisa los logs en GitHub Actions para más detalles.",
        )
        raise

    print(f"\n✓ Brevo: {len(abiertos)} abierto(s), {len(rebotados)} rebotado(s), {len(omitidos)} no abierto(s)")

    if abiertos:
        monday_actualizar(abiertos,  MONDAY_COLUMN_ID_ABIERTOS, MONDAY_STATUS_LABEL_ABIERTOS,  "abiertos")
    else:
        print(">> No hay correos abiertos, no se actualiza Monday.")

    if rebotados:
        monday_actualizar(rebotados, MONDAY_COLUMN_ID_ABIERTOS, MONDAY_STATUS_LABEL_REBOTADOS, "rebotados")
    else:
        print(">> No hay correos rebotados, no se actualiza Monday.")

    if omitidos:
        monday_actualizar(omitidos,  MONDAY_COLUMN_ID_ABIERTOS, MONDAY_STATUS_LABEL_OMITIDOS,  "omitidos")
    else:
        print(">> No hay correos no abiertos, no se actualiza Monday.")

    print("=" * 50)
    print("PROCESO FINALIZADO")
    print("=" * 50)


if __name__ == "__main__":
    main()
