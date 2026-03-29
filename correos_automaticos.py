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

MONDAY_STATUS_LABEL_ABIERTOS = "Sin respuesta"
MONDAY_STATUS_LABEL_REBOTADOS = "Bounce"
MONDAY_STATUS_LABEL_OMITIDOS  = "No abierto"

MONDAY_API_URL = "https://api.monday.com/v2"
MONDAY_HEADERS = {
    "Authorization": MONDAY_API_TOKEN,
    "Content-Type": "application/json",
}

SMTP_USER     = os.environ.get("EMAIL_USER", "")
SMTP_PASSWORD = os.environ.get("EMAIL_PASSWORD", "")
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

    emails_set = set()
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

        eventos = r.json().get("events", [])
        for ev in eventos:
            email = ev.get("email", "")
            if email and email not in emails_set:
                emails_set.add(email)
                print(f"   >> [{event}] {email}")

        if len(eventos) < limit:
            break
        offset += limit

    return list(emails_set)


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


def obtener_emails_omitidos(abiertos: set) -> list[str]:
    """'Omitidos' = correos entregados que NO fueron abiertos."""
    print(">> Consultando correos 'No abiertos' en Brevo...")
    entregados = set(_obtener_eventos_brevo("delivered"))
    emails     = list(entregados - abiertos)
    print(f">> {len(emails)} no abierto(s) encontrado(s).")
    return emails

# ── Monday.com ────────────────────────────────────────────────────────────────

def _cargar_items_monday() -> list[dict]:
    """Carga todos los items del board con paginación (una sola vez)."""
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
    todos  = []
    cursor = None
    while True:
        r = requests.post(
            MONDAY_API_URL,
            json={"query": query, "variables": {"boardId": MONDAY_BOARD_ID, "cursor": cursor}},
            headers=MONDAY_HEADERS,
        )
        r.raise_for_status()
        page   = r.json()["data"]["boards"][0]["items_page"]
        todos.extend(page["items"])
        cursor = page.get("cursor")
        if not cursor:
            break
    return todos


def _filtrar_items_por_emails(items: list[dict], emails: list[str]) -> list[dict]:
    """Filtra items del board por email (búsqueda en memoria)."""
    emails_lower = {e.lower() for e in emails}
    encontrados  = []
    for item in items:
        for col in item["column_values"]:
            if col["text"] and col["text"].lower() in emails_lower:
                encontrados.append({"id": item["id"], "name": item["name"]})
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


def monday_actualizar(items_board: list[dict], emails: list[str], column_id: str, label: str, descripcion: str):
    if not MONDAY_BOARD_ID or not column_id:
        print(f">> Monday: columna para '{descripcion}' no configurada. Saltando.")
        return
    items = _filtrar_items_por_emails(items_board, emails)
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
        omitidos  = obtener_emails_omitidos(set(abiertos))
    except Exception as e:
        enviar_notificacion(
            "❌ Error en automatización Brevo + Monday",
            f"La automatización falló con el siguiente error:\n\n{e}\n\n"
            "Revisa los logs en GitHub Actions para más detalles.",
        )
        raise

    print(f"\n✓ Brevo: {len(abiertos)} abierto(s), {len(rebotados)} rebotado(s), {len(omitidos)} no abierto(s)")

    # Cargar items del board una sola vez
    items_board = []
    if MONDAY_BOARD_ID and MONDAY_COLUMN_ID_ABIERTOS:
        print(">> Cargando items de Monday...")
        items_board = _cargar_items_monday()
        print(f">> {len(items_board)} item(s) cargados.")

    if abiertos:
        monday_actualizar(items_board, abiertos,  MONDAY_COLUMN_ID_ABIERTOS, MONDAY_STATUS_LABEL_ABIERTOS,  "abiertos")
    else:
        print(">> No hay correos abiertos, no se actualiza Monday.")

    if rebotados:
        monday_actualizar(items_board, rebotados, MONDAY_COLUMN_ID_ABIERTOS, MONDAY_STATUS_LABEL_REBOTADOS, "rebotados")
    else:
        print(">> No hay correos rebotados, no se actualiza Monday.")

    if omitidos:
        monday_actualizar(items_board, omitidos,  MONDAY_COLUMN_ID_ABIERTOS, MONDAY_STATUS_LABEL_OMITIDOS,  "omitidos")
    else:
        print(">> No hay correos no abiertos, no se actualiza Monday.")

    print("=" * 50)
    print("PROCESO FINALIZADO")
    print("=" * 50)


if __name__ == "__main__":
    main()
