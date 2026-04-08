import re
import xml.etree.ElementTree as ET
from datetime import date, timedelta
from pathlib import Path
from playwright.sync_api import sync_playwright
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from supabase import create_client, Client
import os
from dotenv import load_dotenv
import smtplib
import json
import requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Cargar variables de entorno
load_dotenv()

BASE_URL = "https://siga.impi.gob.mx/"
DOWNLOAD_DIR = Path("downloads")
DOWNLOAD_DIR.mkdir(exist_ok=True)

XML_DIR = Path("xml_files")
XML_DIR.mkdir(exist_ok=True)

AREA_TEXT = "Marcas"
GACETA_TEXT = "Notificación de Resoluciones, Requerimientos y demás Actos"
SEARCH_PHRASE = "SE LE CITA ANTERIORIDAD"

# Variables de entorno
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
BREVO_API_KEY     = os.getenv("BREVO_API_KEY")
BREVO_LIST_ID     = int(os.getenv("BREVO_LIST_ID", "2"))
BREVO_TEMPLATE_ID = int(os.getenv("BREVO_TEMPLATE_ID", "1"))
BREVO_TAG         = os.environ["BREVO_TAG"]
MONDAY_API_TOKEN  = os.getenv("MONDAY_API_TOKEN")
MONDAY_BOARD_ID   = os.getenv("MONDAY_BOARD_ID")
MONDAY_COLUMN_ID  = os.getenv("MONDAY_COLUMN_ID")


class SigaDatabase:
    """Clase para gestionar la conexión y operaciones con Supabase"""

    def __init__(self):
        self.client: Client | None = None

    def connect(self) -> bool:
        """Conectar a Supabase"""
        try:
            self.client = create_client(SUPABASE_URL, SUPABASE_KEY)
            print("✓ Conexión a Supabase exitosa\n")
            return True
        except Exception as e:
            print(f"✗ Error de conexión a Supabase: {e}")
            return False

    def disconnect(self):
        """Supabase no requiere cerrar conexión manual"""
        self.client = None

    def insert_expediente(self, data: dict) -> bool:
        """Guardar un expediente en Supabase (una sola llamada, ignora duplicados)."""
        try:
            response = (
                self.client
                .table("expedientes")
                .insert(data)
                .execute()
            )
            if response.data:
                print(f"  ✓ Guardado en BD: {data['expediente']}")
                return True
            print(f"  ✗ Error al guardar: {response}")
            return False
        except Exception as e:
            if "23505" in str(e) or "duplicate" in str(e).lower() or "unique" in str(e).lower():
                print(f"  ⚠ Ya existe en BD: {data['expediente']}")
                return False
            print(f"  ✗ Error al guardar: {e}")
            return False
        
    def insert_titular(self, data: dict) -> bool:
        print("    🔎 Cliente Supabase:", self.client)
        print("    📦 Intentando guardar titular:", data)

        try:
            response = (
                self.client
                .table("expedientes_titular")
                .insert(data)
                .execute()
            )

            print("    📤 Respuesta Supabase:", response)

            if response.data:
                print(f"    ✓ Titular guardado: {data['expediente']}")
                return True
            else:
                print(f"    ✗ No se guardó. Error:", response)
                return False

        except Exception as e:
            print(f"    💥 Error insert titular:", e)
            return False


# ─── Brevo ───────────────────────────────────────────────────────────────────

BREVO_API_URL = "https://api.brevo.com/v3"
BREVO_HEADERS = {
    "api-key": BREVO_API_KEY or "",
    "Content-Type": "application/json",
    "Accept": "application/json",
}


def upsert_perfil_brevo(email: str, titular: str, telefono: str = None, expediente: str = None) -> bool:
    """Crea o actualiza un contacto en Brevo y lo agrega a la lista."""
    if not BREVO_API_KEY:
        print("    ⚠ BREVO_API_KEY no configurado")
        return False

    try:
        attributes = {
            "NOMBRE":     (titular or "").strip(),
            "EXPEDIENTE": expediente or "",
        }
        if telefono:
            attributes["TELEFONO"] = telefono

        body = {
            "email": email,
            "attributes": attributes,
            "listIds": [BREVO_LIST_ID],
            "updateEnabled": True,
        }

        r = requests.post(f"{BREVO_API_URL}/contacts", json=body, headers=BREVO_HEADERS)
        if r.status_code in (200, 201, 204):
            print(f"    ✓ Contacto Brevo upserted: {email}")
        elif r.status_code == 400 and "Contact already exist" in r.text:
            print(f"    ✓ Contacto Brevo ya existía, actualizado: {email}")
        else:
            print(f"    ✗ Error upsert contacto Brevo [{r.status_code}]: {r.text}")
            return False
        return True

    except Exception as e:
        print(f"    ✗ Error upserting contacto Brevo: {e}")
        return False


def enviar_correo_brevo(destinatario: str, titular: str, expediente: str, descripcion: str) -> bool:
    """Envía el correo de notificación via Brevo usando la plantilla configurada."""
    if not BREVO_API_KEY:
        print("    ⚠ BREVO_API_KEY no configurado")
        return False

    try:
        print(f"    📧 Enviando correo Brevo para: {destinatario}")

        parts = (titular or "").strip().split(" ", 1)
        first_name = parts[0]

        body = {
            "to": [{"email": destinatario, "name": titular}],
            "templateId": BREVO_TEMPLATE_ID,
            "params": {
                "titular": titular,
                "FIRSTNAME": first_name,
                "expediente": expediente,
                "descripcion": descripcion,
            },
            "tags": [BREVO_TAG],
        }

        r = requests.post(f"{BREVO_API_URL}/smtp/email", json=body, headers=BREVO_HEADERS)
        if r.status_code in (200, 201):
            print(f"    ✓ Correo Brevo enviado a: {destinatario}")
            return True
        else:
            print(f"    ✗ Error enviando correo Brevo [{r.status_code}]: {r.text}")
            return False

    except Exception as e:
        print(f"    ✗ Error enviando correo Brevo: {e}")
        return False

# ─────────────────────────────────────────────────────────────────────────────

def listar_columns_monday():
    """Lista los IDs y títulos de todas las columnas del board de Monday (diagnóstico)."""
    if not MONDAY_API_TOKEN or not MONDAY_BOARD_ID:
        return
    query = "{ boards(ids: [" + str(MONDAY_BOARD_ID) + "]) { columns { id title } } }"
    try:
        resp = requests.post(
            "https://api.monday.com/v2",
            headers={"Content-Type": "application/json", "Authorization": MONDAY_API_TOKEN, "API-Version": "2024-01"},
            json={"query": query},
            timeout=15,
        )
        cols = resp.json()["data"]["boards"][0]["columns"]
        print("  🗂 Columnas del board de Monday:")
        for c in cols:
            print(f"      id={c['id']!r:30s}  título={c['title']}")
    except Exception as e:
        print(f"  ⚠ No se pudieron listar columnas de Monday: {e}")


def crear_item_monday(expediente: str, registro_marca: str, email: str, telefono: str,
                      enlace_electronico: str, titular: str,
                      fecha_gaceta: date = None, fecha_notificado: str = None) -> bool:
    """Crea un item en el tablero de Monday.com con los datos del expediente."""
    if not MONDAY_API_TOKEN or not MONDAY_BOARD_ID:
        print("    ⚠ Monday.com no configurado (MONDAY_API_TOKEN o MONDAY_BOARD_ID faltante)")
        return False

    try:
        column_values = {
            "text_mm14m7x6": registro_marca or "",
            "text_mm14x6gm": email or "",
            "text_mm14sky8": expediente or "",
            "text_mm14xzyq": enlace_electronico or "",
        }

        telefono_limpio = re.sub(r"[^\d+]", "", telefono or "")
        if telefono_limpio:
            column_values["phone_mm16acyr"] = {"phone": telefono_limpio, "countryShortName": ""}

        # Fecha Gaceta = día anterior al que corrió el script
        if fecha_gaceta:
            column_values["date_mm14td4r"] = {"date": fecha_gaceta.strftime("%Y-%m-%d")}

        # Fecha Notificado = la que extrae del modal de MarcaNet
        if fecha_notificado:
            # fecha_notificado viene como "DD/MM/YYYY", convertir a "YYYY-MM-DD"
            try:
                from datetime import datetime
                fn = datetime.strptime(fecha_notificado, "%d/%m/%Y")
                column_values["date_mm1488d2"] = {"date": fn.strftime("%Y-%m-%d")}
            except Exception:
                pass

        mutation = """
        mutation ($boardId: ID!, $itemName: String!, $columnValues: JSON!) {
            create_item (
                board_id: $boardId,
                item_name: $itemName,
                column_values: $columnValues
            ) { id }
        }
        """

        response = requests.post(
            "https://api.monday.com/v2",
            headers={
                "Content-Type": "application/json",
                "Authorization": MONDAY_API_TOKEN,
                "API-Version": "2024-01"
            },
            json={
                "query": mutation,
                "variables": {
                    "boardId": MONDAY_BOARD_ID,
                    "itemName": titular or expediente or "Sin nombre",
                    "columnValues": json.dumps(column_values)
                }
            },
            timeout=30
        )

        data = response.json()

        print(f"    🔍 Monday column_values enviados: {json.dumps(column_values)}")

        if "errors" in data:
            print(f"    ✗ Error Monday: {data['errors'][0]['message']}")
            print(f"    🔍 Monday response completo: {data}")
            return False

        item_id = data["data"]["create_item"]["id"]
        print(f"    ✓ Item creado en Monday: {item_id}")
        return item_id

    except Exception as e:
        print(f"    ✗ Error creando item en Monday: {e}")
        return None


def marcar_correo_enviado_monday(item_id: str):
    """Marca el item como 'Listo' en la columna de correo enviado."""
    if not MONDAY_API_TOKEN or not MONDAY_BOARD_ID or not MONDAY_COLUMN_ID:
        return
    try:
        mutation = """
        mutation ($boardId: ID!, $itemId: ID!, $columnId: String!, $value: JSON!) {
            change_column_value(board_id: $boardId, item_id: $itemId, column_id: $columnId, value: $value) {
                id
            }
        }
        """
        requests.post(
            "https://api.monday.com/v2",
            headers={"Content-Type": "application/json", "Authorization": MONDAY_API_TOKEN, "API-Version": "2024-01"},
            json={
                "query": mutation,
                "variables": {
                    "boardId": MONDAY_BOARD_ID,
                    "itemId": item_id,
                    "columnId": MONDAY_COLUMN_ID,
                    "value": json.dumps({"label": "Listo"}),
                },
            },
            timeout=30,
        )
        print(f"    ✓ Monday: correo marcado como 'Listo'")
    except Exception as e:
        print(f"    ✗ Error marcando 'Listo' en Monday: {e}")


def enviar_reporte(count_expedientes: int, count_emails: int) -> bool:
    """
    Envía reporte con resumen de expedientes encontrados y correos enviados
    
    Args:
        count_expedientes: Cantidad de expedientes encontrados
        count_emails: Cantidad de correos enviados
    
    Returns:
        bool: True si se envió correctamente
    """
    try:
        if not EMAIL_USER or not EMAIL_PASSWORD:
            print("    ⚠ Credenciales de correo no configuradas")
            return False

        destinatarios = [e.strip() for e in os.getenv("NOTIFICATION_EMAILS", "").split(",") if e.strip()]
        fecha_reporte = date.today().strftime('%d/%m/%Y')

        print(f"    📧 Enviando reporte a: {', '.join(destinatarios)}")

        # Crear mensaje
        mensaje = MIMEMultipart()
        mensaje["From"] = EMAIL_USER
        mensaje["To"] = ", ".join(destinatarios)
        mensaje["Subject"] = f"📊 Reporte Automatización STARGAZING - {fecha_reporte}"

        # Bloque HTML condicional para 0 expedientes
        sin_expedientes_html = f"""
                                        <tr>
                                            <td style="padding: 15px; background-color: #fff8e1; border-left: 4px solid #f0a500; text-align: center;">
                                                <p style="margin: 0; font-size: 15px; color: #7a5c00; font-weight: bold;">No se encontraron expedientes con descripción de oficio</p>
                                                <p style="margin: 8px 0 0 0; font-size: 14px; color: #7a5c00;">"SE LE CITA ANTERIORIDAD"</p>
                                            </td>
                                        </tr>
""" if count_expedientes == 0 else ""

        # Cuerpo del correo HTML
        cuerpo = f"""
        <html>
            <head>
                <style>
                    body {{
                        font-family: "Helvetica Neue", Helvetica, Arial, sans-serif;
                        margin: 0;
                        padding: 0;
                        background-color: #f2f2f2;
                    }}
                    table {{
                        border-collapse: collapse;
                    }}
                </style>
            </head>
            <body style="background-color: #f2f2f2; margin: 0; padding: 0;">
                <table width="100%" style="background-color: #f2f2f2;">
                    <tr>
                        <td>
                            <table align="center" width="675" style="background-color: #f2f2f2; margin: 0 auto;">
                                <tr>
                                    <td>
                                        <!-- HEADER AZUL -->
                                        <table align="center" width="675" style="background-color: #2261dd;">
                                            <tr>
                                                <td style="padding: 20px; text-align: center;">
                                                    <div style="color: white; font-family: 'Helvetica Neue', Arial; font-size: 18px; font-weight: bold;">
                                                        <p style="margin: 0;"> REPORTE DE AUTOMATIZACIÓN SIGA</p>
                                                    </div>
                                                </td>
                                            </tr>
                                        </table>

                                        <!-- CONTENIDO PRINCIPAL -->
                                        <table align="center" width="675" style="background-color: white;">
                                            <tr>
                                                <td style="padding: 30px 20px; text-align: center;">
                                                    <h2 style="margin: 0; color: #224386; font-size: 24px;">Resumen de Ejecución</h2>
                                                    <p style="color: #666; font-size: 14px; margin-top: 5px;">{fecha_reporte}</p>
                                                </td>
                                            </tr>
                                        </table>

                                        <!-- ESTADÍSTICAS -->
                                        <table align="center" width="675" style="background-color: white;">
                                            <tr>
                                                <td style="padding: 20px;">
                                                    <table width="100%" style="border-collapse: collapse;">
                                                        <tr>
                                                            <td style="padding: 15px; background-color: #e8f0f8; border-left: 4px solid #2261dd; margin-bottom: 10px;">
                                                                <p style="margin: 0; font-size: 14px; color: #666;">Expedientes Encontrados</p>
                                                                <p style="margin: 10px 0 0 0; font-size: 32px; color: #2261dd; font-weight: bold;">{count_expedientes}</p>
                                                            </td>
                                                        </tr>
                                                        <tr>
                                                            <td style="padding: 15px; background-color: #e8f8f0; border-left: 4px solid #1aa87a; margin-top: 10px;">
                                                                <p style="margin: 0; font-size: 14px; color: #666;">Correos Enviados</p>
                                                                <p style="margin: 10px 0 0 0; font-size: 32px; color: #1aa87a; font-weight: bold;">{count_emails}</p>
                                                            </td>
                                                        </tr>
                                                        {sin_expedientes_html}
                                                    </table>
                                                </td>
                                            </tr>
                                        </table>

                                        <!-- DETALLES -->
                                        <table align="center" width="675" style="background-color: white;">
                                            <tr>
                                                <td style="padding: 20px; border-top: 1px solid #e0e0e0;">
                                                    <h3 style="color: #2261dd; margin-top: 0;">Detalles</h3>
                                                    <ul style="color: #333; font-size: 14px; line-height: 1.8;">
                                                        <li>Se procesaron expedientes con la restricción 'SE LE CITA ANTERIORIDAD'</li>
                                                        <li>Los datos se han guardado en la base de datos de Supabase</li>
                                                        <li>Se han enviado notificaciones a los titulares correspondientes</li>
                                                    </ul>
                                                </td>
                                            </tr>
                                        </table>

                                        <!-- FOOTER -->
                                        <table align="center" width="675" style="background-color: #092a69; color: white;">
                                            <tr>
                                                <td style="padding: 20px; text-align: center; font-size: 12px;">
                                                    <p style="margin: 5px 0;"><strong>My Dear Lawyers®</strong></p>
                                                    <p style="margin: 5px 0;">Sistema Automatizado de Gestión SIGA</p>
                                                    <p style="margin: 10px 0 0 0;">Monterrey, México</p>
                                                </td>
                                            </tr>
                                        </table>
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>
                </table>
            </body>
        </html>
        """
        
        # Adjuntar HTML
        html_part = MIMEText(cuerpo, "html")
        mensaje.attach(html_part)

        # Enviar correo
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as servidor:
            servidor.login(EMAIL_USER, EMAIL_PASSWORD)
            servidor.send_message(mensaje)
        
        print(f"    ✓ Reporte enviado a: {', '.join(destinatarios)}")
        return True

    except Exception as e:
        print(f"    ✗ Error al enviar reporte: {e}")
        return False


# ============ REPORTE SEMANAL ============
def obtener_datos_semanales() -> dict:
    """Consulta expedientes de la semana pasada (viernes → jueves)."""
    today = date.today()
    week_start = today - timedelta(days=7)
    week_end   = today - timedelta(days=1)

    try:
        client = create_client(SUPABASE_URL, SUPABASE_KEY)
        response = (
            client
            .table("expedientes")
            .select("expediente, descripcion_oficio, fecha_oficio, fecha_descarga")
            .gte("fecha_descarga", f"{week_start.isoformat()}T00:00:00")
            .lte("fecha_descarga", f"{week_end.isoformat()}T23:59:59")
            .execute()
        )
        registros = response.data or []
        por_tipo = {}
        for r in registros:
            tipo = r.get("descripcion_oficio") or "SIN TIPO"
            por_tipo[tipo] = por_tipo.get(tipo, 0) + 1

        return {"total": len(registros), "por_tipo": por_tipo, "week_start": week_start, "week_end": week_end}

    except Exception as e:
        print(f"    ✗ Error consultando datos semanales: {e}")
        return {"total": 0, "por_tipo": {}, "week_start": week_start, "week_end": week_end}


def enviar_reporte_semanal(datos: dict) -> bool:
    """Envía el reporte semanal a Eduardo y Paola cada viernes."""
    try:
        if not EMAIL_USER or not EMAIL_PASSWORD:
            print("    ⚠ Credenciales de correo no configuradas")
            return False

        destinatarios  = [e.strip() for e in os.getenv("NOTIFICATION_EMAILS", "").split(",") if e.strip()]
        week_start_str = datos["week_start"].strftime('%d/%m/%Y')
        week_end_str   = datos["week_end"].strftime('%d/%m/%Y')
        total          = datos["total"]
        por_tipo       = datos["por_tipo"]

        print(f"    📧 Enviando reporte semanal a: {', '.join(destinatarios)}")

        mensaje = MIMEMultipart()
        mensaje["From"]    = EMAIL_USER
        mensaje["To"]      = ", ".join(destinatarios)
        mensaje["Subject"] = f"📅 Reporte Semanal SIGA — {week_start_str} al {week_end_str}"

        if por_tipo:
            filas_tipos = ""
            for tipo, cantidad in sorted(por_tipo.items(), key=lambda x: -x[1]):
                filas_tipos += f"""
                <tr>
                    <td style="padding: 10px 15px; border-bottom: 1px solid #e0e0e0; font-size: 14px; color: #333;">{tipo}</td>
                    <td style="padding: 10px 15px; border-bottom: 1px solid #e0e0e0; font-size: 14px; color: #2261dd; font-weight: bold; text-align: center;">{cantidad}</td>
                </tr>"""
        else:
            filas_tipos = """
                <tr>
                    <td colspan="2" style="padding: 15px; text-align: center; color: #999; font-size: 14px;">Sin expedientes esta semana</td>
                </tr>"""

        cuerpo = f"""
        <html>
            <head><style>body {{ font-family: "Helvetica Neue", Helvetica, Arial, sans-serif; margin: 0; padding: 0; background-color: #f2f2f2; }} table {{ border-collapse: collapse; }}</style></head>
            <body style="background-color: #f2f2f2; margin: 0; padding: 0;">
                <table width="100%" style="background-color: #f2f2f2;"><tr><td>
                    <table align="center" width="100%" style="margin: 0 auto; max-width: 675px;"><tr><td>
                        <table align="center" width="100%" style="background-color: #092a69;"><tr>
                            <td style="padding: 25px 20px; text-align: center;">
                                <p style="margin: 0; color: white; font-size: 20px; font-weight: bold; letter-spacing: 2px;">📅 REPORTE SEMANAL STARGAZING</p>
                                <p style="margin: 8px 0 0 0; color: #a8c4e8; font-size: 14px;">{week_start_str} — {week_end_str}</p>
                            </td>
                        </tr></table>
                        <table align="center" width="100%" style="background-color: white;"><tr>
                            <td style="padding: 30px 20px; text-align: center;">
                                <p style="margin: 0; color: #666; font-size: 14px; text-transform: uppercase; letter-spacing: 1px;">Total de Expedientes Procesados</p>
                                <p style="margin: 10px 0 0 0; font-size: 56px; font-weight: bold; color: #2261dd;">{total}</p>
                                <p style="margin: 5px 0 0 0; color: #999; font-size: 13px;">semana del {week_start_str} al {week_end_str}</p>
                            </td>
                        </tr></table>
                        <table align="center" width="100%" style="background-color: white; margin-top: 2px;"><tr>
                            <td style="padding: 20px;">
                                <h3 style="margin: 0 0 15px 0; color: #224386; font-size: 16px; border-bottom: 2px solid #2261dd; padding-bottom: 8px;">Desglose por Tipo de Oficio</h3>
                                <table width="100%" style="border-collapse: collapse;">
                                    <thead><tr style="background-color: #f0f4ff;">
                                        <th style="padding: 10px 15px; text-align: left; font-size: 13px; color: #555; font-weight: 600; border-bottom: 2px solid #dde4f5;">Tipo de Oficio</th>
                                        <th style="padding: 10px 15px; text-align: center; font-size: 13px; color: #555; font-weight: 600; border-bottom: 2px solid #dde4f5; width: 80px;">Cantidad</th>
                                    </tr></thead>
                                    <tbody>{filas_tipos}</tbody>
                                </table>
                            </td>
                        </tr></table>
                        <table align="center" width="100%" style="background-color: #092a69; color: white; margin-top: 2px;"><tr>
                            <td style="padding: 20px; text-align: center; font-size: 12px;">
                                <p style="margin: 5px 0;"><strong>My Dear Lawyers®</strong></p>
                                <p style="margin: 5px 0;">Sistema Automatizado de Gestión SIGA</p>
                                <p style="margin: 10px 0 0 0;">Monterrey, México</p>
                            </td>
                        </tr></table>
                    </td></tr></table>
                </td></tr></table>
            </body>
        </html>
        """

        mensaje.attach(MIMEText(cuerpo, "html"))

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as servidor:
            servidor.login(EMAIL_USER, EMAIL_PASSWORD)
            servidor.send_message(mensaje)

        print(f"    ✓ Reporte semanal enviado a: {', '.join(destinatarios)}")
        return True

    except Exception as e:
        print(f"    ✗ Error al enviar reporte semanal: {e}")
        return False


def get_yesterday() -> date:
    return date.today() - timedelta(days=1)


def sanitize_filename(name: str) -> str:
    name = re.sub(r"[^\w\-. ]+", "", name, flags=re.UNICODE).strip()
    name = re.sub(r"\s+", "_", name)
    return name[:180]


def download_and_extract():
    yesterday = get_yesterday()
    print(f"\nDescargando Ejemplar 2 para: {yesterday.strftime('%d/%m/%Y')}\n")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        xml_files_downloaded = []

        try:
            print("Navegando a SIGA...")
            page.goto(BASE_URL, wait_until="domcontentloaded", timeout=60000)
            print("Pagina cargada\n")
            time.sleep(3)

            print("Clickeando en Ejemplares...")
            try:
                page.click('a:has-text("Ejemplares")', timeout=10000)
                print("Ejemplares abierto")
                time.sleep(5)
            except Exception as e:
                print(f"Error: {e}")

            print("\nAplicando filtros...")
            
            # Area = Marcas
            print(f"  - Seleccionando Area: {AREA_TEXT}")
            try:
                page.wait_for_selector("mat-select[aria-disabled='false']", timeout=15000)
                all_selects = page.locator("mat-select:not([disabled]):not([aria-disabled='true'])").all()
                
                if all_selects:
                    all_selects[0].click(timeout=3000)
                    time.sleep(1)
                    page.locator("mat-option").filter(has_text="Marcas").first.click(timeout=3000)
                    print("    Seleccionado")
                else:
                    print("    Intentando sin filtro...")
                    all_selects = page.locator("mat-select").all()
                    if all_selects:
                        all_selects[0].click(timeout=3000, force=True)
                        time.sleep(1)
                        page.locator("mat-option").filter(has_text="Marcas").first.click(timeout=3000)
                        print("    Seleccionado (forzado)")
            except Exception as e:
                print(f"    Error: {e}")

            time.sleep(1)

            # Fecha - ayer dos veces
            print(f"  - Seleccionando fecha: {yesterday.strftime('%d/%m/%Y')} (día {yesterday.day})")
            try:
                page.wait_for_selector("button[aria-label='Open calendar']:not([disabled])", timeout=15000)
                date_button = page.locator("button[aria-label='Open calendar']").first
                date_button.click(timeout=3000)
                time.sleep(1)

                page.wait_for_selector("[role='gridcell']", timeout=5000)
                time.sleep(0.5)

                day_str = str(yesterday.day)
                day_cells = page.locator("[role='gridcell']").filter(has_text=day_str).all()
                print(f"    Buscando día '{day_str}' — {len(day_cells)} celda(s) encontrada(s)")
                for i, cell in enumerate(day_cells):
                    label = cell.get_attribute("aria-label") or cell.text_content()
                    print(f"      [{i}] aria-label: {label}")

                # Si no encontró el día, navegar al mes anterior y reintentar
                if len(day_cells) == 0:
                    print("    Día no encontrado — navegando al mes anterior...")
                    try:
                        prev_btn = page.locator("button[aria-label='Previous month']").first
                        prev_btn.click(timeout=3000)
                        time.sleep(0.5)
                        day_cells = page.locator("[role='gridcell']").filter(has_text=day_str).all()
                        print(f"    Reintento: {len(day_cells)} celda(s) encontrada(s)")
                    except Exception as e2:
                        print(f"    Error navegando mes anterior: {e2}")

                if len(day_cells) > 0:
                    day_cells[0].click(timeout=3000)
                    time.sleep(0.5)
                    day_cells[0].click(timeout=3000)
                    time.sleep(0.5)
                    print(f"    Fecha seleccionada: {yesterday.strftime('%d/%m/%Y')}")
                else:
                    print("    Día no encontrado — cerrando calendario")
                    page.keyboard.press("Escape")
                    time.sleep(0.5)

            except Exception as e:
                print(f"    Error: {e}")
                try:
                    page.keyboard.press("Escape")
                    time.sleep(0.5)
                except:
                    pass

            time.sleep(1)

            # Gaceta
            print(f"  - Seleccionando Gaceta")
            try:
                time.sleep(1)
                
                result = page.evaluate("""
                    () => {
                        const labels = Array.from(document.querySelectorAll('label'));
                        const gacetaLabel = labels.find(l => l.textContent.toLowerCase().includes('gaceta'));
                        
                        let nextElement = null;
                        if (gacetaLabel) {
                            const parent = gacetaLabel.closest('.mat-form-field-wrapper') || gacetaLabel.closest('mat-form-field');
                            if (parent) {
                                nextElement = parent.querySelector('input, select, mat-select, [role="combobox"]');
                            }
                        }
                        
                        return { labelFound: !!gacetaLabel, nextElement: !!nextElement };
                    }
                """)
                
                if result['nextElement']:
                    page.evaluate("""
                        () => {
                            const labels = Array.from(document.querySelectorAll('label'));
                            const gacetaLabel = labels.find(l => l.textContent.toLowerCase().includes('gaceta'));
                            if (gacetaLabel) {
                                const parent = gacetaLabel.closest('.mat-form-field-wrapper') || gacetaLabel.closest('mat-form-field');
                                if (parent) {
                                    const element = parent.querySelector('input, select, mat-select, [role="combobox"]');
                                    if (element) element.click();
                                }
                            }
                        }
                    """)
                    print("    Clickeado elemento")
                    time.sleep(1)
                    
                    page.wait_for_selector("mat-option", timeout=5000)
                    options = page.locator("mat-option").all()
                    print(f"    Opciones encontradas: {len(options)}")
                    
                    for opt in options:
                        opt_text = opt.text_content()
                        if "Notificacion" in opt_text or "Resolucion" in opt_text:
                            opt.click(timeout=3000)
                            print("    Seleccionado")
                            break
                    
            except Exception as e:
                print(f"    Error: {e}")

            time.sleep(1)

            # Buscar
            print("\nEjecutando busqueda...")
            try:
                page.wait_for_function(
                    """() => {
                        const btn = document.querySelector('button[type="submit"]');
                        return btn && !btn.hasAttribute('disabled');
                    }""",
                    timeout=5000
                )
                
                search_btn = page.locator("button[type='submit']").first
                search_btn.click(timeout=3000)
                print("Busqueda ejecutada")
                time.sleep(3)
            except Exception as e:
                print(f"Error en busqueda: {e}")
                try:
                    search_btn = page.locator("button").filter(has_text="Buscar").first
                    search_btn.click(force=True, timeout=3000)
                    print("Busqueda ejecutada (forzado)")
                    time.sleep(3)
                except:
                    print("Error al clickear Buscar")

            time.sleep(2)

            # Descargar XMLs
            print("Buscando Ejemplar 2...")
            
            try:
                page.wait_for_selector("[role='table']", timeout=10000)
                ejemplar2_rows = page.locator("[role='row']").filter(has_text="Ejemplar 2").all()
                print(f"    Encontradas {len(ejemplar2_rows)} filas")
                
                if ejemplar2_rows:
                    for idx, row in enumerate(ejemplar2_rows):
                        row_text = row.text_content()
                        print(f"\n  [{idx+1}/{len(ejemplar2_rows)}] {row_text[:80]}")
                        
                        try:
                            # Usar JavaScript para inspeccionar la fila
                            row_element = row.element_handle()
                            
                            row_info = page.evaluate("""
                                (element) => {
                                    const links = Array.from(element.querySelectorAll('a'));
                                    return links.map((link, idx) => {
                                        const img = link.querySelector('img');
                                        const src = img ? img.src : '';
                                        return {
                                            idx: idx,
                                            text: link.textContent.substring(0, 50),
                                            href: link.href,
                                            hasPDF: src.includes('pdf'),
                                            hasXML: src.includes('xml'),
                                            imgSrc: src.substring(src.length - 30)
                                        };
                                    });
                                }
                            """, row_element)
                            
                            print(f"    Enlaces:")
                            for info in row_info:
                                tipo = "PDF" if info['hasPDF'] else ("XML" if info['hasXML'] else "OTRO")
                                print(f"      {info['idx']}: {tipo} - {info['imgSrc']}")
                            
                            # Buscar enlace XML (que tenga imagen con "xml")
                            xml_link_idx = None
                            for info in row_info:
                                if info['hasXML']:
                                    xml_link_idx = info['idx']
                                    print(f"    Encontrado botón XML en posición {xml_link_idx}")
                                    break
                            
                            if xml_link_idx is not None:
                                try:
                                    print("    Clickeando botón XML...")
                                    xml_link = row.locator("a").nth(xml_link_idx)
                                    
                                    with page.expect_download(timeout=15000) as download:
                                        xml_link.click(timeout=5000, force=True, no_wait_after=False)
                                    
                                    dl = download.value
                                    time.sleep(1)
                                    fname = sanitize_filename(dl.suggested_filename or f"Ejemplar2_{yesterday.strftime('%d%m%Y')}_part{idx+1}.xml")
                                    out_path = XML_DIR / fname
                                    dl.save_as(str(out_path))
                                    print(f"      Descargado: {out_path}")
                                    xml_files_downloaded.append(out_path)
                                    
                                except Exception as e:
                                    print(f"      Error descargando: {str(e)[:80]}")
                            else:
                                print("    No se encontro boton XML")
                                    
                        except Exception as e:
                            print(f"    Error en fila: {e}")
                else:
                    print("    No se encontraron filas")
                    
            except Exception as e:
                print(f"Error buscando: {e}")

            print("\n\nProcesando XMLs...")
            count_expedientes, count_emails = extract_from_xmls(xml_files_downloaded)
            return count_expedientes, count_emails

        except Exception as e:
            print(f"\nError general: {e}")
            return 0, 0
        finally:
            context.close()
            browser.close()
            print("\nNavegador cerrado")


def extraer_fecha_notificacion(texto: str) -> str:
    """Extrae la fecha de un texto como 'Notificado el 03/03/2026' → '03/03/2026'"""
    if not texto:
        return None
    match = re.search(r'\b(\d{2}[/\-]\d{2}[/\-]\d{4})\b', texto)
    return match.group(1) if match else None


def obtener_notificacion(page, numero_oficio: str) -> str:
    """
    Abre cada lupa de la tabla 'Ver detalle' en MarcaNet y busca el número
    de oficio exacto para extraer la fecha del estado de notificación.
    """
    if not numero_oficio:
        return None

    try:
        print(f"      🔎 Buscando fecha notificación para oficio: {numero_oficio}")

        try:
            page.wait_for_selector("#frmDetalleExp\\:tramiteSeccion", timeout=15000)
        except:
            print("      ⚠ No se encontró la sección 'Trámite'")
            return None

        tabla_info = page.evaluate("""
            () => {
                const span = document.querySelector("#frmDetalleExp\\\\:tramiteSeccion");
                if (!span) return { found: false };
                let el = span;
                while (el) {
                    let sibling = el.nextElementSibling;
                    while (sibling) {
                        const table = sibling.tagName === 'TABLE' ? sibling : sibling.querySelector("table");
                        if (table) {
                            const headers = Array.from(table.querySelectorAll("th"));
                            let idxVerDetalle = -1;
                            headers.forEach((th, i) => {
                                const txt = (th.getAttribute("aria-label") || th.textContent || "").trim();
                                if (txt === "Ver detalle") idxVerDetalle = i;
                            });
                            if (idxVerDetalle !== -1) return { found: true, colIndex: idxVerDetalle };
                        }
                        sibling = sibling.nextElementSibling;
                    }
                    el = el.parentElement;
                    if (!el || el.tagName === 'BODY') break;
                }
                return { found: false };
            }
        """)

        if not tabla_info['found']:
            print("      ⚠ No se encontró tabla Trámite con columna 'Ver detalle'")
            return None

        col_index = tabla_info['colIndex']

        tabla_selector = "#frmDetalleExp\\:dtTblTramitesId"
        filas = page.locator(f"{tabla_selector} tbody tr")
        if filas.count() == 0:
            tabla_selector = "table[id*='dtTblTramitesId']"
            filas = page.locator(f"{tabla_selector} tbody tr")

        total_filas = filas.count()
        print(f"      ℹ Filas en tabla Trámite: {total_filas}")
        if total_filas == 0:
            print("      ⚠ Tabla Trámite vacía (0 filas)")
            return None

        for i in range(total_filas):
            fila = filas.nth(i)
            lupa_link = fila.locator("td").nth(col_index).locator("a, button").first

            if lupa_link.count() == 0:
                print(f"      ⚠ Fila {i+1}: sin lupa (col_index={col_index})")
                continue

            try:
                lupa_link.scroll_into_view_if_needed()
                lupa_link.click(force=True, timeout=8000)

                page.wait_for_selector("div.ui-dialog:visible, div[id*='dlg']:visible", timeout=10000)

                modal = page.locator("div.ui-dialog:visible, div[id*='dlg']:visible").first

                resultado = modal.evaluate(f"""
                    (el) => {{
                        const tables = Array.from(el.querySelectorAll("table"));
                        const allHeaders = [];
                        for (const table of tables) {{
                            const headers = Array.from(table.querySelectorAll("th"));
                            let idxOficio = -1, idxEstado = -1;
                            headers.forEach((th, i) => {{
                                const txt = (th.textContent || "").trim().toLowerCase()
                                    .normalize("NFD").replace(/[\u0300-\u036f]/g, "");
                                allHeaders.push(txt);
                                if (txt.includes("oficio")) idxOficio = i;
                                if (txt.includes("estado") || txt.includes("notif")) idxEstado = i;
                            }});
                            if (idxOficio === -1 || idxEstado === -1) continue;
                            const filas = Array.from(table.querySelectorAll("tbody tr"));
                            const oficiosEncontrados = [];
                            for (const fila of filas) {{
                                const celdas = Array.from(fila.querySelectorAll("td"));
                                if (celdas.length <= Math.max(idxOficio, idxEstado)) continue;
                                const textoOficio = (celdas[idxOficio].textContent || "").trim().toUpperCase();
                                oficiosEncontrados.push(textoOficio);
                                if (textoOficio === "{numero_oficio.upper()}") {{
                                    return {{ encontrado: true, estado: (celdas[idxEstado].textContent || "").trim(), oficios: oficiosEncontrados, headers: allHeaders }};
                                }}
                            }}
                            return {{ encontrado: false, estado: null, oficios: oficiosEncontrados, headers: allHeaders }};
                        }}
                        return {{ encontrado: false, estado: null, oficios: [], headers: allHeaders }};
                    }}
                """)

                texto_estado = resultado['estado'] if resultado['encontrado'] else None
                if not resultado['encontrado']:
                    print(f"      ⚠ Lupa {i+1}: oficio buscado='{numero_oficio.upper()}' | oficios en modal={resultado.get('oficios', [])} | headers={resultado.get('headers', [])}")

                try:
                    close_btn = modal.locator("a.ui-dialog-titlebar-close, button.ui-dialog-titlebar-close, span.ui-icon-closethick").first
                    if close_btn.count() > 0:
                        close_btn.click(timeout=3000)
                    else:
                        page.keyboard.press("Escape")
                except:
                    page.keyboard.press("Escape")

                if resultado['encontrado']:
                    fecha = extraer_fecha_notificacion(texto_estado)
                    print(f"      📅 Fecha notificación: {fecha}")
                    return fecha

            except Exception as e:
                print(f"      ⚠ Error en lupa {i+1}: {e}")
                try:
                    page.keyboard.press("Escape")
                except:
                    pass

        return None

    except Exception as e:
        print(f"      ❌ Error en obtener_notificacion: {e}")
        return None


def buscar_datos_titular(page, expediente: str, numero_oficio: str = None) -> dict:
    url = "https://acervomarcas.impi.gob.mx:8181/marcanet/vistas/common/datos/bsqExpedienteCompleto.pgi"

    print(f"    🔎 Buscando titular en MarcaNet: {expediente}")

    try:
        page.goto(url, wait_until="networkidle", timeout=60000)

        # Esperar input real de PrimeFaces
        input_selector = "#frmBsqExp\\:expedienteId"
        btn_selector = "#frmBsqExp\\:busquedaId2"

        page.wait_for_selector(input_selector, timeout=30000)

        input_exp = page.locator(input_selector)
        input_exp.click()
        input_exp.fill(expediente)

        # Click botón Buscar
        page.locator(btn_selector).click()

        # Esperar a que aparezcan los datos del titular en vez de sleep fijo
        page.wait_for_selector("span[id$='dataTitNomId']", timeout=30000)

        # Función para leer tabla
        def safe_text(selector):
            try:
                el = page.locator(selector).first
                if el.count() > 0:
                    return el.inner_text().strip()
            except:
                pass
            return None

        titular  = safe_text("span[id$='dataTitNomId']")
        telefono = safe_text("span[id$='dataTitTelId']")
        email    = safe_text("span[id$='dataTitEmailId']")

        print(f"      👤 Titular: {titular}")
        print(f"      ☎ Teléfono: {telefono}")
        print(f"      ✉ Email: {email}")

        fecha_notificado = obtener_notificacion(page, numero_oficio)

        return {
            "titular": titular,
            "telefono": telefono,
            "email": email,
            "fecha_notificado": fecha_notificado
        }

    except Exception as e:
        print(f"      ⚠ Error buscando titular: {e}")
        return {
            "titular": None,
            "telefono": None,
            "email": None,
            "fecha_notificado": None
        }


def _worker_marcanet(exp_data: dict) -> dict:
    """Cada hilo lanza su propio Playwright — completamente thread-safe."""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        ctx = browser.new_context()
        page = ctx.new_page()
        try:
            datos = buscar_datos_titular(page, exp_data["expediente"], exp_data["numero_oficio"])
            return {**exp_data, "datos_titular": datos}
        except Exception as e:
            print(f"  ✗ Error worker MarcaNet ({exp_data['expediente']}): {e}")
            return {**exp_data, "datos_titular": {"titular": None, "telefono": None, "email": None, "fecha_notificado": None}}
        finally:
            ctx.close()
            browser.close()


def extract_from_xmls(xml_files):
    if not xml_files:
        print("No hay XMLs para procesar")
        return 0, 0

    print(f"Procesando {len(xml_files)} archivo(s)...\n")

    # ── Paso 1: leer todos los XMLs y recolectar expedientes que coincidan ────
    pendientes = []
    _desc_debug = []
    for xml_path in xml_files:
        print(f"Leyendo: {xml_path.name}")
        try:
            tree = ET.parse(xml_path)
            root = tree.getroot()
            for ficha in root.findall(".//ficha"):
                expediente = registro_marca = serie_expediente = None
                descripcion_oficio = numero_oficio = fecha_oficio = enlace_electronico = None

                for campo in ficha.findall("campo"):
                    clave = campo.find("clave")
                    valor = campo.find("valor")
                    if clave is None or valor is None:
                        continue
                    c = (clave.text or "").strip()
                    v = (valor.text or "").strip()
                    if c == "Expediente":               expediente        = v
                    elif c == "Registro de Marca":      registro_marca    = v
                    elif c == "Serie del expediente":   serie_expediente  = v
                    elif c == "Descripción del oficio": descripcion_oficio = v
                    elif c == "Número del oficio":      numero_oficio     = v
                    elif c == "Fecha del oficio":       fecha_oficio      = v
                    elif c == "Enlace electrónico":     enlace_electronico = v

                if descripcion_oficio:
                    _desc_debug.append(descripcion_oficio)
                if descripcion_oficio and SEARCH_PHRASE in descripcion_oficio:
                    pendientes.append({
                        "expediente":        expediente or "N/A",
                        "registro_marca":    registro_marca,
                        "serie_expediente":  serie_expediente,
                        "descripcion_oficio": descripcion_oficio,
                        "numero_oficio":     numero_oficio,
                        "fecha_oficio":      fecha_oficio,
                        "enlace_electronico": enlace_electronico,
                        "archivo_xml":       xml_path.name,
                    })
        except Exception as e:
            print(f"  Error leyendo {xml_path.name}: {e}")

    if not pendientes:
        print(f"\n✗ No se encontraron registros con '{SEARCH_PHRASE}'")
        print(f"   (Descripciones encontradas en el XML: {list(set(_desc_debug))})")
        return 0, 0

    listar_columns_monday()
    print(f"\n>> {len(pendientes)} expediente(s) encontrado(s) — consultando MarcaNet en paralelo...\n")

    # ── Paso 2: lookups en MarcaNet en paralelo ───────────────────────────────
    max_workers = min(10, len(pendientes))
    resultados = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_worker_marcanet, exp): exp for exp in pendientes}
        for future in as_completed(futures):
            try:
                resultados.append(future.result())
            except Exception as e:
                print(f"  ✗ Error en worker: {e}")

    # ── Paso 3: guardar en BD, Brevo y Monday ────────────────────────────────
    db = SigaDatabase()
    if not db.connect():
        print("✗ No se pudo conectar a la BD")
        return 0, 0

    count_saved  = 0
    count_emails = 0

    for r in resultados:
        datos_titular = r["datos_titular"]

        db.insert_expediente({
            "expediente":        r["expediente"],
            "registro_marca":    r["registro_marca"],
            "serie_expediente":  r["serie_expediente"],
            "descripcion_oficio": r["descripcion_oficio"],
            "numero_oficio":     r["numero_oficio"],
            "fecha_oficio":      r["fecha_oficio"],
            "enlace_electronico": r["enlace_electronico"],
            "archivo_xml":       r["archivo_xml"],
        })

        if datos_titular["titular"]:
            db.insert_titular({
                "expediente":      r["expediente"],
                "titular":         datos_titular["titular"],
                "telefono":        datos_titular["telefono"],
                "email":           datos_titular["email"],
                "fecha_notificado": datos_titular["fecha_notificado"],
            })
            count_saved += 1

            correo_ok = False
            if datos_titular["email"]:
                upsert_perfil_brevo(
                    email=datos_titular["email"],
                    titular=datos_titular["titular"],
                    telefono=datos_titular["telefono"],
                    expediente=r["expediente"],
                )
                print(f"  📧 Enviando notificación al titular via Brevo...")
                correo_ok = enviar_correo_brevo(
                    destinatario=datos_titular["email"],
                    titular=datos_titular["titular"],
                    expediente=r["expediente"],
                    descripcion=r["descripcion_oficio"],
                )
                if correo_ok:
                    count_emails += 1

            print(f"  📋 Creando item en Monday.com...")
            item_id = crear_item_monday(
                expediente=r["expediente"],
                registro_marca=r["registro_marca"],
                email=datos_titular.get("email"),
                telefono=datos_titular.get("telefono"),
                enlace_electronico=r["enlace_electronico"],
                titular=datos_titular["titular"],
                fecha_gaceta=get_yesterday(),
                fecha_notificado=datos_titular.get("fecha_notificado"),
            )
            if item_id and correo_ok:
                marcar_correo_enviado_monday(item_id)

    db.disconnect()

    if count_saved > 0:
        print(f"\n✓ Se guardaron {count_saved} registros en la BD")
        print(f"✓ Se enviaron {count_emails} correos de notificación")
    else:
        print(f"\n✗ Ningún expediente tenía titular en MarcaNet")

    return count_saved, count_emails


if __name__ == "__main__":
    expedientes, emails = download_and_extract()
    
    # Enviar reporte diario
    print(f"\n📊 Enviando reporte diario...")
    enviar_reporte(expedientes, emails)

    # Reporte semanal: solo los viernes (weekday() == 4)
    if date.today().weekday() == 4:
        print(f"\n📅 Hoy es viernes — generando reporte semanal...")
        datos_semana = obtener_datos_semanales()
        print(f"    Expedientes de la semana: {datos_semana['total']}")
        enviar_reporte_semanal(datos_semana)
