
import streamlit as st
import streamlit.components.v1 as components
import os
from datetime import datetime, timedelta, date
import json
import base64
import uuid
import pandas as pd
import pdfplumber
import unicodedata
from io import BytesIO
import time
import socket
import re
import gspread
import html
from typing import Dict, List, Optional
from urllib.parse import quote, urlsplit, urlunsplit
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from http.client import InvalidURL
from oauth2client.service_account import ServiceAccountCredentials
from pytz import timezone
from gspread.utils import rowcol_to_a1
from gspread.exceptions import APIError


# NEW: Import boto3 for AWS S3
import boto3

# --- STREAMLIT CONFIGURATION ---
st.set_page_config(page_title="App Vendedores TD", layout="wide")

REFRESH_COOLDOWN = 60


TAB1_PRESERVED_STATE_KEYS: set[str] = {
    "last_selected_vendedor",
    "current_tab_index",
    "tipo_envio_selector_global",
}


TAB1_FORM_STATE_KEYS_TO_CLEAR: set[str] = {
    "registrar_nota_venta_checkbox",
    "registro_cliente",
    "numero_cliente_rfc",
    "tipo_envio_original",
    "estatus_factura_origen",
    "subtipo_local_selector",
    "folio_factura_error_input",
    "nota_venta_input",
    "motivo_nota_venta_input",
    "folio_factura_input",
    "fecha_entrega_input",
    "comentario_detallado",
    "direccion_guia_retorno_foraneo",
    "resultado_esperado",
    "material_devuelto",
    "monto_devuelto",
    "area_responsable",
    "nombre_responsable",
    "motivo_detallado",
    "g_resultado_esperado",
    "g_descripcion_falla",
    "g_piezas_afectadas",
    "g_monto_estimado",
    "g_area_responsable",
    "g_nombre_responsable",
    "g_numero_serie",
    "g_fecha_compra",
    "direccion_guia_retorno",
    "direccion_envio_destino",
    "pedido_adjuntos",
    "comprobante_cliente",
    "estado_pago",
    "chk_doble",
    "chk_triple",
    "comprobante_uploader_final",
    "fecha_pago_input",
    "forma_pago_input",
    "monto_pago_input",
    "terminal_input",
    "banco_destino_input",
    "referencia_pago_input",
    "cp_pago1",
    "fecha_pago1",
    "forma_pago1",
    "monto_pago1",
    "terminal1",
    "banco1",
    "ref1",
    "cp_pago2",
    "fecha_pago2",
    "forma_pago2",
    "monto_pago2",
    "terminal2",
    "banco2",
    "ref2",
    "cp_pago3",
    "fecha_pago3",
    "forma_pago3",
    "monto_pago3",
    "terminal3",
    "banco3",
    "ref3",
}


USUARIOS_VALIDOS = [
    "ADAMARIS47",
    "ALEJANDRO38",
    "ANA45",
    "CURSOS92",
    "DANIELA73",
    "DISTRIBUCION88",
    "EDGAR66",
    "GRISELDA82",
    "GLORIA53",
    "HECTOR64",
    "JUAN24",
    "NORA79",
    "PAULINA57",
    "ROBERTO51",
]



def normalize_case_text(value, placeholder: str = "N/A") -> str:
    """Return a clean string for optional case fields."""
    if value is None:
        return placeholder
    if isinstance(value, str):
        cleaned = value.strip()
        return cleaned if cleaned else placeholder
    return str(value)


def normalize_case_amount(value, placeholder: str = "N/A") -> str:
    """Format optional numeric fields, falling back to ``placeholder`` if empty."""
    try:
        amount = float(value)
    except (TypeError, ValueError):
        return placeholder
    return f"{amount:.2f}" if amount > 0 else placeholder


def format_estado_entrega(value) -> str:
    """Return delivery status text for local orders."""
    if value is None:
        return "Sin info de entrega"
    if isinstance(value, str):
        cleaned = value.strip()
        return cleaned if cleaned else "Sin info de entrega"
    if pd.isna(value):
        return "Sin info de entrega"
    cleaned = str(value).strip()
    return cleaned if cleaned else "Sin info de entrega"


def parse_sheet_row_number(value) -> Optional[int]:
    """Return a normalized Google Sheet row number or ``None`` if missing."""
    if value is None:
        return None
    if isinstance(value, (int, float)) and not pd.isna(value):
        candidate = int(value)
        return candidate if candidate > 0 else None
    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned:
            return None
        try:
            candidate = int(float(cleaned))
        except ValueError:
            return None
        return candidate if candidate > 0 else None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        return None
    try:
        candidate = int(float(value))
    except (TypeError, ValueError):
        return None
    return candidate if candidate > 0 else None


def load_sheet_records_with_row_numbers(worksheet):
    """Return DataFrame rows with their real Google Sheet indices preserved."""

    try:
        all_values = worksheet.get_all_values()
    except Exception:
        return pd.DataFrame(), []

    if not all_values:
        return pd.DataFrame(), []

    headers_raw = all_values[0]
    headers = [str(h).strip() for h in headers_raw]

    max_columns = len(headers)
    records: List[Dict[str, str]] = []
    row_numbers: List[int] = []

    for row_index, row_values in enumerate(all_values[1:], start=2):
        normalized_row = list(row_values[:max_columns])
        if len(normalized_row) < max_columns:
            normalized_row.extend([""] * (max_columns - len(normalized_row)))

        if not any(str(cell).strip() for cell in normalized_row):
            continue

        record = {
            headers[col_idx]: normalized_row[col_idx] if col_idx < len(normalized_row) else ""
            for col_idx in range(max_columns)
        }
        records.append(record)
        row_numbers.append(row_index)

    df_records = pd.DataFrame(records)
    if df_records.empty:
        return df_records, headers

    df_records.insert(0, "Sheet_Row_Number", row_numbers)
    return df_records, headers


def extract_id_vendedor(data, placeholder: str = "N/A") -> str:
    """Return a readable vendor ID from heterogeneous row/dict structures."""

    if data is None:
        return placeholder

    candidate_keys = (
        "id_vendedor",
        "ID_Vendedor",
        "Id_Vendedor",
        "IDVendedor",
        "IdVendedor",
        "ID Vendedor",
        "Id Vendedor",
        "ID_VENDEDOR",
        "IDVENDEDOR",
    )

    for key in candidate_keys:
        value = None
        if hasattr(data, "get"):
            try:
                value = data.get(key)
            except Exception:
                value = None

        if value is None and isinstance(data, pd.Series) and key in data.index:
            value = data[key]

        if value is None:
            continue

        if isinstance(value, str):
            cleaned = value.strip()
            if cleaned and cleaned.lower() not in {"nan", "none"}:
                return cleaned
            continue

        try:
            if pd.isna(value):
                continue
        except Exception:
            pass

        cleaned = str(value).strip()
        if cleaned and cleaned.lower() not in {"nan", "none"}:
            return cleaned

    return placeholder


def extract_id_vendedor_mod(data, placeholder: str = "") -> str:
    """Return normalized modifier vendor IDs, handling multiple entries."""

    if data is None:
        return placeholder

    candidate_keys = (
        "id_vendedor_Mod",
        "ID_Vendedor_Mod",
        "Id_Vendedor_Mod",
        "IDVendedor_Mod",
        "ID_VENDEDOR_MOD",
    )

    raw_value = None
    for key in candidate_keys:
        value = None
        if hasattr(data, "get"):
            try:
                value = data.get(key)
            except Exception:
                value = None

        if value is None and isinstance(data, pd.Series) and key in data.index:
            value = data[key]

        if value is None:
            continue

        raw_value = value
        break

    if raw_value is None:
        return placeholder

    if isinstance(raw_value, str):
        tokens = [
            entry.strip()
            for entry in re.split(r"[;,\n]", raw_value)
            if entry and entry.strip()
        ]
    elif isinstance(raw_value, (list, tuple, set)):
        tokens = [str(entry).strip() for entry in raw_value if str(entry).strip()]
    else:
        normalized = str(raw_value).strip()
        tokens = [normalized] if normalized else []

    if not tokens:
        return placeholder

    unique_tokens: list[str] = []
    seen = set()
    for token in tokens:
        upper_token = token.upper()
        if upper_token not in seen:
            seen.add(upper_token)
            unique_tokens.append(upper_token)

    return ", ".join(unique_tokens) if unique_tokens else placeholder


def format_id_vendedor_with_mod(data, placeholder: str = "N/A") -> str:
    """Compose the display string for vendor IDs including modifiers."""

    id_principal = extract_id_vendedor(data, placeholder)
    id_modificador = extract_id_vendedor_mod(data, "")

    base_segment = f"🆔 **ID Vendedor:** `{id_principal}`"
    if id_modificador:
        base_segment += f"  |  🛠️ **ID Vendedor Mod:** `{id_modificador}`"

    return base_segment


def allow_refresh(key: str, container=st, cooldown: int = REFRESH_COOLDOWN) -> bool:
    """Rate-limit manual reloads to avoid hitting services too often."""
    now = time.time()
    last = st.session_state.get(key)
    if last and now - last < cooldown:
        container.warning("⚠️ Se recargó recientemente. Espera unos segundos.")
        return False
    st.session_state[key] = now
    return True


def clear_app_caches() -> None:
    """Reinicia las conexiones y datos cacheados para forzar una recarga completa."""
    st.cache_data.clear()
    cargar_pedidos.clear()
    get_google_sheets_client.clear()
    get_worksheet.clear()
    get_s3_client.clear()


def ensure_user_logged_in() -> str:
    """Muestra una pantalla de inicio de sesión simple y detiene la app hasta autenticar."""
    st.session_state.setdefault("id_vendedor", "")
    current_user = st.session_state.get("id_vendedor", "")

    if not current_user:
        usuario_param = st.query_params.get("usuario")
        if isinstance(usuario_param, (list, tuple)):
            usuario_param = usuario_param[0] if usuario_param else ""
        if usuario_param:
            candidate = str(usuario_param).strip().upper()
            if candidate and candidate in USUARIOS_VALIDOS:
                st.session_state["id_vendedor"] = candidate
                return candidate

    if current_user:
        return current_user

    st.markdown("## 🔐 Inicio de sesión")
    username_input = st.text_input("Usuario", key="login_usuario")

    if st.button("Ingresar", key="login_ingresar_btn"):
        candidate = username_input.strip()
        if candidate and candidate.upper() in USUARIOS_VALIDOS:
            normalized_candidate = candidate.upper()
            st.session_state["id_vendedor"] = normalized_candidate
            st.query_params["usuario"] = normalized_candidate
            st.rerun()
        else:
            st.error("❌ Usuario no válido. Verifica tu nombre y número.")

    st.stop()


def render_date_filter_controls(
    label: str,
    key_prefix: str,
    *,
    default_range_days: int = 7,
) -> tuple[date, date, bool, bool]:
    """Renderiza un control de fecha con opción de rango y devuelve la selección.

    Returns a tuple ``(fecha_inicio, fecha_fin, rango_activo, rango_valido)``.
    """

    use_range = st.checkbox(
        "🔁 Activar búsqueda por rango de fechas",
        key=f"{key_prefix}_usar_rango",
    )

    if use_range:
        end_default = st.session_state.get(
            f"{key_prefix}_fecha_fin",
            datetime.now().date(),
        )
        start_default = st.session_state.get(
            f"{key_prefix}_fecha_inicio",
            end_default - timedelta(days=default_range_days),
        )

        if start_default > end_default:
            start_default = end_default

        start_date = st.date_input(
            "📅 Fecha inicial:",
            value=start_default,
            key=f"{key_prefix}_fecha_inicio",
        )
        end_date = st.date_input(
            "📅 Fecha final:",
            value=end_default if end_default >= start_date else start_date,
            key=f"{key_prefix}_fecha_fin",
        )

        is_valid = end_date >= start_date
        if not is_valid:
            st.error("La fecha final no puede ser anterior a la fecha inicial.")

        return start_date, end_date, True, is_valid

    selected_date = st.date_input(
        label,
        value=st.session_state.get(
            f"{key_prefix}_fecha",
            datetime.now().date(),
        ),
        key=f"{key_prefix}_fecha",
    )

    return selected_date, selected_date, False, True


def reset_tab1_form_state(additional_preserved: dict[str, object] | None = None) -> None:
    """Elimina los valores capturados en el formulario principal, conservando envío y vendedor."""

    preserved_values = {
        key: st.session_state.get(key)
        for key in TAB1_PRESERVED_STATE_KEYS
    }

    if additional_preserved:
        preserved_values.update(additional_preserved)

    for key in TAB1_FORM_STATE_KEYS_TO_CLEAR:
        st.session_state.pop(key, None)

    for key, value in preserved_values.items():
        if value is None:
            continue

        # Evita sobrescribir el valor si ya existe y coincide
        if key in st.session_state and st.session_state[key] == value:
            continue

        st.session_state[key] = value


def safe_batch_update(worksheet, data, retries: int = 5, base_delay: float = 1.0) -> None:
    """Realiza ``batch_update`` con reintentos ante errores de cuota."""
    for attempt in range(retries):
        try:
            worksheet.batch_update(data)
            return
        except APIError as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            if status == 429 and attempt < retries - 1:
                time.sleep(base_delay * (2 ** attempt))
            else:
                raise

# --- GOOGLE SHEETS CONFIGURATION ---
# Eliminamos la línea SERVICE_ACCOUNT_FILE ya que leeremos de secrets
GOOGLE_SHEET_ID = '1aWkSelodaz0nWfQx7FZAysGnIYGQFJxAN7RO3YgCiZY'

def build_gspread_client():
    credentials_json_str = st.secrets["google_credentials"]
    creds_dict = json.loads(credentials_json_str)
    if "private_key" in creds_dict:
        creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n").strip()
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(creds)

_gsheets_client = None


@st.cache_resource
def get_google_sheets_client():
    def try_get_client():
        credentials_json_str = st.secrets["google_credentials"]
        creds_dict = json.loads(credentials_json_str)
        if "private_key" in creds_dict:
            creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n").strip()
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        return gspread.authorize(creds)

    max_attempts = 5
    for attempt in range(max_attempts):
        try:
            client = try_get_client()
            _ = client.open_by_key(GOOGLE_SHEET_ID)
            st.session_state.pop("gsheet_error", None)
            return client
        except gspread.exceptions.APIError as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            if status == 429 or "RESOURCE_EXHAUSTED" in str(e):
                time.sleep(2 ** attempt)
                continue
            st.session_state["gsheet_error"] = f"❌ Error al conectar con Google Sheets: {e}"
            return None
        except Exception as e:
            st.session_state["gsheet_error"] = f"❌ Error al conectar con Google Sheets: {e}"
            return None

    st.session_state["gsheet_error"] = st.session_state.get(
        "gsheet_error", "❌ No se pudo conectar con Google Sheets."
    )
    return None

@st.cache_resource
def get_worksheet():
    client = get_google_sheets_client()
    if client is None:
        return None
    spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)
    return spreadsheet.worksheet("datos_pedidos")

def get_worksheet_casos_especiales():
    client = build_gspread_client()
    spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)
    return spreadsheet.worksheet("casos_especiales")

@st.cache_data(ttl=300)
def get_sheet_headers(sheet_name: str):
    """Obtiene y cachea los encabezados de la hoja especificada."""
    if sheet_name == "casos_especiales":
        ws = get_worksheet_casos_especiales()
    else:
        ws = get_worksheet()
    return ws.row_values(1) if ws else []


# --- AWS S3 CONFIGURATION (NEW) ---
# Load AWS credentials from Streamlit secrets
try:
    AWS_ACCESS_KEY_ID = st.secrets["aws_access_key_id"]
    AWS_SECRET_ACCESS_KEY = st.secrets["aws_secret_access_key"]
    AWS_REGION = st.secrets["aws_region"]
    S3_BUCKET_NAME = st.secrets["s3_bucket_name"]
except KeyError as e:
    st.error(f"❌ Error: AWS S3 credentials not found in Streamlit secrets. Make sure your .streamlit/secrets.toml file is correctly configured. Missing key: {e}")
    st.stop()


  # --- AUTHENTICATION AND CLIENT FUNCTIONS ---

  # Removed the old load_credentials_from_file and get_gspread_client functions
  # as they are replaced by get_google_sheets_client()

  # NEW: Build S3 client
@st.cache_resource
def get_s3_client():
    """Initializes and returns an S3 client."""
    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
        st.session_state.pop("s3_error", None)
        return s3
    except Exception as e:
        st.session_state["s3_error"] = f"❌ Error al inicializar el cliente S3: {e}"
        return None


@st.cache_data(ttl=60)
def check_basic_internet_connectivity(timeout: float = 5.0) -> tuple[bool, str]:
    """Comprueba si hay conexión básica a Internet realizando una solicitud simple."""
    # Usamos el endpoint generate_204, recomendado por Google para comprobar
    # conectividad sin desencadenar respuestas 403 destinadas a los navegadores.
    test_url = "https://clients3.google.com/generate_204"
    try:
        request = Request(
            test_url,
            headers={
                # Algunos endpoints devuelven 403 si falta un User-Agent.
                "User-Agent": "Mozilla/5.0 (compatible; StreamlitApp/1.0)"
            },
        )
        with urlopen(request, timeout=timeout):
            pass
        return True, "Conexión a Internet estable."
    except HTTPError as exc:
        return False, f"Error HTTP al verificar Internet ({exc.code})."
    except (URLError, InvalidURL, TimeoutError, socket.timeout) as exc:
        return False, f"No hay conexión estable a Internet: {exc}"
    except Exception as exc:  # pragma: no cover - captura errores imprevistos
        return False, f"Error inesperado de Internet: {exc}"


def build_connection_statuses(g_client, s3_client) -> list[dict[str, object]]:
    """Genera el estado de conexión para los servicios críticos de la app."""

    statuses: list[dict[str, object]] = []

    internet_ok, internet_message = check_basic_internet_connectivity()
    statuses.append(
        {
            "name": "Internet",
            "ok": internet_ok,
            "message": internet_message,
            "critical": True,
        }
    )

    if g_client is not None:
        statuses.append(
            {
                "name": "Google Sheets",
                "ok": True,
                "message": "Conexión con Google Sheets activa.",
                "critical": True,
            }
        )
    else:
        statuses.append(
            {
                "name": "Google Sheets",
                "ok": False,
                "message": st.session_state.get(
                    "gsheet_error",
                    "❌ Error desconocido al conectar con Google Sheets.",
                ),
                "critical": True,
            }
        )

    if s3_client is not None:
        try:
            s3_client.head_bucket(Bucket=S3_BUCKET_NAME)
            statuses.append(
                {
                    "name": "AWS S3",
                    "ok": True,
                    "message": "Conexión con AWS S3 verificada.",
                    "critical": True,
                }
            )
        except Exception as exc:
            statuses.append(
                {
                    "name": "AWS S3",
                    "ok": False,
                    "message": f"❌ Error al verificar AWS S3: {exc}",
                    "critical": True,
                }
            )
    else:
        statuses.append(
            {
                "name": "AWS S3",
                "ok": False,
                "message": st.session_state.get(
                    "s3_error",
                    "❌ Error desconocido al inicializar AWS S3.",
                ),
                "critical": True,
            }
        )

    return statuses


def display_connection_status_badge(statuses: list[dict[str, object]]) -> None:
    """Muestra un indicador fijo en pantalla con el estado de las conexiones."""

    overall_ok = all(status.get("ok", False) for status in statuses)
    icon = "🟢" if overall_ok else "🔴"
    label = "Conexión segura" if overall_ok else "Problemas de conexión"

    detail_lines = [
        f"{status['name']}: {'✅' if status.get('ok') else '❌'} {status.get('message', '')}"
        for status in statuses
    ]
    escaped_lines = [html.escape(line, quote=True) for line in detail_lines]
    tooltip_text = "&#10;".join(escaped_lines)

    status_class = "ok" if overall_ok else "error"

    badge_html = f"""
    <style>
    .connection-status-container {{
        position: sticky;
        top: 4.5rem;
        z-index: 1000;
        display: flex;
        justify-content: flex-end;
        width: 100%;
        max-width: 1200px;
        margin: 0 auto 0.5rem;
        padding: 0 1.5rem;
    }}
    .connection-status-badge {{
        display: inline-flex;
        align-items: center;
        padding: 0.45rem 0.9rem;
        border-radius: 999px;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.12);
        font-weight: 600;
        font-size: 0.95rem;
        transition: transform 0.2s ease, box-shadow 0.2s ease;
        cursor: default;
        gap: 0.5rem;
    }}
    .connection-status-badge--ok {{
        background: linear-gradient(135deg, #1f8f4d, #16693a);
        color: #ffffff;
    }}
    .connection-status-badge--error {{
        background: linear-gradient(135deg, #e74c3c, #c0392b);
        color: #ffffff;
    }}
    .connection-status-badge:hover {{
        transform: translateY(-1px);
        box-shadow: 0 6px 16px rgba(0, 0, 0, 0.18);
    }}
    .connection-status-icon {{
        font-size: 1.1rem;
    }}
    @media (max-width: 768px) {{
        .connection-status-container {{
            top: 3.5rem;
            padding: 0 1rem;
        }}
        .connection-status-badge {{
            width: 100%;
            justify-content: center;
        }}
    }}
    </style>
    <div class="connection-status-container">
        <div class="connection-status-badge connection-status-badge--{status_class}" title="{tooltip_text}">
            <span class="connection-status-icon">{icon}</span>
            <span>{label}</span>
        </div>
    </div>
    """

    st.markdown(badge_html, unsafe_allow_html=True)


# ✅ Clientes listos para usar en cualquier parte
g_spread_client = get_google_sheets_client()
s3_client = get_s3_client()

def upload_file_to_s3(s3_client, bucket_name, file_obj, s3_key):
    """
    Sube un archivo a un bucket de S3.

    Args:
        s3_client: El cliente S3 inicializado.
        bucket_name: El nombre del bucket S3.
        file_obj: El objeto de archivo cargado por st.file_uploader.
        s3_key: La ruta completa y nombre del archivo en S3 (ej. 'pedido_id/filename.pdf').

    Returns:
        tuple: (True, URL del archivo, None) si tiene éxito.
               (False, None, str(error)) cuando ocurre un problema.
    """
    try:
        # Asegúrate de que el puntero del archivo esté al principio
        file_obj.seek(0)
        s3_client.upload_fileobj(file_obj, bucket_name, s3_key)
        file_url = f"https://{bucket_name}.s3.{AWS_REGION}.amazonaws.com/{s3_key}"
        return True, file_url, None
    except Exception as e:
        return False, None, str(e)
    
# --- Función para actualizar una celda de Google Sheets de forma segura ---
def update_gsheet_cell(worksheet, headers, row_index, col_name, value):
    try:
        if col_name not in headers:
            st.error(f"❌ Error: La columna '{col_name}' no se encontró en Google Sheets para la actualización.")
            return False
        col_index = headers.index(col_name) + 1
        worksheet.update_cell(row_index, col_index, value)
        return True
    except Exception as e:
        st.error(f"❌ Error al actualizar la celda ({row_index}, {col_name}) en Google Sheets: {e}")
        return False


def set_pedido_submission_status(
    status: str,
    message: str,
    detail: str | None = None,
    attachments: list[str] | None = None,
    missing_attachments_warning: bool = False,
) -> None:
    """Guarda el resultado del registro de un pedido para mostrarlo en la UI."""
    st.session_state["pedido_submission_status"] = {
        "status": status,
        "message": message,
        "detail": detail or "",
        "attachments": attachments or [],
        "missing_attachments_warning": missing_attachments_warning,
    }

@st.cache_data(ttl=300)
def cargar_pedidos():
    sheet = g_spread_client.open_by_key("1aWkSelodaz0nWfQx7FZAysGnIYGQFJxAN7RO3YgCiZY").worksheet("datos_pedidos")
    data = sheet.get_all_records()
    return pd.DataFrame(data)


usuario_activo = ensure_user_logged_in()

connection_statuses = build_connection_statuses(g_spread_client, s3_client)
display_connection_status_badge(connection_statuses)

status_by_name = {status["name"]: status for status in connection_statuses}

internet_status = status_by_name.get("Internet")
if internet_status and not internet_status.get("ok", False):
    st.warning(internet_status.get("message", "Problema al verificar la conexión a Internet."))

gsheet_status = status_by_name.get("Google Sheets")
if gsheet_status and not gsheet_status.get("ok", False):
    st.error(gsheet_status.get("message", "No se pudo conectar con Google Sheets."))
    if st.button("Reintentar conexión con Google Sheets", key="retry_gsheets_badge"):
        get_google_sheets_client.clear()
        st.session_state.pop("gsheet_error", None)
        st.rerun()
    st.stop()

s3_status = status_by_name.get("AWS S3")
if s3_status and not s3_status.get("ok", False):
    st.error(s3_status.get("message", "No se pudo conectar con AWS S3."))
    if st.button("Reintentar conexión con AWS S3", key="retry_s3_badge"):
        get_s3_client.clear()
        st.session_state.pop("s3_error", None)
        st.rerun()
    st.stop()

st.markdown(f"### 👋 Bienvenido, {usuario_activo}")

if st.button("🔄 Recargar Página y Conexión", help="Haz clic aquí si algo no carga o da error de Google Sheets."):
    if allow_refresh("main_last_refresh"):
        clear_app_caches()
        st.rerun()

st.title("🛒 App de Vendedores TD")
st.write("¡Bienvenido! Aquí puedes registrar y gestionar tus pedidos.")

def normalizar(texto):
    return unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('utf-8').lower()

def obtener_prefijo_s3(pedido_id):
    posibles_prefijos = [
        f"{pedido_id}/", f"adjuntos_pedidos/{pedido_id}/",
        f"adjuntos_pedidos/{pedido_id}", f"{pedido_id}"
    ]
    for prefix in posibles_prefijos:
        try:
            respuesta = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=prefix, MaxKeys=1)
            if "Contents" in respuesta:
                return prefix if prefix.endswith("/") else prefix + "/"
        except Exception:
            continue
    return None

def obtener_archivos_pdf_validos(prefix):
    try:
        respuesta = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=prefix)
        archivos = respuesta.get("Contents", [])
        return [f for f in archivos if f["Key"].lower().endswith(".pdf") and any(x in f["Key"].lower() for x in ["guia", "guía", "descarga"])]
    except Exception as e:
        st.error(f"❌ Error al listar archivos en S3 para prefijo {prefix}: {e}")
        return []

def obtener_todos_los_archivos(prefix):
    try:
        respuesta = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=prefix)
        return respuesta.get("Contents", [])
    except Exception:
        return []

def extraer_texto_pdf(s3_key):
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=s3_key)
        with pdfplumber.open(BytesIO(response["Body"].read())) as pdf:
            return "\n".join(page.extract_text() or "" for page in pdf.pages)
    except Exception as e:
        return f"[ERROR AL LEER PDF]: {e}"

def generar_url_s3(s3_key):
    return s3_client.generate_presigned_url(
        'get_object',
        Params={'Bucket': S3_BUCKET_NAME, 'Key': s3_key},
        ExpiresIn=3600
    )

# --- Utilidades y renderizado de casos especiales ---
def partir_urls(value):
    """Normaliza campos de adjuntos que pueden venir como JSON o texto separado."""
    if value is None:
        return []
    s = str(value).strip()
    if not s or s.lower() in ("nan", "none", "n/a"):
        return []
    urls = []
    try:
        obj = json.loads(s)
        if isinstance(obj, list):
            for it in obj:
                if isinstance(it, str) and it.strip():
                    urls.append(it.strip())
                elif isinstance(it, dict):
                    for k in ("url", "URL", "href", "link"):
                        if k in it and str(it[k]).strip():
                            urls.append(str(it[k]).strip())
        elif isinstance(obj, dict):
            for k in ("url", "URL", "href", "link"):
                if k in obj and str(obj[k]).strip():
                    urls.append(str(obj[k]).strip())
    except Exception:
        for p in re.split(r"[,\n;]+", s):
            p = p.strip()
            if p:
                urls.append(p)
    out, seen = [], set()
    for u in urls:
        if u not in seen:
            seen.add(u); out.append(u)
    return out

def __s(v):
    return "" if v is None else str(v).strip()

def __has(v):
    s = __s(v)
    return bool(s) and s.lower() not in ("nan", "none", "n/a")

def __is_url(v):
    s = __s(v).lower()
    return s.startswith("http://") or s.startswith("https://")

def __link(url, label=None):
    u = __s(url)
    if __is_url(u):
        import os
        return f"[{label or (os.path.basename(u) or 'Abrir')}]({u})"
    return u

IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".gif", ".bmp", ".webp"}
PDF_EXTENSIONS = {".pdf"}

MAX_INLINE_PDF_BYTES = 10 * 1024 * 1024  # 10 MB límite para incrustar PDFs en base64


def _normalize_url(value: str) -> str:
    """Return a URL with unsafe characters percent-encoded."""
    raw = __s(value)
    if not raw:
        return ""

    try:
        parts = urlsplit(raw)
        if not parts.scheme or not parts.netloc:
            return raw

        safe_path = quote(parts.path, safe="/%:@")
        safe_query = quote(parts.query, safe="=&%:@")
        safe_fragment = quote(parts.fragment, safe="=&%:@")
        return urlunsplit((parts.scheme, parts.netloc, safe_path, safe_query, safe_fragment))
    except Exception:
        return raw


def _render_pdf_iframe_from_base64(data: bytes) -> None:
    """Render a PDF preview from raw bytes encoded in base64."""
    if not data:
        st.info("El archivo no contiene datos para mostrar.")
        return
    b64_pdf = base64.b64encode(data).decode("utf-8")
    iframe = (
        "<iframe src=\"data:application/pdf;base64,{data}\" width=\"100%\" height=\"600\" style=\"border:none;\"></iframe>"
    ).format(data=b64_pdf)
    components.html(iframe, height=620, scrolling=True)


def _render_pdf_iframe_via_google(url: str) -> None:
    """Fallback to Google Docs viewer for remote PDF URLs."""
    viewer_url = f"https://docs.google.com/gview?url={quote(_normalize_url(url), safe='')}\u0026embedded=true"
    iframe = (
        "<iframe src=\"{src}\" width=\"100%\" height=\"600\" style=\"border:none;\" allow=\"fullscreen\"></iframe>"
    ).format(src=html.escape(viewer_url, quote=True))
    components.html(iframe, height=620, scrolling=True)


def _clean_url_path(value: str) -> str:
    """Remove query/hash parameters from a URL or filename."""
    cleaned = __s(value)
    if not cleaned:
        return ""
    return cleaned.split("?")[0].split("#")[0]


def _infer_extension(value: str) -> str:
    """Infer lowercase file extension from a path or URL."""
    cleaned = _clean_url_path(value)
    return os.path.splitext(cleaned)[1].lower()


def _infer_display_name(value: str) -> str:
    """Return a friendly filename to display for a URL or path."""
    cleaned = _clean_url_path(value)
    name = os.path.basename(cleaned)
    return name or cleaned or "Archivo"


def render_remote_file_preview(url: str, display_label: str) -> None:
    """Render an inline preview for a remote file when possible."""
    if not __is_url(url):
        st.info("El archivo no es una URL válida para previsualizar.")
        return

    normalized_url = _normalize_url(url)
    if not normalized_url:
        st.info("El archivo no es una URL válida para previsualizar.")
        return

    ext = _infer_extension(normalized_url)
    if ext in IMAGE_EXTENSIONS:
        st.image(normalized_url, caption=display_label, use_container_width=True)
    elif ext in PDF_EXTENSIONS:
        rendered = False
        try:
            request = Request(normalized_url, headers={"User-Agent": "Mozilla/5.0"})
            with urlopen(request, timeout=10) as response:
                data = response.read(MAX_INLINE_PDF_BYTES + 1)
                if len(data) > MAX_INLINE_PDF_BYTES:
                    raise ValueError("PDF supera el límite para vista previa embebida")
                _render_pdf_iframe_from_base64(data)
                rendered = True
        except (HTTPError, URLError, ValueError, TimeoutError, OSError, InvalidURL):
            st.caption("No se pudo generar la vista previa directa del PDF. Se usa visor alternativo.")

        if not rendered:
            _render_pdf_iframe_via_google(normalized_url)
    else:
        st.info("Vista previa no disponible para este tipo de archivo.")


def add_url_preview_expander(url: str, display_label: str) -> None:
    """Attach an expander with a preview for a given URL."""
    if not __is_url(url):
        return
    with st.expander(f"👁️ Vista previa • {display_label}", expanded=False):
        render_remote_file_preview(url, display_label)


def render_attachment_link(url: str, label: str | None = None, icon: str | None = None, bullet: bool = True) -> None:
    """Render a file link and automatically include an expandable preview."""
    if not __has(url):
        return

    display_label = label or _infer_display_name(url)
    prefix = f"{icon} " if icon else ""

    if bullet:
        if __is_url(url):
            sanitized = _normalize_url(url)
            st.markdown(f"- {prefix}[{display_label}]({sanitized or url})")
        else:
            st.markdown(f"- {prefix}{__s(url)}")
    else:
        if __is_url(url):
            sanitized = _normalize_url(url)
            st.markdown(f"{prefix}[{display_label}]({sanitized or url})")
        else:
            st.markdown(f"{prefix}{__s(url)}")

    if __is_url(url):
        add_url_preview_expander(url, display_label)


def render_uploaded_file_preview(file_obj) -> None:
    """Show a preview expander for an uploaded Streamlit file."""
    file_name = getattr(file_obj, "name", "Archivo")
    display_label = file_name or "Archivo"
    ext = _infer_extension(file_name)

    with st.expander(f"👁️ Vista previa • {display_label}", expanded=False):
        if ext in IMAGE_EXTENSIONS:
            file_obj.seek(0)
            st.image(file_obj.read(), caption=display_label, use_container_width=True)
            file_obj.seek(0)
        elif ext in PDF_EXTENSIONS:
            file_obj.seek(0)
            data = file_obj.read()
            file_obj.seek(0)
            if data:
                b64_pdf = base64.b64encode(data).decode("utf-8")
                iframe = (
                    "<iframe src=\"data:application/pdf;base64,{data}\" width=\"100%\" height=\"600\" style=\"border:none;\"></iframe>"
                ).format(data=b64_pdf)
                components.html(iframe, height=620, scrolling=True)
            else:
                st.info("El archivo no contiene datos para mostrar.")
        else:
            st.info("Vista previa no disponible para este tipo de archivo.")


def render_uploaded_files_preview(title: str, files) -> None:
    """Render previews for uploaded files unless Tab 1 is active."""
    if st.session_state.get("current_tab_index", 0) == 0:
        return

    if not files:
        return

    st.markdown(f"##### 👁️ {title}")
    for file_obj in files:
        render_uploaded_file_preview(file_obj)

def render_caso_especial(row):
    tipo = __s(row.get("Tipo_Envio", ""))
    is_dev = (tipo == "🔁 Devolución")
    title = "🧾 Caso Especial – 🔁 Devolución" if is_dev else "🧾 Caso Especial – 🛠 Garantía"
    st.markdown(f"### {title}")

    vendedor = row.get("Vendedor_Registro", "") or row.get("Vendedor", "")
    id_vendedor_segment = format_id_vendedor_with_mod(row)
    hora = row.get("Hora_Registro", "")

    if is_dev:
        folio_nuevo = row.get("Folio_Factura", "")
        folio_error = row.get("Folio_Factura_Error", "")
        st.markdown(
            f"📄 **Folio Nuevo:** `{folio_nuevo or 'N/A'}`  |  "
            f"📄 **Folio Error:** `{folio_error or 'N/A'}`  |  "
            f"🧑‍💼 **Vendedor:** `{vendedor or 'N/A'}`  |  "
            f"{id_vendedor_segment}  |  "
            f"🕒 **Hora:** `{hora or 'N/A'}`"
        )
    else:
        st.markdown(
            f"📄 **Folio:** `{row.get('Folio_Factura','') or 'N/A'}`  |  "
            f"🧑‍💼 **Vendedor:** `{vendedor or 'N/A'}`  |  "
            f"{id_vendedor_segment}  |  "
            f"🕒 **Hora:** `{hora or 'N/A'}`"
        )

        num_serie = __s(row.get("Numero_Serie", ""))
        fec_compra = __s(row.get("Fecha_Compra", "")) or __s(row.get("FechaCompra", ""))
        if __has(num_serie) or __has(fec_compra):
            st.markdown("**🧷 Datos de compra y serie:**")
            st.markdown(f"- **Número de serie / lote:** `{num_serie or 'N/A'}`")
            st.markdown(f"- **Fecha de compra:** `{fec_compra or 'N/A'}`")

    st.markdown(
        f"**👤 Cliente:** {row.get('Cliente','N/A')}  |  **RFC:** {row.get('Numero_Cliente_RFC','') or 'N/A'}"
    )
    st.markdown(
        f"**Estado:** {row.get('Estado','') or 'N/A'}  |  "
        f"**Estado del Caso:** {row.get('Estado_Caso','') or 'N/A'}  |  "
        f"**Turno:** {row.get('Turno','') or 'N/A'}  |  "
        f"**Tipo Envío Original:** {row.get('Tipo_Envio_Original','') or 'N/A'}"
    )
    st.markdown(f"**📌 Seguimiento:** {row.get('Seguimiento', 'N/A')}")

    rt = __s(row.get("Refacturacion_Tipo",""))
    rs = __s(row.get("Refacturacion_Subtipo",""))
    rf = __s(row.get("Folio_Factura_Refacturada",""))
    if __has(rt) or __has(rs) or __has(rf):
        st.markdown("**♻️ Refacturación:**")
        if __has(rt): st.markdown(f"- **Tipo:** {rt}")
        if __has(rs): st.markdown(f"- **Subtipo:** {rs}")
        if __has(rf): st.markdown(f"- **Folio refacturado:** {rf}")

    if __has(row.get("Resultado_Esperado","")):
        st.markdown(f"**🎯 Resultado Esperado:** {row.get('Resultado_Esperado')}")
    if __has(row.get("Motivo_Detallado","")):
        st.markdown("**📝 Motivo / Descripción:**")
        st.info(__s(row.get("Motivo_Detallado","")))
    if __has(row.get("Material_Devuelto","")):
        st.markdown("**📦 Piezas / Material:**")
        st.info(__s(row.get("Material_Devuelto","")))
    if __has(row.get("Monto_Devuelto","")):
        st.markdown(f"**💵 Monto (dev./estimado):** {row.get('Monto_Devuelto')}")

    if __has(row.get("Area_Responsable","")) or __has(row.get("Nombre_Responsable","")):
        st.markdown(
            f"**🏢 Área Responsable:** {row.get('Area_Responsable','') or 'N/A'}  |  "
            f"**👥 Responsable del Error:** {row.get('Nombre_Responsable','') or 'N/A'}"
        )

    dir_guia = row.get("Direccion_Guia_Retorno", "")
    dir_envio = row.get("Direccion_Envio", "")
    if __has(dir_guia) or __has(dir_envio):
        st.markdown("#### 🏠 Direcciones")
        if __has(dir_guia):
            st.markdown(f"- **Guía de retorno:** {__s(dir_guia)}")
        if __has(dir_envio):
            st.markdown(f"- **Envío destino:** {__s(dir_envio)}")

    if __has(row.get("Fecha_Entrega","")) or __has(row.get("Fecha_Recepcion_Devolucion","")) or __has(row.get("Estado_Recepcion","")):
        st.markdown(
            f"**📅 Fecha Entrega/Cierre:** {row.get('Fecha_Entrega','') or 'N/A'}  |  "
            f"**📅 Recepción:** {row.get('Fecha_Recepcion_Devolucion','') or 'N/A'}  |  "
            f"**📦 Recepción:** {row.get('Estado_Recepcion','') or 'N/A'}"
        )

    nota = __s(row.get("Nota_Credito_URL",""))
    docad = __s(row.get("Documento_Adicional_URL",""))
    if __has(nota):
        if __is_url(nota):
            st.markdown(f"**🧾 Nota de Crédito:** {__link(nota, 'Nota de Crédito')}")
            add_url_preview_expander(nota, "Nota de Crédito")
        else:
            st.markdown(f"**🧾 Nota de Crédito:** {nota}")
    if __has(docad):
        if __is_url(docad):
            st.markdown(f"**📂 Documento Adicional:** {__link(docad, 'Documento Adicional')}")
            add_url_preview_expander(docad, "Documento Adicional")
        else:
            st.markdown(f"**📂 Documento Adicional:** {docad}")
    if __has(row.get("Comentarios_Admin_Devolucion","")):
        st.markdown("**🗒️ Comentario Administrativo:**")
        st.info(__s(row.get("Comentarios_Admin_Devolucion","")))

    mod_txt = __s(row.get("Modificacion_Surtido",""))
    adj_mod_raw = row.get("Adjuntos_Surtido","")
    adj_mod = partir_urls(adj_mod_raw)
    if __has(mod_txt) or adj_mod:
        st.markdown("#### 🛠 Modificación de surtido")
        if __has(mod_txt):
            st.info(mod_txt)
        if adj_mod:
            st.markdown("**Archivos de modificación:**")
            for u in adj_mod:
                render_attachment_link(u)

    with st.expander("📎 Archivos (Adjuntos y Guía)", expanded=False):
        adj_raw = row.get("Adjuntos","")
        adj = partir_urls(adj_raw)
        guia = __s(row.get("Hoja_Ruta_Mensajero","")) or __s(row.get("Adjuntos_Guia",""))
        has_any = False
        if adj:
            has_any = True
            st.markdown("**Adjuntos:**")
            for u in adj:
                render_attachment_link(u)
        if __has(guia) and __is_url(guia):
            has_any = True
            st.markdown("**Guía:**")
            render_attachment_link(guia, "Abrir guía")
        if not has_any:
            st.info("Sin archivos registrados en la hoja.")
    st.markdown("---")

# --- Initialize Gspread Client and S3 Client ---
s3_client = get_s3_client()  # Initialize S3 client

# Removed the old try-except block for client initialization

# --- Tab Definition ---
tabs_labels = [
    "🛒 Registrar Nuevo Pedido",
    "✏️ Modificar Pedido Existente",
    "🧾 Pedidos Pendientes de Comprobante",
    "🗂 Casos Especiales",
    "📦 Guías Cargadas",
    "⏳ Pedidos No Entregados",
    "⬇️ Descargar Datos",
    "🔍 Buscar Pedido"
]

# Leer índice de pestaña desde los parámetros de la URL
try:
    default_tab = int(st.query_params.get("tab", ["0"])[0])
except (TypeError, ValueError):
    default_tab = 0

if tabs_labels:
    default_tab = max(0, min(len(tabs_labels) - 1, default_tab))
else:
    default_tab = 0

st.session_state.setdefault("current_tab_index", default_tab)

# Crear pestañas y mantener referencia
tabs = st.tabs(tabs_labels)

components.html(
    f"""
    <script>
    (function() {{
        const desiredIndex = {default_tab};
        const parentWindow = window.parent;
        const parentDocument = parentWindow.document;

        function updateQueryParam(index) {{
            try {{
                const url = new URL(parentWindow.location.href);
                url.searchParams.set('tab', index);
                parentWindow.history.replaceState(null, '', url.toString());
            }} catch (error) {{
                console.error('Error syncing tab param', error);
            }}
        }}

        function attachListeners() {{
            const buttons = parentDocument.querySelectorAll('div[data-baseweb="tab-list"] button');
            if (!buttons || !buttons.length) {{
                setTimeout(attachListeners, 100);
                return;
            }}

            buttons.forEach((button, index) => {{
                if (button.getAttribute('data-tab-listener') === 'true') {{
                    return;
                }}
                button.setAttribute('data-tab-listener', 'true');
                button.addEventListener('click', () => updateQueryParam(index));
            }});

            if (desiredIndex >= 0 && desiredIndex < buttons.length) {{
                const targetButton = buttons[desiredIndex];
                if (targetButton && targetButton.getAttribute('aria-selected') !== 'true') {{
                    targetButton.click();
                }}
            }}
        }}

        if (document.readyState === 'complete') {{
            attachListeners();
        }} else {{
            window.addEventListener('load', attachListeners);
            document.addEventListener('DOMContentLoaded', attachListeners);
            setTimeout(attachListeners, 500);
        }}
    }})();
    </script>
    """,
    height=0,
)
tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = tabs

# --- List of Vendors (reusable and explicitly alphabetically sorted) ---
VENDEDORES_LIST = sorted([
    "ADAMARIS",
    "ALEJANDRO RODRIGUEZ",
    "ANA KAREN ORTEGA MAHUAD",
    "CURSOS Y EVENTOS",
    "DANIELA LOPEZ RAMIREZ",
    "DISTRIBUCION Y UNIVERSIDADES",
    "EDGAR ORLANDO GOMEZ VILLAGRAN",
    "GLORIA MICHELLE GARCIA TORRES",
    "GRISELDA CAROLINA SANCHEZ GARCIA",
    "HECTOR DEL ANGEL AREVALO ALCALA",
    "JUAN CASTILLEJO",
    "NORA ALEJANDRA MARTINEZ MORENO",
    "PAULINA TREJO",
    "ROBERTO LEGRA"
])

# Initialize session state for vendor
if 'last_selected_vendedor' not in st.session_state:
    st.session_state.last_selected_vendedor = VENDEDORES_LIST[0] if VENDEDORES_LIST else ""

# --- TAB 1: REGISTER NEW ORDER ---
with tab1:
    tab1_is_active = default_tab == 0
    if tab1_is_active:
        st.session_state["current_tab_index"] = 0
    st.header("📝 Nuevo Pedido")
    tipo_envio = st.selectbox(
        "📦 Tipo de Envío",
        [
            "🚚 Pedido Foráneo",
            "🏙️ Pedido CDMX",
            "📋 Solicitudes de Guía",
            "📍 Pedido Local",
            "🎓 Cursos y Eventos",
            "🔁 Devolución",
            "🛠 Garantía",
        ],
        index=0,
        key="tipo_envio_selector_global",
    )

    subtipo_local = ""
    if tipo_envio == "📍 Pedido Local":
        st.markdown("---")
        st.subheader("⏰ Detalle de Pedido Local")
        subtipo_local = st.selectbox(
            "Turno/Locales",
            ["☀️ Local Mañana", "🌙 Local Tarde", "🌵 Saltillo", "📦 Pasa a Bodega"],
            index=0,
            key="subtipo_local_selector",
            help="Selecciona el turno o tipo de entrega para pedidos locales."
        )

    registrar_nota_venta = st.checkbox(
        "🧾 Registrar nota de venta",
        key="registrar_nota_venta_checkbox",
        help="Activa para capturar los datos de una nota de venta.",
    )

    # -------------------------------
    # Inicialización de variables
    # -------------------------------
    vendedor = ""
    registro_cliente = ""
    numero_cliente_rfc = ""
    nota_venta = ""
    motivo_nota_venta = ""
    folio_factura_input_value = ""
    folio_factura = ""
    folio_factura_error = ""  # 🆕 NUEVO para devoluciones
    fecha_entrega = datetime.now().date()
    comentario = ""
    uploaded_files = []

    # Variables Devolución
    tipo_envio_original = ""
    estatus_origen_factura = ""
    resultado_esperado = ""
    material_devuelto = ""
    motivo_detallado = ""
    area_responsable = ""
    nombre_responsable = ""
    monto_devuelto = 0.0
    comprobante_cliente = None

    # Variables Garantía
    g_resultado_esperado = ""
    g_descripcion_falla = ""
    g_piezas_afectadas = ""
    g_monto_estimado = 0.0
    g_area_responsable = ""
    g_nombre_responsable = ""
    g_numero_serie = ""
    g_fecha_compra = None
    direccion_guia_retorno = ""
    direccion_envio_destino = ""

    # -------------------------------
    # --- FORMULARIO PRINCIPAL ---
    # -------------------------------
    with st.form(key="new_pedido_form", clear_on_submit=True):
        st.markdown("---")
        st.subheader("Información Básica del Cliente y Pedido")

        try:
            initial_vendedor_index = VENDEDORES_LIST.index(st.session_state.last_selected_vendedor)
        except Exception:
            initial_vendedor_index = 0

        vendedor = st.selectbox("👤 Vendedor", VENDEDORES_LIST, index=initial_vendedor_index)
        if vendedor != st.session_state.get("last_selected_vendedor", None):
            st.session_state.last_selected_vendedor = vendedor

        registro_cliente = st.text_input("🤝 Cliente", key="registro_cliente")

        # Número de cliente / RFC para Casos Especiales (Devolución y Garantía)
        if tipo_envio in ["🔁 Devolución", "🛠 Garantía"]:
            numero_cliente_rfc = st.text_input("🆔 Número de Cliente o RFC (opcional)", key="numero_cliente_rfc")

        # Tipo de Envío Original (solo Devolución)
        if tipo_envio == "🔁 Devolución":
            tipo_envio_original = st.selectbox(
                "📦 Tipo de Envío Original",
                ["📍 Local", "🚚 Foráneo"],
                index=0,
                key="tipo_envio_original",
                help="Selecciona el tipo de envío del pedido que se va a devolver."
            )

            estatus_origen_factura = st.selectbox(
                "📊 Estatus de Factura Origen",
                ["Pagado", "Crédito", "Otro"],
                index=0,
                key="estatus_factura_origen",
                help="Indica el estatus de la factura original asociada al pedido devuelto."
            )

            # 🆕 NUEVO: Folio Error arriba del folio normal
            folio_factura_error = st.text_input(
                "📄 Folio Error (factura equivocada, si aplica)",
                key="folio_factura_error_input"
            )

        if registrar_nota_venta:
            nota_venta = st.text_input(
                "🧾 Nota de Venta",
                key="nota_venta_input",
                help="Ingresa el número de nota de venta si aplica. Se guardará en la misma columna que el folio.",
            )
            motivo_nota_venta = st.text_area(
                "✏️ Motivo de nota de venta",
                key="motivo_nota_venta_input",
                help="Describe el motivo de la nota de venta, si se registró una.",
            )
            st.session_state.pop("folio_factura_input", None)
        else:
            # Folio normal (renombrado a 'Folio Nuevo' en devoluciones)
            folio_label = "📄 Folio Nuevo" if tipo_envio == "🔁 Devolución" else "📄 Folio de Factura"
            folio_factura_input_value = st.text_input(folio_label, key="folio_factura_input")

        # Campos de pedido normal (no Casos Especiales)
        if tipo_envio not in ["🔁 Devolución", "🛠 Garantía"]:
            fecha_entrega = st.date_input(
                "🗓 Fecha de Entrega Requerida",
                value=datetime.now().date(),
                key="fecha_entrega_input",
            )

        comentario = st.text_area(
            "💬 Comentario / Descripción Detallada",
            key="comentario_detallado",
        )

        if tipo_envio == "🚚 Pedido Foráneo":
            direccion_guia_retorno = st.text_area(
                "📬 Dirección para guía de retorno",
                key="direccion_guia_retorno_foraneo",
            )

        # --- Campos adicionales para Devolución ---
        if tipo_envio == "🔁 Devolución":
            st.markdown("### 🔁 Información de Devolución")

            resultado_esperado = st.selectbox(
                "🎯 Resultado Esperado",
                ["Cambio de Producto", "Devolución de Dinero", "Saldo a Favor"],
                key="resultado_esperado"
            )

            material_devuelto = st.text_area(
                "📦 Material a Devolver (códigos, descripciones, cantidades y monto individual con IVA)",
                key="material_devuelto"
            )

            monto_devuelto = st.number_input(
                "💲 Total de Materiales a Devolver (con IVA)",
                min_value=0.0,
                format="%.2f",
                key="monto_devuelto"
            )

            area_responsable = st.selectbox(
                "🏷 Área Responsable del Error",
                ["Vendedor", "Almacén", "Cliente", "Proveedor", "Otro"],
                key="area_responsable"
            )

            if area_responsable in ["Vendedor", "Almacén"]:
                nombre_responsable = st.text_input("👤 Nombre del Empleado Responsable", key="nombre_responsable")
            else:
                nombre_responsable = "No aplica"

            motivo_detallado = st.text_area("📝 Explicación Detallada del Caso", key="motivo_detallado")

        # --- Campos adicionales para Garantía ---
        if tipo_envio == "🛠 Garantía":
            st.markdown("### 🛠 Información de Garantía")

            g_resultado_esperado = st.selectbox(
                "🎯 Resultado Esperado",
                ["Reparación", "Cambio por Garantía", "Nota de Crédito"],
                key="g_resultado_esperado"
            )

            g_descripcion_falla = st.text_area(
                "🧩 Descripción de la Falla (detallada)",
                key="g_descripcion_falla"
            )

            g_piezas_afectadas = st.text_area(
                "🧰 Piezas/Material afectado (códigos, descripciones, cantidades y monto individual con IVA si aplica)",
                key="g_piezas_afectadas"
            )

            g_monto_estimado = st.number_input(
                "💲 Monto estimado de atención (con IVA, opcional)",
                min_value=0.0,
                format="%.2f",
                key="g_monto_estimado"
            )

            g_area_responsable = st.selectbox(
                "🏷 Área posiblemente responsable",
                ["Vendedor", "Almacén", "Cliente", "Proveedor", "Otro"],
                key="g_area_responsable"
            )

            if g_area_responsable in ["Vendedor", "Almacén"]:
                g_nombre_responsable = st.text_input("👤 Nombre del Empleado Responsable", key="g_nombre_responsable")
            else:
                g_nombre_responsable = "No aplica"

            col_g1, col_g2 = st.columns(2)
            with col_g1:
                g_numero_serie = st.text_input("🔢 Número de serie / lote (opcional)", key="g_numero_serie")
            with col_g2:
                g_fecha_compra = st.date_input("🗓 Fecha de compra (opcional)", value=None, key="g_fecha_compra")

        if tipo_envio in ["🔁 Devolución", "🛠 Garantía"]:
            st.markdown("### 🏠 Direcciones")
            direccion_guia_retorno = st.text_area(
                "📬 Dirección para guía de retorno",
                key="direccion_guia_retorno",
            )
            direccion_envio_destino = st.text_area(
                "📦 Dirección de envío destino",
                key="direccion_envio_destino",
            )

        st.markdown("---")
        st.subheader("📎 Adjuntos del Pedido")
        uploaded_files = st.file_uploader(
            "Sube archivos del pedido",
            type=["pdf", "jpg", "jpeg", "png", "xlsx", "docx"],
            accept_multiple_files=True,
            key="pedido_adjuntos",
        )
        render_uploaded_files_preview("Archivos del pedido seleccionados", uploaded_files)

        # --- Evidencias/Comprobantes PARA DEVOLUCIONES y GARANTÍAS ---
        if tipo_envio in ["🔁 Devolución", "🛠 Garantía"]:
            st.markdown("---")
            st.subheader("📎 Evidencias / Comprobantes del Caso")
            comprobante_cliente = st.file_uploader(
                "Sube evidencia(s) del caso (comprobantes, fotos, PDF, etc.)",
                type=["pdf", "jpg", "jpeg", "png"],
                accept_multiple_files=True,
                key="comprobante_cliente",
                help="Sube archivos relacionados con esta devolución o garantía"
            )
            render_uploaded_files_preview("Evidencias seleccionadas", comprobante_cliente)

        # AL FINAL DEL FORMULARIO: botón submit
        submit_button = st.form_submit_button("✅ Registrar Pedido")

    should_process_submission = submit_button

    if not registrar_nota_venta:
        nota_venta = ""
        motivo_nota_venta = ""

    folio_factura = (
        nota_venta.strip() if registrar_nota_venta and isinstance(nota_venta, str) else ""
    )
    if not folio_factura:
        folio_factura = (
            folio_factura_input_value.strip()
            if isinstance(folio_factura_input_value, str)
            else ""
        )
    motivo_nota_venta = (
        motivo_nota_venta.strip()
        if registrar_nota_venta and isinstance(motivo_nota_venta, str)
        else ""
    )

    message_container = st.container()

    with message_container:
        status_data = st.session_state.get("pedido_submission_status")
        if status_data:
            status = status_data.get("status")
            detail = status_data.get("detail")
            attachments = status_data.get("attachments") or []

            if status == "success":
                st.success(status_data.get("message", "✅ Pedido registrado correctamente."))
                if attachments:
                    st.info("📎 Archivos subidos: " + ", ".join(os.path.basename(url) for url in attachments))
                if detail:
                    st.write(detail)
                if status_data.get("missing_attachments_warning"):
                    st.warning("⚠️ Pedido registrado sin archivos adjuntos.")
            else:
                error_message = status_data.get("message", "❌ Falla al subir el pedido.")
                if detail:
                    error_message = f"{error_message}\n\n🔍 Detalle: {detail}"
                st.error(error_message)

            if st.button("Aceptar", key="acknowledge_pedido_status"):
                preserved_keys = {
                    key: st.session_state[key]
                    for key in [
                        "id_vendedor",
                        "last_selected_vendedor",
                        "tipo_envio_selector_global",
                    ]
                    if key in st.session_state
                }

                keys_to_remove = [
                    key for key in list(st.session_state.keys()) if key not in preserved_keys
                ]
                for key in keys_to_remove:
                    del st.session_state[key]

                for key, value in preserved_keys.items():
                    if key not in st.session_state:
                        st.session_state[key] = value

                clear_app_caches()
                st.session_state.pop("pedido_submission_status", None)
                st.rerun()

    # -------------------------------
    # SECCIÓN DE ESTADO DE PAGO (FUERA DEL FORM) - sin cambios
    # -------------------------------
    comprobante_pago_files = []
    fecha_pago = None
    forma_pago = ""
    terminal = ""
    banco_destino = ""
    monto_pago = 0.0
    referencia_pago = ""
    pago_doble = False
    pago_triple = False
    estado_pago = "🔴 No Pagado"  # Valor por defecto

    if tipo_envio in ["🚚 Pedido Foráneo", "🏙️ Pedido CDMX", "📍 Pedido Local"]:
        st.markdown("---")
        st.subheader("💰 Estado de Pago")
        estado_pago = st.selectbox("Estado de Pago", ["🔴 No Pagado", "✅ Pagado", "💳 CREDITO"], index=0, key="estado_pago")

        if estado_pago == "✅ Pagado":
            col_pago_doble, col_pago_triple = st.columns([1, 1])
            with col_pago_doble:
                pago_doble = st.checkbox("✅ Pago en dos partes distintas", key="chk_doble")
            with col_pago_triple:
                pago_triple = st.checkbox("✅ Pago en tres partes distintas", key="chk_triple")

            # --- Un solo comprobante ---
            if not pago_doble and not pago_triple:
                comprobante_pago_files = st.file_uploader(
                    "💲 Comprobante(s) de Pago",
                    type=["pdf", "jpg", "jpeg", "png"],
                    accept_multiple_files=True,
                    key="comprobante_uploader_final"
                )
                st.info("⚠️ El comprobante es obligatorio si el estado es 'Pagado'.")
                render_uploaded_files_preview("Comprobantes de pago seleccionados", comprobante_pago_files)

                with st.expander("🧾 Detalles del Pago (opcional)"):
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        fecha_pago = st.date_input("📅 Fecha del Pago", value=datetime.today().date(), key="fecha_pago_input")
                    with col2:
                        forma_pago = st.selectbox("💳 Forma de Pago", [
                            "Transferencia", "Depósito en Efectivo", "Tarjeta de Débito", "Tarjeta de Crédito", "Cheque"
                        ], key="forma_pago_input")
                    with col3:
                        monto_pago = st.number_input("💲 Monto del Pago", min_value=0.0, format="%.2f", key="monto_pago_input")

                    col4, col5 = st.columns(2)
                    with col4:
                        if forma_pago in ["Tarjeta de Débito", "Tarjeta de Crédito"]:
                            terminal = st.selectbox(
                                "🏧 Terminal",
                                [
                                    "BANORTE",
                                    "AFIRME",
                                    "VELPAY",
                                    "CLIP",
                                    "PAYPAL",
                                    "BBVA",
                                    "CONEKTA",
                                    "MERCADO PAGO",
                                ],
                                key="terminal_input",
                            )
                            banco_destino = ""
                        else:
                            banco_destino = st.selectbox("🏦 Banco Destino", ["BANORTE", "BANAMEX", "AFIRME", "BANCOMER OP", "BANCOMER CURSOS"], key="banco_destino_input")
                            terminal = ""
                    with col5:
                        referencia_pago = st.text_input("🔢 Referencia (opcional)", key="referencia_pago_input")

            # --- Dos comprobantes ---
            elif pago_doble:
                st.markdown("### 1️⃣ Primer Pago")
                comp1 = st.file_uploader("💳 Comprobante 1", type=["pdf", "jpg", "jpeg", "png"], accept_multiple_files=True, key="cp_pago1")
                render_uploaded_files_preview("Comprobantes del primer pago", comp1)
                fecha1 = st.date_input("📅 Fecha 1", value=datetime.today().date(), key="fecha_pago1")
                forma1 = st.selectbox("💳 Forma 1", ["Transferencia", "Depósito en Efectivo", "Tarjeta de Débito", "Tarjeta de Crédito", "Cheque"], key="forma_pago1")
                monto1 = st.number_input("💲 Monto 1", min_value=0.0, format="%.2f", key="monto_pago1")
                terminal1 = banco1 = ""
                if forma1 in ["Tarjeta de Débito", "Tarjeta de Crédito"]:
                    terminal1 = st.selectbox(
                        "🏧 Terminal 1",
                        [
                            "BANORTE",
                            "AFIRME",
                            "VELPAY",
                            "CLIP",
                            "PAYPAL",
                            "BBVA",
                            "CONEKTA",
                            "MERCADO PAGO",
                        ],
                        key="terminal1",
                    )
                else:
                    banco1 = st.selectbox("🏦 Banco 1", ["BANORTE", "BANAMEX", "AFIRME", "BANCOMER OP", "BANCOMER CURSOS"], key="banco1")
                ref1 = st.text_input("🔢 Referencia 1", key="ref1")

                st.markdown("### 2️⃣ Segundo Pago")
                comp2 = st.file_uploader("💳 Comprobante 2", type=["pdf", "jpg", "jpeg", "png"], accept_multiple_files=True, key="cp_pago2")
                render_uploaded_files_preview("Comprobantes del segundo pago", comp2)
                fecha2 = st.date_input("📅 Fecha 2", value=datetime.today().date(), key="fecha_pago2")
                forma2 = st.selectbox("💳 Forma 2", ["Transferencia", "Depósito en Efectivo", "Tarjeta de Débito", "Tarjeta de Crédito", "Cheque"], key="forma_pago2")
                monto2 = st.number_input("💲 Monto 2", min_value=0.0, format="%.2f", key="monto_pago2")
                terminal2 = banco2 = ""
                if forma2 in ["Tarjeta de Débito", "Tarjeta de Crédito"]:
                    terminal2 = st.selectbox(
                        "🏧 Terminal 2",
                        [
                            "BANORTE",
                            "AFIRME",
                            "VELPAY",
                            "CLIP",
                            "PAYPAL",
                            "BBVA",
                            "CONEKTA",
                            "MERCADO PAGO",
                        ],
                        key="terminal2",
                    )
                else:
                    banco2 = st.selectbox("🏦 Banco 2", ["BANORTE", "BANAMEX", "AFIRME", "BANCOMER OP", "BANCOMER CURSOS"], key="banco2")
                ref2 = st.text_input("🔢 Referencia 2", key="ref2")

                comprobante_pago_files = (comp1 or []) + (comp2 or [])
                fecha_pago = f"{fecha1.strftime('%Y-%m-%d')} y {fecha2.strftime('%Y-%m-%d')}"
                forma_pago = f"{forma1}, {forma2}"
                terminal = f"{terminal1}, {terminal2}" if forma1.startswith("Tarjeta") or forma2.startswith("Tarjeta") else ""
                banco_destino = f"{banco1}, {banco2}" if forma1 not in ["Tarjeta de Débito", "Tarjeta de Crédito"] or forma2 not in ["Tarjeta de Débito", "Tarjeta de Crédito"] else ""
                monto_pago = monto1 + monto2
                referencia_pago = f"{ref1}, {ref2}"

            # --- Tres comprobantes ---
            elif pago_triple:
                st.markdown("### 1️⃣ Primer Pago")
                comp1 = st.file_uploader("💳 Comprobante 1", type=["pdf", "jpg", "jpeg", "png"], accept_multiple_files=True, key="cp_pago1")
                render_uploaded_files_preview("Comprobantes del primer pago", comp1)
                fecha1 = st.date_input("📅 Fecha 1", value=datetime.today().date(), key="fecha_pago1")
                forma1 = st.selectbox("💳 Forma 1", ["Transferencia", "Depósito en Efectivo", "Tarjeta de Débito", "Tarjeta de Crédito", "Cheque"], key="forma_pago1")
                monto1 = st.number_input("💲 Monto 1", min_value=0.0, format="%.2f", key="monto_pago1")
                terminal1 = banco1 = ""
                if forma1 in ["Tarjeta de Débito", "Tarjeta de Crédito"]:
                    terminal1 = st.selectbox(
                        "🏧 Terminal 1",
                        [
                            "BANORTE",
                            "AFIRME",
                            "VELPAY",
                            "CLIP",
                            "PAYPAL",
                            "BBVA",
                            "CONEKTA",
                            "MERCADO PAGO",
                        ],
                        key="terminal1",
                    )
                else:
                    banco1 = st.selectbox("🏦 Banco 1", ["BANORTE", "BANAMEX", "AFIRME", "BANCOMER OP", "BANCOMER CURSOS"], key="banco1")
                ref1 = st.text_input("🔢 Referencia 1", key="ref1")

                st.markdown("### 2️⃣ Segundo Pago")
                comp2 = st.file_uploader("💳 Comprobante 2", type=["pdf", "jpg", "jpeg", "png"], accept_multiple_files=True, key="cp_pago2")
                render_uploaded_files_preview("Comprobantes del segundo pago", comp2)
                fecha2 = st.date_input("📅 Fecha 2", value=datetime.today().date(), key="fecha_pago2")
                forma2 = st.selectbox("💳 Forma 2", ["Transferencia", "Depósito en Efectivo", "Tarjeta de Débito", "Tarjeta de Crédito", "Cheque"], key="forma_pago2")
                monto2 = st.number_input("💲 Monto 2", min_value=0.0, format="%.2f", key="monto_pago2")
                terminal2 = banco2 = ""
                if forma2 in ["Tarjeta de Débito", "Tarjeta de Crédito"]:
                    terminal2 = st.selectbox(
                        "🏧 Terminal 2",
                        [
                            "BANORTE",
                            "AFIRME",
                            "VELPAY",
                            "CLIP",
                            "PAYPAL",
                            "BBVA",
                            "CONEKTA",
                            "MERCADO PAGO",
                        ],
                        key="terminal2",
                    )
                else:
                    banco2 = st.selectbox("🏦 Banco 2", ["BANORTE", "BANAMEX", "AFIRME", "BANCOMER OP", "BANCOMER CURSOS"], key="banco2")
                ref2 = st.text_input("🔢 Referencia 2", key="ref2")

                st.markdown("### 3️⃣ Tercer Pago")
                comp3 = st.file_uploader("💳 Comprobante 3", type=["pdf", "jpg", "jpeg", "png"], accept_multiple_files=True, key="cp_pago3")
                render_uploaded_files_preview("Comprobantes del tercer pago", comp3)
                fecha3 = st.date_input("📅 Fecha 3", value=datetime.today().date(), key="fecha_pago3")
                forma3 = st.selectbox("💳 Forma 3", ["Transferencia", "Depósito en Efectivo", "Tarjeta de Débito", "Tarjeta de Crédito", "Cheque"], key="forma_pago3")
                monto3 = st.number_input("💲 Monto 3", min_value=0.0, format="%.2f", key="monto_pago3")
                terminal3 = banco3 = ""
                if forma3 in ["Tarjeta de Débito", "Tarjeta de Crédito"]:
                    terminal3 = st.selectbox(
                        "🏧 Terminal 3",
                        [
                            "BANORTE",
                            "AFIRME",
                            "VELPAY",
                            "CLIP",
                            "PAYPAL",
                            "BBVA",
                            "CONEKTA",
                            "MERCADO PAGO",
                        ],
                        key="terminal3",
                    )
                else:
                    banco3 = st.selectbox("🏦 Banco 3", ["BANORTE", "BANAMEX", "AFIRME", "BANCOMER OP", "BANCOMER CURSOS"], key="banco3")
                ref3 = st.text_input("🔢 Referencia 3", key="ref3")

                comprobante_pago_files = (comp1 or []) + (comp2 or []) + (comp3 or [])
                fecha_pago = f"{fecha1.strftime('%Y-%m-%d')}, {fecha2.strftime('%Y-%m-%d')} y {fecha3.strftime('%Y-%m-%d')}"
                forma_pago = f"{forma1}, {forma2}, {forma3}"
                terminal = ", ".join(filter(None, [terminal1, terminal2, terminal3]))
                banco_destino = ", ".join(filter(None, [banco1, banco2, banco3]))
                monto_pago = monto1 + monto2 + monto3
                referencia_pago = f"{ref1}, {ref2}, {ref3}"

    # -------------------------------
    # Registro del Pedido
    # -------------------------------
    if should_process_submission:
        st.session_state.pop("pedido_submission_status", None)
        try:
            if not vendedor or not registro_cliente:
                st.warning("⚠️ Completa los campos obligatorios.")
                st.stop()

            pedido_sin_adjuntos = not uploaded_files

            # Normalización de campos para Casos Especiales
            if tipo_envio == "🔁 Devolución":
                resultado_esperado = normalize_case_text(resultado_esperado)
                material_devuelto = normalize_case_text(material_devuelto)
                motivo_detallado = normalize_case_text(motivo_detallado)
                nombre_responsable = normalize_case_text(nombre_responsable)
            if tipo_envio == "🛠 Garantía":
                g_resultado_esperado = normalize_case_text(g_resultado_esperado)
                g_descripcion_falla = normalize_case_text(g_descripcion_falla)
                g_piezas_afectadas = normalize_case_text(g_piezas_afectadas)
                g_nombre_responsable = normalize_case_text(g_nombre_responsable)
                g_numero_serie = normalize_case_text(g_numero_serie)
            if tipo_envio in ["🔁 Devolución", "🛠 Garantía"]:
                direccion_guia_retorno = normalize_case_text(direccion_guia_retorno)
                direccion_envio_destino = normalize_case_text(direccion_envio_destino)

            # Validar comprobante de pago para tipos normales
            if tipo_envio in [
                "🚚 Pedido Foráneo",
                "🏙️ Pedido CDMX",
                "📍 Pedido Local",
                "🎓 Cursos y Eventos",
            ] and estado_pago == "✅ Pagado" and not comprobante_pago_files:
                st.warning("⚠️ Suba un comprobante si el pedido está marcado como pagado.")
                st.stop()

            # Acceso a la hoja
            headers = []
            try:
                if tipo_envio in ["🔁 Devolución", "🛠 Garantía"]:
                    worksheet = get_worksheet_casos_especiales()
                    if worksheet is None:
                        set_pedido_submission_status(
                            "error",
                            "❌ Falla al subir el pedido.",
                            "No fue posible acceder a la hoja de casos especiales.",
                        )
                        st.rerun()

                    headers = worksheet.row_values(1)
                    required_headers = ["Direccion_Guia_Retorno", "Direccion_Envio", "Estatus_OrigenF"]
                    missing_headers = [col for col in required_headers if col not in headers]
                    if missing_headers:
                        try:
                            new_headers = headers + missing_headers
                            worksheet.update('A1', [new_headers])
                            get_sheet_headers.clear()
                            headers = worksheet.row_values(1)
                        except Exception as header_error:
                            set_pedido_submission_status(
                                "error",
                                "❌ Falla al subir el pedido.",
                                f"No se pudieron preparar las columnas de direcciones: {header_error}",
                            )
                            st.rerun()
                else:
                    worksheet = get_worksheet()
                    if worksheet is None:
                        set_pedido_submission_status(
                            "error",
                            "❌ Falla al subir el pedido.",
                            "No fue posible acceder a la hoja de pedidos.",
                        )
                        st.rerun()
                    headers = worksheet.row_values(1)
                    required_headers = []
                    if tipo_envio == "🚚 Pedido Foráneo":
                        required_headers.append("Direccion_Guia_Retorno")
                    if required_headers:
                        missing_headers = [col for col in required_headers if col not in headers]
                        if missing_headers:
                            try:
                                new_headers = headers + missing_headers
                                worksheet.update('A1', [new_headers])
                                get_sheet_headers.clear()
                                headers = worksheet.row_values(1)
                            except Exception as header_error:
                                set_pedido_submission_status(
                                    "error",
                                    "❌ Falla al subir el pedido.",
                                    f"No se pudieron preparar las columnas de direcciones: {header_error}",
                                )
                                st.rerun()

                if not headers:
                    set_pedido_submission_status(
                        "error",
                        "❌ Falla al subir el pedido.",
                        "La hoja de cálculo está vacía.",
                    )
                    st.rerun()

                # Hora local de CDMX para ID y Hora_Registro
                zona_mexico = timezone("America/Mexico_City")
                now = datetime.now(zona_mexico)
                id_pedido = f"PED-{now.strftime('%Y%m%d%H%M%S')}-{str(uuid.uuid4())[:4].upper()}"
                hora_registro = now.strftime('%Y-%m-%d %H:%M:%S')

            except gspread.exceptions.APIError as e:
                if "RESOURCE_EXHAUSTED" in str(e):
                    st.warning("⚠️ Cuota de Google Sheets alcanzada. Reintentando...")
                    st.cache_resource.clear()
                    time.sleep(6)
                    st.rerun()
                else:
                    set_pedido_submission_status(
                        "error",
                        "❌ Falla al subir el pedido.",
                        f"Error al acceder a Google Sheets: {e}",
                    )
                    st.rerun()

            # Subida de adjuntos (pedido + pagos + evidencias)
            adjuntos_urls = []

            if uploaded_files:
                for file in uploaded_files:
                    ext = os.path.splitext(file.name)[1]
                    s3_key = f"{id_pedido}/{file.name.replace(' ', '_').replace(ext, '')}_{uuid.uuid4().hex[:4]}{ext}"
                    success, url, error_msg = upload_file_to_s3(s3_client, S3_BUCKET_NAME, file, s3_key)
                    if success:
                        adjuntos_urls.append(url)
                    else:
                        set_pedido_submission_status(
                            "error",
                            "❌ Falla al subir el pedido.",
                            f"No se pudo subir {file.name} a S3: {error_msg or 'Error desconocido'}",
                        )
                        st.rerun()

            if comprobante_pago_files:
                for archivo in comprobante_pago_files:
                    ext_cp = os.path.splitext(archivo.name)[1]
                    s3_key_cp = f"{id_pedido}/comprobante_{id_pedido}_{now.strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:4]}{ext_cp}"
                    success_cp, url_cp, error_msg_cp = upload_file_to_s3(s3_client, S3_BUCKET_NAME, archivo, s3_key_cp)
                    if success_cp:
                        adjuntos_urls.append(url_cp)
                    else:
                        set_pedido_submission_status(
                            "error",
                            "❌ Falla al subir el pedido.",
                            f"No se pudo subir {archivo.name} a S3: {error_msg_cp or 'Error desconocido'}",
                        )
                        st.rerun()

            # Evidencias de Casos Especiales (Devolución/Garantía)
            if comprobante_cliente:
                for archivo_cc in comprobante_cliente:
                    ext_cc = os.path.splitext(archivo_cc.name)[1]
                    s3_key_cc = f"{id_pedido}/evidencia_{id_pedido}_{now.strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:4]}{ext_cc}"
                    success_cc, url_cc, error_msg_cc = upload_file_to_s3(s3_client, S3_BUCKET_NAME, archivo_cc, s3_key_cc)
                    if success_cc:
                        adjuntos_urls.append(url_cc)
                    else:
                        set_pedido_submission_status(
                            "error",
                            "❌ Falla al subir el pedido.",
                            f"No se pudo subir {archivo_cc.name} a S3: {error_msg_cc or 'Error desconocido'}",
                        )
                        st.rerun()

            adjuntos_str = ", ".join(adjuntos_urls)

            # Mapeo de columnas a valores
            values = []
            for header in headers:
                if header == "ID_Pedido":
                    values.append(id_pedido)
                elif header == "Hora_Registro":
                    values.append(hora_registro)
                elif header.lower() == "id_vendedor":
                    values.append(st.session_state.get("id_vendedor", ""))
                elif header in ["Vendedor", "Vendedor_Registro"]:
                    values.append(vendedor)
                elif header in ["Cliente", "RegistroCliente"]:
                    values.append(registro_cliente)
                elif header == "Numero_Cliente_RFC":
                    if tipo_envio in ["🔁 Devolución", "🛠 Garantía"]:
                        values.append(numero_cliente_rfc)
                    else:
                        values.append("")
                elif header == "Folio_Factura":
                    values.append(folio_factura)  # en devoluciones es "Folio Nuevo" o Nota de Venta
                elif header == "Folio_Factura_Error":  # 🆕 mapeo adicional
                    values.append(folio_factura_error if tipo_envio == "🔁 Devolución" else "")
                elif header == "Motivo_NotaVenta":
                    values.append(motivo_nota_venta)
                elif header == "Tipo_Envio":
                    values.append(tipo_envio)
                elif header == "Tipo_Envio_Original":
                    values.append(tipo_envio_original if tipo_envio == "🔁 Devolución" else "")
                elif header == "Estatus_OrigenF":
                    values.append(estatus_origen_factura if tipo_envio == "🔁 Devolución" else "")
                elif header == "Turno":
                    values.append(subtipo_local)
                elif header == "Fecha_Entrega":
                    if tipo_envio in ["🔁 Devolución", "🛠 Garantía"]:
                        values.append("")
                    else:
                        values.append(fecha_entrega.strftime('%Y-%m-%d'))
                elif header == "Comentario":
                    values.append(comentario)
                elif header == "Adjuntos":
                    values.append(adjuntos_str)
                elif header == "Adjuntos_Surtido":
                    values.append("")
                elif header == "Estado":
                    values.append("🟡 Pendiente")
                elif header == "Estado_Pago":
                    if tipo_envio in ["🚚 Pedido Foráneo", "🏙️ Pedido CDMX", "📍 Pedido Local"]:
                        values.append(estado_pago)
                    else:
                        values.append("")
                elif header == "Fecha_Pago_Comprobante":
                    if tipo_envio in ["🚚 Pedido Foráneo", "🏙️ Pedido CDMX", "📍 Pedido Local"]:
                        values.append(fecha_pago if isinstance(fecha_pago, str) else (fecha_pago.strftime('%Y-%m-%d') if fecha_pago else ""))
                    else:
                        values.append("")
                elif header == "Forma_Pago_Comprobante":
                    if tipo_envio in ["🚚 Pedido Foráneo", "🏙️ Pedido CDMX", "📍 Pedido Local"]:
                        values.append(forma_pago)
                    else:
                        values.append("")
                elif header == "Terminal":
                    if tipo_envio in ["🚚 Pedido Foráneo", "🏙️ Pedido CDMX", "📍 Pedido Local"]:
                        values.append(terminal)
                    else:
                        values.append("")
                elif header == "Banco_Destino_Pago":
                    if tipo_envio in ["🚚 Pedido Foráneo", "🏙️ Pedido CDMX", "📍 Pedido Local"]:
                        values.append(banco_destino)
                    else:
                        values.append("")
                elif header == "Monto_Comprobante":
                    if tipo_envio in ["🚚 Pedido Foráneo", "🏙️ Pedido CDMX", "📍 Pedido Local"]:
                        values.append(f"{monto_pago:.2f}" if monto_pago > 0 else "")
                    else:
                        values.append("")
                elif header == "Referencia_Comprobante":
                    if tipo_envio in ["🚚 Pedido Foráneo", "🏙️ Pedido CDMX", "📍 Pedido Local"]:
                        values.append(referencia_pago)
                    else:
                        values.append("")
                elif header in ["Fecha_Completado", "Hora_Proceso", "Modificacion_Surtido"]:
                    values.append("")

                # -------- Campos Casos Especiales (reutilizados) --------
                elif header == "Resultado_Esperado":
                    if tipo_envio == "🔁 Devolución":
                        values.append(resultado_esperado)
                    elif tipo_envio == "🛠 Garantía":
                        values.append(g_resultado_esperado)
                    else:
                        values.append("")
                elif header == "Material_Devuelto":
                    if tipo_envio == "🔁 Devolución":
                        values.append(material_devuelto)
                    elif tipo_envio == "🛠 Garantía":
                        values.append(g_piezas_afectadas)  # Reuso columna para piezas afectadas
                    else:
                        values.append("")
                elif header == "Monto_Devuelto":
                    if tipo_envio == "🔁 Devolución":
                        values.append(normalize_case_amount(monto_devuelto))
                    elif tipo_envio == "🛠 Garantía":
                        values.append(normalize_case_amount(g_monto_estimado))
                    else:
                        values.append("")
                elif header == "Motivo_Detallado":
                    if tipo_envio == "🔁 Devolución":
                        values.append(motivo_detallado)
                    elif tipo_envio == "🛠 Garantía":
                        values.append(g_descripcion_falla)
                    else:
                        values.append("")
                elif header == "Area_Responsable":
                    if tipo_envio == "🔁 Devolución":
                        values.append(area_responsable)
                    elif tipo_envio == "🛠 Garantía":
                        values.append(g_area_responsable)
                    else:
                        values.append("")
                elif header == "Nombre_Responsable":
                    if tipo_envio == "🔁 Devolución":
                        values.append(nombre_responsable)
                    elif tipo_envio == "🛠 Garantía":
                        values.append(g_nombre_responsable)
                    else:
                        values.append("")
                elif header == "Direccion_Guia_Retorno":
                    if tipo_envio in ["🔁 Devolución", "🛠 Garantía"]:
                        values.append(direccion_guia_retorno)
                    elif tipo_envio == "🚚 Pedido Foráneo" and direccion_guia_retorno.strip():
                        values.append(direccion_guia_retorno)
                    else:
                        values.append("")
                elif header == "Direccion_Envio":
                    if tipo_envio in ["🔁 Devolución", "🛠 Garantía"]:
                        values.append(direccion_envio_destino)
                    else:
                        values.append("")
                # -------- Opcionales si existen en la hoja --------
                elif header == "Numero_Serie":
                    values.append(g_numero_serie if tipo_envio == "🛠 Garantía" else "")
                elif header in ["Fecha_Compra", "FechaCompra"]:
                    if tipo_envio == "🛠 Garantía":
                        values.append(g_fecha_compra.strftime('%Y-%m-%d') if g_fecha_compra else "")
                    else:
                        values.append("")
                else:
                    values.append("")

            exito = False
            for intento in range(3):
                try:
                    worksheet.append_row(values)
                    exito = True
                    break
                except gspread.exceptions.APIError as e:
                    if "RESOURCE_EXHAUSTED" in str(e) or (
                        hasattr(e, "response") and getattr(e.response, "status_code", None) == 429
                    ):
                        time.sleep(2 ** intento)
                    else:
                        set_pedido_submission_status(
                            "error",
                            "❌ Falla al subir el pedido.",
                            f"Error al registrar el pedido: {e}",
                        )
                        st.rerun()
                        break
            if exito:
                reset_tab1_form_state()
                id_vendedor_actual = str(st.session_state.get("id_vendedor", "")).strip()
                id_vendedor_segment = (
                    f" (ID vendedor: {id_vendedor_actual})" if id_vendedor_actual else ""
                )
                set_pedido_submission_status(
                    "success",
                    f"✅ El pedido {id_pedido}{id_vendedor_segment} fue subido correctamente.",
                    attachments=adjuntos_urls,
                    missing_attachments_warning=pedido_sin_adjuntos,
                )
                if tab1_is_active and st.session_state.get("current_tab_index") == 0:
                    st.query_params.update({"tab": "0"})
                st.rerun()
            else:
                set_pedido_submission_status(
                    "error",
                    "❌ Falla al subir el pedido.",
                    "No se pudo registrar el pedido después de varios intentos.",
                )
                st.rerun()

        except Exception as e:
            set_pedido_submission_status(
                "error",
                "❌ Falla al subir el pedido.",
                f"Error inesperado al registrar el pedido: {e}",
            )
            st.rerun()



@st.cache_data(ttl=300)
def cargar_pedidos_combinados():
    """
    Carga y unifica pedidos de 'datos_pedidos' y 'casos_especiales'.
    Devuelve un DataFrame con columna 'Fuente' indicando el origen.
    Garantiza columnas usadas por la UI (modificación de surtido, refacturación, folio error, documentos, etc.)
    y mapea Hoja_Ruta_Mensajero -> Adjuntos_Guia para homogeneizar.
    """
    client = build_gspread_client()
    sh = client.open_by_key(GOOGLE_SHEET_ID)

    # ---------------------------
    # datos_pedidos
    # ---------------------------
    try:
        ws_datos = sh.worksheet("datos_pedidos")
        df_datos, headers_datos = load_sheet_records_with_row_numbers(ws_datos)
    except Exception:
        headers_datos = []
        df_datos = pd.DataFrame()

    if not df_datos.empty:
        # quita filas totalmente vacías en claves mínimas
        claves = ['ID_Pedido', 'Cliente', 'Folio_Factura']
        df_datos = df_datos.dropna(subset=claves, how='all')
        if 'ID_Pedido' in df_datos.columns:
            df_datos = df_datos[df_datos['ID_Pedido'].astype(str).str.strip().ne("")]

        # columnas que la UI puede usar desde datos_pedidos
        needed_datos: list[str] = []
        needed_datos += [
            'ID_Pedido','Cliente','Folio_Factura','Vendedor_Registro','Estado','Hora_Registro','Turno','Fecha_Entrega',
            'Comentario','Estado_Pago','Motivo_NotaVenta',
            # archivos/adjuntos
            'Adjuntos','Adjuntos_Guia','Adjuntos_Surtido','Modificacion_Surtido',
            # refacturación
            'Refacturacion_Tipo','Refacturacion_Subtipo','Folio_Factura_Refacturada',
            # seguimiento de modificaciones
            'id_vendedor_Mod',
            # para homogeneidad con casos (puede venir vacío en datos)
            'Folio_Factura_Error','Estado_Caso','Numero_Cliente_RFC','Tipo_Envio','Tipo_Envio_Original',
            'Resultado_Esperado','Motivo_Detallado','Material_Devuelto','Monto_Devuelto',
            'Nota_Credito_URL','Documento_Adicional_URL','Comentarios_Admin_Devolucion',
            'Hoja_Ruta_Mensajero','Fecha_Recepcion_Devolucion','Hora_Proceso','Area_Responsable','Nombre_Responsable',
            'Direccion_Guia_Retorno','Direccion_Envio',
            # seguimiento
            'Seguimiento'
        ]
        for c in needed_datos:
            if c not in df_datos.columns:
                df_datos[c] = ""

        df_datos['Seguimiento'] = df_datos['Seguimiento'].fillna("")

        # asegura tipos string uniformes importantes
        for c in ['Tipo_Envio','Vendedor_Registro','Estado','Folio_Factura','Folio_Factura_Refacturada','id_vendedor_Mod']:
            if c in df_datos.columns:
                df_datos[c] = df_datos[c].astype(str)

        df_datos["Fuente"] = "datos_pedidos"

    # ---------------------------
    # casos_especiales
    # ---------------------------
    try:
        ws_casos = sh.worksheet("casos_especiales")
        df_casos, headers_casos = load_sheet_records_with_row_numbers(ws_casos)
    except Exception:
        headers_casos = []
        df_casos = pd.DataFrame()

    if not df_casos.empty:
        if 'ID_Pedido' in df_casos.columns:
            df_casos = df_casos[df_casos['ID_Pedido'].astype(str).str.strip().ne("")]
        else:
            df_casos["ID_Pedido"] = ""

        # columnas mínimas + TODAS las que usa la UI (incluye Garantías)
        base_cols = [
            'ID_Pedido','Cliente','Folio_Factura','Folio_Factura_Error','Estado','Tipo_Envio','Tipo_Envio_Original',
            'Turno','Fecha_Entrega','Hora_Registro','Hora_Proceso','Vendedor_Registro','Comentario','Estado_Pago',
            # adjuntos/guía/modificación
            'Adjuntos','Adjuntos_Guia','Hoja_Ruta_Mensajero',
            'Adjuntos_Surtido','Modificacion_Surtido',
            # cliente/estatus caso
            'Numero_Cliente_RFC','Estado_Caso',
            # refacturación
            'Refacturacion_Tipo','Refacturacion_Subtipo','Folio_Factura_Refacturada',
            # detalle del caso (dev/garantía)
            'Resultado_Esperado','Motivo_Detallado','Material_Devuelto','Monto_Devuelto',
            'Area_Responsable','Nombre_Responsable',
            'Direccion_Guia_Retorno','Direccion_Envio',
            # ⚙️ NUEVO: Garantías
            'Numero_Serie','Fecha_Compra',   # si tu hoja usa "FechaCompra", abajo lo normalizamos
            # recepción/cierre
            'Fecha_Recepcion_Devolucion','Estado_Recepcion',
            # documentos de cierre
            'Nota_Credito_URL','Documento_Adicional_URL','Comentarios_Admin_Devolucion',
            # seguimiento
            'Seguimiento'
        ]
        for c in base_cols:
            if c not in df_casos.columns:
                df_casos[c] = ""

        df_casos['Seguimiento'] = df_casos['Seguimiento'].fillna("")

        # Normalizar fecha de compra si el encabezado real es "FechaCompra"
        if 'Fecha_Compra' not in headers_casos and 'FechaCompra' in headers_casos:
            df_casos['Fecha_Compra'] = df_casos['FechaCompra']

        # Inferir Tipo_Envio desde Tipo_Caso si viene vacío
        if 'Tipo_Envio' in df_casos.columns:
            df_casos['Tipo_Envio'] = df_casos['Tipo_Envio'].astype(str)
        if 'Tipo_Envio' in df_casos.columns and df_casos['Tipo_Envio'].eq("").any():
            if 'Tipo_Caso' in df_casos.columns:
                def _infer_tipo_envio(row):
                    t_env = str(row.get("Tipo_Envio","")).strip()
                    if t_env:
                        return t_env
                    t_caso = str(row.get("Tipo_Caso","")).lower()
                    if t_caso.startswith("devol"):
                        return "🔁 Devolución"
                    if t_caso.startswith("garan"):
                        return "🛠 Garantía"
                    return "Caso especial"
                df_casos['Tipo_Envio'] = df_casos.apply(_infer_tipo_envio, axis=1)

        # Mapear Hoja_Ruta_Mensajero -> Adjuntos_Guia si esta última está vacía
        if 'Adjuntos_Guia' in df_casos.columns and 'Hoja_Ruta_Mensajero' in df_casos.columns:
            mask_vacios = df_casos['Adjuntos_Guia'].astype(str).str.strip().eq("")
            df_casos.loc[mask_vacios, 'Adjuntos_Guia'] = df_casos.loc[mask_vacios, 'Hoja_Ruta_Mensajero']

        # asegura tipos string uniformes importantes
        for c in ['Tipo_Envio','Vendedor_Registro','Estado','Folio_Factura','Folio_Factura_Error','Folio_Factura_Refacturada']:
            if c in df_casos.columns:
                df_casos[c] = df_casos[c].astype(str)

        df_casos["Fuente"] = "casos_especiales"

    # ---------------------------
    # Unir respetando columnas
    # ---------------------------
    if df_datos.empty and df_casos.empty:
        return pd.DataFrame()
    if df_datos.empty:
        return df_casos.copy()
    if df_casos.empty:
        return df_datos.copy()

    all_cols = list(set(df_datos.columns).union(set(df_casos.columns)))
    df_datos = df_datos.reindex(columns=all_cols, fill_value="")
    df_casos = df_casos.reindex(columns=all_cols, fill_value="")
    df_all = pd.concat([df_datos, df_casos], ignore_index=True)
    return df_all

            
# --- TAB 2: MODIFY EXISTING ORDER ---
reset_inputs_tab2_flag = st.session_state.pop("reset_inputs_tab2", False)
if reset_inputs_tab2_flag:
    # Limpiar entradas controladas por widgets antes de instanciarlos
    for key in [
        "new_modificacion_surtido_input",
        "uploaded_files_surtido",
        "uploaded_comprobantes_extra",
        "tipo_modificacion_mod",
        "refact_tipo_mod_outside",
        "subtipo_datos_outside",
        "subtipo_material_outside",
        "folio_refact_outside",
    ]:
        st.session_state.pop(key, None)

with tab2:
    tab2_is_active = default_tab == 1
    if tab2_is_active:
        st.session_state["current_tab_index"] = 1
    st.header("✏️ Modificar Pedido Existente")
    if st.button("🔄 Actualizar pedidos"):
        cargar_pedidos_combinados.clear()

    message_placeholder_tab2 = st.empty()

    # 🔄 Cargar pedidos combinados (datos_pedidos + casos_especiales)
    try:
        df_pedidos = cargar_pedidos_combinados()
    except Exception as e:
        message_placeholder_tab2.error(f"❌ Error al cargar pedidos para modificación: {e}")
        st.stop()

    # ----------------- Estado local -----------------
    selected_order_id = None
    selected_row_data = None
    selected_source = "datos_pedidos"  # por defecto
    current_modificacion_surtido_value = ""
    current_estado_pago_value = "🔴 No Pagado"
    current_adjuntos_list = []
    current_adjuntos_surtido_list = []

    if df_pedidos.empty:
        message_placeholder_tab2.warning("No hay pedidos registrados para modificar.")
    else:
        # 🔧 Normaliza 'Vendedor_Registro' usando 'Vendedor' como respaldo
        if 'Vendedor_Registro' not in df_pedidos.columns:
            df_pedidos['Vendedor_Registro'] = ""
        if 'Vendedor' in df_pedidos.columns:
            df_pedidos['Vendedor_Registro'] = df_pedidos['Vendedor_Registro'].astype(str).str.strip()
            fallback_v = df_pedidos['Vendedor'].astype(str).str.strip()
            df_pedidos.loc[df_pedidos['Vendedor_Registro'] == "", 'Vendedor_Registro'] = fallback_v

        # 🔽 Filtro combinado por envío (usa Turno si es Local)
        df_pedidos['Filtro_Envio_Combinado'] = df_pedidos.apply(
            lambda row: row['Turno'] if (str(row.get('Tipo_Envio',"")) == "📍 Pedido Local" and pd.notna(row.get('Turno')) and str(row.get('Turno')).strip()) else row.get('Tipo_Envio', ''),
            axis=1
        )

        # ----------------- Controles de filtro -----------------
        col1, col2 = st.columns(2)

        with col1:
            if 'Vendedor_Registro' in df_pedidos.columns:
                unique_vendedores_mod = sorted(
                    [v for v in df_pedidos['Vendedor_Registro'].dropna().astype(str).str.strip().unique().tolist()
                     if v and v.lower() not in ["none", "nan"]]
                )
                unique_vendedores_mod = ["Todos"] + unique_vendedores_mod
                selected_vendedor_mod = st.selectbox(
                    "Filtrar por Vendedor:",
                    options=unique_vendedores_mod,
                    key="vendedor_filter_mod"
                )
            else:
                selected_vendedor_mod = "Todos"

        with col2:
            (
                fecha_inicio_mod,
                fecha_fin_mod,
                _rango_activo_mod,
                rango_valido_mod,
            ) = render_date_filter_controls(
                "📅 Filtrar por Fecha de Registro:",
                "tab2_modificar_filtro",
            )

        # ----------------- Aplicar filtros -----------------
        filtered_orders = df_pedidos.copy()

        if selected_vendedor_mod != "Todos":
            filtered_orders = filtered_orders[filtered_orders['Vendedor_Registro'] == selected_vendedor_mod]

        # Filtrar por fecha usando 'Hora_Registro' si existe
        if 'Hora_Registro' in filtered_orders.columns:
            filtered_orders = filtered_orders.copy()
            filtered_orders['Hora_Registro'] = pd.to_datetime(filtered_orders['Hora_Registro'], errors='coerce')
            if rango_valido_mod:
                start_dt = datetime.combine(fecha_inicio_mod, datetime.min.time())
                end_dt = datetime.combine(fecha_fin_mod, datetime.max.time())
                filtered_orders = filtered_orders[
                    filtered_orders['Hora_Registro'].between(start_dt, end_dt)
                ]
            else:
                filtered_orders = filtered_orders.iloc[0:0]

        if filtered_orders.empty:
            if not rango_valido_mod:
                st.info("Ajusta el rango de fechas para continuar.")
            else:
                st.warning("No hay pedidos que coincidan con los filtros seleccionados.")
        else:
            # 🔧 Limpieza para evitar 'nan' en el select
            for col in ['Folio_Factura', 'ID_Pedido', 'Cliente', 'Estado', 'Tipo_Envio']:
                if col in filtered_orders.columns:
                    filtered_orders[col] = (
                        filtered_orders[col]
                        .astype(str)
                        .replace(['nan', 'None'], '')
                        .fillna('')
                        .str.strip()
                    )

            # 🧹 Orden por Fecha_Entrega (más reciente primero) si existe
            if 'Fecha_Entrega' in filtered_orders.columns:
                filtered_orders['Fecha_Entrega'] = pd.to_datetime(filtered_orders['Fecha_Entrega'], errors='coerce')
                filtered_orders = filtered_orders.sort_values(by='Fecha_Entrega', ascending=False).reset_index(drop=True)

            # 🏷️ Etiqueta de display (marca [CE] si es de casos_especiales)
            def _s(x):
                return "" if pd.isna(x) else str(x)

            filtered_orders['display_label'] = filtered_orders.apply(
                lambda row: (
                    f"📄 {(_s(row['Folio_Factura']) or 'Sin Folio')}"
                    f" | ID {(_s(row.get('ID_Pedido', '')) or 'S/I')}"
                    f" - {_s(row['Cliente'])}"
                    f" - {_s(row['Estado'])}"
                    f" - {_s(row['Tipo_Envio'])}"
                    f" {'[CE]' if row.get('Fuente','')=='casos_especiales' else ''}"
                ),
                axis=1
            )

            base_option_values = filtered_orders.apply(
                lambda row: (
                    f"{row.get('Fuente', 'datos_pedidos')}|"
                    f"{_s(row.get('ID_Pedido', '')) or 'sin_id'}"
                ),
                axis=1
            )

            filtered_orders['option_value'] = base_option_values

            duplicate_mask = filtered_orders['option_value'].duplicated(keep=False)
            if duplicate_mask.any():
                filtered_orders.loc[duplicate_mask, 'option_value'] = filtered_orders.loc[duplicate_mask].apply(
                    lambda row: f"{base_option_values[row.name]}|{row.name}",
                    axis=1
                )

            option_label_map = dict(
                zip(filtered_orders['option_value'], filtered_orders['display_label'])
            )

            def _format_option(option_key: str) -> str:
                return option_label_map.get(option_key, option_key)

            # ----------------- Selector de pedido -----------------
            selected_option_key = st.selectbox(
                "📝 Seleccionar Pedido para Modificar",
                list(option_label_map.keys()),
                format_func=_format_option,
                key="select_order_to_modify"
            )

            if selected_option_key:
                matched = filtered_orders[
                    filtered_orders['option_value'] == selected_option_key
                ].iloc[0]
                selected_order_id = matched['ID_Pedido']
                selected_source = matched.get('Fuente', 'datos_pedidos')  # 'datos_pedidos' o 'casos_especiales'

                selected_row_data = matched.copy()
                if 'Seguimiento' not in selected_row_data.index:
                    selected_row_data['Seguimiento'] = ''

                # Si viene de casos_especiales y es Devolución/Garantía -> render especial
                tipo_det = __s(selected_row_data.get('Tipo_Envio', ''))
                if selected_source == "casos_especiales" and tipo_det in ("🔁 Devolución", "🛠 Garantía"):
                    render_caso_especial(selected_row_data)
                else:
                    # ----------------- Detalles básicos (para datos_pedidos u otros) -----------------
                    st.subheader(
                        f"Detalles del Pedido: Folio {selected_row_data.get('Folio_Factura', 'N/A')} (ID {selected_order_id})"
                    )

                    fuente_display = (
                        "📄 datos_pedidos"
                        if selected_source == "datos_pedidos"
                        else "🔁 casos_especiales"
                    )
                    vendedor_preferido = selected_row_data.get("Vendedor", "")
                    if not vendedor_preferido or str(vendedor_preferido).strip().lower() in {"nan", "none"}:
                        vendedor_preferido = selected_row_data.get(
                            "Vendedor_Registro", "No especificado"
                        )
                    vendedor_display = normalize_case_text(
                        vendedor_preferido, "No especificado"
                    )
                    tipo_envio_val = selected_row_data.get("Tipo_Envio", "N/A")
                    es_local = tipo_envio_val == "📍 Pedido Local"
                    if es_local:
                        turno_local = selected_row_data.get("Turno", "N/A")
                        estado_entrega_local = format_estado_entrega(
                            selected_row_data.get("Estado_Entrega")
                        )
                    else:
                        turno_local = None
                        estado_entrega_local = None

                    detalles_col_izq, detalles_col_der = st.columns(2)

                    with detalles_col_izq:
                        st.markdown(f"**Fuente:** {fuente_display}")
                        st.markdown(f"**Vendedor:** {vendedor_display}")
                        st.markdown(format_id_vendedor_with_mod(selected_row_data))
                        st.markdown(f"**Cliente:** {selected_row_data.get('Cliente', 'N/A')}")
                        st.markdown(
                            f"**Folio de Factura:** {selected_row_data.get('Folio_Factura', 'N/A')}"
                        )

                    with detalles_col_der:
                        st.markdown(f"**Estado Actual:** {selected_row_data.get('Estado', 'N/A')}")
                        st.markdown(
                            f"**Estado de Pago:** {selected_row_data.get('Estado_Pago', '🔴 No Pagado')}"
                        )
                        st.markdown(f"**Tipo de Envío:** {tipo_envio_val}")
                        if es_local:
                            st.markdown(f"**Turno Local:** {turno_local}")
                            st.markdown(f"**Estado_Entrega:** {estado_entrega_local}")
                        st.markdown(
                            f"**Fecha de Entrega:** {selected_row_data.get('Fecha_Entrega', 'N/A')}"
                        )

                    st.markdown("**Comentario Original:**")
                    st.write(selected_row_data.get("Comentario", "N/A"))

                    current_adjuntos_str_basic = selected_row_data.get('Adjuntos', '')
                    current_adjuntos_list_basic = [f.strip() for f in str(current_adjuntos_str_basic).split(',') if f.strip()]
                    current_adjuntos_surtido_str_basic = selected_row_data.get('Adjuntos_Surtido', '')
                    current_adjuntos_surtido_list_basic = [f.strip() for f in str(current_adjuntos_surtido_str_basic).split(',') if f.strip()]

                    if current_adjuntos_list_basic:
                        st.write("**Adjuntos Originales:**")
                        for adj in current_adjuntos_list_basic:
                            render_attachment_link(adj)
                    else:
                        st.write("**Adjuntos Originales:** Ninguno")

                    if current_adjuntos_surtido_list_basic:
                        st.write("**Adjuntos de Modificación/Surtido:**")
                        for adj_surtido in current_adjuntos_surtido_list_basic:
                            render_attachment_link(adj_surtido)
                    else:
                        st.write("**Adjuntos de Modificación/Surtido:** Ninguno")

                # ----------------- Valores actuales (para formulario) -----------------
                current_modificacion_surtido_value = selected_row_data.get('Modificacion_Surtido', '')
                current_estado_pago_value = selected_row_data.get('Estado_Pago', '🔴 No Pagado')
                current_adjuntos_str = selected_row_data.get('Adjuntos', '')
                current_adjuntos_list = [f.strip() for f in str(current_adjuntos_str).split(',') if f.strip()]
                current_adjuntos_surtido_str = selected_row_data.get('Adjuntos_Surtido', '')
                current_adjuntos_surtido_list = [f.strip() for f in str(current_adjuntos_surtido_str).split(',') if f.strip()]

                st.markdown("---")
                st.subheader("Modificar Campos y Adjuntos (Surtido)")
                st.markdown("### 🛠 Tipo de modificación")

                # ----------------- Tipo de modificación -----------------
                tipo_modificacion_seleccionada = st.selectbox(
                    "📌 ¿Qué tipo de modificación estás registrando?",
                    ["Otro", "Nueva Ruta", "Refacturación"],
                    index=2,
                    key="tipo_modificacion_mod"
                )

                refact_tipo = ""
                refact_subtipo_val = ""
                refact_folio_nuevo = ""

                if tipo_modificacion_seleccionada == "Refacturación":
                    st.markdown("### 🧾 Detalles de Refacturación")

                    refact_tipo = st.selectbox(
                        "🔍 Razón Principal",
                        ["Datos Fiscales", "Material"],
                        key="refact_tipo_mod_outside"
                    )

                    if refact_tipo == "Datos Fiscales":
                        refact_subtipo_val = st.selectbox(
                            "📌 Subtipo",
                            ["Cambio de RFC", "Cambio de Régimen Fiscal", "Error en Forma de Pago", "Error de uso de Cfdi", "Otro"],
                            key="subtipo_datos_outside",
                            placeholder="Selecciona una opción..."
                        )
                    else:
                        refact_subtipo_val = st.selectbox(
                            "📌 Subtipo",
                            ["Agrego Material", "Quito Material", "Clave de Producto Errónea", "Otro"],
                            key="subtipo_material_outside",
                            placeholder="Selecciona una opción..."
                        )

                    refact_folio_nuevo = st.text_input("📄 Folio de la Nueva Factura", key="folio_refact_outside")

                # ----------------- Formulario de modificación -----------------
                with st.form(key="modify_pedido_form_inner", clear_on_submit=False):
                    default_modificacion_text = "" if reset_inputs_tab2_flag else current_modificacion_surtido_value

                    new_modificacion_surtido_input = st.text_area(
                        "✍️ Notas de Modificación/Surtido",
                        value=default_modificacion_text,
                        height=100,
                        key="new_modificacion_surtido_input"
                    )

                    uploaded_files_surtido = st.file_uploader(
                        "📎 Subir Archivos para Modificación/Surtido",
                        type=["pdf", "jpg", "jpeg", "png", "xlsx", "docx"],
                        accept_multiple_files=True,
                        key="uploaded_files_surtido"
                    )

                    uploaded_comprobantes_extra = st.file_uploader(
                        "🧾 Subir Comprobante(s) Adicional(es)",
                        type=["pdf", "jpg", "jpeg", "png"],
                        accept_multiple_files=True,
                        key="uploaded_comprobantes_extra"
                    )

                    # Botón para procesar la modificación del pedido
                    modify_button = st.form_submit_button("✅ Procesar Modificación")
                    feedback_slot = st.empty()

                    if modify_button:
                        feedback_slot.empty()
                        if not new_modificacion_surtido_input.strip():
                            feedback_slot.empty()
                            feedback_slot.error(
                                "⚠️ El campo 'Notas de Modificación/Surtido' es obligatorio para procesar la modificación."
                            )
                        else:
                            try:
                                # 1) Enrutar a la hoja correcta según la fuente
                                client = build_gspread_client()
                                sh = client.open_by_key(GOOGLE_SHEET_ID)
                                hoja_objetivo = "datos_pedidos" if selected_source == "datos_pedidos" else "casos_especiales"
                                worksheet = sh.worksheet(hoja_objetivo)

                                headers = worksheet.row_values(1)
                                df_actual = df_pedidos[df_pedidos["Fuente"] == selected_source].reset_index(drop=True)

                                if df_actual.empty or 'ID_Pedido' not in df_actual.columns:
                                    feedback_slot.empty()
                                    feedback_slot.error(f"❌ No se encontró 'ID_Pedido' en la hoja {hoja_objetivo}.")
                                    st.stop()

                                sheet_row_number = parse_sheet_row_number(
                                    selected_row_data.get("Sheet_Row_Number")
                                )
                                row_candidates = pd.DataFrame()
                                gsheet_row_index = None

                                if sheet_row_number is not None and "Sheet_Row_Number" in df_actual.columns:
                                    sheet_numbers_series = pd.to_numeric(
                                        df_actual["Sheet_Row_Number"], errors="coerce"
                                    )
                                    row_candidates = df_actual.loc[
                                        sheet_numbers_series == sheet_row_number
                                    ]
                                    if not row_candidates.empty:
                                        gsheet_row_index = sheet_row_number

                                if row_candidates.empty:
                                    if selected_order_id not in df_actual['ID_Pedido'].values:
                                        feedback_slot.empty()
                                        feedback_slot.error(
                                            f"❌ El ID {selected_order_id} no existe en {hoja_objetivo}."
                                        )
                                        st.stop()

                                    row_candidates = df_actual[df_actual['ID_Pedido'] == selected_order_id]
                                    if row_candidates.empty:
                                        feedback_slot.empty()
                                        feedback_slot.error(
                                            f"❌ No se encontró la fila para el pedido {selected_order_id}."
                                        )
                                        st.stop()

                                    if "Sheet_Row_Number" in row_candidates.columns:
                                        candidate_numbers = pd.to_numeric(
                                            row_candidates["Sheet_Row_Number"], errors="coerce"
                                        ).dropna()
                                        if not candidate_numbers.empty:
                                            gsheet_row_index = int(candidate_numbers.iloc[0])

                                    if gsheet_row_index is None:
                                        feedback_slot.empty()
                                        feedback_slot.error(
                                            "❌ No se pudo determinar la fila en Google Sheets para el pedido seleccionado."
                                        )
                                        st.stop()

                                changes_made = False

                                cell_updates = []
                                actual_row = row_candidates.iloc[0]

                                def col_exists(col):
                                    return col in headers

                                def col_idx(col):
                                    return headers.index(col) + 1

                                # 2) Guardar Modificacion_Surtido (si cambió)
                                if col_exists("Modificacion_Surtido"):
                                    if new_modificacion_surtido_input.strip() != current_modificacion_surtido_value.strip():
                                        cell_updates.append({
                                            "range": rowcol_to_a1(
                                                gsheet_row_index,
                                                col_idx("Modificacion_Surtido"),
                                            ),
                                            "values": [[new_modificacion_surtido_input.strip()]],
                                        })
                                        changes_made = True

                                # 3) Subida de archivos de Surtido -> Adjuntos_Surtido
                                new_adjuntos_surtido_urls = []
                                if uploaded_files_surtido:
                                    for f in uploaded_files_surtido:
                                        ext = os.path.splitext(f.name)[1]
                                        s3_key = f"{selected_order_id}/surtido_{f.name.replace(' ', '_').replace(ext, '')}_{uuid.uuid4().hex[:4]}{ext}"
                                        success, url, error_msg = upload_file_to_s3(s3_client, S3_BUCKET_NAME, f, s3_key)
                                        if success:
                                            new_adjuntos_surtido_urls.append(url)
                                            changes_made = True
                                        else:
                                            feedback_slot.empty()
                                            feedback_slot.warning(
                                                f"⚠️ Falló la subida de {f.name}: {error_msg or 'Error desconocido'}"
                                            )

                                if new_adjuntos_surtido_urls and col_exists("Adjuntos_Surtido"):
                                    current_urls = [x.strip() for x in str(actual_row.get("Adjuntos_Surtido","")).split(",") if x.strip()]
                                    updated_str = ", ".join(current_urls + new_adjuntos_surtido_urls)
                                    cell_updates.append({
                                        "range": rowcol_to_a1(
                                            gsheet_row_index,
                                            col_idx("Adjuntos_Surtido"),
                                        ),
                                        "values": [[updated_str]],
                                    })

                                # 4) Comprobantes extra -> concatenar en 'Adjuntos'
                                comprobante_urls = []
                                if uploaded_comprobantes_extra:
                                    for archivo in uploaded_comprobantes_extra:
                                        ext = os.path.splitext(archivo.name)[1]
                                        s3_key = f"{selected_order_id}/comprobante_{selected_order_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:4]}{ext}"
                                        success, url, error_msg = upload_file_to_s3(s3_client, S3_BUCKET_NAME, archivo, s3_key)
                                        if success:
                                            comprobante_urls.append(url)
                                            changes_made = True
                                        else:
                                            feedback_slot.empty()
                                            feedback_slot.warning(
                                                f"⚠️ Falló la subida del comprobante {archivo.name}: {error_msg or 'Error desconocido'}"
                                            )

                                    if comprobante_urls and col_exists("Adjuntos"):
                                        current_adjuntos = [x.strip() for x in str(actual_row.get("Adjuntos","")).split(",") if x.strip()]
                                        updated_adjuntos = ", ".join(current_adjuntos + comprobante_urls)
                                        cell_updates.append({
                                            "range": rowcol_to_a1(
                                                gsheet_row_index,
                                                col_idx("Adjuntos"),
                                            ),
                                            "values": [[updated_adjuntos]],
                                        })

                                # 5) Refacturación (si las columnas existen en ESA hoja)
                                if tipo_modificacion_seleccionada == "Refacturación":
                                    campos_refact = {
                                        "Refacturacion_Tipo": refact_tipo,
                                        "Refacturacion_Subtipo": refact_subtipo_val,
                                        "Folio_Factura_Refacturada": refact_folio_nuevo
                                    }
                                    for campo, valor in campos_refact.items():
                                        if col_exists(campo):
                                            cell_updates.append({
                                                "range": rowcol_to_a1(
                                                    gsheet_row_index,
                                                    col_idx(campo),
                                                ),
                                                "values": [[valor]],
                                            })
                                    st.toast("🧾 Refacturación registrada.")
                                else:
                                    for campo in ["Refacturacion_Tipo","Refacturacion_Subtipo","Folio_Factura_Refacturada"]:
                                        if col_exists(campo):
                                            cell_updates.append({
                                                "range": rowcol_to_a1(
                                                    gsheet_row_index,
                                                    col_idx(campo),
                                                ),
                                                "values": [[""]],
                                            })

                                # 6) Ajustar estado del pedido según su valor actual
                                if col_exists("Estado"):
                                    estado_actual_raw = str(actual_row.get("Estado", "")).strip()
                                    estado_actual_lower = estado_actual_raw.lower()

                                    nuevo_estado = estado_actual_raw or "🟡 Pendiente"
                                    if "proceso" in estado_actual_lower:
                                        nuevo_estado = "🔵 En Proceso"
                                    elif "pendiente" in estado_actual_lower:
                                        nuevo_estado = "🟡 Pendiente"
                                    elif "demorado" in estado_actual_lower:
                                        nuevo_estado = "🟡 Pendiente"
                                    elif "complet" in estado_actual_lower:
                                        nuevo_estado = "🟡 Pendiente"

                                    if nuevo_estado != estado_actual_raw:
                                        cell_updates.append({
                                            "range": rowcol_to_a1(
                                                gsheet_row_index,
                                                col_idx("Estado"),
                                            ),
                                            "values": [[nuevo_estado]],
                                        })
                                        changes_made = True
                                        feedback_slot.empty()
                                        feedback_slot.info(
                                            f"📌 El estado del pedido se actualizó a '{nuevo_estado}'."
                                        )

                                if (
                                    selected_source == "datos_pedidos"
                                    and col_exists("Fecha_Completado")
                                ):
                                    cell_updates.append({
                                        "range": rowcol_to_a1(
                                            gsheet_row_index,
                                            col_idx("Fecha_Completado"),
                                        ),
                                        "values": [[""]],
                                    })

                                # 7) Registrar quién modificó el pedido
                                id_vendedor_actual = str(st.session_state.get("id_vendedor", "")).strip()
                                if id_vendedor_actual and col_exists("id_vendedor_Mod"):
                                    existing_ids_raw = str(actual_row.get("id_vendedor_Mod", "")).strip()
                                    existing_ids = [
                                        entry.strip().upper()
                                        for entry in existing_ids_raw.split(",")
                                        if entry.strip()
                                    ]
                                    normalized_vendedor = id_vendedor_actual.upper()
                                    if normalized_vendedor not in existing_ids:
                                        updated_ids = existing_ids + [normalized_vendedor]
                                        cell_updates.append({
                                            "range": rowcol_to_a1(
                                                gsheet_row_index,
                                                col_idx("id_vendedor_Mod"),
                                            ),
                                            "values": [[", ".join(updated_ids)]],
                                        })
                                        changes_made = True

                                if cell_updates:
                                    safe_batch_update(worksheet, cell_updates)

                                # 8) Mensajes y limpieza de inputs
                                if changes_made:
                                    st.session_state["reset_inputs_tab2"] = True
                                    st.session_state["show_success_message"] = True
                                    st.session_state["last_updated_order_id"] = selected_order_id
                                    if tab2_is_active and st.session_state.get("current_tab_index") == 1:
                                        st.query_params.update({"tab": "1"})  # mantener UX actual
                                    st.rerun()
                                else:
                                    feedback_slot.empty()
                                    feedback_slot.info("ℹ️ No se detectaron cambios nuevos para guardar.")

                            except Exception as e:
                                feedback_slot.empty()
                                feedback_slot.error(f"❌ Error inesperado al guardar: {e}")

    # ----------------- Mensaje de éxito persistente -----------------
    if (
        'show_success_message' in st.session_state and
        st.session_state.show_success_message and
        'last_updated_order_id' in st.session_state
    ):
        pedido_id = st.session_state.last_updated_order_id
        with message_placeholder_tab2.container():
            st.success(
                f"🎉 ¡Cambios guardados con éxito para el pedido **{pedido_id}**!"
            )
            if st.button("Aceptar", key="ack_mod_success"):
                for state_key in (
                    "show_success_message",
                    "last_updated_order_id",
                    "_mod_tab2_success_feedback_sent",
                ):
                    st.session_state.pop(state_key, None)
                message_placeholder_tab2.empty()
        if (
            st.session_state.get("show_success_message")
            and not st.session_state.get("_mod_tab2_success_feedback_sent")
        ):
            st.toast(f"✅ Pedido {pedido_id} actualizado", icon="📦")
            st.session_state["_mod_tab2_success_feedback_sent"] = True


# --- TAB 3: PENDING PROOF OF PAYMENT ---
with tab3:
    tab3_is_active = default_tab == 2
    if tab3_is_active:
        st.session_state["current_tab_index"] = 2
    st.header("🧾 Pedidos Pendientes de Comprobante")

    df_pedidos_comprobante = pd.DataFrame()
    try:
        worksheet = get_worksheet()
        headers = worksheet.row_values(1)
        if headers:
            df_pedidos_comprobante = pd.DataFrame(worksheet.get_all_records())
            if "Adjuntos_Guia" not in df_pedidos_comprobante.columns:
                df_pedidos_comprobante["Adjuntos_Guia"] = ""

            if 'Folio_Factura' in df_pedidos_comprobante.columns:
                df_pedidos_comprobante['Folio_Factura'] = df_pedidos_comprobante['Folio_Factura'].astype(str).replace('nan', '')
            if 'Vendedor_Registro' in df_pedidos_comprobante.columns:
                df_pedidos_comprobante['Vendedor_Registro'] = df_pedidos_comprobante['Vendedor_Registro'].astype(str).str.strip()
                df_pedidos_comprobante.loc[~df_pedidos_comprobante['Vendedor_Registro'].isin(VENDEDORES_LIST), 'Vendedor_Registro'] = 'Otro/Desconocido'
                df_pedidos_comprobante.loc[df_pedidos_comprobante['Vendedor_Registro'].isin(['', 'nan', 'None']), 'Vendedor_Registro'] = 'N/A'

        else:
            st.warning("No se pudieron cargar los encabezados del Google Sheet. Asegúrate de que la primera fila no esté vacía.")
    except Exception as e:
        st.error(f"❌ Error al cargar pedidos para comprobante: {e}")

    if df_pedidos_comprobante.empty:
        st.info("No hay pedidos registrados.")
    else:
        filtered_pedidos_comprobante = df_pedidos_comprobante.copy()

        col3_tab3, col4_tab3 = st.columns(2)
        with col3_tab3:
            if 'Vendedor_Registro' in filtered_pedidos_comprobante.columns:
                unique_vendedores_comp = ["Todos"] + sorted(filtered_pedidos_comprobante['Vendedor_Registro'].unique().tolist())
                selected_vendedor_comp = st.selectbox(
                    "Filtrar por Vendedor:",
                    options=unique_vendedores_comp,
                    key="comprobante_vendedor_filter"
                )
                if selected_vendedor_comp != "Todos":
                    filtered_pedidos_comprobante = filtered_pedidos_comprobante[filtered_pedidos_comprobante['Vendedor_Registro'] == selected_vendedor_comp]

        with col4_tab3:
            (
                fecha_inicio_comp,
                fecha_fin_comp,
                _rango_activo_comp,
                rango_valido_comp,
            ) = render_date_filter_controls(
                "📅 Filtrar por Fecha de Registro:",
                "tab3_comprobantes_filtro",
            )

        # Filtrar por fecha si existe la columna 'Hora_Registro'
        if 'Hora_Registro' in filtered_pedidos_comprobante.columns:
            filtered_pedidos_comprobante = filtered_pedidos_comprobante.copy()
            filtered_pedidos_comprobante['Hora_Registro'] = pd.to_datetime(
                filtered_pedidos_comprobante['Hora_Registro'],
                errors='coerce'
            )
            if rango_valido_comp:
                start_dt = datetime.combine(fecha_inicio_comp, datetime.min.time())
                end_dt = datetime.combine(fecha_fin_comp, datetime.max.time())
                filtered_pedidos_comprobante = filtered_pedidos_comprobante[
                    filtered_pedidos_comprobante['Hora_Registro'].between(start_dt, end_dt)
                ]
            else:
                filtered_pedidos_comprobante = filtered_pedidos_comprobante.iloc[0:0]

        filtered_pedidos_comprobante = filtered_pedidos_comprobante[
            filtered_pedidos_comprobante['ID_Pedido'].astype(str).str.strip().ne('') &
            filtered_pedidos_comprobante['Cliente'].astype(str).str.strip().ne('') &
            filtered_pedidos_comprobante['Folio_Factura'].astype(str).str.strip().ne('')
        ]

        if 'Estado_Pago' in filtered_pedidos_comprobante.columns and 'Adjuntos' in filtered_pedidos_comprobante.columns:
            pedidos_sin_comprobante = filtered_pedidos_comprobante[
                (filtered_pedidos_comprobante['Estado_Pago'] == '🔴 No Pagado') &
                (~filtered_pedidos_comprobante['Adjuntos'].astype(str).str.contains('comprobante', na=False, case=False))
            ].copy()
        else:
            st.warning("Las columnas 'Estado_Pago' o 'Adjuntos' no se encontraron. No se puede filtrar por comprobantes.")
            pedidos_sin_comprobante = pd.DataFrame()

        if pedidos_sin_comprobante.empty:
            if not rango_valido_comp:
                st.info("Ajusta el rango de fechas para continuar.")
            else:
                st.success("🎉 Todos los pedidos están marcados como pagados o tienen comprobante.")
        else:
            st.warning(f"⚠️ Hay {len(pedidos_sin_comprobante)} pedidos pendientes de comprobante.")

            columnas_mostrar = [
                'ID_Pedido', 'Cliente', 'Folio_Factura', 'Vendedor_Registro', 'Tipo_Envio', 'Turno',
                'Fecha_Entrega', 'Estado', 'Estado_Pago', 'Comentario', 'Modificacion_Surtido', 'Adjuntos', 'Adjuntos_Surtido'
            ]
            columnas_mostrar = [c for c in columnas_mostrar if c in pedidos_sin_comprobante.columns]

            st.dataframe(pedidos_sin_comprobante[columnas_mostrar].sort_values(by='Fecha_Entrega'), use_container_width=True, hide_index=True)

            # ✅ Bloque de subida o marca sin comprobante SOLO si hay pedidos pendientes
            st.markdown("---")
            st.subheader("Subir Comprobante para un Pedido")

            # 🆕 Ordenar por Fecha_Entrega descendente para mostrar los más recientes primero
            if 'Fecha_Entrega' in pedidos_sin_comprobante.columns:
                pedidos_sin_comprobante['Fecha_Entrega'] = pd.to_datetime(pedidos_sin_comprobante['Fecha_Entrega'], errors='coerce')
                pedidos_sin_comprobante = pedidos_sin_comprobante.sort_values(by='Fecha_Entrega', ascending=False).reset_index(drop=True)



            pedidos_sin_comprobante['display_label'] = pedidos_sin_comprobante.apply(lambda row:
                f"📄 {row.get('Folio_Factura', 'N/A') or row.get('ID_Pedido', 'N/A')} - {row.get('Cliente', 'N/A')} - {row.get('Estado', 'N/A')}", axis=1)
            # ❌ NO volver a ordenar aquí


            selected_pending_order_display = st.selectbox(
                "📝 Seleccionar Pedido para Subir Comprobante",
                pedidos_sin_comprobante['display_label'].tolist(),
                key="select_pending_order_comprobante"
            )

            if selected_pending_order_display:
                selected_pending_order_id = pedidos_sin_comprobante[pedidos_sin_comprobante['display_label'] == selected_pending_order_display]['ID_Pedido'].iloc[0]
                selected_pending_row_data = pedidos_sin_comprobante[pedidos_sin_comprobante['ID_Pedido'] == selected_pending_order_id].iloc[0]

                st.info(f"Subiendo comprobante para: Folio {selected_pending_row_data.get('Folio_Factura')} (ID {selected_pending_order_id})")

                with st.form(key=f"upload_comprobante_form_{selected_pending_order_id}"):
                    comprobante_files = st.file_uploader(
                        "💲 Comprobante(s) de Pago",
                        type=["pdf", "jpg", "jpeg", "png"],
                        accept_multiple_files=True,
                        key=f"comprobante_uploader_{selected_pending_order_id}"
                    )

                    submit_comprobante = st.form_submit_button("✅ Subir Comprobante y Actualizar Estado")

                    if submit_comprobante:
                        if comprobante_files:
                            try:
                                headers = worksheet.row_values(1)
                                all_data_actual = worksheet.get_all_records()
                                df_actual = pd.DataFrame(all_data_actual)

                                if selected_pending_order_id not in df_actual['ID_Pedido'].values:
                                    st.error("❌ No se encontró el ID del pedido en la hoja. Verifica que no se haya borrado.")
                                    st.stop()

                                df_index = df_actual[df_actual['ID_Pedido'] == selected_pending_order_id].index[0]
                                sheet_row = df_index + 2

                                new_urls = []
                                for archivo in comprobante_files:
                                    ext = os.path.splitext(archivo.name)[1]
                                    s3_key = f"{selected_pending_order_id}/comprobante_{selected_pending_order_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:4]}{ext}"
                                    success, url, error_msg = upload_file_to_s3(s3_client, S3_BUCKET_NAME, archivo, s3_key)
                                    if success:
                                        new_urls.append(url)
                                    else:
                                        st.warning(f"⚠️ Falló la subida de {archivo.name}: {error_msg or 'Error desconocido'}")

                                if new_urls:
                                    current_adjuntos = df_pedidos_comprobante.loc[df_index, 'Adjuntos'] if 'Adjuntos' in df_pedidos_comprobante.columns else ""
                                    adjuntos_list = [x.strip() for x in current_adjuntos.split(',') if x.strip()]
                                    adjuntos_list.extend(new_urls)
                                    updates = [
                                        {
                                            "range": rowcol_to_a1(
                                                sheet_row,
                                                headers.index('Adjuntos') + 1,
                                            ),
                                            "values": [[", ".join(adjuntos_list)]],
                                        },
                                        {
                                            "range": rowcol_to_a1(
                                                sheet_row,
                                                headers.index('Estado_Pago') + 1,
                                            ),
                                            "values": [["✅ Pagado"]],
                                        },
                                        {
                                            "range": rowcol_to_a1(
                                                sheet_row,
                                                headers.index('Fecha_Pago_Comprobante') + 1,
                                            ),
                                            "values": [[datetime.now(timezone("America/Mexico_City")).strftime('%Y-%m-%d')]],
                                        },
                                    ]
                                    safe_batch_update(worksheet, updates)

                                    st.success("✅ Comprobantes subidos y estado actualizado con éxito.")
                                    st.rerun()
                                else:
                                    st.warning("⚠️ No se subió ningún archivo correctamente.")
                            except Exception as e:
                                st.error(f"❌ Error al subir comprobantes: {e}")
                        else:
                            st.warning("⚠️ Por favor, sube al menos un archivo.")

                if st.button("✅ Marcar como Pagado sin Comprobante", key=f"btn_sin_cp_{selected_pending_order_id}"):
                    try:
                        headers = worksheet.row_values(1)
                        df_index = df_pedidos_comprobante[df_pedidos_comprobante['ID_Pedido'] == selected_pending_order_id].index[0]
                        sheet_row = df_index + 2

                        updates = [
                            {
                                "range": rowcol_to_a1(
                                    sheet_row,
                                    headers.index('Estado_Pago') + 1,
                                ),
                                "values": [["✅ Pagado"]],
                            }
                        ]

                        if 'Fecha_Pago_Comprobante' in headers:
                            updates.append({
                                "range": rowcol_to_a1(
                                    sheet_row,
                                    headers.index('Fecha_Pago_Comprobante') + 1,
                                ),
                                "values": [[datetime.now(timezone("America/Mexico_City")).strftime('%Y-%m-%d')]],
                            })

                        safe_batch_update(worksheet, updates)

                        st.success("✅ Pedido marcado como pagado sin comprobante.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ Error al marcar como pagado sin comprobante: {e}")

# ----------------- HELPERS FALTANTES -----------------

def partir_urls(value):
    """
    Normaliza un campo de adjuntos que puede venir como JSON (lista o dict),
    o como texto separado por comas/; / saltos de línea. Devuelve lista de URLs únicas.
    """
    if value is None:
        return []
    s = str(value).strip()
    if not s or s.lower() in ("nan", "none", "n/a"):
        return []
    urls = []
    # Intento como JSON
    try:
        obj = json.loads(s)
        if isinstance(obj, list):
            for it in obj:
                if isinstance(it, str) and it.strip():
                    urls.append(it.strip())
                elif isinstance(it, dict):
                    for k in ("url", "URL", "href", "link"):
                        if k in it and str(it[k]).strip():
                            urls.append(str(it[k]).strip())
        elif isinstance(obj, dict):
            for k in ("url", "URL", "href", "link"):
                if k in obj and str(obj[k]).strip():
                    urls.append(str(obj[k]).strip())
    except Exception:
        # Separadores comunes
        for p in re.split(r"[,\n;]+", s):
            p = p.strip()
            if p:
                urls.append(p)
    # De-duplicar preservando orden
    out, seen = [], set()
    for u in urls:
        if u not in seen:
            seen.add(u); out.append(u)
    return out


@st.cache_data(ttl=300)
def cargar_casos_especiales():
    """
    Lee la hoja 'casos_especiales' usando tu helper get_worksheet_casos_especiales()
    y garantiza todas las columnas que la UI usa.
    """
    ws = get_worksheet_casos_especiales()
    data = ws.get_all_records()
    df = pd.DataFrame(data)

    columnas_necesarias = [
        # Identificación y encabezado
        "ID_Pedido","Cliente","Vendedor_Registro","Folio_Factura","Folio_Factura_Error",
        "Hora_Registro","Tipo_Envio","Estado","Estado_Caso","Turno",
        # Refacturación
        "Refacturacion_Tipo","Refacturacion_Subtipo","Folio_Factura_Refacturada",
        # Detalle del caso
        "Resultado_Esperado","Motivo_Detallado","Material_Devuelto","Monto_Devuelto","Motivo_NotaVenta",
        "Area_Responsable","Nombre_Responsable","Numero_Cliente_RFC","Tipo_Envio_Original","Estatus_OrigenF",
        "Direccion_Guia_Retorno","Direccion_Envio",
        # ⚙️ NUEVO: Garantías
        "Numero_Serie","Fecha_Compra",  # (si tu hoja usa 'FechaCompra', abajo la normalizamos)
        # Fechas/recepción
        "Fecha_Entrega","Fecha_Recepcion_Devolucion","Estado_Recepcion",
        # Documentos de cierre
        "Nota_Credito_URL","Documento_Adicional_URL","Comentarios_Admin_Devolucion",
        # Modificación de surtido
        "Modificacion_Surtido","Adjuntos_Surtido",
        # Adjuntos/guía
        "Adjuntos","Hoja_Ruta_Mensajero",
        # Otros
        "Hora_Proceso",
        # Seguimiento
        "Seguimiento"
    ]
    for c in columnas_necesarias:
        if c not in df.columns:
            df[c] = ""

    # Quitar casos cerrados
    df["Seguimiento"] = df["Seguimiento"].fillna("")
    df = df[~df["Seguimiento"].astype(str).str.lower().eq("cerrado")]

    # Normaliza 'Fecha_Compra' si en la hoja existe como 'FechaCompra'
    if "Fecha_Compra" not in df.columns and "FechaCompra" in df.columns:
        df["Fecha_Compra"] = df["FechaCompra"]
    elif "Fecha_Compra" in df.columns and "FechaCompra" in df.columns and df["Fecha_Compra"].eq("").all():
        # Si ambas existen pero 'Fecha_Compra' viene vacía, usa 'FechaCompra'
        df["Fecha_Compra"] = df["Fecha_Compra"].where(df["Fecha_Compra"].astype(str).str.strip() != "", df["FechaCompra"])

    return df




# --- TAB 4: CASOS ESPECIALES ---
with tab4:
    tab4_is_active = default_tab == 3
    if tab4_is_active:
        st.session_state["current_tab_index"] = 3
    st.header("🗂 Casos Especiales")

    try:
        df_casos = cargar_casos_especiales()
    except Exception as e:
        st.error(f"❌ Error al cargar casos especiales: {e}")
        df_casos = pd.DataFrame()

    if df_casos.empty:
        st.info("No hay casos especiales.")
    else:
        df_casos = df_casos[
            df_casos["Tipo_Envio"].isin(["🔁 Devolución", "🛠 Garantía"]) &
            (df_casos["Seguimiento"] != "Cerrado")
        ]

        if df_casos.empty:
            st.info("No hay casos especiales abiertos.")
        else:
            if "Hora_Registro" in df_casos.columns:
                df_casos["Hora_Registro"] = pd.to_datetime(df_casos["Hora_Registro"], errors="coerce")

            if "Vendedor_Registro" in df_casos.columns:
                df_casos["Vendedor_Registro"] = (
                    df_casos["Vendedor_Registro"].astype(str).str.strip()
                )

            col_vend_casos, col_fecha_casos = st.columns(2)

            with col_vend_casos:
                vendedores_casos = ["Todos"]
                if "Vendedor_Registro" in df_casos.columns:
                    unique_vendedores_casos = sorted(
                        [
                            v
                            for v in df_casos["Vendedor_Registro"].dropna().astype(str).str.strip().unique().tolist()
                            if v and v.lower() not in ["none", "nan"]
                        ]
                    )
                    vendedores_casos.extend(unique_vendedores_casos)
                selected_vendedor_casos = st.selectbox(
                    "Filtrar por Vendedor:",
                    options=vendedores_casos,
                    key="filtro_vendedor_casos_especiales"
                )

            with col_fecha_casos:
                (
                    fecha_inicio_casos,
                    fecha_fin_casos,
                    _rango_activo_casos,
                    rango_valido_casos,
                ) = render_date_filter_controls(
                    "📅 Filtrar por Fecha de Registro:",
                    "tab4_casos_filtro",
                )

            filtered_casos = df_casos.copy()

            if (
                selected_vendedor_casos != "Todos"
                and "Vendedor_Registro" in filtered_casos.columns
            ):
                filtered_casos = filtered_casos[
                    filtered_casos["Vendedor_Registro"] == selected_vendedor_casos
                ]

            if "Hora_Registro" in filtered_casos.columns:
                filtered_casos = filtered_casos.copy()
                hora_registro_series = filtered_casos["Hora_Registro"]
                if not pd.api.types.is_datetime64_any_dtype(hora_registro_series):
                    hora_registro_series = pd.to_datetime(
                        hora_registro_series,
                        errors="coerce",
                    )
                    filtered_casos["Hora_Registro"] = hora_registro_series

                if rango_valido_casos:
                    start_dt = datetime.combine(fecha_inicio_casos, datetime.min.time())
                    end_dt = datetime.combine(fecha_fin_casos, datetime.max.time())
                    filtered_casos = filtered_casos[
                        hora_registro_series.between(start_dt, end_dt)
                    ]
                else:
                    filtered_casos = filtered_casos.iloc[0:0]

            if filtered_casos.empty:
                if not rango_valido_casos:
                    st.info("Ajusta el rango de fechas para continuar.")
                else:
                    st.warning("No hay casos especiales que coincidan con los filtros seleccionados.")
            else:
                filtered_casos = filtered_casos.reset_index(drop=True)
                columnas_mostrar = ["Estado","Cliente","Vendedor_Registro","Tipo_Envio","Seguimiento"]
                st.dataframe(filtered_casos[columnas_mostrar], use_container_width=True, hide_index=True)

                filtered_casos = filtered_casos.copy()
                filtered_casos["display_label"] = filtered_casos.apply(
                    lambda r: f"{r['Estado']} - {r['Cliente']} ({r['Tipo_Envio']})", axis=1
                )
                selected_case = st.selectbox(
                    "📂 Selecciona un caso para ver detalles",
                    filtered_casos["display_label"].tolist(),
                    key="select_caso_especial_tab4"
                )

                if selected_case:
                    case_row = filtered_casos[
                        filtered_casos["display_label"] == selected_case
                    ].iloc[0]
                    render_caso_especial(case_row)


# --- TAB 5: GUIAS CARGADAS ---
def fijar_tab5_activa():
    if st.session_state.get("current_tab_index") == 4:
        st.query_params.update({"tab": "4"})

@st.cache_data(ttl=60)
def cargar_datos_guias_unificadas():
    # ---------- A) datos_pedidos ----------
    ws_ped = get_worksheet()
    df_ped = pd.DataFrame(ws_ped.get_all_records())

    if df_ped.empty:
        df_ped = pd.DataFrame()

    for col in ["ID_Pedido","Cliente","Vendedor_Registro","Tipo_Envio","Estado",
                "Fecha_Entrega","Hora_Registro","Folio_Factura","Adjuntos_Guia"]:
        if col not in df_ped.columns:
            df_ped[col] = ""

    df_a = df_ped[df_ped["Adjuntos_Guia"].astype(str).str.strip() != ""].copy()
    if not df_a.empty:
        df_a["Fuente"] = "datos_pedidos"
        df_a["URLs_Guia"] = df_a["Adjuntos_Guia"].astype(str)
        df_a["Ultima_Guia"] = df_a["URLs_Guia"].apply(
            lambda s: s.split(",")[-1].strip() if isinstance(s, str) and s.strip() else ""
        )

    # ---------- B) casos_especiales ----------
    try:
        ws_casos = get_worksheet_casos_especiales()
        df_casos = pd.DataFrame(ws_casos.get_all_records())
    except Exception:
        df_casos = pd.DataFrame()

    if df_casos.empty:
        df_b = pd.DataFrame(columns=[
            "ID_Pedido","Cliente","Vendedor_Registro","Tipo_Envio","Estado",
            "Fecha_Entrega","Hora_Registro","Folio_Factura","Adjuntos_Guia",
            "URLs_Guia","Ultima_Guia","Fuente"
        ])
    else:
        for col in ["ID_Pedido","Cliente","Vendedor_Registro","Tipo_Envio","Estado",
                    "Fecha_Entrega","Hora_Registro","Folio_Factura","Hoja_Ruta_Mensajero","Tipo_Caso"]:
            if col not in df_casos.columns:
                df_casos[col] = ""

        df_b = df_casos[df_casos["Hoja_Ruta_Mensajero"].astype(str).str.strip() != ""].copy()
        if df_b.empty:
            df_b = pd.DataFrame(columns=[
                "ID_Pedido","Cliente","Vendedor_Registro","Tipo_Envio","Estado",
                "Fecha_Entrega","Hora_Registro","Folio_Factura","Adjuntos_Guia",
                "URLs_Guia","Ultima_Guia","Fuente"
            ])
        else:
            df_b["Adjuntos_Guia"] = df_b["Hoja_Ruta_Mensajero"].astype(str)
            df_b["URLs_Guia"] = df_b["Adjuntos_Guia"]
            df_b["Ultima_Guia"] = df_b["URLs_Guia"].apply(
                lambda s: s.split(",")[-1].strip() if isinstance(s, str) and s.strip() else ""
            )

            def _infer_tipo_envio(row):
                t_env = str(row.get("Tipo_Envio","")).strip()
                if t_env:
                    return t_env
                t_caso = str(row.get("Tipo_Caso","")).lower()
                if t_caso.startswith("devol"):
                    return "🔁 Devolución"
                if t_caso.startswith("garan"):
                    return "🛠 Garantía"
                return "Caso especial"
            df_b["Tipo_Envio"] = df_b.apply(_infer_tipo_envio, axis=1)
            df_b["Fuente"] = "casos_especiales"

        for col in ["Adjuntos_Guia","URLs_Guia","Ultima_Guia","Fuente"]:
            if col not in df_b.columns:
                df_b[col] = ""

    columnas_finales = ["ID_Pedido","Cliente","Vendedor_Registro","Tipo_Envio","Estado",
                        "Fecha_Entrega","Hora_Registro","Folio_Factura",
                        "Adjuntos_Guia","URLs_Guia","Ultima_Guia","Fuente"]
    df_a = df_a[columnas_finales] if not df_a.empty else pd.DataFrame(columns=columnas_finales)
    df_b = df_b[columnas_finales] if not df_b.empty else pd.DataFrame(columns=columnas_finales)

    df = pd.concat([df_a, df_b], ignore_index=True)

    if not df.empty:
        for col_fecha in ["Fecha_Entrega","Hora_Registro"]:
            df[col_fecha] = pd.to_datetime(df[col_fecha], errors="coerce")

        df["Folio_O_ID"] = df["Folio_Factura"].astype(str).str.strip()
        df.loc[df["Folio_O_ID"] == "", "Folio_O_ID"] = df["ID_Pedido"].astype(str).str.strip()

        if df["Fecha_Entrega"].notna().any():
            df = df.sort_values(by="Fecha_Entrega", ascending=False)
        elif df["Hora_Registro"].notna().any():
            df = df.sort_values(by="Hora_Registro", ascending=False)

    return df

with tab5:
    tab5_is_active = default_tab == 4
    if tab5_is_active:
        st.session_state["current_tab_index"] = 4
    st.header("📦 Pedidos con Guías Subidas desde Almacén y Casos Especiales")

    if st.button("🔄 Actualizar guías"):
        if allow_refresh("guias_last_refresh"):
            cargar_datos_guias_unificadas.clear()
            get_worksheet.clear()
            if hasattr(get_worksheet_casos_especiales, "clear"):
                get_worksheet_casos_especiales.clear()
            st.rerun()

    try:
        df_guias = cargar_datos_guias_unificadas()
    except Exception as e:
        st.error(f"❌ Error al cargar datos de guías: {e}")
        df_guias = pd.DataFrame()

    if df_guias.empty:
        st.info("No hay pedidos o casos especiales con guías subidas.")
    else:
        st.markdown("### 🔍 Filtros")
        col1_tab5, col2_tab5 = st.columns(2)

        with col1_tab5:
            vendedores = ["Todos"] + sorted(df_guias["Vendedor_Registro"].dropna().unique().tolist())
            vendedor_filtrado = st.selectbox(
                "Filtrar por Vendedor",
                vendedores,
                key="filtro_vendedor_guias",
                on_change=fijar_tab5_activa
            )

        fecha_inicio_rango = None
        fecha_fin_rango = None
        fecha_filtro_tab5 = None

        with col2_tab5:
            usar_rango_fechas = st.checkbox(
                "Activar búsqueda por rango de fechas",
                key="filtro_guias_rango_activo",
                on_change=fijar_tab5_activa
            )
            if usar_rango_fechas and st.session_state.get("filtro_guias_7_dias"):
                st.session_state["filtro_guias_7_dias"] = False
            filtrar_7_dias = st.checkbox(
                "Mostrar últimos 7 días",
                key="filtro_guias_7_dias",
                disabled=usar_rango_fechas,
                on_change=fijar_tab5_activa
            )

            if usar_rango_fechas:
                fecha_inicio_rango = st.date_input(
                    "📅 Fecha inicial:",
                    value=st.session_state.get(
                        "filtro_fecha_inicio_guias",
                        datetime.now().date() - timedelta(days=7)
                    ),
                    key="filtro_fecha_inicio_guias",
                    on_change=fijar_tab5_activa
                )
                fecha_fin_rango = st.date_input(
                    "📅 Fecha final:",
                    value=st.session_state.get(
                        "filtro_fecha_fin_guias",
                        datetime.now().date()
                    ),
                    key="filtro_fecha_fin_guias",
                    on_change=fijar_tab5_activa
                )
                if fecha_inicio_rango and fecha_fin_rango and fecha_inicio_rango > fecha_fin_rango:
                    st.warning("⚠️ La fecha inicial no puede ser mayor que la fecha final.")
            else:
                fecha_filtro_tab5 = st.date_input(
                    "📅 Filtrar por Fecha de Registro:",
                    value=st.session_state.get("filtro_fecha_guias", datetime.now().date()),
                    key="filtro_fecha_guias",
                    disabled=filtrar_7_dias,
                    on_change=fijar_tab5_activa
                )

        fecha_col_para_filtrar = None
        if "Hora_Registro" in df_guias.columns and df_guias["Hora_Registro"].notna().any():
            fecha_col_para_filtrar = "Hora_Registro"
        elif "Fecha_Entrega" in df_guias.columns and df_guias["Fecha_Entrega"].notna().any():
            fecha_col_para_filtrar = "Fecha_Entrega"

        if fecha_col_para_filtrar:
            if usar_rango_fechas:
                fecha_inicio_rango = fecha_inicio_rango or st.session_state.get("filtro_fecha_inicio_guias")
                fecha_fin_rango = fecha_fin_rango or st.session_state.get("filtro_fecha_fin_guias")
                if fecha_inicio_rango and fecha_fin_rango and fecha_inicio_rango <= fecha_fin_rango:
                    df_guias = df_guias[df_guias[fecha_col_para_filtrar].dt.date.between(fecha_inicio_rango, fecha_fin_rango)]
            elif filtrar_7_dias:
                hoy = datetime.now().date()
                rango_inicio = hoy - timedelta(days=6)
                df_guias = df_guias[df_guias[fecha_col_para_filtrar].dt.date.between(rango_inicio, hoy)]
            else:
                fecha_filtro_tab5 = fecha_filtro_tab5 or st.session_state.get("filtro_fecha_guias", datetime.now().date())
                df_guias = df_guias[df_guias[fecha_col_para_filtrar].dt.date == fecha_filtro_tab5]

        if vendedor_filtrado != "Todos":
            df_guias = df_guias[df_guias["Vendedor_Registro"] == vendedor_filtrado]

        columnas_mostrar = ["ID_Pedido","Cliente","Vendedor_Registro","Tipo_Envio","Estado","Fecha_Entrega","Fuente"]
        tabla_guias = df_guias[columnas_mostrar].copy()
        tabla_guias["Fecha_Entrega"] = pd.to_datetime(tabla_guias["Fecha_Entrega"], errors="coerce").dt.strftime("%d/%m/%y")
        st.dataframe(tabla_guias, use_container_width=True, hide_index=True)

        st.markdown("### 📥 Selecciona un Pedido para Ver la Última Guía Subida")

        df_guias["display_label"] = df_guias.apply(
            lambda row: f"📄 {row['Folio_O_ID']} – {row['Cliente']} – {row['Vendedor_Registro']} ({row['Tipo_Envio']}) · {row['Fuente']}",
            axis=1
        )

        pedido_seleccionado = st.selectbox(
            "📦 Pedido/Caso con Guía",
            options=df_guias["display_label"].tolist(),
            key="select_pedido_con_guia"
        )

        if pedido_seleccionado:
            pedido_row = df_guias[df_guias["display_label"] == pedido_seleccionado].iloc[0]
            ultima_guia = str(pedido_row["Ultima_Guia"]).strip()
            fuente = ""
            if "Fuente" in pedido_row:
                fuente = str(pedido_row["Fuente"]).strip()

            st.markdown("### 📎 Última Guía Subida")
            if ultima_guia:
                url_encoded = quote(ultima_guia, safe=':/')
                if fuente == "casos_especiales":
                    render_attachment_link(url_encoded, _infer_display_name(ultima_guia), bullet=False)
                else:
                    nombre = ultima_guia.split("/")[-1]
                    render_attachment_link(url_encoded, f"📄 {nombre}")
            else:
                st.warning("⚠️ No se encontró una URL válida para la guía.")

# --- TAB 6: PEDIDOS NO ENTREGADOS ---
with tab6:
    tab6_is_active = default_tab == 5
    if tab6_is_active:
        st.session_state["current_tab_index"] = 5
    st.header("⏳ Pedidos No Entregados")

    if st.button("🔄 Actualizar listado", key="refresh_no_entregados"):
        if allow_refresh("no_entregados_last_refresh"):
            cargar_pedidos.clear()
            st.toast("🔄 Datos de pedidos recargados")
            st.rerun()

    try:
        df_pedidos_no_entregados = cargar_pedidos()
    except Exception as e:
        st.error(f"❌ Error al cargar los pedidos: {e}")
        df_pedidos_no_entregados = pd.DataFrame()

    if df_pedidos_no_entregados.empty:
        st.info("No se encontraron pedidos para mostrar.")
    elif "Estado_Entrega" not in df_pedidos_no_entregados.columns:
        st.warning("La columna 'Estado_Entrega' no se encontró en los datos de pedidos.")
    else:
        df_pedidos_no_entregados = df_pedidos_no_entregados.copy()
        df_pedidos_no_entregados["Estado_Entrega"] = (
            df_pedidos_no_entregados["Estado_Entrega"].astype(str).str.strip()
        )
        filtro_no_entregados = df_pedidos_no_entregados["Estado_Entrega"] == "⏳ No Entregado"
        df_pedidos_no_entregados = df_pedidos_no_entregados[filtro_no_entregados].reset_index(drop=True)

        if df_pedidos_no_entregados.empty:
            st.success("🎉 No hay pedidos marcados como '⏳ No Entregado' en este momento.")
        else:
            columnas_tabla = [
                col
                for col in [
                    "ID_Pedido",
                    "Cliente",
                    "Tipo_Envio",
                    "Estado",
                    "Fecha_Entrega",
                    "Turno",
                    "Comprobante_Confirmado",
                ]
                if col in df_pedidos_no_entregados.columns
            ]

            if columnas_tabla:
                tabla_visual = df_pedidos_no_entregados[columnas_tabla].copy()
                if "Fecha_Entrega" in tabla_visual.columns:
                    tabla_visual["Fecha_Entrega"] = pd.to_datetime(
                        tabla_visual["Fecha_Entrega"], errors="coerce"
                    ).dt.strftime("%Y-%m-%d")
                st.dataframe(tabla_visual, use_container_width=True, hide_index=True)

            df_pedidos_no_entregados["display_label"] = df_pedidos_no_entregados.apply(
                lambda row: " - ".join(
                    [
                        (
                            str(row.get("Folio_Factura", "")).strip()
                            or str(row.get("ID_Pedido", "")).strip()
                            or "Sin folio"
                        ),
                        str(row.get("Cliente", "")).strip() or "Sin Cliente",
                        str(row.get("Tipo_Envio", "")).strip() or "Sin Tipo",
                    ]
                ),
                axis=1,
            )

            pedido_seleccionado_no_entregado = st.selectbox(
                "📋 Selecciona un pedido para actualizar la entrega",
                options=df_pedidos_no_entregados["display_label"].tolist(),
                key="select_pedido_no_entregado",
            )

            if pedido_seleccionado_no_entregado:
                pedido_fila = df_pedidos_no_entregados[
                    df_pedidos_no_entregados["display_label"] == pedido_seleccionado_no_entregado
                ].iloc[0]

                pedido_id = str(pedido_fila.get("ID_Pedido", "")).strip()
                pedido_folio = str(pedido_fila.get("Folio_Factura", "")).strip()
                folio_display = pedido_folio or pedido_id
                tipo_envio = str(pedido_fila.get("Tipo_Envio", "")).strip()
                fecha_actual = pd.to_datetime(pedido_fila.get("Fecha_Entrega"), errors="coerce")
                turno_actual = str(pedido_fila.get("Turno", "")).strip()

                st.markdown(
                    f"**Cliente:** {pedido_fila.get('Cliente', 'N/D')}  |  **Folio:** {folio_display or 'N/D'}"
                )
                st.markdown(
                    f"**Tipo de envío:** {tipo_envio or 'N/D'}  |  **Estado actual de entrega:** {pedido_fila.get('Estado_Entrega', 'N/D')}"
                )
                st.markdown(
                    f"**Fecha de entrega registrada:** {fecha_actual.date() if pd.notna(fecha_actual) else 'Sin fecha'}  |  **Turno registrado:** {turno_actual or 'Sin turno'}"
                )

                if tipo_envio != "📍 Pedido Local":
                    st.info("Solo se pueden actualizar fecha y turno para pedidos con tipo de envío '📍 Pedido Local'.")
                elif not pedido_id:
                    st.warning("El pedido seleccionado no tiene un 'ID_Pedido' válido para actualizar en Google Sheets.")
                else:
                    turno_options = [
                        "",
                        "🌙 Local Tarde",
                        "☀️ Local Mañana",
                        "📦 Pasa a Bodega",
                        "🌵 Saltillo",
                    ]
                    turno_normalization_map = {
                        "🌙 local tarde": "🌙 Local Tarde",
                        "local tarde": "🌙 Local Tarde",
                        "tarde": "🌙 Local Tarde",
                        "☀️ local mañana": "☀️ Local Mañana",
                        "local mañana": "☀️ Local Mañana",
                        "mañana": "☀️ Local Mañana",
                        "📦 pasa a bodega": "📦 Pasa a Bodega",
                        "pasa a bodega": "📦 Pasa a Bodega",
                        "en bodega": "📦 Pasa a Bodega",
                        "bodega": "📦 Pasa a Bodega",
                        "🌵 saltillo": "🌵 Saltillo",
                        "saltillo": "🌵 Saltillo",
                    }
                    turno_index = 0
                    turno_actual_key = turno_actual.lower()
                    if turno_actual_key == "nan":
                        turno_actual_key = ""
                    turno_actual_estandar = turno_normalization_map.get(
                        turno_actual_key, turno_actual
                    )
                    if turno_actual_estandar in turno_options:
                        turno_index = turno_options.index(turno_actual_estandar)

                    fecha_defecto = fecha_actual.date() if pd.notna(fecha_actual) else date.today()

                    with st.form(key=f"form_actualizar_entrega_{pedido_id}"):
                        nueva_fecha_entrega = st.date_input(
                            "Nueva fecha de entrega",
                            value=fecha_defecto,
                            key=f"fecha_no_entregado_{pedido_id}",
                        )
                        nuevo_turno = st.selectbox(
                            "Selecciona el turno",
                            turno_options,
                            index=turno_index,
                            format_func=lambda x: "Selecciona un turno" if x == "" else x,
                            key=f"turno_no_entregado_{pedido_id}",
                        )
                        submitted = st.form_submit_button("💾 Guardar cambios")

                    if submitted:
                        if nuevo_turno == "":
                            st.warning("Selecciona un turno para continuar.")
                        else:
                            worksheet = get_worksheet()
                            if worksheet is None:
                                st.error("❌ No se pudo acceder a la hoja de Google Sheets para actualizar el pedido.")
                            else:
                                headers = worksheet.row_values(1)
                                try:
                                    df_completo = cargar_pedidos()
                                except Exception as e:
                                    st.error(f"❌ No se pudieron recargar los pedidos desde Google Sheets: {e}")
                                    df_completo = pd.DataFrame()

                                if df_completo.empty or "ID_Pedido" not in df_completo.columns:
                                    st.error("❌ No se encontró la columna 'ID_Pedido' en los datos originales.")
                                elif pedido_id not in df_completo["ID_Pedido"].astype(str).str.strip().tolist():
                                    st.error("❌ No se encontró el pedido seleccionado en los datos originales.")
                                else:
                                    fila_filtrada = df_completo[
                                        df_completo["ID_Pedido"].astype(str).str.strip() == pedido_id
                                    ]
                                    if fila_filtrada.empty:
                                        st.error("❌ No se encontró el pedido seleccionado en la hoja de cálculo.")
                                    else:
                                        fila_original = fila_filtrada.iloc[0]
                                        gsheet_row_index = fila_filtrada.index[0] + 2

                                        updates = []
                                        missing_columns = []

                                        def agregar_actualizacion(columna: str, valor: str) -> None:
                                            if columna not in headers:
                                                missing_columns.append(columna)
                                                return
                                            updates.append(
                                                {
                                                    "range": rowcol_to_a1(
                                                        gsheet_row_index,
                                                        headers.index(columna) + 1,
                                                    ),
                                                    "values": [[valor]],
                                                }
                                            )

                                        fecha_formateada = (
                                            nueva_fecha_entrega.strftime("%Y-%m-%d")
                                            if isinstance(nueva_fecha_entrega, date)
                                            else ""
                                        )

                                        if fecha_formateada:
                                            fecha_existente = pd.to_datetime(
                                                fila_original.get("Fecha_Entrega"), errors="coerce"
                                            )
                                            fecha_existente_date = (
                                                fecha_existente.date() if pd.notna(fecha_existente) else None
                                            )
                                            if fecha_existente_date != nueva_fecha_entrega:
                                                agregar_actualizacion("Fecha_Entrega", fecha_formateada)

                                        turno_actual_estandar = turno_normalization_map.get(
                                            turno_actual.lower() if turno_actual else "",
                                            turno_actual,
                                        )
                                        if turno_actual_estandar == "nan":
                                            turno_actual_estandar = ""
                                        if (
                                            nuevo_turno
                                            and turno_actual_estandar != nuevo_turno
                                        ):
                                            agregar_actualizacion("Turno", nuevo_turno)

                                        comprobante_actual = str(
                                            fila_original.get("Comprobante_Confirmado", "")
                                        ).strip()
                                        comprobante_normalizado = unicodedata.normalize(
                                            "NFKD", comprobante_actual
                                        ).encode("ASCII", "ignore").decode("utf-8").lower()
                                        if comprobante_normalizado == "si":
                                            agregar_actualizacion("Comprobante_Confirmado", "")

                                        if missing_columns:
                                            st.warning(
                                                "No se pudieron actualizar algunas columnas porque no existen en la hoja: "
                                                + ", ".join(missing_columns)
                                            )

                                        if not updates:
                                            st.info("No hay cambios para guardar.")
                                        else:
                                            try:
                                                safe_batch_update(worksheet, updates)
                                                cargar_pedidos.clear()
                                                st.success("✅ Pedido actualizado correctamente.")
                                                st.rerun()
                                            except Exception as e:
                                                st.error(f"❌ Error al actualizar el pedido: {e}")

# --- TAB 7: DOWNLOAD DATA ---
with tab7:
    tab7_is_active = default_tab == 6
    if tab7_is_active:
        st.session_state["current_tab_index"] = 6
    st.header("⬇️ Descargar Datos de Pedidos")

    @st.cache_data(ttl=60)
    def cargar_todos_los_pedidos():
        worksheet = get_worksheet()
        headers = worksheet.row_values(1)
        if headers:
            df = pd.DataFrame(worksheet.get_all_records())
            if "Adjuntos_Guia" not in df.columns:
                df["Adjuntos_Guia"] = ""
            return df, headers
        return pd.DataFrame(), []

    try:
        df_all_pedidos, headers = cargar_todos_los_pedidos()
    
        if "Adjuntos_Guia" not in df_all_pedidos.columns:
            df_all_pedidos["Adjuntos_Guia"] = ""
    
        # 🧹 AÑADIDO: Filtrar filas donde 'Folio_Factura' y 'ID_Pedido' son ambos vacíos
        df_all_pedidos = df_all_pedidos.dropna(subset=['Folio_Factura', 'ID_Pedido'], how='all')
    
        # 🧹 Eliminar registros vacíos o inválidos con ID_Pedido en blanco, 'nan', 'N/A'
        df_all_pedidos = df_all_pedidos[
            df_all_pedidos['ID_Pedido'].astype(str).str.strip().ne('') &
            df_all_pedidos['ID_Pedido'].astype(str).str.lower().ne('n/a') &
            df_all_pedidos['ID_Pedido'].astype(str).str.lower().ne('nan')
        ]
    
        if 'Fecha_Entrega' in df_all_pedidos.columns:
            df_all_pedidos['Fecha_Entrega'] = pd.to_datetime(df_all_pedidos['Fecha_Entrega'], errors='coerce')
    
        if 'Vendedor_Registro' in df_all_pedidos.columns:
            df_all_pedidos['Vendedor_Registro'] = df_all_pedidos['Vendedor_Registro'].apply(
                lambda x: x if x in VENDEDORES_LIST else 'Otro/Desconocido' if pd.notna(x) and str(x).strip() != '' else 'N/A'
            ).astype(str)
        else:
            st.warning("La columna 'Vendedor_Registro' no se encontró en el Google Sheet para el filtrado. Asegúrate de que exista y esté correctamente nombrada.")
    
        if 'Folio_Factura' in df_all_pedidos.columns:
            df_all_pedidos['Folio_Factura'] = df_all_pedidos['Folio_Factura'].astype(str).replace('nan', '')
        else:
            st.warning("La columna 'Folio_Factura' no se encontró en el Google Sheet. No se podrá mostrar en la vista previa.")
    except Exception as e:
        st.error(f"❌ Error al cargar datos para descarga: {e}")
        st.info("Asegúrate de que la primera fila de tu Google Sheet contiene los encabezados esperados y que la API de Google Sheets está habilitada.")

    if df_all_pedidos.empty:
        st.info("No hay datos de pedidos para descargar.")
    else:
        st.markdown("---")
        st.subheader("Opciones de Filtro")

        time_filter = st.radio(
            "Selecciona un rango de tiempo:",
            ("Todos los datos", "Últimas 24 horas", "Últimos 7 días", "Últimos 30 días"),
            key="download_time_filter"
        )

        filtered_df_download = df_all_pedidos.copy()

        if time_filter != "Todos los datos" and 'Fecha_Entrega' in filtered_df_download.columns:
            current_time = datetime.now()
            # MODIFICATION 3: Convert Fecha_Entrega to date only for comparison
            filtered_df_download['Fecha_Solo_Fecha'] = filtered_df_download['Fecha_Entrega'].dt.date

            if time_filter == "Últimas 24 horas":
                start_datetime = current_time - timedelta(hours=24)
                filtered_df_download = filtered_df_download[filtered_df_download['Fecha_Entrega'] >= start_datetime]
            else:
                if time_filter == "Últimos 7 días":
                    start_date = current_time.date() - timedelta(days=7)
                elif time_filter == "Últimos 30 días":
                    start_date = current_time.date() - timedelta(days=30)

                filtered_df_download = filtered_df_download[filtered_df_download['Fecha_Solo_Fecha'] >= start_date]

            filtered_df_download = filtered_df_download.drop(columns=['Fecha_Solo_Fecha'])


        if 'Vendedor_Registro' in df_all_pedidos.columns:
            unique_vendedores_en_df = set(filtered_df_download['Vendedor_Registro'].unique())

            options_for_selectbox = ["Todos"]
            for vendedor_nombre in VENDEDORES_LIST:
                if vendedor_nombre in unique_vendedores_en_df:
                    options_for_selectbox.append(vendedor_nombre)

            if 'Otro/Desconocido' in unique_vendedores_en_df and 'Otro/Desconocido' not in options_for_selectbox:
                options_for_selectbox.append('Otro/Desconocido')

            if 'N/A' in unique_vendedores_en_df and 'N/A' not in options_for_selectbox:
                options_for_selectbox.append('N/A')

            selected_vendedor = st.selectbox(
                "Filtrar por Vendedor:",
                options=options_for_selectbox,
                key="download_vendedor_filter_tab6_final"
            )

            if selected_vendedor != "Todos":
                filtered_df_download = filtered_df_download[filtered_df_download['Vendedor_Registro'] == selected_vendedor]
        else:
            st.warning("La columna 'Vendedor_Registro' no está disponible en los datos cargados para aplicar este filtro. Por favor, asegúrate de que el nombre de la columna en tu Google Sheet sea 'Vendedor_Registro'.")

        if 'Tipo_Envio' in filtered_df_download.columns:
            unique_tipos_envio_download = [
                "Todos",
                "📍 Pedido Local",
                "🚚 Pedido Foráneo",
                "🎓 Cursos y Eventos",
                "🔁 Devolución",
                "🛠 Garantía",
            ]
            selected_tipo_envio_download = st.selectbox(
                "Filtrar por Tipo de Envío:",
                options=unique_tipos_envio_download,
                key="download_tipo_envio_filter"
            )
            if selected_tipo_envio_download != "Todos":
                filtered_df_download = filtered_df_download[filtered_df_download['Tipo_Envio'] == selected_tipo_envio_download]
        else:
            st.warning("La columna 'Tipo_Envio' no se encontró para aplicar el filtro de tipo de envío.")


        if 'Estado' in filtered_df_download.columns:
            unique_estados = ["Todos"] + list(filtered_df_download['Estado'].dropna().unique())
            selected_estado = st.selectbox("Filtrar por Estado:", unique_estados, key="download_estado_filter_tab6")
            if selected_estado != "Todos":
                filtered_df_download = filtered_df_download[filtered_df_download['Estado'] == selected_estado]

        st.markdown("---")
        st.subheader("Vista Previa de Datos a Descargar")

        # MODIFICATION 3: Format 'Fecha_Entrega' for display
        columnas_excluidas_preview = [
            "ID_Pedido", "Adjuntos", "Adjuntos_Surtido", "Adjuntos_Guia",
            "Completados_Limpiado", "Fecha_Pago_Comprobante",
            "Terminal", "Banco_Destino_Pago", "Forma_Pago_Comprobante",
            "Monto_Comprobante", "Referencia_Comprobante"
        ]
        columnas_preview = [col for col in filtered_df_download.columns if col not in columnas_excluidas_preview]
        display_df = filtered_df_download[columnas_preview].copy()
                
        if 'Fecha_Entrega' in display_df.columns:
            display_df['Fecha_Entrega'] = display_df['Fecha_Entrega'].dt.strftime('%Y-%m-%d')

        st.dataframe(display_df, use_container_width=True, hide_index=True)

        if not filtered_df_download.empty:
            output = BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                # Crear copia para exportar solo columnas seguras
                columnas_excluidas = [
                    "ID_Pedido", "Adjuntos", "Adjuntos_Surtido", "Adjuntos_Guia",
                    "Completados_Limpiado", "Fecha_Pago_Comprobante",
                    "Terminal", "Banco_Destino_Pago", "Forma_Pago_Comprobante",
                    "Monto_Comprobante", "Referencia_Comprobante"
                ]
                columnas_finales = [col for col in filtered_df_download.columns if col not in columnas_excluidas]

                excel_df = filtered_df_download[columnas_finales].copy()

                # Convertir fechas a texto legible
                for col in excel_df.columns:
                    if "fecha" in col.lower() or "Fecha" in col:
                        excel_df[col] = pd.to_datetime(excel_df[col], errors='coerce').dt.strftime('%Y-%m-%d')


                # Asegúrate de que las fechas estén en formato string
                for fecha_col in ['Fecha_Entrega', 'Fecha_Pago_Comprobante']:
                    if fecha_col in excel_df.columns:
                        excel_df[fecha_col] = pd.to_datetime(excel_df[fecha_col], errors='coerce').dt.strftime('%Y-%m-%d')

                excel_df.to_excel(writer, index=False, sheet_name='Pedidos_Filtrados')

            processed_data = output.getvalue()

            st.download_button(
                label="📥 Descargar Excel Filtrado",
                data=processed_data,
                file_name=f"pedidos_filtrados_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                help="Haz clic para descargar los datos de la tabla mostrada arriba en formato Excel."
            )
        else:
            st.info("No hay datos que coincidan con los filtros seleccionados para descargar.")
# --- TAB 8: SEARCH ORDER ---
with tab8:
    tab8_is_active = default_tab == 7
    if tab8_is_active:
        st.session_state["current_tab_index"] = 7
    st.subheader("🔍 Buscador de Pedidos por Guía o Cliente")

    modo_busqueda = st.radio(
        "Selecciona el modo de búsqueda:",
        ["🔢 Por número de guía", "🧑 Por cliente/factura"],
        key="tab8_modo_busqueda_radio"
    )

    if modo_busqueda == "🔢 Por número de guía":
        keyword = st.text_input(
            "📦 Ingresa una palabra clave, número de guía, fragmento o código a buscar:",
            key="tab8_keyword_guia"
        )
        buscar_btn = st.button("🔎 Buscar", key="tab8_btn_buscar_guia")
    else:
        keyword = st.text_input(
            "🧑 Ingresa el nombre del cliente o el folio de la factura a buscar (sin importar mayúsculas ni acentos para el cliente):",
            key="tab8_keyword_cliente"
        )
        buscar_btn = st.button("🔍 Buscar Pedido por Cliente o Folio", key="tab8_btn_buscar_cliente")

    if buscar_btn:
        if modo_busqueda == "🔢 Por número de guía":
            st.info("🔄 Buscando, por favor espera... puede tardar unos segundos...")

        resultados = []

        # ====== Siempre cargamos pedidos (datos_pedidos) ======
        df_pedidos = cargar_pedidos()
        if 'Hora_Registro' in df_pedidos.columns:
            df_pedidos['Hora_Registro'] = pd.to_datetime(df_pedidos['Hora_Registro'], errors='coerce')
            df_pedidos = df_pedidos.sort_values(by='Hora_Registro', ascending=False).reset_index(drop=True)

        # ====== BÚSQUEDA POR CLIENTE: también en casos_especiales ======
        if modo_busqueda == "🧑 Por cliente/factura":
            criterio = keyword.strip()
            if not criterio:
                st.warning("⚠️ Ingresa un cliente o folio de factura.")
                st.stop()

            cliente_normalizado = normalizar(criterio)
            criterio_minusculas = criterio.lower()

            # 1) datos_pedidos (S3 + archivos)
            for _, row in df_pedidos.iterrows():
                nombre = str(row.get("Cliente", "")).strip()
                folio_factura = str(row.get("Folio_Factura", "")).strip()
                folio_factura = "" if folio_factura.lower() == "nan" else folio_factura
                folio_factura_minusculas = folio_factura.lower()
                if not nombre and not folio_factura:
                    continue
                nombre_normalizado = normalizar(nombre) if nombre else ""
                coincide_cliente = bool(cliente_normalizado) and cliente_normalizado in nombre_normalizado
                coincide_folio = bool(criterio_minusculas) and criterio_minusculas == folio_factura_minusculas
                if not coincide_cliente and not coincide_folio:
                    continue

                pedido_id = str(row.get("ID_Pedido", "")).strip()
                if not pedido_id:
                    continue

                prefix = obtener_prefijo_s3(pedido_id)
                todos_los_archivos = obtener_todos_los_archivos(prefix) if prefix else []

                comprobantes = [f for f in todos_los_archivos if "comprobante" in f["Key"].lower()]
                facturas = [f for f in todos_los_archivos if "factura" in f["Key"].lower()]
                otros = [f for f in todos_los_archivos if f not in comprobantes and f not in facturas]

                resultados.append({
                    "__source": "pedidos",
                    "ID_Pedido": pedido_id,
                    "Cliente": row.get("Cliente", ""),
                    "Estado": row.get("Estado", ""),
                    "Tipo_Envio": row.get("Tipo_Envio", ""),
                    "Turno": row.get("Turno", ""),
                    "Estado_Entrega": row.get("Estado_Entrega", ""),
                    "Vendedor": row.get("Vendedor_Registro", ""),
                    "ID_Vendedor": extract_id_vendedor(row, ""),
                    "Folio": row.get("Folio_Factura", ""),
                    "Motivo_NotaVenta": row.get("Motivo_NotaVenta", ""),
                    "Hora_Registro": row.get("Hora_Registro", ""),
                    "Seguimiento": row.get("Seguimiento", ""),
                    # 🛠 Modificación de surtido
                    "Modificacion_Surtido": str(row.get("Modificacion_Surtido", "")).strip(),
                    "Adjuntos_Surtido_urls": partir_urls(row.get("Adjuntos_Surtido", "")),
                    # ♻️ Refacturación
                    "Refacturacion_Tipo": str(row.get("Refacturacion_Tipo","")).strip(),
                    "Refacturacion_Subtipo": str(row.get("Refacturacion_Subtipo","")).strip(),
                    "Folio_Factura_Refacturada": str(row.get("Folio_Factura_Refacturada","")).strip(),
                    # Archivos S3
                    "Coincidentes": [],
                    "Comprobantes": [(f["Key"], generar_url_s3(f["Key"])) for f in comprobantes],
                    "Facturas": [(f["Key"], generar_url_s3(f["Key"])) for f in facturas],
                    "Otros": [(f["Key"], generar_url_s3(f["Key"])) for f in otros],
                })

            # 2) casos_especiales
            df_casos = cargar_casos_especiales()  # << garantiza Numero_Serie y Fecha_Compra
            if "Hora_Registro" in df_casos.columns:
                df_casos["Hora_Registro"] = pd.to_datetime(df_casos["Hora_Registro"], errors="coerce")

            for _, row in df_casos.iterrows():
                nombre = str(row.get("Cliente", "")).strip()
                folio_factura = str(row.get("Folio_Factura", "")).strip()
                folio_factura = "" if folio_factura.lower() == "nan" else folio_factura
                folio_factura_minusculas = folio_factura.lower()
                if not nombre and not folio_factura:
                    continue
                nombre_normalizado = normalizar(nombre) if nombre else ""
                coincide_cliente = bool(cliente_normalizado) and cliente_normalizado in nombre_normalizado
                coincide_folio = bool(criterio_minusculas) and criterio_minusculas == folio_factura_minusculas
                if not coincide_cliente and not coincide_folio:
                    continue

                resultados.append({
                    "__source": "casos",
                    "ID_Pedido": str(row.get("ID_Pedido","")).strip(),
                    "Cliente": row.get("Cliente",""),
                    "Vendedor": row.get("Vendedor_Registro",""),
                    "ID_Vendedor": extract_id_vendedor(row, ""),
                    # Folios
                    "Folio": row.get("Folio_Factura",""),
                    "Folio_Factura_Error": row.get("Folio_Factura_Error",""),
                    "Motivo_NotaVenta": row.get("Motivo_NotaVenta", ""),
                    "Hora_Registro": row.get("Hora_Registro",""),
                    "Tipo_Envio": row.get("Tipo_Envio",""),
                    "Estado": row.get("Estado",""),
                    "Estado_Caso": row.get("Estado_Caso",""),
                    "Seguimiento": row.get("Seguimiento",""),
                    # ♻️ Refacturación
                    "Refacturacion_Tipo": str(row.get("Refacturacion_Tipo","")).strip(),
                    "Refacturacion_Subtipo": str(row.get("Refacturacion_Subtipo","")).strip(),
                    "Folio_Factura_Refacturada": str(row.get("Folio_Factura_Refacturada","")).strip(),
                    # Detalle
                    "Resultado_Esperado": row.get("Resultado_Esperado",""),
                    "Material_Devuelto": row.get("Material_Devuelto",""),
                    "Monto_Devuelto": row.get("Monto_Devuelto",""),
                    "Motivo_Detallado": row.get("Motivo_Detallado",""),
                    "Area_Responsable": row.get("Area_Responsable",""),
                    "Nombre_Responsable": row.get("Nombre_Responsable",""),
                    "Numero_Cliente_RFC": row.get("Numero_Cliente_RFC",""),
                    "Tipo_Envio_Original": row.get("Tipo_Envio_Original",""),
                    "Fecha_Entrega": row.get("Fecha_Entrega",""),
                    "Fecha_Recepcion_Devolucion": row.get("Fecha_Recepcion_Devolucion",""),
                    "Estado_Recepcion": row.get("Estado_Recepcion",""),
                    "Nota_Credito_URL": row.get("Nota_Credito_URL",""),
                    "Documento_Adicional_URL": row.get("Documento_Adicional_URL",""),
                    "Comentarios_Admin_Devolucion": row.get("Comentarios_Admin_Devolucion",""),
                    "Turno": row.get("Turno",""),
                    "Hora_Proceso": row.get("Hora_Proceso",""),
                    # 🛠 Modificación de surtido
                    "Modificacion_Surtido": str(row.get("Modificacion_Surtido","")).strip(),
                    "Adjuntos_Surtido_urls": partir_urls(row.get("Adjuntos_Surtido","")),
                    # Archivos del caso
                    "Adjuntos_urls": partir_urls(row.get("Adjuntos", "")),
                    "Guia_url": str(row.get("Hoja_Ruta_Mensajero", "")).strip(),

                    # ⭐⭐⭐ NUEVO: Campos de Garantía ⭐⭐⭐
                    "Numero_Serie": row.get("Numero_Serie",""),
                    "Fecha_Compra": row.get("Fecha_Compra","") or row.get("FechaCompra",""),
                })

        # ====== BÚSQUEDA POR NÚMERO DE GUÍA ======
        elif modo_busqueda == "🔢 Por número de guía":
            clave = keyword.strip()
            if not clave:
                st.warning("⚠️ Ingresa una palabra clave o número de guía.")
                st.stop()

            for _, row in df_pedidos.iterrows():
                pedido_id = str(row.get("ID_Pedido", "")).strip()
                if not pedido_id:
                    continue

                prefix = obtener_prefijo_s3(pedido_id)
                if not prefix:
                    continue

                archivos_validos = obtener_archivos_pdf_validos(prefix)
                archivos_coincidentes = []

                for archivo in archivos_validos:
                    key = archivo["Key"]
                    texto = extraer_texto_pdf(key)

                    clave_sin_espacios = clave.replace(" ", "")
                    texto_limpio = texto.replace(" ", "").replace("\n", "")

                    coincide = (
                        clave in texto
                        or clave_sin_espacios in texto_limpio
                        or re.search(re.escape(clave), texto_limpio)
                        or re.search(re.escape(clave_sin_espacios), texto_limpio)
                    )

                    if coincide:
                        waybill_match = re.search(r"WAYBILL[\s:]*([0-9 ]{8,})", texto, re.IGNORECASE)
                        if waybill_match:
                            st.code(f"📦 WAYBILL detectado: {waybill_match.group(1)}")

                        archivos_coincidentes.append((key, generar_url_s3(key)))
                        todos_los_archivos = obtener_todos_los_archivos(prefix)
                        comprobantes = [f for f in todos_los_archivos if "comprobante" in f["Key"].lower()]
                        facturas = [f for f in todos_los_archivos if "factura" in f["Key"].lower()]
                        otros = [f for f in todos_los_archivos if f not in comprobantes and f not in facturas and f["Key"] != archivos_coincidentes[0][0]]

                        resultados.append({
                            "__source": "pedidos",
                            "ID_Pedido": pedido_id,
                            "Cliente": row.get("Cliente", ""),
                            "Estado": row.get("Estado", ""),
                            "Tipo_Envio": row.get("Tipo_Envio", ""),
                            "Turno": row.get("Turno", ""),
                            "Estado_Entrega": row.get("Estado_Entrega", ""),
                            "Vendedor": row.get("Vendedor_Registro", ""),
                            "ID_Vendedor": extract_id_vendedor(row, ""),
                            "Folio": row.get("Folio_Factura", ""),
                            "Hora_Registro": row.get("Hora_Registro", ""),
                            "Seguimiento": row.get("Seguimiento", ""),
                            # 🛠 Modificación de surtido
                            "Modificacion_Surtido": str(row.get("Modificacion_Surtido", "")).strip(),
                            "Adjuntos_Surtido_urls": partir_urls(row.get("Adjuntos_Surtido", "")),
                            # ♻️ Refacturación
                            "Refacturacion_Tipo": str(row.get("Refacturacion_Tipo","")).strip(),
                            "Refacturacion_Subtipo": str(row.get("Refacturacion_Subtipo","")).strip(),
                            "Folio_Factura_Refacturada": str(row.get("Folio_Factura_Refacturada","")).strip(),
                            # Archivos S3
                            "Coincidentes": archivos_coincidentes,
                            "Comprobantes": [(f["Key"], generar_url_s3(f["Key"])) for f in comprobantes],
                            "Facturas": [(f["Key"], generar_url_s3(f["Key"])) for f in facturas],
                            "Otros": [(f["Key"], generar_url_s3(f["Key"])) for f in otros],
                        })
                        break  # detener búsqueda tras encontrar coincidencia
                else:
                    continue  # ningún PDF coincidió

                break  # Solo un pedido en búsqueda por guía

        # ====== RENDER DE RESULTADOS ======
        st.markdown("---")
        if resultados:
            st.success(f"✅ Se encontraron coincidencias en {len(resultados)} registro(s).")

            # Ordena por Hora_Registro descendente cuando exista
            def _parse_dt(v):
                try:
                    return pd.to_datetime(v)
                except Exception:
                    return pd.NaT
            resultados = sorted(resultados, key=lambda r: _parse_dt(r.get("Hora_Registro")), reverse=True)

            for res in resultados:
                id_vendedor_segment = format_id_vendedor_with_mod(res)
                if res.get("__source") == "casos":
                    # ---------- Render de CASOS ESPECIALES (solo lectura) ----------
                    titulo = f"🧾 Caso Especial – {res.get('Tipo_Envio','') or 'N/A'}"
                    st.markdown(f"### {titulo}")

                    # Folio Nuevo / Folio Error para Devoluciones
                    is_devolucion = (str(res.get('Tipo_Envio','')).strip() == "🔁 Devolución")
                    if is_devolucion:
                        folio_nuevo = res.get("Folio","") or "N/A"
                        folio_error = res.get("Folio_Factura_Error","") or "N/A"
                        st.markdown(
                            f"📄 **Folio Nuevo:** `{folio_nuevo}`  |  📄 **Folio Error:** `{folio_error}`  |  "
                            f"🧑‍💼 **Vendedor:** `{res.get('Vendedor','') or 'N/A'}`  |  "
                            f"{id_vendedor_segment}  |  "
                            f"🕒 **Hora:** `{res.get('Hora_Registro','') or 'N/A'}`"
                        )
                    else:
                        st.markdown(
                            f"📄 **Folio:** `{res.get('Folio','') or 'N/A'}`  |  "
                            f"🧑‍💼 **Vendedor:** `{res.get('Vendedor','') or 'N/A'}`  |  "
                            f"{id_vendedor_segment}  |  "
                            f"🕒 **Hora:** `{res.get('Hora_Registro','') or 'N/A'}`"
                        )

                        # ⭐⭐⭐ NUEVO: Mostrar datos de Garantía ⭐⭐⭐
                        if str(res.get("Tipo_Envio","")).strip() == "🛠 Garantía":
                            num_serie = str(res.get("Numero_Serie") or "").strip()
                            fec_compra = str(res.get("Fecha_Compra") or "").strip()
                            if num_serie or fec_compra:
                                st.markdown("**🧷 Datos de compra y serie (Garantía):**")
                                st.markdown(f"- **Número de serie / lote:** `{num_serie or 'N/A'}`")
                                st.markdown(f"- **Fecha de compra:** `{fec_compra or 'N/A'}`")

                    st.markdown(
                        f"**👤 Cliente:** {res.get('Cliente','N/A')}  |  **RFC:** {res.get('Numero_Cliente_RFC','') or 'N/A'}"
                    )
                    estado = res.get('Estado','') or 'N/A'
                    estado_caso = res.get('Estado_Caso','') or 'N/A'
                    turno = res.get('Turno','') or 'N/A'
                    turno_line = f"**Turno:** {turno}"
                    tipo_envio = str(res.get('Tipo_Envio','')).strip()
                    if tipo_envio in ["🔁 Devolución", "🛠 Garantía"]:
                        tipo_orig = res.get('Tipo_Envio_Original','') or 'N/A'
                        turno_line += f" | **Tipo Envío Original:** {tipo_orig}"
                    st.markdown(
                        f"**Estado:** {estado}  |  **Estado del Caso:** {estado_caso}  |  {turno_line}"
                    )
                    st.markdown(f"**📌 Seguimiento:** {res.get('Seguimiento','N/A')}")

                    # ♻️ Refacturación (si hay)
                    ref_t = res.get("Refacturacion_Tipo","")
                    ref_st = res.get("Refacturacion_Subtipo","")
                    ref_f = res.get("Folio_Factura_Refacturada","")
                    if any([ref_t, ref_st, ref_f]):
                        st.markdown("**♻️ Refacturación:**")
                        st.markdown(f"- **Tipo:** {ref_t or 'N/A'}")
                        st.markdown(f"- **Subtipo:** {ref_st or 'N/A'}")
                        st.markdown(f"- **Folio refacturado:** {ref_f or 'N/A'}")

                    if str(res.get("Resultado_Esperado","")).strip():
                        st.markdown(f"**🎯 Resultado Esperado:** {res.get('Resultado_Esperado','')}")
                    if str(res.get("Motivo_Detallado","")).strip():
                        st.markdown("**📝 Motivo / Descripción:**")
                        st.info(str(res.get("Motivo_Detallado","")).strip())
                    if str(res.get("Material_Devuelto","")).strip():
                        st.markdown("**📦 Piezas / Material:**")
                        st.info(str(res.get("Material_Devuelto","")).strip())
                    if str(res.get("Monto_Devuelto","")).strip():
                        st.markdown(f"**💵 Monto (dev./estimado):** {res.get('Monto_Devuelto','')}")

                    st.markdown(
                        f"**🏢 Área Responsable:** {res.get('Area_Responsable','') or 'N/A'}  |  **👥 Responsable del Error:** {res.get('Nombre_Responsable','') or 'N/A'}"
                    )
                    st.markdown(
                        f"**📅 Fecha Entrega/Cierre (si aplica):** {res.get('Fecha_Entrega','') or 'N/A'}  |  "
                        f"**📅 Recepción:** {res.get('Fecha_Recepcion_Devolucion','') or 'N/A'}  |  "
                        f"**📦 Recepción:** {res.get('Estado_Recepcion','') or 'N/A'}"
                    )
                    nota_url = __s(res.get('Nota_Credito_URL',''))
                    docad_url = __s(res.get('Documento_Adicional_URL',''))
                    if __has(nota_url):
                        if __is_url(nota_url):
                            st.markdown(f"**🧾 Nota de Crédito:** {__link(nota_url, 'Nota de Crédito')}")
                            add_url_preview_expander(nota_url, "Nota de Crédito")
                        else:
                            st.markdown(f"**🧾 Nota de Crédito:** {nota_url}")
                    else:
                        st.markdown("**🧾 Nota de Crédito:** N/A")

                    if __has(docad_url):
                        if __is_url(docad_url):
                            st.markdown(f"**📂 Documento Adicional:** {__link(docad_url, 'Documento Adicional')}")
                            add_url_preview_expander(docad_url, "Documento Adicional")
                        else:
                            st.markdown(f"**📂 Documento Adicional:** {docad_url}")
                    else:
                        st.markdown("**📂 Documento Adicional:** N/A")
                    if str(res.get("Comentarios_Admin_Devolucion","")).strip():
                        st.markdown("**🗒️ Comentario Administrativo:**")
                        st.info(str(res.get("Comentarios_Admin_Devolucion","")).strip())

                    # 🛠 Modificación de surtido (si existe)
                    mod_txt = res.get("Modificacion_Surtido", "") or ""
                    mod_urls = res.get("Adjuntos_Surtido_urls", []) or []
                    if mod_txt or mod_urls:
                        st.markdown("#### 🛠 Modificación de surtido")
                        if mod_txt:
                            st.info(mod_txt)
                        if mod_urls:
                            st.markdown("**Archivos de modificación:**")
                            for u in mod_urls:
                                render_attachment_link(u)

                    with st.expander("📎 Archivos (Adjuntos y Guía)", expanded=False):
                        adj = res.get("Adjuntos_urls", []) or []
                        guia = res.get("Guia_url", "")
                        if adj:
                            st.markdown("**Adjuntos:**")
                            for u in adj:
                                render_attachment_link(u)
                        if guia and guia.lower() not in ("nan","none","n/a"):
                            st.markdown("**Guía:**")
                            render_attachment_link(guia, "Abrir guía")
                        if not adj and not guia:
                            st.info("Sin archivos registrados en la hoja.")

                    st.markdown("---")

                else:
                    # ---------- Render de PEDIDOS ----------
                    st.markdown(f"### 🤝 {res['Cliente'] or 'Cliente N/D'}")
                    st.markdown(
                        f"📄 **Folio:** `{res['Folio'] or 'N/D'}`  |  🔍 **Estado:** `{res['Estado'] or 'N/D'}`  |  "
                        f"🧑‍💼 **Vendedor:** `{res['Vendedor'] or 'N/D'}`  |  "
                        f"{id_vendedor_segment}  |  "
                        f"🕒 **Hora:** `{res['Hora_Registro'] or 'N/D'}`"
                    )
                    if res.get("Tipo_Envio") == "📍 Pedido Local":
                        turno_local = normalize_case_text(res.get("Turno"), "N/A")
                        estado_entrega_local = format_estado_entrega(res.get("Estado_Entrega"))
                        st.markdown(
                            f"**📍 Pedido Local:** Turno `{turno_local}`  |  Estado_Entrega `{estado_entrega_local}`"
                        )
                    st.markdown(f"**📌 Seguimiento:** {res.get('Seguimiento','N/A')}")

                    # ♻️ Refacturación (si hay)
                    ref_t = res.get("Refacturacion_Tipo","")
                    ref_st = res.get("Refacturacion_Subtipo","")
                    ref_f = res.get("Folio_Factura_Refacturada","")
                    if any([ref_t, ref_st, ref_f]):
                        with st.expander("♻️ Refacturación", expanded=False):
                            st.markdown(f"- **Tipo:** {ref_t or 'N/A'}")
                            st.markdown(f"- **Subtipo:** {ref_st or 'N/A'}")
                            st.markdown(f"- **Folio refacturado:** {ref_f or 'N/A'}")

                    with st.expander("📁 Archivos del Pedido", expanded=True):
                        if res.get("Coincidentes"):
                            st.markdown("#### 🔍 Guías:")
                            for key, url in res["Coincidentes"]:
                                nombre = key.split("/")[-1]
                                render_attachment_link(url, f"🔍 {nombre}")
                        if res.get("Comprobantes"):
                            st.markdown("#### 🧾 Comprobantes:")
                            for key, url in res["Comprobantes"]:
                                nombre = key.split("/")[-1]
                                render_attachment_link(url, f"📄 {nombre}")
                        if res.get("Facturas"):
                            st.markdown("#### 📁 Facturas:")
                            for key, url in res["Facturas"]:
                                nombre = key.split("/")[-1]
                                render_attachment_link(url, f"📄 {nombre}")
                        if res.get("Otros"):
                            st.markdown("#### 📂 Otros Archivos:")
                            for key, url in res["Otros"]:
                                nombre = key.split("/")[-1]
                                render_attachment_link(url, f"📌 {nombre}")

                        # 🛠 Modificación de surtido (si existe)
                        mod_txt = res.get("Modificacion_Surtido", "") or ""
                        mod_urls = res.get("Adjuntos_Surtido_urls", []) or []
                        if mod_txt or mod_urls:
                            st.markdown("#### 🛠 Modificación de surtido")
                            if mod_txt:
                                st.info(mod_txt)
                            if mod_urls:
                                st.markdown("**Archivos de modificación:**")
                                for u in mod_urls:
                                    render_attachment_link(u)

        else:
            mensaje = (
                "⚠️ No se encontraron coincidencias en ningún archivo PDF."
                if modo_busqueda == "🔢 Por número de guía"
                else "⚠️ No se encontraron pedidos o casos para el cliente o folio ingresado."
            )
            st.warning(mensaje)
