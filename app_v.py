import streamlit as st
import os
from datetime import datetime, timedelta
import json
import uuid
import pandas as pd
import pdfplumber
import unicodedata
from io import BytesIO
import time
import re
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pytz import timezone


# NEW: Import boto3 for AWS S3
import boto3

# --- STREAMLIT CONFIGURATION ---
st.set_page_config(page_title="App Vendedores TD", layout="wide")

if st.button("🔄 Recargar Página y Conexión", help="Haz clic aquí si algo no carga o da error de Google Sheets."):
    st.cache_data.clear()
    st.cache_resource.clear()
    st.rerun()


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

    try:
        client = try_get_client()
        _ = client.open_by_key(GOOGLE_SHEET_ID)
        return client
    except gspread.exceptions.APIError as e:
        if "RESOURCE_EXHAUSTED" in str(e) or "expired" in str(e).lower():
            st.warning("🔁 Token expirado o cuota alcanzada. Reintentando con nuevo cliente...")
            st.cache_resource.clear()
            time.sleep(2)

            try:
                client = try_get_client()
                _ = client.open_by_key(GOOGLE_SHEET_ID)
                return client
            except Exception as e2:
                st.error(f"❌ Falló la reconexión con Google Sheets: {e2}")
                st.stop()
        else:
            st.error(f"❌ Error al conectar con Google Sheets: {e}")
            st.stop()

@st.cache_resource
def get_worksheet():
    client = get_google_sheets_client()
    spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)
    return spreadsheet.worksheet("datos_pedidos")

# ✅ Cliente listo para usar en cualquier parte
g_spread_client = get_google_sheets_client()


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


st.title("🛒 App de Vendedores TD")
st.write("¡Bienvenido! Aquí puedes registrar y gestionar tus pedidos.")

# --- AUTHENTICATION AND CLIENT FUNCTIONS ---

# Removed the old load_credentials_from_file and get_gspread_client functions
# as they are replaced by get_google_sheets_client()

# NEW: Function to upload files to AWS S3
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
        return s3
    except Exception as e:
        st.error(f"❌ Error al inicializar el cliente S3: {e}")
        st.stop()

def upload_file_to_s3(s3_client, bucket_name, file_obj, s3_key):
    """
    Sube un archivo a un bucket de S3.

    Args:
        s3_client: El cliente S3 inicializado.
        bucket_name: El nombre del bucket S3.
        file_obj: El objeto de archivo cargado por st.file_uploader.
        s3_key: La ruta completa y nombre del archivo en S3 (ej. 'pedido_id/filename.pdf').

    Returns:
        tuple: (True, URL del archivo) si tiene éxito, (False, None) en caso de error.
    """
    try:
        # Asegúrate de que el puntero del archivo esté al principio
        file_obj.seek(0)
        s3_client.upload_fileobj(file_obj, bucket_name, s3_key)
        file_url = f"https://{bucket_name}.s3.{AWS_REGION}.amazonaws.com/{s3_key}"
        return True, file_url
    except Exception as e:
        st.error(f"❌ Error al subir el archivo '{s3_key}' a S3: {e}")
        return False, None
    
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
    
@st.cache_data(ttl=300)
def cargar_pedidos():
    sheet = g_spread_client.open_by_key("1aWkSelodaz0nWfQx7FZAysGnIYGQFJxAN7RO3YgCiZY").worksheet("datos_pedidos")
    data = sheet.get_all_records()
    return pd.DataFrame(data)

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

# --- Initialize Gspread Client and S3 Client ---
# NEW: Initialize gspread client using the new function
g_spread_client = get_google_sheets_client()
s3_client = get_s3_client() # Initialize S3 client

# Removed the old try-except block for client initialization

# --- Tab Definition ---
tabs_labels = [
    "🛒 Registrar Nuevo Pedido",
    "✏️ Modificar Pedido Existente",
    "🧾 Pedidos Pendientes de Comprobante",
    "📦 Guías Cargadas",
    "⬇️ Descargar Datos",
    "🔍 Buscar Pedido"
]

# Leer índice de pestaña desde los parámetros de la URL
params = st.query_params
active_tab_index = int(params.get("tab", ["0"])[0])

# Crear pestañas y mantener referencia
tabs = st.tabs(tabs_labels)
tab1, tab2, tab3, tab4, tab5, tab6 = tabs

# --- List of Vendors (reusable and explicitly alphabetically sorted) ---
VENDEDORES_LIST = sorted([
    "ALEJANDRO RODRIGUEZ",
    "ANA KAREN ORTEGA MAHUAD",
    "DANIELA LOPEZ RAMIREZ",
    "EDGAR ORLANDO GOMEZ VILLAGRAN",
    "GLORIA MICHELLE GARCIA TORRES", 
    "GRISELDA CAROLINA SANCHEZ GARCIA",
    "HECTOR DEL ANGEL AREVALO ALCALA",
    "JOSELIN TRUJILLO PATRACA",
    "NORA ALEJANDRA MARTINEZ MORENO",
    "PAULINA TREJO"
])


# Initialize session state for vendor
if 'last_selected_vendedor' not in st.session_state:
    st.session_state.last_selected_vendedor = VENDEDORES_LIST[0] if VENDEDORES_LIST else ""
# --- TAB 1: REGISTER NEW ORDER ---
with tab1:
    st.header("📝 Nuevo Pedido")
    # ✅ Mostrar mensaje persistente si se acaba de registrar un pedido
    if "success_pedido_registrado" in st.session_state:
        st.success(f"🎉 Pedido {st.session_state['success_pedido_registrado']} registrado con éxito.")
        if "success_adjuntos" in st.session_state and st.session_state["success_adjuntos"]:
            st.info("📎 Archivos subidos: " + ", ".join(os.path.basename(u) for u in st.session_state["success_adjuntos"]))
        st.balloons()
        del st.session_state["success_pedido_registrado"]
        if "success_adjuntos" in st.session_state:
            del st.session_state["success_adjuntos"]


    tipo_envio = st.selectbox(
        "📦 Tipo de Envío",
        ["🚚 Pedido Foráneo", "📍 Pedido Local", "🔁 Devolución", "🛠 Garantía"],
        index=0,
        key="tipo_envio_selector_global"
    )

    subtipo_local = ""
    if tipo_envio == "📍 Pedido Local":
        st.markdown("---")
        st.subheader("⏰ Detalle de Pedido Local")
        subtipo_local = st.selectbox(
            "Turno/Locales",
            ["☀️ Local Mañana", "🌙 Local Tarde", "🌵 Saltillo", "📦 Pasa a Bodega"],
            index=0,
            help="Selecciona el turno o tipo de entrega para pedidos locales."
        )

    with st.form(key="new_pedido_form", clear_on_submit=True):
        st.markdown("---")
        st.subheader("Información Básica del Cliente y Pedido")

        try:
            initial_vendedor_index = VENDEDORES_LIST.index(st.session_state.last_selected_vendedor)
        except ValueError:
            initial_vendedor_index = 0

        vendedor = st.selectbox("👤 Vendedor", VENDEDORES_LIST, index=initial_vendedor_index)
        if vendedor != st.session_state.last_selected_vendedor:
            st.session_state.last_selected_vendedor = vendedor

        registro_cliente = st.text_input("🤝 Cliente")
        folio_factura = st.text_input("📄 Folio de Factura")
        fecha_entrega = st.date_input("🗓 Fecha de Entrega Requerida", datetime.now().date())
        comentario = st.text_area("💬 Comentario / Descripción Detallada")

        st.markdown("---")
        st.subheader("📎 Adjuntos del Pedido")
        uploaded_files = st.file_uploader(
            "Sube archivos del pedido",
            type=["pdf", "jpg", "jpeg", "png", "xlsx", "docx"],
            accept_multiple_files=True
        )

        submit_button = st.form_submit_button("✅ Registrar Pedido")

    # --- Estado de pago después del formulario ---
    st.markdown("---")
    st.subheader("💰 Estado de Pago")
    estado_pago = st.selectbox("Estado de Pago", ["🔴 No Pagado", "✅ Pagado"], index=0, key="estado_pago")

    comprobante_pago_files = None
    fecha_pago = None
    forma_pago = ""
    terminal = ""
    banco_destino = ""
    monto_pago = 0.0
    referencia_pago = ""

    if estado_pago == "✅ Pagado":
        pago_doble = st.checkbox("✅ Pago en dos partes distintas")

        if not pago_doble:
            comprobante_pago_files = st.file_uploader(
                "💲 Comprobante(s) de Pago",
                type=["pdf", "jpg", "jpeg", "png"],
                accept_multiple_files=True,
                key="comprobante_uploader_final"
            )
            st.info("⚠️ El comprobante es obligatorio si el estado es 'Pagado'.")

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
                        terminal = st.selectbox("🏧 Terminal", ["BANORTE", "AFIRME", "VELPAY", "CLIP", "PAYPAL", "BBVA", "CONEKTA"], key="terminal_input")
                        banco_destino = ""
                    else:
                        banco_destino = st.selectbox("🏦 Banco Destino", ["BANORTE", "BANAMEX", "AFIRME", "BANCOMER OP", "BANCOMER CURSOS"], key="banco_destino_input")
                        terminal = ""
                with col5:
                    referencia_pago = st.text_input("🔢 Referencia (opcional)", key="referencia_pago_input")

        else:
            st.markdown("### 1️⃣ Primer Pago")
            comp1 = st.file_uploader("💳 Comprobante 1", type=["pdf", "jpg", "jpeg", "png"], accept_multiple_files=True, key="cp_pago1")
            fecha1 = st.date_input("📅 Fecha 1", value=datetime.today().date(), key="fecha_pago1")
            forma1 = st.selectbox("💳 Forma 1", ["Transferencia", "Depósito en Efectivo", "Tarjeta de Débito", "Tarjeta de Crédito", "Cheque"], key="forma_pago1")
            monto1 = st.number_input("💲 Monto 1", min_value=0.0, format="%.2f", key="monto_pago1")
            terminal1 = banco1 = ""
            if forma1 in ["Tarjeta de Débito", "Tarjeta de Crédito"]:
                terminal1 = st.selectbox("🏧 Terminal 1", ["BANORTE", "AFIRME", "VELPAY", "CLIP", "PAYPAL", "BBVA", "CONEKTA"], key="terminal1")
            else:
                banco1 = st.selectbox("🏦 Banco 1", ["BANORTE", "BANAMEX", "AFIRME", "BANCOMER OP", "BANCOMER CURSOS"], key="banco1")
            ref1 = st.text_input("🔢 Referencia 1", key="ref1")

            st.markdown("### 2️⃣ Segundo Pago")
            comp2 = st.file_uploader("💳 Comprobante 2", type=["pdf", "jpg", "jpeg", "png"], accept_multiple_files=True, key="cp_pago2")
            fecha2 = st.date_input("📅 Fecha 2", value=datetime.today().date(), key="fecha_pago2")
            forma2 = st.selectbox("💳 Forma 2", ["Transferencia", "Depósito en Efectivo", "Tarjeta de Débito", "Tarjeta de Crédito", "Cheque"], key="forma_pago2")
            monto2 = st.number_input("💲 Monto 2", min_value=0.0, format="%.2f", key="monto_pago2")
            terminal2 = banco2 = ""
            if forma2 in ["Tarjeta de Débito", "Tarjeta de Crédito"]:
                terminal2 = st.selectbox("🏧 Terminal 2", ["BANORTE", "AFIRME", "VELPAY", "CLIP", "PAYPAL", "BBVA", "CONEKTA"], key="terminal2")
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


    # --- Registro del Pedido si se presionó el botón ---
    if submit_button:
        try:
            if not vendedor or not registro_cliente:
                st.warning("⚠️ Completa los campos obligatorios.")
                st.stop()
            if estado_pago == "✅ Pagado" and not comprobante_pago_files:
                st.warning("⚠️ Suba un comprobante si el pedido está marcado como pagado.")
                st.stop()

            headers = []
            try:
                worksheet = get_worksheet()

                all_data = worksheet.get_all_values()
                if not all_data:
                    st.error("❌ La hoja de cálculo está vacía.")
                    st.stop()
                headers = all_data[0]
                # ✅ Obtener hora local de CDMX para Hora_Registro e ID
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
                    st.error(f"❌ Error al acceder a Google Sheets: {e}")
                    st.stop()
                    
            # Inicializar la lista de URLs de adjuntos antes de usarla
            adjuntos_urls = []

            if uploaded_files:
                for file in uploaded_files:
                    ext = os.path.splitext(file.name)[1]
                    s3_key = f"{id_pedido}/{file.name.replace(' ', '_').replace(ext, '')}_{uuid.uuid4().hex[:4]}{ext}"
                    success, url = upload_file_to_s3(s3_client, S3_BUCKET_NAME, file, s3_key)
                    if success:
                        adjuntos_urls.append(url)
                    else:
                        st.error(f"❌ Falló la subida de {file.name}")
                        st.stop()

            if comprobante_pago_files:
                for archivo in comprobante_pago_files:
                    ext_cp = os.path.splitext(archivo.name)[1]
                    s3_key_cp = f"{id_pedido}/comprobante_{id_pedido}_{now.strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:4]}{ext_cp}"
                    success_cp, url_cp = upload_file_to_s3(s3_client, S3_BUCKET_NAME, archivo, s3_key_cp)
                    if success_cp:
                        adjuntos_urls.append(url_cp)
                    else:
                        st.error(f"❌ Falló la subida de {archivo.name}")
                        st.stop()



            adjuntos_str = ", ".join(adjuntos_urls)

            values = []
            for header in headers:
                if header == "ID_Pedido":
                    values.append(id_pedido)
                elif header == "Hora_Registro":
                    values.append(hora_registro)
                elif header in ["Vendedor", "Vendedor_Registro"]:
                    values.append(vendedor)
                elif header in ["Cliente", "RegistroCliente"]:
                    values.append(registro_cliente)
                elif header == "Folio_Factura":
                    values.append(folio_factura)
                elif header == "Tipo_Envio":
                    values.append(tipo_envio)
                elif header == "Turno":
                    values.append(subtipo_local)
                elif header == "Fecha_Entrega":
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
                    values.append(estado_pago)
                elif header == "Fecha_Pago_Comprobante":
                    values.append(fecha_pago if isinstance(fecha_pago, str) else (fecha_pago.strftime('%Y-%m-%d') if fecha_pago else ""))
                elif header == "Forma_Pago_Comprobante":
                    values.append(forma_pago)
                elif header == "Terminal":
                    values.append(terminal)
                elif header == "Banco_Destino_Pago":
                    values.append(banco_destino)
                elif header == "Monto_Comprobante":
                    values.append(f"{monto_pago:.2f}" if monto_pago > 0 else "")
                elif header == "Referencia_Comprobante":
                    values.append(referencia_pago)
                elif header in ["Fecha_Completado", "Hora_Proceso", "Modificacion_Surtido"]:
                    values.append("")
                else:
                    values.append("")

            worksheet.append_row(values)
            # ✅ Marcar registro exitoso antes de redirigir
            st.session_state["success_pedido_registrado"] = id_pedido
            st.session_state["success_adjuntos"] = adjuntos_urls

            # 🔁 Redirigir sin st.rerun inmediato
            st.query_params.clear()
            st.query_params.update({"tab": "0"})

            st.rerun()

        except Exception as e:
            st.error(f"❌ Error inesperado al registrar el pedido: {e}")

# --- TAB 2: MODIFY EXISTING ORDER ---
if "reset_inputs_tab2" in st.session_state:
    del st.session_state["reset_inputs_tab2"]
with tab2:
    st.header("✏️ Modificar Pedido Existente")

    message_placeholder_tab2 = st.empty()

    @st.cache_data(ttl=30)
    def cargar_datos_pedidos():
        client = build_gspread_client()
        worksheet = client.open_by_key(GOOGLE_SHEET_ID).worksheet("datos_pedidos")  # ✅ conexión segura y moderna

        headers = worksheet.row_values(1)
        df = pd.DataFrame(worksheet.get_all_records()) if headers else pd.DataFrame()

        # 🔧 Limpiar filas completamente vacías o sin campos clave
        campos_clave = ['ID_Pedido', 'Cliente', 'Folio_Factura']
        df = df.dropna(subset=campos_clave, how='all')  # elimina si todas las claves están vacías
        df = df[df['ID_Pedido'].astype(str).str.strip().ne("")]  # adicional para asegurar que ID_Pedido no esté vacío
        if "Adjuntos_Guia" not in df.columns:
            df["Adjuntos_Guia"] = ""

        return df, headers, worksheet

    try:
        df_pedidos, headers, worksheet = cargar_datos_pedidos()
    except Exception as e:
        message_placeholder_tab2.error(f"❌ Error al cargar pedidos para modificación: {e}")
        message_placeholder_tab2.info("ℹ️ Asegúrate de que la primera fila de tu Google Sheet contiene los encabezados esperados.")
        st.stop()

    selected_order_id = None
    selected_row_data = None
    current_modificacion_surtido_value = ""
    current_estado_pago_value = "🔴 No Pagado"
    current_adjuntos_list = []
    current_adjuntos_surtido_list = []

    if df_pedidos.empty:
        message_placeholder_tab2.warning("No hay pedidos registrados para modificar.")
    else:
        df_pedidos['Filtro_Envio_Combinado'] = df_pedidos.apply(
            lambda row: row['Turno'] if row['Tipo_Envio'] == "📍 Pedido Local" and pd.notna(row['Turno']) and row['Turno'] else row['Tipo_Envio'],
            axis=1
        )

        all_filter_options = ["Todos"] + df_pedidos['Filtro_Envio_Combinado'].unique().tolist()

        unique_filter_options = []
        for option in all_filter_options:
            if option not in unique_filter_options:
                unique_filter_options.append(option)

        col1, col2 = st.columns(2)

        
        # Mostrar siempre los selectbox de filtro
        col1, col2 = st.columns(2)

        with col1:
            if 'Vendedor_Registro' in df_pedidos.columns:
                unique_vendedores_mod = sorted(df_pedidos['Vendedor_Registro'].dropna().astype(str).str.strip().unique().tolist())
                unique_vendedores_mod = [v for v in unique_vendedores_mod if v != "" and v.lower() not in ["none", "nan"]]
                unique_vendedores_mod = ["Todos"] + unique_vendedores_mod
                selected_vendedor_mod = st.selectbox(
                    "Filtrar por Vendedor:",
                    options=unique_vendedores_mod,
                    key="vendedor_filter_mod"
                )
            else:
                selected_vendedor_mod = "Todos"

        with col2:
            fecha_filtro = st.date_input(
                "📅 Filtrar por Fecha de Registro:",
                value=datetime.now().date(),
                key="filtro_fecha_registro"
            )


        # Aplicar los filtros seleccionados
        filtered_orders = df_pedidos.copy()
        if selected_vendedor_mod != "Todos":
            filtered_orders = filtered_orders[filtered_orders['Vendedor_Registro'] == selected_vendedor_mod]
        # Filtrar por fecha si existe columna 'Hora_Registro'
        if 'Hora_Registro' in filtered_orders.columns:
            filtered_orders['Hora_Registro'] = pd.to_datetime(filtered_orders['Hora_Registro'], errors='coerce')
            filtered_orders = filtered_orders[
                filtered_orders['Hora_Registro'].dt.date == fecha_filtro
            ]
        if filtered_orders.empty:
            st.warning("No hay pedidos que coincidan con los filtros seleccionados.")
        else:
            # 🔧 Limpieza previa de columnas clave para evitar valores vacíos en el selectbox
            for col in ['Folio_Factura', 'ID_Pedido', 'Cliente', 'Estado', 'Tipo_Envio']:
                if col in filtered_orders.columns:
                    filtered_orders[col] = (
                        filtered_orders[col]
                        .astype(str)
                        .replace(['nan', 'None'], '')
                        .fillna('')
                        .str.strip()
                    )

            # 🧠 Fallback: usar ID_Pedido si falta Folio_Factura
            filtered_orders['display_label'] = filtered_orders.apply(lambda row:
                f"📄 {(row['Folio_Factura'] if row['Folio_Factura'] else row['ID_Pedido'])} - {row['Cliente']} - {row['Estado']} - {row['Tipo_Envio']}",
                axis=1
            )
            # 🆕 Ordenar por fecha de entrega descendente (más reciente primero)
            if 'Fecha_Entrega' in filtered_orders.columns:
                filtered_orders['Fecha_Entrega'] = pd.to_datetime(filtered_orders['Fecha_Entrega'], errors='coerce')
                filtered_orders = filtered_orders.sort_values(by='Fecha_Entrega', ascending=False).reset_index(drop=True)



            # Mostrar selectbox limpio
            selected_order_display = st.selectbox(
                "📝 Seleccionar Pedido para Modificar",
                filtered_orders['display_label'].tolist(),
                key="select_order_to_modify"
            )


            if selected_order_display:
                selected_order_id = filtered_orders[filtered_orders['display_label'] == selected_order_display]['ID_Pedido'].iloc[0]
                selected_row_data = filtered_orders[filtered_orders['ID_Pedido'] == selected_order_id].iloc[0]

                st.subheader(f"Detalles del Pedido: Folio {selected_row_data.get('Folio_Factura', 'N/A')} (ID {selected_order_id})")
                st.write(f"**Vendedor:** {selected_row_data.get('Vendedor', selected_row_data.get('Vendedor_Registro', 'No especificado'))}")
                st.write(f"**Cliente:** {selected_row_data.get('Cliente', 'N/A')}")
                st.write(f"**Folio de Factura:** {selected_row_data.get('Folio_Factura', 'N/A')}")
                st.write(f"**Estado Actual:** {selected_row_data.get('Estado', 'N/A')}")
                st.write(f"**Tipo de Envío:** {selected_row_data.get('Tipo_Envio', 'N/A')}")
                if selected_row_data.get('Tipo_Envio') == "📍 Pedido Local":
                    st.write(f"**Turno Local:** {selected_row_data.get('Turno', 'N/A')}")
                st.write(f"**Fecha de Entrega:** {selected_row_data.get('Fecha_Entrega', 'N/A')}")
                st.write(f"**Comentario Original:** {selected_row_data.get('Comentario', 'N/A')}")
                st.write(f"**Estado de Pago:** {selected_row_data.get('Estado_Pago', '🔴 No Pagado')}")

                current_modificacion_surtido_value = selected_row_data.get('Modificacion_Surtido', '')
                current_estado_pago_value = selected_row_data.get('Estado_Pago', '🔴 No Pagado')

                current_adjuntos_str = selected_row_data.get('Adjuntos', '')
                current_adjuntos_list = [f.strip() for f in current_adjuntos_str.split(',') if f.strip()]

                current_adjuntos_surtido_str = selected_row_data.get('Adjuntos_Surtido', '')
                current_adjuntos_surtido_list = [f.strip() for f in current_adjuntos_surtido_str.split(',') if f.strip()]

                if current_adjuntos_list:
                    st.write("**Adjuntos Originales:**")
                    for adj in current_adjuntos_list:
                        # Displaying URLs for existing attachments
                        st.markdown(f"- [{os.path.basename(adj)}]({adj})")
                else:
                    st.write("**Adjuntos Originales:** Ninguno")

                if current_adjuntos_surtido_list:
                    st.write("**Adjuntos de Modificación/Surtido:**")
                    for adj_surtido in current_adjuntos_surtido_list:
                        # Displaying URLs for existing attachments
                        st.markdown(f"- [{os.path.basename(adj_surtido)}]({adj_surtido})")
                else:
                    st.write("**Adjuntos de Modificación/Surtido:** Ninguno")


                st.markdown("---")
                st.subheader("Modificar Campos y Adjuntos (Surtido)")
                st.markdown("### 🛠 Tipo de modificación")

                tipo_modificacion_seleccionada = st.selectbox(
                    "📌 ¿Qué tipo de modificación estás registrando?",
                    ["Refacturación", "Nueva Ruta", "Otro"],
                    index=0,
                    key="tipo_modificacion_mod"
                )

                with st.form(key="modify_pedido_form_inner", clear_on_submit=False):
                    default_modificacion_text = "" if st.session_state.get("reset_inputs_tab2") else current_modificacion_surtido_value

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


                    if tipo_modificacion_seleccionada == "Refacturación":
                        st.markdown("### 🧾 Detalles de Refacturación")

                        refact_tipo = st.selectbox(
                            "🔍 Razón Principal",
                            ["Datos Fiscales", "Material"],
                            key="refact_tipo_mod"
                        )

                        # Mostrar ambos selectbox pero ocultar el que no aplica, usando claves distintas
                        if refact_tipo == "Datos Fiscales":
                            refact_subtipo = st.selectbox(
                                "📄 Subtipo",
                                ["Cambio de RFC", "Cambio de Régimen Fiscal", "Error en Forma de Pago", "Error de uso de Cfdi", "Otro"],
                                key="refact_subtipo_datos_mod"
                            )
                            refact_subtipo_val = refact_subtipo
                        elif refact_tipo == "Material":
                            refact_subtipo = st.selectbox(
                                "📦 Subtipo",
                                ["Agrego Material", "Quito Material", "Clave de Producto Errónea", "Otro"],
                                key="refact_subtipo_material_mod"
                            )
                            refact_subtipo_val = refact_subtipo
                        else:
                            refact_subtipo_val = ""



                        refact_folio_nuevo = st.text_input("📄 Folio de la Nueva Factura", key="refact_folio_mod")
                    else:
                        refact_tipo = ""
                        refact_subtipo = ""
                        refact_folio_nuevo = ""



                    modify_button = st.form_submit_button("💾 Guardar Cambios")

                    if modify_button:
                        message_placeholder_tab2.empty()
                        try:
                            # ✅ Reconexión directa y segura igual que en Tab 3
                            client = build_gspread_client()
                            worksheet = client.open_by_key(GOOGLE_SHEET_ID).worksheet("datos_pedidos")

                            headers = worksheet.row_values(1)
                            all_data_actual = worksheet.get_all_records()
                            df_actual = pd.DataFrame(all_data_actual)
                            selected_row_data = df_actual[df_actual['ID_Pedido'] == selected_order_id].iloc[0]


                            if selected_order_id not in df_actual['ID_Pedido'].values:
                                message_placeholder_tab2.error("❌ No se encontró el ID del pedido en la hoja.")
                                st.stop()

                            gsheet_row_index = df_actual[df_actual['ID_Pedido'] == selected_order_id].index[0] + 2
                            changes_made = False

                            # ✍️ Modificación_Surtido
                            if new_modificacion_surtido_input.strip() != current_modificacion_surtido_value.strip():
                                col_mod = headers.index("Modificacion_Surtido") + 1
                                worksheet.update_cell(gsheet_row_index, col_mod, new_modificacion_surtido_input.strip())
                                changes_made = True

                                # 🔁 Cambiar estado si estaba en completado
                                if selected_row_data.get('Estado') == "🟢 Completado":
                                    col_estado = headers.index("Estado") + 1
                                    col_fecha = headers.index("Fecha_Completado") + 1
                                    worksheet.update_cell(gsheet_row_index, col_estado, "🔵 En Proceso")
                                    worksheet.update_cell(gsheet_row_index, col_fecha, "")
                                    message_placeholder_tab2.warning("🔁 El pedido fue regresado a 'En Proceso' por modificación.")

                            # 📎 Adjuntos Surtido
                            new_adjuntos_surtido_urls = []
                            if uploaded_files_surtido:
                                for f in uploaded_files_surtido:
                                    ext = os.path.splitext(f.name)[1]
                                    s3_key = f"{selected_order_id}/surtido_{f.name.replace(' ', '_').replace(ext, '')}_{uuid.uuid4().hex[:4]}{ext}"
                                    success, url = upload_file_to_s3(s3_client, S3_BUCKET_NAME, f, s3_key)
                                    if success:
                                        new_adjuntos_surtido_urls.append(url)
                                        changes_made = True
                                    else:
                                        message_placeholder_tab2.warning(f"⚠️ Falló la subida de {f.name}")
                                        
                            # 🧾 Adjuntar Comprobantes Extra
                            comprobante_urls = []
                            if uploaded_comprobantes_extra:
                                for archivo in uploaded_comprobantes_extra:
                                    ext = os.path.splitext(archivo.name)[1]
                                    s3_key = f"{selected_order_id}/comprobante_{selected_order_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:4]}{ext}"
                                    success, url = upload_file_to_s3(s3_client, S3_BUCKET_NAME, archivo, s3_key)
                                    if success:
                                        comprobante_urls.append(url)
                                        changes_made = True
                                    else:
                                        message_placeholder_tab2.warning(f"⚠️ Falló la subida del comprobante {archivo.name}")

                                if comprobante_urls:
                                    current_adjuntos = [x.strip() for x in selected_row_data.get("Adjuntos", "").split(",") if x.strip()]
                                    updated_adjuntos = ", ".join(current_adjuntos + comprobante_urls)
                                    col_idx_adj = headers.index("Adjuntos") + 1
                                    worksheet.update_cell(gsheet_row_index, col_idx_adj, updated_adjuntos)


                            if new_adjuntos_surtido_urls:
                                current_urls = [x.strip() for x in selected_row_data.get("Adjuntos_Surtido", "").split(",") if x.strip()]
                                updated_str = ", ".join(current_urls + new_adjuntos_surtido_urls)
                                col_adj = headers.index("Adjuntos_Surtido") + 1
                                worksheet.update_cell(gsheet_row_index, col_adj, updated_str)

                            # 🧾 Guardar campos de refacturación si aplica
                            if tipo_modificacion_seleccionada == "Refacturación":
                                campos_refact = {
                                    "Refacturacion_Tipo": refact_tipo,
                                    "Refacturacion_Subtipo": refact_subtipo_val,
                                    "Folio_Factura_Refacturada": refact_folio_nuevo
                                }
                                for campo, valor in campos_refact.items():
                                    if campo in headers:
                                        col_idx = headers.index(campo) + 1
                                        worksheet.update_cell(gsheet_row_index, col_idx, valor)
                                st.toast("🧾 Refacturación registrada con los detalles capturados.")
                            else:
                                # 🧹 Limpiar campos si se cambió a Otro o Nueva Ruta
                                campos_refact = [
                                    "Refacturacion_Tipo",
                                    "Refacturacion_Subtipo",
                                    "Folio_Factura_Refacturada"
                                ]
                                for campo in campos_refact:
                                    if campo in headers:
                                        col_idx = headers.index(campo) + 1
                                        worksheet.update_cell(gsheet_row_index, col_idx, "")


                            if changes_made:
                                st.session_state["reset_inputs_tab2"] = True
                                st.session_state["show_success_message"] = True
                                st.session_state["last_updated_order_id"] = selected_order_id
                                st.session_state["new_modificacion_surtido_input"] = ""  # 🔁 limpiar textarea
                                st.session_state["uploaded_files_surtido"] = []          # 🔁 limpiar uploader
                                st.query_params.update({"tab": "1"})
                                st.rerun()

                            else:
                                message_placeholder_tab2.info("ℹ️ No se detectaron cambios nuevos para guardar.")

                        except Exception as e:
                            message_placeholder_tab2.error(f"❌ Error inesperado al guardar: {e}")


    if (
        'show_success_message' in st.session_state and
        st.session_state.show_success_message and
        'last_updated_order_id' in st.session_state
    ):
        message_placeholder_tab2.success(f"✅ Pedido {st.session_state.last_updated_order_id} actualizado con éxito.")
        del st.session_state.show_success_message
        del st.session_state.last_updated_order_id


# --- TAB 3: PENDING PROOF OF PAYMENT ---
with tab3:
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
            fecha_filtro_tab3 = st.date_input(
                "📅 Filtrar por Fecha de Registro:",
                value=datetime.now().date(),
                key="filtro_fecha_comprobante"
            )
            
        # Filtrar por fecha si existe la columna 'Hora_Registro'
        if 'Hora_Registro' in filtered_pedidos_comprobante.columns:
            filtered_pedidos_comprobante['Hora_Registro'] = pd.to_datetime(filtered_pedidos_comprobante['Hora_Registro'], errors='coerce')
            filtered_pedidos_comprobante = filtered_pedidos_comprobante[
                filtered_pedidos_comprobante['Hora_Registro'].dt.date == fecha_filtro_tab3
            ]

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
                                    success, url = upload_file_to_s3(s3_client, S3_BUCKET_NAME, archivo, s3_key)
                                    if success:
                                        new_urls.append(url)
                                    else:
                                        st.warning(f"⚠️ Falló la subida de {archivo.name}")

                                if new_urls:
                                    current_adjuntos = df_pedidos_comprobante.loc[df_index, 'Adjuntos'] if 'Adjuntos' in df_pedidos_comprobante.columns else ""
                                    adjuntos_list = [x.strip() for x in current_adjuntos.split(',') if x.strip()]
                                    adjuntos_list.extend(new_urls)

                                    worksheet.update_cell(sheet_row, headers.index('Adjuntos') + 1, ", ".join(adjuntos_list))
                                    worksheet.update_cell(sheet_row, headers.index('Estado_Pago') + 1, "✅ Pagado")
                                    worksheet.update_cell(sheet_row, headers.index('Fecha_Pago_Comprobante') + 1, datetime.now(timezone("America/Mexico_City")).strftime('%Y-%m-%d'))

                                    st.success("✅ Comprobantes subidos y estado actualizado con éxito.")
                                    st.balloons()
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

                        worksheet.update_cell(sheet_row, headers.index('Estado_Pago') + 1, "✅ Pagado")

                        if 'Fecha_Pago_Comprobante' in headers:
                            worksheet.update_cell(sheet_row, headers.index('Fecha_Pago_Comprobante') + 1, datetime.now(timezone("America/Mexico_City")).strftime('%Y-%m-%d'))

                        st.success("✅ Pedido marcado como pagado sin comprobante.")
                        st.balloons()
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ Error al marcar como pagado sin comprobante: {e}")


# --- TAB 4: GUIAS CARGADAS ---
def fijar_tab4_activa():
    st.query_params.update({"tab": "3"})

@st.cache_data(ttl=60)
def cargar_datos_guias():
    worksheet = get_worksheet()
    headers = worksheet.row_values(1)
    if headers:
        df = pd.DataFrame(worksheet.get_all_records())
        if "Adjuntos_Guia" not in df.columns:
            df["Adjuntos_Guia"] = ""
        df = df[df["Adjuntos_Guia"].astype(str).str.strip() != ""]
        return df
    return pd.DataFrame()

with tab4:
    st.header("📦 Pedidos con Guías Subidas desde Almacén")

    try:
        df_guias = cargar_datos_guias()
    except Exception as e:
        st.error(f"❌ Error al cargar datos de guías: {e}")
        df_guias = pd.DataFrame()

    if df_guias.empty:
        st.info("No hay pedidos con guías subidas.")
    else:
        st.markdown("### 🔍 Filtros")
        col1_tab4, col2_tab4 = st.columns(2)

        with col1_tab4:
            vendedores = ["Todos"] + sorted(df_guias["Vendedor_Registro"].dropna().unique().tolist())
            vendedor_filtrado = st.selectbox(
                "Filtrar por Vendedor",
                vendedores,
                key="filtro_vendedor_guias",
                on_change=fijar_tab4_activa
            )

        with col2_tab4:
            fecha_filtro_tab4 = st.date_input(
                "📅 Filtrar por Fecha de Registro:",
                value=datetime.now().date(),
                key="filtro_fecha_guias"
            )

        # Filtro por fecha
        if 'Hora_Registro' in df_guias.columns:
            df_guias['Hora_Registro'] = pd.to_datetime(df_guias['Hora_Registro'], errors='coerce')
            df_guias = df_guias[df_guias['Hora_Registro'].dt.date == fecha_filtro_tab4]

        # Filtro por vendedor
        if vendedor_filtrado != "Todos":
            df_guias = df_guias[df_guias["Vendedor_Registro"] == vendedor_filtrado]

        # Mostrar tabla básica
        columnas_mostrar = ["ID_Pedido", "Cliente", "Vendedor_Registro", "Tipo_Envio", "Estado", "Fecha_Entrega"]
        tabla_guias = df_guias[columnas_mostrar].copy()
        tabla_guias["Fecha_Entrega"] = pd.to_datetime(tabla_guias["Fecha_Entrega"], errors="coerce").dt.strftime("%d/%m/%y")
        st.dataframe(tabla_guias, use_container_width=True, hide_index=True)

        # 🔍 Selección para mostrar solo una guía a la vez
        st.markdown("### 📥 Selecciona un Pedido para Ver la Última Guía Subida")

        df_guias['Folio_O_ID'] = df_guias['Folio_Factura'].astype(str).str.strip()
        df_guias.loc[df_guias['Folio_O_ID'] == '', 'Folio_O_ID'] = df_guias['ID_Pedido']

        # Ordenar por fecha de entrega descendente
        if 'Fecha_Entrega' in df_guias.columns:
            df_guias['Fecha_Entrega'] = pd.to_datetime(df_guias['Fecha_Entrega'], errors='coerce')
            df_guias = df_guias.sort_values(by='Fecha_Entrega', ascending=False)

        df_guias['display_label'] = df_guias.apply(lambda row:
            f"📄 {row['Folio_O_ID']} – {row['Cliente']} – {row['Vendedor_Registro']} ({row['Tipo_Envio']})", axis=1)

        pedido_seleccionado = st.selectbox(
            "📦 Pedido con Guía",
            options=df_guias['display_label'].tolist(),
            key="select_pedido_con_guia"
        )

        if pedido_seleccionado:
            pedido_row = df_guias[df_guias['display_label'] == pedido_seleccionado].iloc[0]
            ultima_guia = str(pedido_row["Adjuntos_Guia"]).split(",")[-1].strip()

            st.markdown("### 📎 Última Guía Subida")
            if ultima_guia:
                nombre = ultima_guia.split("/")[-1]
                st.markdown(f"- [📄 {nombre}]({ultima_guia})")
            else:
                st.warning("⚠️ No se encontró una URL válida para la guía.")

# --- TAB 5: DOWNLOAD DATA ---
with tab5:
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
                key="download_vendedor_filter_tab4_final"
            )

            if selected_vendedor != "Todos":
                filtered_df_download = filtered_df_download[filtered_df_download['Vendedor_Registro'] == selected_vendedor]
        else:
            st.warning("La columna 'Vendedor_Registro' no está disponible en los datos cargados para aplicar este filtro. Por favor, asegúrate de que el nombre de la columna en tu Google Sheet sea 'Vendedor_Registro'.")

        if 'Tipo_Envio' in filtered_df_download.columns:
            unique_tipos_envio_download = ["Todos", "📍 Pedido Local", "🚚 Pedido Foráneo", "🔁 Devolución", "🛠 Garantía"]
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
            selected_estado = st.selectbox("Filtrar por Estado:", unique_estados, key="download_estado_filter_tab4")
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

# --- TAB 6: SEARCH ORDER ---
with tab6:
    st.subheader("🔍 Buscador de Pedidos por Guía o Cliente")

    modo_busqueda = st.radio("Selecciona el modo de búsqueda:", ["🔢 Por número de guía", "🧑 Por cliente"], key="modo_busqueda_radio")

    if modo_busqueda == "🔢 Por número de guía":
        keyword = st.text_input("📦 Ingresa una palabra clave, número de guía, fragmento o código a buscar:")
        buscar_btn = st.button("🔎 Buscar")
    else:
        keyword = st.text_input("🧑 Ingresa el nombre del cliente a buscar (sin importar mayúsculas ni acentos):")
        buscar_btn = st.button("🔍 Buscar Pedido del Cliente")
        cliente_normalizado = normalizar(keyword.strip()) if keyword else ""

    if buscar_btn:
        if modo_busqueda == "🔢 Por número de guía":
            st.info("🔄 Buscando, por favor espera... puede tardar unos segundos...")
        df_pedidos = cargar_pedidos()
        resultados = []

        if 'Hora_Registro' in df_pedidos.columns:
            df_pedidos['Hora_Registro'] = pd.to_datetime(df_pedidos['Hora_Registro'], errors='coerce')
            df_pedidos = df_pedidos.sort_values(by='Hora_Registro', ascending=False).reset_index(drop=True)

        for _, row in df_pedidos.iterrows():
            pedido_id = str(row.get("ID_Pedido", "")).strip()
            if not pedido_id:
                continue

            if modo_busqueda == "🧑 Por cliente":
                cliente_row = row.get("Cliente", "").strip()
                if not cliente_row:
                    continue
                cliente_row_normalizado = normalizar(cliente_row)
                if cliente_normalizado not in cliente_row_normalizado:
                    continue

                prefix = obtener_prefijo_s3(pedido_id)
                if not prefix:
                    continue

                archivos_coincidentes = []  # no se buscan coincidencias
                todos_los_archivos = obtener_todos_los_archivos(prefix)

            elif modo_busqueda == "🔢 Por número de guía":
                prefix = obtener_prefijo_s3(pedido_id)
                if not prefix:
                    continue

                archivos_validos = obtener_archivos_pdf_validos(prefix)
                archivos_coincidentes = []

                for archivo in archivos_validos:
                    key = archivo["Key"]
                    texto = extraer_texto_pdf(key)

                    clave = keyword.strip()
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
                        break  # detener búsqueda tras encontrar coincidencia
                else:
                    continue  # ningún PDF coincidió

            else:
                continue  # modo no reconocido

            # Una vez tenemos los archivos del pedido
            comprobantes = [f for f in todos_los_archivos if "comprobante" in f["Key"].lower()]
            facturas = [f for f in todos_los_archivos if "factura" in f["Key"].lower()]
            otros = [
                f for f in todos_los_archivos
                if f not in comprobantes and f not in facturas and
                (modo_busqueda == "🧑 Por cliente" or f["Key"] != archivos_coincidentes[0][0])
            ]

            comprobantes_links = [(f["Key"], generar_url_s3(f["Key"])) for f in comprobantes]
            facturas_links = [(f["Key"], generar_url_s3(f["Key"])) for f in facturas]
            otros_links = [(f["Key"], generar_url_s3(f["Key"])) for f in otros]

            resultados.append({
                "ID_Pedido": pedido_id,
                "Cliente": row.get("Cliente", ""),
                "Estado": row.get("Estado", ""),
                "Vendedor": row.get("Vendedor_Registro", ""),
                "Folio": row.get("Folio_Factura", ""),
                "Hora_Registro": row.get("Hora_Registro", ""),  # 🆕 Agregamos este campo
                "Coincidentes": archivos_coincidentes,
                "Comprobantes": comprobantes_links,
                "Facturas": facturas_links,
                "Otros": otros_links
            })


            if modo_busqueda == "🔢 Por número de guía":
                break  # Solo detener si es búsqueda por guía

        st.markdown("---")
        if resultados:
            st.success(f"✅ Se encontraron coincidencias en {len(resultados)} pedido(s).")

            for res in resultados:
                st.markdown(f"### 🤝 {res['Cliente']}")
                st.markdown(f"📄 **Folio:** `{res['Folio']}`  |  🔍 **Estado:** `{res['Estado']}`  |  🧑‍💼 **Vendedor:** `{res['Vendedor']}`  |  🕒 **Hora:** `{res['Hora_Registro']}`")

                with st.expander("📁 Archivos del Pedido", expanded=True):
                    if res["Coincidentes"]:
                        st.markdown("#### 🔍 Guías:")
                        for key, url in res["Coincidentes"]:
                            nombre = key.split("/")[-1]
                            st.markdown(f"- [🔍 {nombre}]({url})")

                    if res["Comprobantes"]:
                        st.markdown("#### 🧾 Comprobantes:")
                        for key, url in res["Comprobantes"]:
                            nombre = key.split("/")[-1]
                            st.markdown(f"- [📄 {nombre}]({url})")

                    if res["Facturas"]:
                        st.markdown("#### 📁 Facturas:")
                        for key, url in res["Facturas"]:
                            nombre = key.split("/")[-1]
                            st.markdown(f"- [📄 {nombre}]({url})")

                    if res["Otros"]:
                        st.markdown("#### 📂 Otros Archivos:")
                        for key, url in res["Otros"]:
                            nombre = key.split("/")[-1]
                            st.markdown(f"- [📌 {nombre}]({url})")

        else:
            mensaje = (
                "⚠️ No se encontraron coincidencias en ningún archivo PDF."
                if modo_busqueda == "🔢 Por número de guía"
                else "⚠️ No se encontraron pedidos para el cliente ingresado."
            )
            st.warning(mensaje)
