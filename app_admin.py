#app_admin.py
import streamlit as st
import json
import time
import pandas as pd
import boto3
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from io import BytesIO
from datetime import datetime

st.set_page_config(page_title="App Admin TD", layout="wide")

# --- GOOGLE SHEETS CONFIGURATION ---
GOOGLE_SHEET_ID = '1aWkSelodaz0nWfQx7FZAysGnIYGQFJxAN7RO3YgCiZY'

@st.cache_resource
def get_google_sheets_client():
    try:
        credentials_json_str = st.secrets["google_credentials"]
        creds_dict = json.loads(credentials_json_str)
        creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n").strip()
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)

        # Verificación temprana del token
        _ = client.open_by_key(GOOGLE_SHEET_ID)
        return client

    except gspread.exceptions.APIError:
        # Si el token expiró o hubo error, reintentamos
        st.cache_resource.clear()
        st.warning("🔁 Token expirado o inválido. Reintentando autenticación...")

        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        _ = client.open_by_key(GOOGLE_SHEET_ID)
        return client

    except Exception as e:
        st.error(f"❌ Error crítico al autenticar con Google Sheets: {e}")
        st.stop()

# --- CONFIGURACIÓN DE AWS S3 ---
try:
    AWS_ACCESS_KEY_ID = st.secrets["aws_access_key_id"]
    AWS_SECRET_ACCESS_KEY = st.secrets["aws_secret_access_key"]
    AWS_REGION_NAME = st.secrets["aws_region"]
    S3_BUCKET_NAME = st.secrets["s3_bucket_name"]
except KeyError as e:
    st.error(f"❌ Error: Las credenciales de AWS S3 no se encontraron en Streamlit secrets. Asegúrate de que las claves 'aws_access_key_id', 'aws_secret_access_key', 'aws_region' y 's3_bucket_name' estén directamente en tus secretos de Streamlit. Clave faltante: {e}")
    st.stop() # Detiene la ejecución de la app si no se encuentran las credenciales

S3_ATTACHMENT_PREFIX = 'adjuntos_pedidos/'

st.title("👨‍💼 App de Administración TD")
st.write("Panel de administración para revisar y confirmar comprobantes de pago.")

# --- FUNCIONES DE CARGA DE DATOS Y S3 (Adaptadas) ---

@st.cache_resource
def get_s3_client_cached(): # Renombrado para evitar conflicto con la variable global s3_client
    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION_NAME
        )
        return s3
    except Exception as e:
        st.error(f"❌ Error al autenticar AWS S3: {e}")
        return None

def find_pedido_subfolder_prefix(s3_client_instance, parent_prefix, folder_name): # Acepta s3_client_instance
    if not s3_client_instance:
        return None
    
    possible_prefixes = [
        f"{parent_prefix}{folder_name}/",
        f"{parent_prefix}{folder_name}",
        f"adjuntos_pedidos/{folder_name}/",
        f"adjuntos_pedidos/{folder_name}",
        f"{folder_name}/",
        folder_name
    ]
    
    for pedido_prefix in possible_prefixes:
        try:
            response = s3_client_instance.list_objects_v2(
                Bucket=S3_BUCKET_NAME,
                Prefix=pedido_prefix,
                MaxKeys=1
            )
            
            if 'Contents' in response and response['Contents']:
                return pedido_prefix
            
        except Exception:
            continue
    
    try:
        response = s3_client_instance.list_objects_v2( # Usa s3_client_instance
            Bucket=S3_BUCKET_NAME,
            MaxKeys=100
        )
        
        if 'Contents' in response:
            for obj in response['Contents']:
                if folder_name in obj['Key']:
                    if '/' in obj['Key']:
                        prefix_parts = obj['Key'].split('/')[:-1]
                        return '/'.join(prefix_parts) + '/'
            
    except Exception:
        pass
    
    return None

def get_files_in_s3_prefix(s3_client_instance, prefix): # Acepta s3_client_instance
    if not s3_client_instance or not prefix:
        return []
    
    try:
        response = s3_client_instance.list_objects_v2( # Usa s3_client_instance
            Bucket=S3_BUCKET_NAME, 
            Prefix=prefix,
            MaxKeys=100
        )
        
        files = []
        if 'Contents' in response:
            for item in response['Contents']:
                if not item['Key'].endswith('/'):
                    file_name = item['Key'].split('/')[-1]
                    if file_name:
                        files.append({
                            'title': file_name,
                            'key': item['Key'],
                            'size': item['Size'],
                            'last_modified': item['LastModified']
                        })
        return files
        
    except Exception as e:
        st.error(f"❌ Error al obtener archivos del prefijo S3 '{prefix}': {e}")
        return []

def get_s3_file_download_url(s3_client_instance, object_key): # Acepta s3_client_instance
    if not s3_client_instance or not object_key:
        return "#"
    
    try:
        url = s3_client_instance.generate_presigned_url( # Usa s3_client_instance
            'get_object',
            Params={'Bucket': S3_BUCKET_NAME, 'Key': object_key},
            ExpiresIn=7200
        )
        return url
    except Exception as e:
        st.error(f"❌ Error al generar URL pre-firmada para '{object_key}': {e}")
        return "#"

# --- Inicializar clientes de Gspread y S3 ---
try:
    gc = get_google_sheets_client()
    s3_client = get_s3_client_cached() # Ahora llama a la función cacheada
    
    if not s3_client:
        st.error("❌ No se pudo inicializar el cliente de AWS S3.")
        st.stop()
    
except Exception as e:
    st.error(f"❌ Error al autenticarse o inicializar clientes de Google Sheets/AWS S3: {e}")
    st.info("ℹ️ Asegúrate de que:")
    st.info("- Tus credenciales de Google Sheets ('google_credentials') sean correctas en secrets.toml")
    st.info("- Las APIs de Drive/Sheets estén habilitadas en Google Cloud")
    st.info("- La cuenta de servicio de Google tenga permisos en el Sheet")
    st.info("- Tus credenciales de AWS S3 (aws_access_key_id, aws_secret_access_key, aws_region) y el s3_bucket_name sean correctos en secrets.toml.")
    st.info("- La cuenta de AWS tenga permisos de lectura en el bucket S3.")
    st.stop()

# --- INTERFAZ PRINCIPAL ---

st.header("💳 Comprobantes de Pago Pendientes de Confirmación")

if st.button("🔄 Recargar Pedidos desde Google Sheets", type="secondary"):
    st.cache_data.clear()
    st.cache_resource.clear()
    st.rerun()

def cargar_pedidos_desde_google_sheet(sheet_id, worksheet_name):
    @st.cache_data(ttl=60)
    def _load():
        gc = get_google_sheets_client()
        spreadsheet = gc.open_by_key(sheet_id)
        worksheet = spreadsheet.worksheet(worksheet_name)
        headers = worksheet.row_values(1)

        if headers:
            df = pd.DataFrame(worksheet.get_all_records())
            df = df.dropna(subset=['Folio_Factura', 'ID_Pedido'], how='all')
            df = df[
                df['ID_Pedido'].astype(str).str.strip().ne('') &
                df['ID_Pedido'].astype(str).str.lower().ne('n/a') &
                df['ID_Pedido'].astype(str).str.lower().ne('nan')
            ]
            return df, headers
        else:
            return pd.DataFrame(), []
    return _load()

df_pedidos, headers = cargar_pedidos_desde_google_sheet(GOOGLE_SHEET_ID, "datos_pedidos")
worksheet = get_google_sheets_client().open_by_key(GOOGLE_SHEET_ID).worksheet("datos_pedidos")

if df_pedidos.empty:
    st.info("ℹ️ No hay pedidos cargados en este momento.")
else:
    if 'Comprobante_Confirmado' in df_pedidos.columns:
        pedidos_pagados_no_confirmados = df_pedidos[df_pedidos['Comprobante_Confirmado'] != 'Sí'].copy()
    else:
        st.warning("⚠️ La columna 'Comprobante_Confirmado' no se encontró en la hoja de cálculo.")
        pedidos_pagados_no_confirmados = pd.DataFrame()

    if pedidos_pagados_no_confirmados.empty:
        st.success("🎉 ¡No hay comprobantes pendientes de confirmación!")
        st.info("Todos los pedidos pagados han sido confirmados.")
    else:
        st.warning(f"📋 Hay {len(pedidos_pagados_no_confirmados)} comprobantes pendientes.")

        columns_to_show = [
            'Folio_Factura', 'Cliente', 'Vendedor_Registro', 'Tipo_Envio',
            'Fecha_Entrega', 'Estado', 'Estado_Pago'
        ]
        existing_columns = [col for col in columns_to_show if col in pedidos_pagados_no_confirmados.columns]

        if existing_columns:
            df_vista = pedidos_pagados_no_confirmados[existing_columns].copy()

            # 🔧 Formatear Fecha_Entrega si existe
            if 'Fecha_Entrega' in df_vista.columns:
                df_vista['Fecha_Entrega'] = pd.to_datetime(df_vista['Fecha_Entrega'], errors='coerce').dt.strftime('%d/%m/%Y')

            st.dataframe(
                df_vista.sort_values(by='Fecha_Entrega' if 'Fecha_Entrega' in df_vista.columns else existing_columns[0]),
                use_container_width=True,
                hide_index=True
            )

        st.markdown("---")
        st.subheader("🔍 Revisar Comprobante de Pago")

        # 💄 Mostrar pedidos con formato limpio y emojis bonitos, sin repetir los del Excel
        pedidos_pagados_no_confirmados['display_label'] = pedidos_pagados_no_confirmados.apply(lambda row: (
            f"📄 {row.get('Folio_Factura', 'N/A')} - "
            f"👤 {row.get('Cliente', 'N/A')} - "
            f"{row.get('Estado', 'N/A')} - "
            f"{row.get('Tipo_Envio', 'N/A')}"
        ), axis=1)


        pedido_options = pedidos_pagados_no_confirmados['display_label'].tolist()
        selected_index = st.selectbox(
            "📝 Seleccionar Pedido para Revisar Comprobante",
            options=range(len(pedido_options)),
            format_func=lambda i: pedido_options[i],
            key="select_pedido_comprobante"
        )

        if selected_index is not None:
            selected_pedido_data = pedidos_pagados_no_confirmados.iloc[selected_index]
            selected_pedido_id_for_s3_search = selected_pedido_data.get('ID_Pedido', 'N/A')

            st.session_state.selected_admin_pedido_id = selected_pedido_id_for_s3_search
            fecha_pago_raw = selected_pedido_data.get('Fecha_Pago_Comprobante')
            if isinstance(fecha_pago_raw, str) and " y " in fecha_pago_raw:
                st.session_state.fecha_pago = fecha_pago_raw  # ya viene listo como string concatenado
            else:
                st.session_state.fecha_pago = pd.to_datetime(fecha_pago_raw).date() if fecha_pago_raw else None

            st.session_state.forma_pago = selected_pedido_data.get('Forma_Pago_Comprobante', 'Transferencia')
            st.session_state.terminal = selected_pedido_data.get('Terminal', 'BANORTE')
            st.session_state.banco_destino_pago = selected_pedido_data.get('Banco_Destino_Pago', 'BANORTE')
            try:
                st.session_state.monto_pago = float(selected_pedido_data.get('Monto_Comprobante', 0.0))
            except Exception:
                st.session_state.monto_pago = 0.0
            st.session_state.referencia_pago = selected_pedido_data.get('Referencia_Comprobante', '')

            col1, col2 = st.columns(2)
            with col1:
                st.subheader("📋 Información del Pedido")
                st.write(f"**Folio Factura:** {selected_pedido_data.get('Folio_Factura', 'N/A')}")
                st.write(f"**ID Pedido (interno):** {selected_pedido_data.get('ID_Pedido', 'N/A')}")
                st.write(f"**Cliente:** {selected_pedido_data.get('Cliente', 'N/A')}")
                st.write(f"**Vendedor:** {selected_pedido_data.get('Vendedor_Registro', 'N/A')}")
                st.write(f"**Tipo de Envío:** {selected_pedido_data.get('Tipo_Envio', 'N/A')}")
                st.write(f"**Fecha de Entrega:** {selected_pedido_data.get('Fecha_Entrega', 'N/A')}")
                st.write(f"**Estado:** {selected_pedido_data.get('Estado', 'N/A')}")
                st.write(f"**Estado de Pago:** {selected_pedido_data.get('Estado_Pago', 'N/A')}")

            with col2:
                st.subheader("📎 Archivos y Comprobantes")

                if s3_client:
                    pedido_folder_prefix = find_pedido_subfolder_prefix(s3_client, S3_ATTACHMENT_PREFIX, selected_pedido_id_for_s3_search)
                    files = get_files_in_s3_prefix(s3_client, pedido_folder_prefix) if pedido_folder_prefix else []

                    if files:
                        comprobantes = [f for f in files if 'comprobante' in f['title'].lower()]
                        facturas = [f for f in files if 'factura' in f['title'].lower()]
                        otros = [f for f in files if f not in comprobantes and f not in facturas]

                        if comprobantes:
                            st.write("**🧾 Comprobantes de Pago:**")
                            for f in comprobantes:
                                url = get_s3_file_download_url(s3_client, f['key'])
                                nombre = f['title'].replace(selected_pedido_id_for_s3_search, "").strip("_-")
                                st.markdown(f"- 📄 **{nombre}** ({f['size']} bytes) [🔗 Ver/Descargar]({url})")
                        else:
                            st.warning("⚠️ No se encontraron comprobantes.")

                        if facturas:
                            st.write("**📑 Facturas de Venta:**")
                            for f in facturas:
                                url = get_s3_file_download_url(s3_client, f['key'])
                                nombre = f['title'].replace(selected_pedido_id_for_s3_search, "").strip("_-")
                                st.markdown(f"- 📄 **{nombre}** ({f['size']} bytes) [🔗 Ver/Descargar]({url})")

                        if otros:
                            with st.expander("📂 Otros archivos del pedido"):
                                for f in otros:
                                    url = get_s3_file_download_url(s3_client, f['key'])
                                    st.markdown(f"- 📄 **{f['title']}** ({f['size']} bytes) [🔗 Ver/Descargar]({url})")
                    else:
                        st.info("📁 No se encontraron archivos en la carpeta del pedido.")
                else:
                    st.error("❌ Error de conexión con S3. Revisa las credenciales.")


            st.subheader("✅ Confirmar Comprobante")

            if 'fecha_pago' not in st.session_state:
                st.session_state.fecha_pago = None
            if 'banco_destino_pago' not in st.session_state:
                st.session_state.banco_destino_pago = "BANORTE"
            if 'terminal' not in st.session_state:
                st.session_state.terminal = "BANORTE"
            if 'forma_pago' not in st.session_state:
                st.session_state.forma_pago = "Transferencia"
            if 'monto_pago' not in st.session_state:
                st.session_state.monto_pago = 0.0
            if 'referencia_pago' not in st.session_state:
                st.session_state.referencia_pago = ""

            estado_pago = "✅ Pagado"  # aquí no se cambia, ya está validado antes
            comprobante_pago_files = []
            fecha_pago = None
            forma_pago = ""
            terminal = ""
            banco_destino = ""
            monto_pago = 0.0
            referencia_pago = ""

            pago_doble = st.checkbox("✅ Pago en dos partes distintas (Admin)")

            if not pago_doble:
                comprobante_pago_files = st.file_uploader(
                    "💲 Subir comprobante(s) de pago",
                    type=["pdf", "jpg", "jpeg", "png"],
                    accept_multiple_files=True,
                    key="comprobante_admin_1"
                )
                st.info("⚠️ El comprobante es obligatorio si el estado es 'Pagado'.")

                with st.expander("🧾 Detalles del Pago"):
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        fecha_pago = st.date_input("📅 Fecha del Pago", value=datetime.today().date(), key="fecha_pago_admin")
                    with col2:
                        forma_pago = st.selectbox("💳 Forma de Pago", [
                            "Transferencia", "Depósito en Efectivo", "Tarjeta de Débito", "Tarjeta de Crédito", "Cheque"
                        ], key="forma_pago_admin")
                    with col3:
                        monto_pago = st.number_input("💲 Monto del Pago", min_value=0.0, format="%.2f", key="monto_pago_admin")

                    col4, col5 = st.columns(2)
                    with col4:
                        if forma_pago in ["Tarjeta de Débito", "Tarjeta de Crédito"]:
                            terminal = st.selectbox("🏧 Terminal", ["BANORTE", "AFIRME", "VELPAY", "CLIP", "PAYPAL", "BBVA", "CONEKTA"], key="terminal_admin")
                            banco_destino = ""
                        else:
                            banco_destino = st.selectbox("🏦 Banco Destino", ["BANORTE", "BANAMEX", "AFIRME", "BANCOMER OP", "BANCOMER CURSOS"], key="banco_destino_admin")
                            terminal = ""
                    with col5:
                        referencia_pago = st.text_input("🔢 Referencia (opcional)", key="referencia_admin")

            else:
                st.markdown("### 1️⃣ Primer Pago")
                comp1 = st.file_uploader("💳 Comprobante 1", type=["pdf", "jpg", "jpeg", "png"], accept_multiple_files=True, key="comp_admin1")
                fecha1 = st.date_input("📅 Fecha 1", value=datetime.today().date(), key="fecha_admin1")
                forma1 = st.selectbox("💳 Forma 1", ["Transferencia", "Depósito en Efectivo", "Tarjeta de Débito", "Tarjeta de Crédito", "Cheque"], key="forma_admin1")
                monto1 = st.number_input("💲 Monto 1", min_value=0.0, format="%.2f", key="monto_admin1")
                terminal1 = banco1 = ""
                if forma1 in ["Tarjeta de Débito", "Tarjeta de Crédito"]:
                    terminal1 = st.selectbox("🏧 Terminal 1", ["BANORTE", "AFIRME", "VELPAY", "CLIP", "PAYPAL", "BBVA", "CONEKTA"], key="terminal_admin1")
                else:
                    banco1 = st.selectbox("🏦 Banco 1", ["BANORTE", "BANAMEX", "AFIRME", "BANCOMER OP", "BANCOMER CURSOS"], key="banco_admin1")
                ref1 = st.text_input("🔢 Referencia 1", key="ref_admin1")

                st.markdown("### 2️⃣ Segundo Pago")
                comp2 = st.file_uploader("💳 Comprobante 2", type=["pdf", "jpg", "jpeg", "png"], accept_multiple_files=True, key="comp_admin2")
                fecha2 = st.date_input("📅 Fecha 2", value=datetime.today().date(), key="fecha_admin2")
                forma2 = st.selectbox("💳 Forma 2", ["Transferencia", "Depósito en Efectivo", "Tarjeta de Débito", "Tarjeta de Crédito", "Cheque"], key="forma_admin2")
                monto2 = st.number_input("💲 Monto 2", min_value=0.0, format="%.2f", key="monto_admin2")
                terminal2 = banco2 = ""
                if forma2 in ["Tarjeta de Débito", "Tarjeta de Crédito"]:
                    terminal2 = st.selectbox("🏧 Terminal 2", ["BANORTE", "AFIRME", "VELPAY", "CLIP", "PAYPAL", "BBVA", "CONEKTA"], key="terminal_admin2")
                else:
                    banco2 = st.selectbox("🏦 Banco 2", ["BANORTE", "BANAMEX", "AFIRME", "BANCOMER OP", "BANCOMER CURSOS"], key="banco_admin2")
                ref2 = st.text_input("🔢 Referencia 2", key="ref_admin2")

                comprobante_pago_files = (comp1 or []) + (comp2 or [])
                fecha_pago = f"{fecha1.strftime('%Y-%m-%d')} y {fecha2.strftime('%Y-%m-%d')}"
                forma_pago = f"{forma1}, {forma2}"
                terminal = f"{terminal1}, {terminal2}" if forma1.startswith("Tarjeta") or forma2.startswith("Tarjeta") else ""
                banco_destino = f"{banco1}, {banco2}" if forma1 not in ["Tarjeta de Débito", "Tarjeta de Crédito"] or forma2 not in ["Tarjeta de Débito", "Tarjeta de Crédito"] else ""
                monto_pago = monto1 + monto2
                referencia_pago = f"{ref1}, {ref2}"


# --- ESTADÍSTICAS GENERALES ---
st.markdown("---")
st.header("📊 Estadísticas Generales")

if not df_pedidos.empty:
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_pedidos = len(df_pedidos)
        st.metric("Total Pedidos", total_pedidos)
    
    with col2:
        pedidos_pagados = len(df_pedidos[df_pedidos.get('Estado_Pago') == '✅ Pagado']) if 'Estado_Pago' in df_pedidos.columns else 0
        st.metric("Pedidos Pagados", pedidos_pagados)
    
    with col3:
        pedidos_confirmados = len(df_pedidos[df_pedidos.get('Comprobante_Confirmado') == 'Sí']) if 'Comprobante_Confirmado' in df_pedidos.columns else 0
        st.metric("Comprobantes Confirmados", pedidos_confirmados)
    
    with col4:
        pedidos_pendientes_confirmacion = len(pedidos_pagados_no_confirmados) if 'pedidos_pagados_no_confirmados' in locals() else 0
        st.metric("Pendientes Confirmación", pedidos_pendientes_confirmacion)

# --- NUEVA PESTAÑA: DESCARGA DE COMPROBANTES CONFIRMADOS ---
st.markdown("---")
mostrar_descarga_confirmados = st.toggle("🔽 Mostrar/Descargar Pedidos Confirmados", value=False)

# 🧠 Inicializar caché de sesión
if "confirmados_cargados" not in st.session_state:
    st.session_state.confirmados_cargados = 0
if "df_confirmados_cache" not in st.session_state:
    st.session_state.df_confirmados_cache = pd.DataFrame()

if mostrar_descarga_confirmados:
    st.markdown("### 📥 Pedidos Confirmados - Comprobantes de Pago")

    # Filtrar pedidos confirmados
    df_confirmados_actuales = df_pedidos[
        (df_pedidos['Estado_Pago'] == '✅ Pagado') &
        (df_pedidos['Comprobante_Confirmado'] == 'Sí')
    ].copy()

    total_actual = len(df_confirmados_actuales)

    if total_actual == 0:
        st.info("ℹ️ No hay pedidos con comprobantes confirmados para mostrar.")
    elif total_actual == st.session_state.confirmados_cargados:
        st.success("✅ Mostrando comprobantes confirmados en caché.")
        df_vista = st.session_state.df_confirmados_cache
    else:
        st.info("🔄 Cargando comprobantes confirmados actualizados...")
        with st.spinner("Buscando archivos en S3..."):

            df_confirmados_actuales = df_confirmados_actuales.sort_values(by='Fecha_Pago_Comprobante', ascending=False)

            link_comprobantes = []
            link_facturas = []
            link_guias = []
            link_refacturaciones = [] 


            for _, row in df_confirmados_actuales.iterrows():
                pedido_id = row.get("ID_Pedido")
                tipo_envio = "foráneo" if "foráneo" in row.get("Tipo_Envio", "").lower() else "local"
                comprobante_url = ""
                factura_url = ""
                guia_url = ""

                if pedido_id:
                    prefix = f"{S3_ATTACHMENT_PREFIX}{pedido_id}/"
                    files = get_files_in_s3_prefix(s3_client, prefix)

                    if not files:
                        prefix = find_pedido_subfolder_prefix(s3_client, S3_ATTACHMENT_PREFIX, pedido_id)
                        files = get_files_in_s3_prefix(s3_client, prefix) if prefix else []

                    # 📄 COMPROBANTE
                    comprobantes = [f for f in files if "comprobante" in f["title"].lower()]
                    if comprobantes:
                        comprobante_url = comprobantes[0]['key']
                        comprobante_url = f"https://{S3_BUCKET_NAME}.s3.{AWS_REGION_NAME}.amazonaws.com/{comprobante_url}"

                    # 📑 FACTURA
                    facturas = [f for f in files if "factura" in f["title"].lower()]
                    if facturas:
                        factura_url = facturas[0]['key']
                        factura_url = f"https://{S3_BUCKET_NAME}.s3.{AWS_REGION_NAME}.amazonaws.com/{factura_url}"

                    # 📦 GUIA
                    import re

                    if tipo_envio == "foráneo":
                        guias_filtradas = [
                            f for f in files
                            if f["title"].lower().endswith(".pdf")
                            and re.search(r"(gu[ií]a|descarga)", f["title"].lower())
                        ]

                    else:  # local
                        guias_filtradas = [
                            f for f in files
                            if f["title"].lower().endswith(".xlsx")
                        ]


                    if guias_filtradas:
                        guias_con_surtido = [f for f in guias_filtradas if "surtido" in f["title"].lower()]
                        guia_final = guias_con_surtido[0] if guias_con_surtido else guias_filtradas[0]
                        guia_url = f"https://{S3_BUCKET_NAME}.s3.{AWS_REGION_NAME}.amazonaws.com/{guia_final['key']}"

                    # 📌 REFACTURACIÓN (afuera del bloque anterior)
                    refacturas = [f for f in files if "surtido_factura" in f["title"].lower()]
                    if refacturas:
                        refact_url = f"https://{S3_BUCKET_NAME}.s3.{AWS_REGION_NAME}.amazonaws.com/{refacturas[0]['key']}"
                    else:
                        refact_url = ""


                link_comprobantes.append(comprobante_url)
                link_facturas.append(factura_url)
                link_guias.append(guia_url)
                link_refacturaciones.append(refact_url)



            df_confirmados_actuales["Link_Comprobante"] = link_comprobantes
            df_confirmados_actuales["Link_Factura"] = link_facturas
            df_confirmados_actuales["Link_Guia"] = link_guias
            df_confirmados_actuales["Link_Refacturacion"] = link_refacturaciones

            


            st.session_state.df_confirmados_cache = df_confirmados_actuales
            st.session_state.confirmados_cargados = total_actual
            df_vista = df_confirmados_actuales

    columnas_a_mostrar = [
        'Folio_Factura', 'Folio_Factura_Refacturada',  # 🆕 Nuevo campo
        'Cliente', 'Vendedor_Registro', 'Tipo_Envio', 'Fecha_Entrega',
        'Estado', 'Estado_Pago',
        'Refacturacion_Tipo', 'Refacturacion_Subtipo',  # 🆕 Nuevos campos
        'Forma_Pago_Comprobante', 'Monto_Comprobante',
        'Fecha_Pago_Comprobante', 'Banco_Destino_Pago', 'Terminal', 'Referencia_Comprobante',
        'Link_Comprobante', 'Link_Factura', 'Link_Refacturacion', 'Link_Guia'  # 🆕 Nuevo link
    ]

    columnas_existentes = [col for col in columnas_a_mostrar if col in df_vista.columns]
    st.dataframe(df_vista[columnas_existentes], use_container_width=True, hide_index=True)

    # Botón de descarga
    output_confirmados = BytesIO()
    with pd.ExcelWriter(output_confirmados, engine='xlsxwriter') as writer:
        df_vista[columnas_existentes].to_excel(writer, index=False, sheet_name='Confirmados')
    data_xlsx = output_confirmados.getvalue()

    st.download_button(
        label="📤 Descargar Excel de Confirmados",
        data=data_xlsx,
        file_name=f"pedidos_confirmados_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
