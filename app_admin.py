# app_admin.py
import streamlit as st
import json
import time
import pandas as pd
import boto3
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from io import BytesIO
from datetime import datetime
import os
import uuid

# --- CONFIGURACIÓN DE LA PÁGINA ---
st.set_page_config(page_title="App Admin TD", layout="wide")

# --- GOOGLE SHEETS CONFIGURATION ---
GOOGLE_SHEET_ID = '1aWkSelodaz0nWfQx7FZAysGnIYGQFJxAN7RO3YgCiZY'
@st.cache_data(ttl=60)
def cargar_pedidos_desde_google_sheet(sheet_id, worksheet_name):
    """
    Carga los datos de la hoja de Google Sheets en un DataFrame de Pandas.
    Aplica una caché para no recargar los datos en cada interacción si no ha pasado el tiempo TTL.
    """
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

@st.cache_resource
def get_google_sheets_client():
    """
    Establece la conexión con Google Sheets usando las credenciales de los secretos de Streamlit.
    """
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
        st.cache_resource.clear()
        st.warning("🔁 Token expirado o inválido. Reintentando autenticación...")
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        _ = client.open_by_key(GOOGLE_SHEET_ID)
        return client

    except Exception as e:
        st.error(f"❌ Error crítico al autenticar con Google Sheets: {e}")
        st.stop()

# Cargar datos iniciales
df_pedidos, headers = cargar_pedidos_desde_google_sheet(GOOGLE_SHEET_ID, "datos_pedidos")
worksheet = get_google_sheets_client().open_by_key(GOOGLE_SHEET_ID).worksheet("datos_pedidos")

# --- CONFIGURACIÓN DE AWS S3 ---
try:
    AWS_ACCESS_KEY_ID = st.secrets["aws_access_key_id"]
    AWS_SECRET_ACCESS_KEY = st.secrets["aws_secret_access_key"]
    AWS_REGION_NAME = st.secrets["aws_region"]
    S3_BUCKET_NAME = st.secrets["s3_bucket_name"]
except KeyError as e:
    st.error(f"❌ Error: Las credenciales de AWS S3 no se encontraron en Streamlit secrets. Asegúrate de que las claves 'aws_access_key_id', 'aws_secret_access_key', 'aws_region' y 's3_bucket_name' estén directamente en tus secretos de Streamlit. Clave faltante: {e}")
    st.stop() 

S3_ATTACHMENT_PREFIX = 'adjuntos_pedidos/'

# --- FUNCIONES DE CARGA DE DATOS Y S3 ---

@st.cache_resource
def get_s3_client_cached():
    """
    Retorna un cliente de S3. Usa @st.cache_resource para evitar reconexiones.
    """
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

def find_pedido_subfolder_prefix(s3_client_instance, parent_prefix, folder_name):
    """
    Busca la carpeta de un pedido en S3.
    """
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
        response = s3_client_instance.list_objects_v2(
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

def get_files_in_s3_prefix(s3_client_instance, prefix):
    """
    Obtiene la lista de archivos dentro de un prefijo de S3.
    """
    if not s3_client_instance or not prefix:
        return []
    
    try:
        response = s3_sclient_instance.list_objects_v2(
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

def get_s3_file_download_url(s3_client_instance, object_key):
    """
    Genera una URL pre-firmada para descargar un archivo de S3.
    """
    if not s3_client_instance or not object_key:
        return "#"
    
    try:
        url = s3_client_instance.generate_presigned_url(
            'get_object',
            Params={'Bucket': S3_BUCKET_NAME, 'Key': object_key},
            ExpiresIn=7200
        )
        return url
    except Exception as e:
        st.error(f"❌ Error al generar URL pre-firmada para '{object_key}': {e}")
        return "#"
    
def upload_file_to_s3(s3_client, bucket_name, file_obj, s3_key):
    """
    Sube un objeto de archivo a S3.
    Retorna (success: bool, url: str)
    """
    try:
        s3_client.upload_fileobj(file_obj, bucket_name, s3_key)
        url = f"https://{bucket_name}.s3.amazonaws.com/{s3_key}"
        return True, url
    except Exception as e:
        st.error(f"Error subiendo a S3: {e}")
        return False, ""

# --- Inicializar clientes de Gspread y S3 ---
try:
    gc = get_google_sheets_client()
    s3_client = get_s3_client_cached()
    
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

st.title("👨‍💼 App de Administración TD")
st.write("Panel de administración para revisar y confirmar comprobantes de pago.")

# --- SECCIÓN 1: PENDIENTES DE CONFIRMAR ---
st.header("💳 Comprobantes de Pago Pendientes de Confirmación")
st.markdown("---")

if st.button("🔄 Recargar Pedidos desde Google Sheets", type="secondary"):
    st.cache_data.clear()
    st.cache_resource.clear()
    st.rerun()

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

            if 'Fecha_Entrega' in df_vista.columns:
                df_vista['Fecha_Entrega'] = pd.to_datetime(df_vista['Fecha_Entrega'], errors='coerce').dt.strftime('%d/%m/%Y')

            st.dataframe(
                df_vista.sort_values(by='Fecha_Entrega' if 'Fecha_Entrega' in df_vista.columns else existing_columns[0]),
                use_container_width=True,
                hide_index=True
            )

        st.markdown("---")
        st.subheader("🔍 Revisar Comprobante de Pago")
        
        pedidos_pagados_no_confirmados['display_label'] = pedidos_pagados_no_confirmados.apply(lambda row: (
            f"📄 {row.get('Folio_Factura', 'N/A')} - "
            f"� {row.get('Cliente', 'N/A')} - "
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

            if selected_pedido_data.get("Estado_Pago", "").strip() == "💳 CREDITO":
                st.subheader("📝 Confirmación de Pedido a Crédito")
                
                col1, col2 = st.columns(2)
                with col1:
                    st.subheader("📋 Información del Pedido")
                    st.write(f"**📄 Folio Factura:** {selected_pedido_data.get('Folio_Factura', 'N/A')}")
                    st.write(f"**🗒 Comentario del Pedido:** {selected_pedido_data.get('Comentario', 'Sin comentario')}")
                    st.write(f"**🤝 Cliente:** {selected_pedido_data.get('Cliente', 'N/A')}")
                    st.write(f"**🧑‍💼 Vendedor:** {selected_pedido_data.get('Vendedor_Registro', 'N/A')}")
                    st.write(f"**Tipo de Envío:** {selected_pedido_data.get('Tipo_Envio', 'N/A')}")
                    st.write(f"**📅 Fecha de Entrega:** {selected_pedido_data.get('Fecha_Entrega', 'N/A')}")
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
                
                confirmacion_credito = st.selectbox("¿Confirmar que el pedido fue autorizado como crédito?", ["", "Sí", "No"])
                comentario_credito = st.text_area("✍️ Comentario administrativo")
                
                if confirmacion_credito:
                    if st.button("💾 Guardar Confirmación de Crédito"):
                        try:
                            gsheet_row_index = df_pedidos[df_pedidos['ID_Pedido'] == selected_pedido_data["ID_Pedido"]].index[0] + 2
                            if "Comprobante_Confirmado" in headers:
                                worksheet.update_cell(gsheet_row_index, headers.index("Comprobante_Confirmado") + 1, confirmacion_credito)
                            if "Comentario" in headers:
                                comentario_existente = selected_pedido_data.get("Comentario", "")
                                nuevo_comentario = f"Comentario de CREDITO: {comentario_credito.strip()}"
                                comentario_final = f"{comentario_existente}\n{nuevo_comentario}" if comentario_existente else nuevo_comentario
                                worksheet.update_cell(gsheet_row_index, headers.index("Comentario") + 1, comentario_final)
                            st.success("✅ Confirmación de crédito guardada exitosamente.")
                            st.balloons()
                            time.sleep(2)
                            st.cache_data.clear()
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ Error al guardar la confirmación: {e}")
                else:
                    st.info("Selecciona una opción para confirmar el crédito.")
            
            elif (
                selected_pedido_data.get("Estado_Pago", "").strip() == "🔴 No Pagado" and
                selected_pedido_data.get("Tipo_Envio", "").strip() == "📍 Pedido Local"
            ):
                st.subheader("🧾 Subir Comprobante de Pago")
                pago_doble = st.checkbox("✅ Pago en dos partes distintas", key="pago_doble_admin")
                comprobantes_nuevo = []
                if not pago_doble:
                    comprobantes_nuevo = st.file_uploader(
                        "📤 Subir Comprobante(s) de Pago",
                        type=["pdf", "jpg", "jpeg", "png"],
                        accept_multiple_files=True,
                        key="comprobante_local_no_pagado"
                    )
                    with st.expander("📝 Detalles del Pago"):
                        fecha_pago = st.date_input("📅 Fecha del Pago", value=datetime.today().date(), key="fecha_pago_local")
                        forma_pago = st.selectbox("💳 Forma de Pago", [
                            "Transferencia", "Depósito en Efectivo", "Tarjeta de Débito", "Tarjeta de Crédito", "Cheque"
                        ], key="forma_pago_local")
                        monto_pago = st.number_input("💲 Monto del Pago", min_value=0.0, format="%.2f", key="monto_pago_local")
                        if forma_pago in ["Tarjeta de Débito", "Tarjeta de Crédito"]:
                            terminal = st.selectbox("🏧 Terminal", ["BANORTE", "AFIRME", "VELPAY", "CLIP", "PAYPAL", "BBVA", "CONEKTA"], key="terminal_local")
                            banco_destino = ""
                        else:
                            banco_destino = st.selectbox("🏦 Banco Destino", ["BANORTE", "BANAMEX", "AFIRME", "BANCOMER OP", "BANCOMER CURSOS"], key="banco_destino_local")
                            terminal = ""
                        referencia = st.text_input("🔢 Referencia (opcional)", key="referencia_local")
                else:
                    st.markdown("### 1️⃣ Primer Pago")
                    comp1 = st.file_uploader("💳 Comprobante 1", type=["pdf", "jpg", "jpeg", "png"], accept_multiple_files=True, key="cp_pago1_admin")
                    fecha1 = st.date_input("📅 Fecha 1", value=datetime.today().date(), key="fecha_pago1_admin")
                    forma1 = st.selectbox("💳 Forma 1", ["Transferencia", "Depósito en Efectivo", "Tarjeta de Débito", "Tarjeta de Crédito", "Cheque"], key="forma_pago1_admin")
                    monto1 = st.number_input("💲 Monto 1", min_value=0.0, format="%.2f", key="monto_pago1_admin")
                    terminal1 = banco1 = ""
                    if forma1 in ["Tarjeta de Débito", "Tarjeta de Crédito"]:
                        terminal1 = st.selectbox("🏧 Terminal 1", ["BANORTE", "AFIRME", "VELPAY", "CLIP", "PAYPAL", "BBVA", "CONEKTA"], key="terminal1_admin")
                    else:
                        banco1 = st.selectbox("🏦 Banco 1", ["BANORTE", "BANAMEX", "AFIRME", "BANCOMER OP", "BANCOMER CURSOS"], key="banco1_admin")
                    ref1 = st.text_input("🔢 Referencia 1", key="ref1_admin")
                    st.markdown("### 2️⃣ Segundo Pago")
                    comp2 = st.file_uploader("💳 Comprobante 2", type=["pdf", "jpg", "jpeg", "png"], accept_multiple_files=True, key="cp_pago2_admin")
                    fecha2 = st.date_input("📅 Fecha 2", value=datetime.today().date(), key="fecha_pago2_admin")
                    forma2 = st.selectbox("💳 Forma 2", ["Transferencia", "Depósito en Efectivo", "Tarjeta de Débito", "Tarjeta de Crédito", "Cheque"], key="forma_pago2_admin")
                    monto2 = st.number_input("💲 Monto 2", min_value=0.0, format="%.2f", key="monto_pago2_admin")
                    terminal2 = banco2 = ""
                    if forma2 in ["Tarjeta de Débito", "Tarjeta de Crédito"]:
                        terminal2 = st.selectbox("🏧 Terminal 2", ["BANORTE", "AFIRME", "VELPAY", "CLIP", "PAYPAL", "BBVA", "CONEKTA"], key="terminal2_admin")
                    else:
                        banco2 = st.selectbox("🏦 Banco 2", ["BANORTE", "BANAMEX", "AFIRME", "BANCOMER OP", "BANCOMER CURSOS"], key="banco2_admin")
                    ref2 = st.text_input("🔢 Referencia 2", key="ref2_admin")
                    comprobantes_nuevo = (comp1 or []) + (comp2 or [])
                    fecha_pago = f"{fecha1.strftime('%Y-%m-%d')} y {fecha2.strftime('%Y-%m-%d')}"
                    forma_pago = f"{forma1}, {forma2}"
                    terminal = f"{terminal1}, {terminal2}" if forma1.startswith("Tarjeta") or forma2.startswith("Tarjeta") else ""
                    banco_destino = f"{banco1}, {banco2}" if forma1 not in ["Tarjeta de Débito", "Tarjeta de Crédito"] or forma2 not in ["Tarjeta de Débito", "Tarjeta de Crédito"] else ""
                    monto_pago = monto1 + monto2
                    referencia = f"{ref1}, {ref2}"

                if st.button("💾 Guardar Comprobante y Datos de Pago"):
                    try:
                        gsheet_row_index = df_pedidos[df_pedidos['ID_Pedido'] == selected_pedido_data["ID_Pedido"]].index[0] + 2
                        adjuntos_urls = []
                        if comprobantes_nuevo:
                            for file in comprobantes_nuevo:
                                ext = os.path.splitext(file.name)[1]
                                s3_key = f"{selected_pedido_data['ID_Pedido']}/comprobante_{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:4]}{ext}"
                                success, url = upload_file_to_s3(s3_client, S3_BUCKET_NAME, file, s3_key)
                                if success:
                                    adjuntos_urls.append(url)
                        updates = {
                            'Estado_Pago': '✅ Pagado',
                            'Comprobante_Confirmado': 'Sí',
                            'Fecha_Pago_Comprobante': fecha_pago.strftime('%Y-%m-%d') if not isinstance(fecha_pago, str) else fecha_pago,
                            'Forma_Pago_Comprobante': forma_pago,
                            'Monto_Comprobante': monto_pago,
                            'Referencia_Comprobante': referencia,
                            'Terminal': terminal,
                            'Banco_Destino_Pago': banco_destino,
                        }
                        for col, val in updates.items():
                            if col in headers:
                                worksheet.update_cell(gsheet_row_index, headers.index(col) + 1, val)
                        if adjuntos_urls and "Adjuntos" in headers:
                            adjuntos_actuales = selected_pedido_data.get("Adjuntos", "")
                            nuevo_valor_adjuntos = ", ".join(filter(None, [adjuntos_actuales] + adjuntos_urls))
                            worksheet.update_cell(gsheet_row_index, headers.index("Adjuntos") + 1, nuevo_valor_adjuntos)
                        st.success("✅ Comprobante y datos de pago guardados exitosamente.")
                        st.balloons()
                        time.sleep(2)
                        st.cache_data.clear()
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ Error al guardar el comprobante: {e}")
            
            else: # Caso de pedidos que ya tienen comprobante subido por el cliente
                st.subheader("✅ Confirmar Comprobante")
                
                col1, col2 = st.columns(2)
                with col1:
                    st.subheader("📋 Información del Pedido")
                    st.write(f"**Folio Factura:** {selected_pedido_data.get('Folio_Factura', 'N/A')}")
                    st.write(f"**🗒 Comentario del Pedido:** {selected_pedido_data.get('Comentario', 'Sin comentario')}")
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
                
                num_comprobantes = len(comprobantes)
                if num_comprobantes == 0:
                    st.warning("⚠️ No hay comprobantes para confirmar.")
                else:
                    st.markdown("---")
                    st.subheader("✅ Confirmar Comprobante")
                    
                    # Cargar datos existentes para prellenar
                    fecha_list = str(selected_pedido_data.get('Fecha_Pago_Comprobante', '')).split(" y ")
                    forma_list = str(selected_pedido_data.get('Forma_Pago_Comprobante', '')).split(", ")
                    banco_list = str(selected_pedido_data.get('Banco_Destino_Pago', '')).split(", ")
                    terminal_list = str(selected_pedido_data.get('Terminal', '')).split(", ")
                    monto_list_raw = selected_pedido_data.get('Monto_Comprobante', '')
                    ref_list = str(selected_pedido_data.get('Referencia_Comprobante', '')).split(", ")

                    if isinstance(monto_list_raw, str) and "," in monto_list_raw:
                        monto_list = [float(m.strip()) if m.strip() else 0.0 for m in monto_list_raw.split(",")]
                    else:
                        try:
                            monto_list = [float(monto_list_raw)] if monto_list_raw else []
                        except Exception:
                            monto_list = []
                    
                    while len(fecha_list) < num_comprobantes: fecha_list.append("")
                    while len(forma_list) < num_comprobantes: forma_list.append("Transferencia")
                    while len(banco_list) < num_comprobantes: banco_list.append("")
                    while len(terminal_list) < num_comprobantes: terminal_list.append("")
                    while len(monto_list) < num_comprobantes: monto_list.append(0.0)
                    while len(ref_list) < num_comprobantes: ref_list.append("")
                    
                    final_fechas, final_formas, final_bancos, final_terminales, final_montos, final_refs = [], [], [], [], [], []

                    for i in range(num_comprobantes):
                        st.markdown(f"**Comprobante #{i+1}: {comprobantes[i]['title']}**")
                        
                        col_form1, col_form2 = st.columns(2)
                        with col_form1:
                            fecha_pago_val = fecha_list[i]
                            fecha_pago_obj = pd.to_datetime(fecha_pago_val, errors='coerce').date() if fecha_pago_val else datetime.today().date()
                            fecha_input = st.date_input("📅 Fecha del Pago", value=fecha_pago_obj, key=f"fecha_pago_{i}")
                            final_fechas.append(fecha_input)
                            
                            forma_pago_val = forma_list[i] if i < len(forma_list) else "Transferencia"
                            forma_input = st.selectbox("💳 Forma de Pago", ["Transferencia", "Depósito en Efectivo", "Tarjeta de Débito", "Tarjeta de Crédito", "Cheque"], index=["Transferencia", "Depósito en Efectivo", "Tarjeta de Débito", "Tarjeta de Crédito", "Cheque"].index(forma_pago_val), key=f"forma_pago_{i}")
                            final_formas.append(forma_input)
                            
                        with col_form2:
                            monto_pago_val = monto_list[i] if i < len(monto_list) else 0.0
                            monto_input = st.number_input("💲 Monto del Pago", min_value=0.0, format="%.2f", value=monto_pago_val, key=f"monto_pago_{i}")
                            final_montos.append(monto_input)

                            if forma_input in ["Tarjeta de Débito", "Tarjeta de Crédito"]:
                                terminal_val = terminal_list[i] if i < len(terminal_list) else "BANORTE"
                                terminal_input = st.selectbox("🏧 Terminal", ["BANORTE", "AFIRME", "VELPAY", "CLIP", "PAYPAL", "BBVA", "CONEKTA"], index=["BANORTE", "AFIRME", "VELPAY", "CLIP", "PAYPAL", "BBVA", "CONEKTA"].index(terminal_val), key=f"terminal_{i}")
                                final_terminales.append(terminal_input)
                                final_bancos.append("")
                            else:
                                banco_val = banco_list[i] if i < len(banco_list) else "BANORTE"
                                banco_input = st.selectbox("🏦 Banco Destino", ["BANORTE", "BANAMEX", "AFIRME", "BANCOMER OP", "BANCOMER CURSOS"], index=["BANORTE", "BANAMEX", "AFIRME", "BANCOMER OP", "BANCOMER CURSOS"].index(banco_val), key=f"banco_destino_{i}")
                                final_bancos.append(banco_input)
                                final_terminales.append("")
                    
                    referencia_val = ref_list[0] if len(ref_list) > 0 else ""
                    referencia_input = st.text_input("🔢 Referencia (opcional)", value=referencia_val, key="referencia_comprobante")
                    final_refs.append(referencia_input)

                    if st.button("💾 Confirmar Pedido y Guardar Datos"):
                        try:
                            gsheet_row_index = df_pedidos[df_pedidos['ID_Pedido'] == selected_pedido_data["ID_Pedido"]].index[0] + 2

                            updates = {
                                'Estado_Pago': '✅ Pagado',
                                'Comprobante_Confirmado': 'Sí',
                                'Fecha_Pago_Comprobante': ' y '.join([f.strftime('%Y-%m-%d') for f in final_fechas]),
                                'Forma_Pago_Comprobante': ', '.join(final_formas),
                                'Monto_Comprobante': sum(final_montos),
                                'Referencia_Comprobante': ', '.join(final_refs),
                                'Terminal': ', '.join(filter(None, final_terminales)),
                                'Banco_Destino_Pago': ', '.join(filter(None, final_bancos)),
                            }

                            for col, val in updates.items():
                                if col in headers:
                                    worksheet.update_cell(gsheet_row_index, headers.index(col) + 1, val)
                            
                            st.success("✅ ¡Comprobante de pago confirmado y datos guardados exitosamente!")
                            st.balloons()
                            time.sleep(2)
                            st.cache_data.clear()
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ Error al guardar la confirmación: {e}")
                            st.stop()


# --- SECCIÓN 2: CONFIRMADOS ---
st.header("📥 Pedidos Confirmados")
st.markdown("---")

if st.button("Generate Excel for Confirmed Orders", type="primary"):
    if not df_pedidos.empty and 'Comprobante_Confirmado' in df_pedidos.columns:
        df_confirmados = df_pedidos[df_pedidos['Comprobante_Confirmado'] == 'Sí'].copy()
        
        if df_confirmados.empty:
            st.info("ℹ️ No hay pedidos confirmados para descargar.")
        else:
            output_confirmados = BytesIO()
            with pd.ExcelWriter(output_confirmados, engine='xlsxwriter') as writer:
                df_confirmados.to_excel(writer, index=False, sheet_name='Pedidos Confirmados')
            data_xlsx = output_confirmados.getvalue()

            st.download_button(
                label="📤 Descargar Excel de Confirmados",
                data=data_xlsx,
                file_name=f"pedidos_confirmados_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
    else:
        st.info("ℹ️ Haz clic en el botón para generar los pedidos confirmados.")

# --- SECCIÓN 3: ESTADÍSTICAS GENERALES ---
st.header("📊 Estadísticas Generales")
st.markdown("---")

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
        pedidos_no_confirmados = total_pedidos - pedidos_confirmados
        st.metric("Pendientes de Confirmar", pedidos_no_confirmados)

    st.markdown("---")
    st.subheader("Total de Ventas por Vendedor (Pedidos Pagados)")

    if 'Vendedor_Registro' in df_pedidos.columns and 'Monto_Comprobante' in df_pedidos.columns:
        df_ventas = df_pedidos[df_pedidos['Estado_Pago'] == '✅ Pagado'].copy()
        if not df_ventas.empty:
            df_ventas['Monto_Comprobante'] = pd.to_numeric(df_ventas['Monto_Comprobante'], errors='coerce')
            df_ventas_agrupado = df_ventas.groupby('Vendedor_Registro')['Monto_Comprobante'].sum().reset_index()
            df_ventas_agrupado.columns = ['Vendedor', 'Monto Total']
            st.dataframe(df_ventas_agrupado, use_container_width=True)
    else:
        st.info("ℹ️ Las columnas 'Vendedor_Registro' o 'Monto_Comprobante' no existen para generar las estadísticas de ventas.")
�
