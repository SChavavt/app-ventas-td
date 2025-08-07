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

st.set_page_config(page_title="App Admin TD", layout="wide")

# --- GOOGLE SHEETS CONFIGURATION ---
GOOGLE_SHEET_ID = '1aWkSelodaz0nWfQx7FZAysGnIYGQFJxAN7RO3YgCiZY'

@st.cache_data(ttl=60)
def cargar_pedidos_desde_google_sheet(sheet_id, worksheet_name):
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
    try:
        credentials_json_str = st.secrets["google_credentials"]
        creds_dict = json.loads(credentials_json_str)
        creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n").strip()
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
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
    if not s3_client_instance or not prefix:
        return []
    
    try:
        response = s3_client_instance.list_objects_v2(
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
    Uploads a file-like object to S3.
    Returns (success: bool, url: str)
    """
    try:
        s3_client.upload_fileobj(file_obj, bucket_name, s3_key)
        url = f"https://{bucket_name}.s3.amazonaws.com/{s3_key}"
        return True, url
    except Exception as e:
        print(f"Error uploading to S3: {e}")
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
    st.stop()

# Cargar datos
df_pedidos, headers = cargar_pedidos_desde_google_sheet(GOOGLE_SHEET_ID, "datos_pedidos")
worksheet = get_google_sheets_client().open_by_key(GOOGLE_SHEET_ID).worksheet("datos_pedidos")

st.title("👨‍💼 App de Administración TD")
st.write("Panel de administración para revisar y confirmar comprobantes de pago.")

# --- SECCIÓN 1: COMPROBANTES PENDIENTES ---
st.header("💳 Comprobantes de Pago Pendientes de Confirmación")

if st.button("🔄 Recargar Pedidos desde Google Sheets", type="secondary"):
    st.cache_data.clear()
    st.cache_resource.clear()
    st.rerun()

if df_pedidos.empty:
    st.info("ℹ️ No hay pedidos cargados en este momento.")
else:
    if 'Comprobante_Confirmado' in df_pedidos.columns:
        pedidos_pagados_no_confirmados = df_pedidos[df_pedidos['Estado_Pago'] == '✅ Pagado'].copy()
        pedidos_pagados_no_confirmados = pedidos_pagados_no_confirmados[pedidos_pagados_no_confirmados['Comprobante_Confirmado'] != 'Sí']
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
        st.subheader("🔍 Revisar/Subir Comprobante de Pago")

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
            
            # Lógica para Pedidos a Crédito
            if selected_pedido_data.get("Estado_Pago", "").strip() == "💳 CREDITO":
                st.subheader("📝 Confirmación de Pedido a Crédito")
                selected_pedido_id_for_s3_search = selected_pedido_data.get('ID_Pedido', 'N/A')

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
                            for f in files:
                                url = get_s3_file_download_url(s3_client, f['key'])
                                nombre = f['title'].replace(selected_pedido_id_for_s3_search, "").strip("_-")
                                st.markdown(f"- 📄 **{nombre}** ({f['size']} bytes) [🔗 Ver/Descargar]({url})")
                        else:
                            st.info("📁 No se encontraron archivos en la carpeta del pedido.")

                confirmacion_credito = st.selectbox("¿Confirmar que el pedido fue autorizado como crédito?", ["", "Sí", "No"], key="confirmacion_credito")
                comentario_credito = st.text_area("✍️ Comentario administrativo", key="comentario_credito")

                if confirmacion_credito:
                    if st.button("💾 Guardar Confirmación de Crédito", key="btn_guardar_credito"):
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
            
            # Lógica para Pedido Local y No Pagado
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
                    
                if st.button("💾 Guardar Comprobante y Datos de Pago", key="btn_guardar_pago_local"):
                    try:
                        gsheet_row_index = df_pedidos[df_pedidos['ID_Pedido'] == selected_pedido_data["ID_Pedido"]].index[0] + 2
                        pedido_id = selected_pedido_data["ID_Pedido"]
                        pedido_folder_prefix = f"{S3_ATTACHMENT_PREFIX}{pedido_id}/"
                        adjuntos_urls = []
                        
                        if comprobantes_nuevo:
                            for file in comprobantes_nuevo:
                                file_uuid = str(uuid.uuid4())
                                file_extension = os.path.splitext(file.name)[1]
                                s3_key = f"{pedido_folder_prefix}comprobante_{file_uuid}{file_extension}"
                                success, url = upload_file_to_s3(s3_client, S3_BUCKET_NAME, file, s3_key)
                                if success:
                                    adjuntos_urls.append(url)
                                else:
                                    st.error(f"❌ Error al subir el archivo {file.name} a S3.")
                                    st.stop()
                        
                        if "Comprobante_Confirmado" in headers:
                            worksheet.update_cell(gsheet_row_index, headers.index("Comprobante_Confirmado") + 1, "Sí")
                        
                        if "Estado_Pago" in headers:
                            worksheet.update_cell(gsheet_row_index, headers.index("Estado_Pago") + 1, "✅ Pagado")

                        campos_a_actualizar = {
                            "Fecha_Pago": str(fecha_pago),
                            "Forma_Pago": forma_pago,
                            "Monto_Pago": monto_pago,
                            "Banco_Destino": banco_destino,
                            "Terminal": terminal,
                            "Referencia": referencia,
                            "Comprobante_URL": ", ".join(adjuntos_urls)
                        }
                        
                        for campo, valor in campos_a_actualizar.items():
                            if campo in headers:
                                worksheet.update_cell(gsheet_row_index, headers.index(campo) + 1, valor)
                        
                        st.success("✅ Comprobante y detalles de pago guardados exitosamente.")
                        st.balloons()
                        time.sleep(2)
                        st.cache_data.clear()
                        st.rerun()

                    except Exception as e:
                        st.error(f"❌ Error al guardar el comprobante: {e}")

            # Lógica para Pedidos con Comprobante pero no Confirmados
            else:
                st.subheader("🧾 Revisar Comprobante(s) Existente(s)")
                selected_pedido_id_for_s3_search = selected_pedido_data.get('ID_Pedido', 'N/A')
                
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
                    st.write(f"**Monto:** {selected_pedido_data.get('Monto', 'N/A')}")
                    
                with col2:
                    st.subheader("📎 Archivos y Comprobantes")
                    if s3_client:
                        pedido_folder_prefix = find_pedido_subfolder_prefix(s3_client, S3_ATTACHMENT_PREFIX, selected_pedido_id_for_s3_search)
                        files = get_files_in_s3_prefix(s3_client, pedido_folder_prefix) if pedido_folder_prefix else []
                        if files:
                            comprobantes = [f for f in files if 'comprobante' in f['title'].lower()]
                            facturas = [f for f in files if 'factura' in f['title'].lower()]
                            guias = [f for f in files if 'guia' in f['title'].lower()]
                            refacturas = [f for f in files if 'refactura' in f['title'].lower()]
                            otros = [f for f in files if f not in comprobantes and f not in facturas and f not in guias and f not in refacturas]

                            if comprobantes:
                                st.write("**🧾 Comprobantes de Pago:**")
                                for f in comprobantes:
                                    url = get_s3_file_download_url(s3_client, f['key'])
                                    nombre = f['title'].replace(selected_pedido_id_for_s3_search, "").strip("_-")
                                    st.markdown(f"- 📄 **{nombre}** ({f['size']} bytes) [🔗 Ver/Descargar]({url})")
                            else:
                                st.warning("⚠️ No se encontraron comprobantes de pago.")

                            if facturas:
                                with st.expander("📑 Facturas de Venta"):
                                    for f in facturas:
                                        url = get_s3_file_download_url(s3_client, f['key'])
                                        nombre = f['title'].replace(selected_pedido_id_for_s3_search, "").strip("_-")
                                        st.markdown(f"- 📄 **{nombre}** ({f['size']} bytes) [🔗 Ver/Descargar]({url})")

                            if guias:
                                with st.expander("📦 Guías de Envío"):
                                    for f in guias:
                                        url = get_s3_file_download_url(s3_client, f['key'])
                                        nombre = f['title'].replace(selected_pedido_id_for_s3_search, "").strip("_-")
                                        st.markdown(f"- 📄 **{nombre}** ({f['size']} bytes) [🔗 Ver/Descargar]({url})")

                            if refacturas:
                                with st.expander("📝 Refacturas"):
                                    for f in refacturas:
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

                confirmacion_pago_completo = st.selectbox("¿Confirmar este comprobante como válido y completo?", ["", "Sí", "No"], key="confirmacion_pago_completo")
                comentario_admin = st.text_area("✍️ Comentario administrativo", key="comentario_admin")
                
                if confirmacion_pago_completo:
                    if st.button("💾 Guardar Confirmación de Pago", key="btn_guardar_confirmacion"):
                        try:
                            gsheet_row_index = df_pedidos[df_pedidos['ID_Pedido'] == selected_pedido_data["ID_Pedido"]].index[0] + 2

                            if "Comprobante_Confirmado" in headers:
                                worksheet.update_cell(gsheet_row_index, headers.index("Comprobante_Confirmado") + 1, confirmacion_pago_completo)

                            if "Comentario" in headers:
                                comentario_existente = selected_pedido_data.get("Comentario", "")
                                nuevo_comentario = f"Comentario administrativo: {comentario_admin.strip()}"
                                comentario_final = f"{comentario_existente}\n{nuevo_comentario}" if comentario_existente else nuevo_comentario
                                worksheet.update_cell(gsheet_row_index, headers.index("Comentario") + 1, comentario_final)
                            
                            st.success("✅ Confirmación de pago guardada exitosamente.")
                            st.balloons()
                            time.sleep(2)
                            st.cache_data.clear()
                            st.rerun()

                        except Exception as e:
                            st.error(f"❌ Error al guardar la confirmación: {e}")
                else:
                    st.info("Selecciona una opción para confirmar el pago.")

st.markdown("---")
# --- SECCIÓN 2: PEDIDOS CONFIRMADOS ---
st.header("📥 Base de Datos de Pedidos Confirmados")

if st.button("🔍 Generar Base de Datos de Pedidos Confirmados", type="primary"):
    with st.spinner("Buscando pedidos confirmados y archivos en S3..."):
        pedidos_confirmados = df_pedidos[
            (df_pedidos.get('Estado_Pago', '') == '✅ Pagado') &
            (df_pedidos.get('Comprobante_Confirmado', '') == 'Sí')
        ].copy()

        if not pedidos_confirmados.empty:
            pedidos_confirmados['Links_S3_Comprobantes'] = ""
            pedidos_confirmados['Links_S3_Facturas'] = ""
            pedidos_confirmados['Links_S3_Guias'] = ""
            pedidos_confirmados['Links_S3_Refacturas'] = ""

            for index, row in pedidos_confirmados.iterrows():
                pedido_id = row['ID_Pedido']
                pedido_folder_prefix = find_pedido_subfolder_prefix(s3_client, S3_ATTACHMENT_PREFIX, pedido_id)
                
                if pedido_folder_prefix:
                    files = get_files_in_s3_prefix(s3_client, pedido_folder_prefix)
                    comprobantes = []
                    facturas = []
                    guias = []
                    refacturas = []

                    for f in files:
                        url = get_s3_file_download_url(s3_client, f['key'])
                        if 'comprobante' in f['title'].lower():
                            comprobantes.append(url)
                        elif 'factura' in f['title'].lower():
                            facturas.append(url)
                        elif 'guia' in f['title'].lower():
                            guias.append(url)
                        elif 'refactura' in f['title'].lower():
                            refacturas.append(url)
                    
                    pedidos_confirmados.at[index, 'Links_S3_Comprobantes'] = "\n".join(comprobantes)
                    pedidos_confirmados.at[index, 'Links_S3_Facturas'] = "\n".join(facturas)
                    pedidos_confirmados.at[index, 'Links_S3_Guias'] = "\n".join(guias)
                    pedidos_confirmados.at[index, 'Links_S3_Refacturas'] = "\n".join(refacturas)

            st.success("✅ Base de datos de pedidos confirmados generada exitosamente.")
            st.dataframe(pedidos_confirmados, use_container_width=True)

            # Preparar y descargar Excel
            output_confirmados = BytesIO()
            with pd.ExcelWriter(output_confirmados, engine='xlsxwriter') as writer:
                pedidos_confirmados.to_excel(writer, index=False, sheet_name='Pedidos Confirmados')
            data_xlsx = output_confirmados.getvalue()

            st.download_button(
                label="📤 Descargar Excel de Confirmados",
                data=data_xlsx,
                file_name=f"pedidos_confirmados_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            st.info("ℹ️ No se encontraron pedidos que cumplan los criterios de confirmación.")
st.markdown("---")
# --- SECCIÓN 3: ESTADÍSTICAS GENERALES ---
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
        st.metric("Pedidos Confirmados", pedidos_confirmados)

    with col4:
        pedidos_pendientes = total_pedidos - pedidos_confirmados
        st.metric("Pedidos Pendientes", pedidos_pendientes)

else:
    st.info("ℹ️ No hay datos para mostrar estadísticas.")

