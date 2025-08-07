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

# --- CONFIGURACIÓN DE GOOGLE SHEETS ---
GOOGLE_SHEET_ID = '1aWkSelodaz0nWfQx7FZAysGnIYGQFJxAN7RO3YgCiZY'

@st.cache_data(ttl=60)
def cargar_pedidos_desde_google_sheet(sheet_id, worksheet_name):
    """
    Carga todos los pedidos desde una hoja de Google Sheets y los retorna como un DataFrame.
    """
    try:
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
    except Exception as e:
        st.error(f"❌ Error al cargar datos de Google Sheets: {e}")
        return pd.DataFrame(), []

@st.cache_resource
def get_google_sheets_client():
    """
    Crea y retorna un cliente de gspread, manejando la autenticación.
    """
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
        st.warning("🔁 Token de Google expirado o inválido. Reintentando autenticación...")
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
    st.error(f"❌ Error: Las credenciales de AWS S3 no se encontraron. Clave faltante: {e}")
    st.stop()

S3_ATTACHMENT_PREFIX = 'adjuntos_pedidos/'

# --- FUNCIONES DE CARGA Y S3 ---
@st.cache_resource
def get_s3_client_cached():
    """
    Retorna un cliente de S3 cacheado para evitar re-crearlo en cada re-ejecución.
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
    return None

def get_files_in_s3_prefix(s3_client_instance, prefix):
    """
    Obtiene la lista de archivos en un prefijo S3.
    """
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
    """
    try:
        s3_client.upload_fileobj(file_obj, bucket_name, s3_key)
        url = f"https://{bucket_name}.s3.amazonaws.com/{s3_key}"
        return True, url
    except Exception as e:
        print(f"Error uploading to S3: {e}")
        return False, ""

def update_google_sheets(worksheet, row_index, headers, column_name, value):
    """
    Función auxiliar para actualizar una celda en Google Sheets.
    """
    if column_name in headers:
        col_index = headers.index(column_name) + 1
        worksheet.update_cell(row_index, col_index, value)

# --- Inicializar clientes ---
try:
    gc = get_google_sheets_client()
    s3_client = get_s3_client_cached()
    
    if not s3_client:
        st.error("❌ No se pudo inicializar el cliente de AWS S3.")
        st.stop()
    
    df_pedidos, headers = cargar_pedidos_desde_google_sheet(GOOGLE_SHEET_ID, "datos_pedidos")
    worksheet = get_google_sheets_client().open_by_key(GOOGLE_SHEET_ID).worksheet("datos_pedidos")

except Exception as e:
    st.error(f"❌ Error al autenticarse o inicializar clientes: {e}")
    st.info("ℹ️ Asegúrate de que tus credenciales estén configuradas correctamente en Streamlit secrets.")
    st.stop()

# --- VISTA CONTINUA ---
st.title("👨‍💼 App de Administración TD")
st.write("Panel de administración para revisar y confirmar comprobantes de pago.")
st.markdown("---")

# --- SECCIÓN 1: PENDIENTES DE CONFIRMAR ---
st.header("💳 Comprobantes de Pago Pendientes de Confirmación")

if st.button("🔄 Recargar Pedidos desde Google Sheets", type="secondary"):
    st.cache_data.clear()
    st.cache_resource.clear()
    st.rerun()

if df_pedidos.empty:
    st.info("ℹ️ No hay pedidos cargados en este momento.")
else:
    pedidos_pagados_no_confirmados = df_pedidos[
        (df_pedidos.get('Estado_Pago') == '✅ Pagado') &
        (df_pedidos.get('Comprobante_Confirmado') != 'Sí')
    ].copy()

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
            f"👤 {row.get('Cliente', 'N/A')} - "
            f"{row.get('Estado', 'N/A')} - "
            f"{row.get('Tipo_Envio', 'N/A')}"
        ), axis=1)

        pedido_options = pedidos_pagados_no_confirmados['display_label'].tolist()
        if pedido_options:
            selected_index = st.selectbox(
                "📝 Seleccionar Pedido para Revisar Comprobante",
                options=range(len(pedido_options)),
                format_func=lambda i: pedido_options[i],
                key="select_pedido_comprobante"
            )

            if selected_index is not None:
                selected_pedido_data = pedidos_pagados_no_confirmados.iloc[selected_index]
                st.session_state.selected_admin_pedido_id = selected_pedido_data.get('ID_Pedido', 'N/A')
                
                # Buscamos la fila para futuras actualizaciones
                try:
                    gsheet_row_index = df_pedidos[df_pedidos['ID_Pedido'] == selected_pedido_data["ID_Pedido"]].index[0] + 2
                except IndexError:
                    st.error("❌ No se encontró la fila del pedido en la hoja de cálculo. Por favor, recarga los datos.")
                    st.stop()


                # --- Lógica para pedidos a CRÉDITO ---
                if selected_pedido_data.get("Estado_Pago", "").strip() == "💳 CREDITO":
                    st.subheader("📝 Confirmación de Pedido a Crédito")
                    col1, col2 = st.columns(2)
                    with col1:
                        st.subheader("📋 Información del Pedido")
                        st.write(f"**📄 Folio Factura:** {selected_pedido_data.get('Folio_Factura', 'N/A')}")
                        st.write(f"**🤝 Cliente:** {selected_pedido_data.get('Cliente', 'N/A')}")
                        st.write(f"**🧑‍💼 Vendedor:** {selected_pedido_data.get('Vendedor_Registro', 'N/A')}")
                        st.write(f"**Tipo de Envío:** {selected_pedido_data.get('Tipo_Envio', 'N/A')}")
                        st.write(f"**📅 Fecha de Entrega:** {selected_pedido_data.get('Fecha_Entrega', 'N/A')}")
                    with col2:
                        st.subheader("📎 Archivos")
                        pedido_folder_prefix = find_pedido_subfolder_prefix(s3_client, S3_ATTACHMENT_PREFIX, st.session_state.selected_admin_pedido_id)
                        files = get_files_in_s3_prefix(s3_client, pedido_folder_prefix) if pedido_folder_prefix else []
                        if files:
                            for f in files:
                                url = get_s3_file_download_url(s3_client, f['key'])
                                st.markdown(f"- 📄 **{f['title']}** [🔗 Ver/Descargar]({url})")
                        else:
                            st.info("📁 No se encontraron archivos.")
                    
                    confirmacion_credito = st.selectbox("¿Confirmar que el pedido fue autorizado como crédito?", ["", "Sí", "No"], key="confirmacion_credito")
                    comentario_credito = st.text_area("✍️ Comentario administrativo", key="comentario_credito")

                    if st.button("💾 Guardar Confirmación de Crédito", key="btn_guardar_credito"):
                        if confirmacion_credito:
                            try:
                                update_google_sheets(worksheet, gsheet_row_index, headers, "Comprobante_Confirmado", confirmacion_credito)
                                comentario_existente = selected_pedido_data.get("Comentario", "")
                                nuevo_comentario = f"Comentario de CREDITO: {comentario_credito.strip()}"
                                comentario_final = f"{comentario_existente}\n{nuevo_comentario}" if comentario_existente else nuevo_comentario
                                update_google_sheets(worksheet, gsheet_row_index, headers, "Comentario", comentario_final)
                                st.success("✅ Confirmación de crédito guardada exitosamente.")
                                st.balloons()
                                time.sleep(2)
                                st.cache_data.clear()
                                st.rerun()
                            except Exception as e:
                                st.error(f"❌ Error al guardar la confirmación de crédito: {e}")
                        else:
                            st.warning("Por favor, selecciona una opción para la confirmación del crédito.")
                    
                # --- Lógica para PEDIDO LOCAL NO PAGADO ---
                elif (
                    selected_pedido_data.get("Estado_Pago", "").strip() == "🔴 No Pagado" and
                    selected_pedido_data.get("Tipo_Envio", "").strip() == "📍 Pedido Local"
                ):
                    st.subheader("🧾 Subir Comprobante de Pago para Pedido Local")
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
                            forma_pago = st.selectbox("💳 Forma de Pago", ["Transferencia", "Depósito en Efectivo", "Tarjeta de Débito", "Tarjeta de Crédito", "Cheque"], key="forma_pago_local")
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
                        comp1 = st.file_uploader("💳 Comprobante 1", type=["pdf", "jpg", "jpeg", "png"], key="cp_pago1_admin")
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
                        comp2 = st.file_uploader("💳 Comprobante 2", type=["pdf", "jpg", "jpeg", "png"], key="cp_pago2_admin")
                        fecha2 = st.date_input("📅 Fecha 2", value=datetime.today().date(), key="fecha_pago2_admin")
                        forma2 = st.selectbox("💳 Forma 2", ["Transferencia", "Depósito en Efectivo", "Tarjeta de Débito", "Tarjeta de Crédito", "Cheque"], key="forma_pago2_admin")
                        monto2 = st.number_input("💲 Monto 2", min_value=0.0, format="%.2f", key="monto_pago2_admin")
                        terminal2 = banco2 = ""
                        if forma2 in ["Tarjeta de Débito", "Tarjeta de Crédito"]:
                            terminal2 = st.selectbox("🏧 Terminal 2", ["BANORTE", "AFIRME", "VELPAY", "CLIP", "PAYPAL", "BBVA", "CONEKTA"], key="terminal2_admin")
                        else:
                            banco2 = st.selectbox("🏦 Banco 2", ["BANORTE", "BANAMEX", "AFIRME", "BANCOMER OP", "BANCOMER CURSOS"], key="banco2_admin")
                        ref2 = st.text_input("🔢 Referencia 2", key="ref2_admin")

                        comprobantes_nuevo = ([comp1] if comp1 else []) + ([comp2] if comp2 else [])
                        fecha_pago = f"{fecha1.strftime('%Y-%m-%d')} y {fecha2.strftime('%Y-%m-%d')}"
                        forma_pago = f"{forma1}, {forma2}"
                        terminal = f"{terminal1}, {terminal2}" if (forma1.startswith("Tarjeta") or forma2.startswith("Tarjeta")) else ""
                        banco_destino = f"{banco1}, {banco2}" if (not forma1.startswith("Tarjeta") or not forma2.startswith("Tarjeta")) else ""
                        monto_pago = monto1 + monto2
                        referencia = f"{ref1}, {ref2}"
                    
                    if st.button("💾 Guardar Comprobante y Datos de Pago", key="btn_guardar_local_pago"):
                        try:
                            # Subir archivos a S3
                            s3_urls = []
                            pedido_id = selected_pedido_data.get("ID_Pedido")
                            s3_subfolder = f"{S3_ATTACHMENT_PREFIX}{pedido_id}/comprobantes/"
                            
                            for comp in comprobantes_nuevo:
                                file_extension = os.path.splitext(comp.name)[1]
                                filename = f"comprobante_{uuid.uuid4()}{file_extension}"
                                s3_key = f"{s3_subfolder}{filename}"
                                success, url = upload_file_to_s3(s3_client, S3_BUCKET_NAME, comp, s3_key)
                                if success:
                                    s3_urls.append(url)

                            # Actualizar Google Sheets
                            update_google_sheets(worksheet, gsheet_row_index, headers, "Estado_Pago", '✅ Pagado')
                            update_google_sheets(worksheet, gsheet_row_index, headers, "Estado_Comprobante", 'Subido por Admin')
                            update_google_sheets(worksheet, gsheet_row_index, headers, "Comprobante_URL", ", ".join(s3_urls))
                            update_google_sheets(worksheet, gsheet_row_index, headers, "Forma_Pago_Admin", forma_pago)
                            update_google_sheets(worksheet, gsheet_row_index, headers, "Monto_Pago_Admin", monto_pago)
                            update_google_sheets(worksheet, gsheet_row_index, headers, "Banco_Destino_Admin", banco_destino)
                            update_google_sheets(worksheet, gsheet_row_index, headers, "Terminal_Admin", terminal)
                            update_google_sheets(worksheet, gsheet_row_index, headers, "Referencia_Admin", referencia)
                            update_google_sheets(worksheet, gsheet_row_index, headers, "Fecha_Pago_Admin", fecha_pago.strftime('%Y-%m-%d') if isinstance(fecha_pago, datetime.date) else fecha_pago)

                            st.success("✅ Comprobantes y datos de pago guardados exitosamente.")
                            st.balloons()
                            time.sleep(2)
                            st.cache_data.clear()
                            st.rerun()

                        except Exception as e:
                            st.error(f"❌ Error al guardar los comprobantes: {e}")

                # --- Lógica para COMPROBANTE EXISTENTE NO CONFIRMADO ---
                else:
                    st.subheader("🧾 Revisar y Confirmar Comprobante Existente")
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        st.subheader("📋 Información del Pedido")
                        st.write(f"**📄 Folio Factura:** {selected_pedido_data.get('Folio_Factura', 'N/A')}")
                        st.write(f"**🤝 Cliente:** {selected_pedido_data.get('Cliente', 'N/A')}")
                        st.write(f"**🧑‍💼 Vendedor:** {selected_pedido_data.get('Vendedor_Registro', 'N/A')}")
                        st.write(f"**Estado de Pago:** {selected_pedido_data.get('Estado_Pago', 'N/A')}")
                        
                        # Detalles de pago registrados
                        with st.expander("📝 Detalles de Pago Registrados"):
                            st.write(f"**📅 Fecha de Pago:** {selected_pedido_data.get('Fecha_Pago_Admin', 'N/A')}")
                            st.write(f"**💳 Forma de Pago:** {selected_pedido_data.get('Forma_Pago_Admin', 'N/A')}")
                            st.write(f"**💲 Monto de Pago:** {selected_pedido_data.get('Monto_Pago_Admin', 'N/A')}")
                            st.write(f"**🏦 Banco Destino:** {selected_pedido_data.get('Banco_Destino_Admin', 'N/A')}")
                            st.write(f"**🏧 Terminal:** {selected_pedido_data.get('Terminal_Admin', 'N/A')}")
                            st.write(f"**🔢 Referencia:** {selected_pedido_data.get('Referencia_Admin', 'N/A')}")
                    
                    with col2:
                        st.subheader("📎 Archivos y Comprobantes")
                        pedido_folder_prefix = find_pedido_subfolder_prefix(s3_client, S3_ATTACHMENT_PREFIX, st.session_state.selected_admin_pedido_id)
                        files = get_files_in_s3_prefix(s3_client, pedido_folder_prefix) if pedido_folder_prefix else []
                        if files:
                            for f in files:
                                url = get_s3_file_download_url(s3_client, f['key'])
                                st.markdown(f"- 📄 **{f['title']}** [🔗 Ver/Descargar]({url})")
                        else:
                            st.info("📁 No se encontraron archivos.")

                    st.markdown("---")
                    st.subheader("✅ Confirmar Comprobantes")
                    comprobante_confirmado = st.selectbox(
                        "¿El comprobante ha sido revisado y confirmado?",
                        ["", "Sí", "No"],
                        key="confirmacion_existente"
                    )
                    comentario_confirmacion = st.text_area("✍️ Comentario de la confirmación (opcional)", key="comentario_confirmacion_existente")

                    if st.button("💾 Guardar Confirmación", key="btn_guardar_existente"):
                        if comprobante_confirmado:
                            try:
                                update_google_sheets(worksheet, gsheet_row_index, headers, "Comprobante_Confirmado", comprobante_confirmado)
                                comentario_existente = selected_pedido_data.get("Comentario", "")
                                nuevo_comentario = f"Confirmación: {comentario_confirmacion.strip()}"
                                comentario_final = f"{comentario_existente}\n{nuevo_comentario}" if comentario_existente else nuevo_comentario
                                update_google_sheets(worksheet, gsheet_row_index, headers, "Comentario", comentario_final)

                                st.success("✅ Confirmación guardada exitosamente.")
                                st.balloons()
                                time.sleep(2)
                                st.cache_data.clear()
                                st.rerun()
                            except Exception as e:
                                st.error(f"❌ Error al guardar la confirmación: {e}")
                        else:
                            st.warning("Por favor, selecciona una opción para la confirmación.")
st.markdown("---")

# --- SECCIÓN 2: GENERAR BASE DE DATOS DE CONFIRMADOS ---
st.header("📥 Generar Base de Datos de Pedidos Confirmados")
st.info("Haz clic en el botón para generar y descargar un Excel con todos los pedidos confirmados.")

if st.button("🔍 Generar Base de Datos de Pedidos Confirmados", key="btn_generar_bd_confirmados"):
    try:
        df_confirmados = df_pedidos[
            (df_pedidos.get('Estado_Pago') == '✅ Pagado') &
            (df_pedidos.get('Comprobante_Confirmado') == 'Sí')
        ].copy()

        if df_confirmados.empty:
            st.warning("⚠️ No hay pedidos confirmados para generar.")
        else:
            st.success(f"🎉 Se encontraron {len(df_confirmados)} pedidos confirmados.")
            
            # Buscamos los links de los archivos en S3
            df_confirmados['Links_Archivos'] = df_confirmados['ID_Pedido'].apply(
                lambda id_pedido: ", ".join([
                    get_s3_file_download_url(s3_client, f['key'])
                    for f in get_files_in_s3_prefix(s3_client, find_pedido_subfolder_prefix(s3_client, S3_ATTACHMENT_PREFIX, id_pedido))
                ]) if id_pedido else ""
            )

            st.dataframe(df_confirmados, use_container_width=True)

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
    except Exception as e:
        st.error(f"❌ Error al generar la base de datos de confirmados: {e}")

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
        pedidos_pendientes = len(df_pedidos[
            (df_pedidos.get('Estado_Pago') == '✅ Pagado') &
            (df_pedidos.get('Comprobante_Confirmado') != 'Sí')
        ]) if all(col in df_pedidos.columns for col in ['Estado_Pago', 'Comprobante_Confirmado']) else 0
        st.metric("Pendientes de Confirmar", pedidos_pendientes)
