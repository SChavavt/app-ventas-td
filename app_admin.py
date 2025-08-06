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
import os
import uuid

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
            # 👉 Lógica alternativa si es un pedido a crédito
            if selected_pedido_data.get("Estado_Pago", "").strip() == "💳 CREDITO":
                st.subheader("📝 Confirmación de Pedido a Crédito")
                selected_pedido_id_for_s3_search = selected_pedido_data.get('ID_Pedido', 'N/A')
                st.session_state.selected_admin_pedido_id = selected_pedido_id_for_s3_search

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
                st.stop()

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

                    # Unificar comprobantes y campos
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

                        # Subir archivos a S3
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
                            'Fecha_Pago_Comprobante': fecha_pago.strftime('%Y-%m-%d'),
                            'Forma_Pago_Comprobante': forma_pago,
                            'Monto_Comprobante': monto_pago,
                            'Referencia_Comprobante': referencia,
                            'Terminal': terminal,
                            'Banco_Destino_Pago': banco_destino,
                        }


                        for col, val in updates.items():
                            if col in headers:
                                worksheet.update_cell(gsheet_row_index, headers.index(col) + 1, val)

                        # Concatenar nuevos adjuntos al campo existente de "Adjuntos"
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

                st.stop()


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


            # Detectar cuántos comprobantes hay
            num_comprobantes = len(comprobantes)
            if num_comprobantes == 0:
                st.warning("⚠️ No hay comprobantes para confirmar.")
                st.stop()


            st.subheader("✅ Confirmar Comprobante")

            fecha_list, forma_list, banco_list, terminal_list, monto_list, ref_list = [], [], [], [], [], []
            # --- Prellenar valores si ya están registrados en la hoja ---
            fecha_list = str(selected_pedido_data.get('Fecha_Pago_Comprobante', '')).split(" y ")
            forma_list = str(selected_pedido_data.get('Forma_Pago_Comprobante', '')).split(", ")
            banco_list = str(selected_pedido_data.get('Banco_Destino_Pago', '')).split(", ")
            terminal_list = str(selected_pedido_data.get('Terminal', '')).split(", ")
            monto_list_raw = selected_pedido_data.get('Monto_Comprobante', '')
            ref_list = str(selected_pedido_data.get('Referencia_Comprobante', '')).split(", ")

            # 🔍 Convertir monto a lista numérica (aunque venga como 3360.00 o "3360.00, 0.00")
            if isinstance(monto_list_raw, str) and "," in monto_list_raw:
                monto_list = [float(m.strip()) if m.strip() else 0.0 for m in monto_list_raw.split(",")]
            else:
                try:
                    monto_list = [float(monto_list_raw)] if monto_list_raw else []
                except Exception:
                    monto_list = []

            # Completar con valores vacíos si alguna lista es más corta que el número de comprobantes
            while len(fecha_list) < num_comprobantes:
                fecha_list.append("")
            while len(forma_list) < num_comprobantes:
                forma_list.append("Transferencia")
            while len(banco_list) < num_comprobantes:
                banco_list.append("")
            while len(terminal_list) < num_comprobantes:
                terminal_list.append("")
            while len(monto_list) < num_comprobantes:
                monto_list.append(0.0)
            while len(ref_list) < num_comprobantes:
                ref_list.append("")


            for i in range(num_comprobantes):
                if num_comprobantes == 1:
                    st.markdown("### 🧾 Comprobante")
                else:
                    emoji_num = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣"]
                    st.markdown(f"### {emoji_num[i]} 🧾 Comprobante {i+1}")

                col_pago = st.columns(4)
                with col_pago[0]:
                    fecha_i = st.date_input(
                        f"📅 Fecha Pago {i+1}",
                        value=pd.to_datetime(fecha_list[i], errors='coerce').date() if fecha_list[i] else None,
                        key=f"fecha_pago_{i}"
                    )
                with col_pago[1]:
                    forma_i = st.selectbox(
                        f"💳 Forma de Pago {i+1}",
                        ["Transferencia", "Depósito en Efectivo", "Tarjeta de Débito", "Tarjeta de Crédito", "Cheque"],
                        index=["Transferencia", "Depósito en Efectivo", "Tarjeta de Débito", "Tarjeta de Crédito", "Cheque"].index(forma_list[i]) if forma_list[i] in ["Transferencia", "Depósito en Efectivo", "Tarjeta de Débito", "Tarjeta de Crédito", "Cheque"] else 0,
                        key=f"forma_pago_{i}"
                    )
                with col_pago[2]:
                    if forma_i in ["Tarjeta de Débito", "Tarjeta de Crédito"]:
                        terminal_options = ["BANORTE", "AFIRME", "VELPAY", "CLIP", "PAYPAL", "BBVA", "CONEKTA"]
                        terminal_i = st.selectbox(
                            f"🏧 Terminal {i+1}",
                            terminal_options,
                            index=terminal_options.index(terminal_list[i]) if terminal_list[i] in terminal_options else 0,
                            key=f"terminal_pago_{i}"
                        )

                        banco_i = ""
                    else:
                        banco_options = ["BANORTE", "BANAMEX", "AFIRME", "BANCOMER OP", "BANCOMER CURSOS"]
                        banco_i = st.selectbox(
                            f"🏦 Banco Destino {i+1}",
                            banco_options,
                            index=banco_options.index(banco_list[i]) if banco_list[i] in banco_options else 0,
                            key=f"banco_pago_{i}"
                        )

                        terminal_i = ""
                with col_pago[3]:
                    monto_i = st.number_input(
                        f"💲 Monto {i+1}",
                        min_value=0.0,
                        format="%.2f",
                        value=monto_list[i] if i < len(monto_list) else 0.0,
                        key=f"monto_pago_{i}"
                    )


                referencia_i = st.text_input(
                    f"🔢 Referencia {i+1}",
                    value=ref_list[i] if i < len(ref_list) else "",
                    key=f"ref_pago_{i}"
                )

                # Guardar en listas
                fecha_list[i] = str(fecha_i)
                forma_list[i] = forma_i
                banco_list[i] = banco_i
                terminal_list[i] = terminal_i
                monto_list[i] = monto_i
                ref_list[i] = referencia_i

            col1, col2, col3 = st.columns([2, 1, 1])
            with col1:
                st.info("👆 Revisa los comprobantes antes de confirmar.")

            with col2:
                if st.button("✅ Confirmar Comprobante", use_container_width=True):
                    try:
                        gsheet_row_index = df_pedidos[df_pedidos['ID_Pedido'] == selected_pedido_id_for_s3_search].index[0] + 2

                        updates = {
                            'Comprobante_Confirmado': 'Sí',
                            'Fecha_Pago_Comprobante': " y ".join(fecha_list),
                            'Forma_Pago_Comprobante': ", ".join(forma_list),
                            'Monto_Comprobante': sum(monto_list),
                            'Referencia_Comprobante': ", ".join(ref_list),
                            'Terminal': ", ".join([t for t in terminal_list if t]),
                            'Banco_Destino_Pago': ", ".join([b for b in banco_list if b]),
                        }

                        for col, val in updates.items():
                            if col in headers:
                                worksheet.update_cell(gsheet_row_index, headers.index(col)+1, val)

                        st.success("🎉 Comprobante confirmado exitosamente.")
                        st.balloons()
                        time.sleep(3)
                        st.cache_data.clear()
                        st.rerun()

                    except Exception as e:
                        st.error(f"❌ Error al confirmar comprobante: {e}")

            with col3:
                if st.button("❌ Rechazar Comprobante", use_container_width=True):
                    st.warning("Funcionalidad pendiente.")


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
