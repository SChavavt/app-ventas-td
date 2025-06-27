import streamlit as st
import time
import pandas as pd
import boto3
import gspread
from oauth2client.service_account import ServiceAccountCredentials

st.set_page_config(page_title="App Admin TD", layout="wide")

# --- CONFIGURACIÓN DE GOOGLE SHEETS ---
# SERVICE_ACCOUNT_FILE = 'sistema-pedidos-td-e80e1a9633c2.json' # Ya no se usa
GOOGLE_SHEET_ID = '1aWkSelodaz0nWfQx7FZAysGnIYGQFJxAN7RO3YgCiZY'

# --- CONFIGURACIÓN DE AWS S3 ---
try:
    AWS_ACCESS_KEY_ID = st.secrets["aws_access_key_id"]
    AWS_SECRET_ACCESS_KEY = st.secrets["aws_secret_access_key"]
    AWS_REGION_NAME = st.secrets["aws_region"]
    S3_BUCKET_NAME = st.secrets["s3_bucket_name"]
except KeyError as e:
    st.error(f"❌ Error: Las credenciales de AWS S3 no se encontraron en Streamlit secrets. Asegúrate de que tu archivo .streamlit/secrets.toml esté configurado correctamente. Clave faltante: {e}")
    st.info("Asegúrate de que tus claves en secrets.toml estén bajo la sección [aws] y se llamen:")
    st.info("aws_access_key_id = \"TU_ACCES_KEY\"")
    st.info("aws_secret_access_key = \"TU_SECRET_KEY\"")
    st.info("aws_region = \"tu-region\"")
    st.info("s3_bucket_name = \"tu-bucket-name\"")
    st.stop()

S3_ATTACHMENT_PREFIX = 'adjuntos_pedidos/'

st.title("👨‍💼 App de Administración TD")
st.write("Panel de administración para revisar y confirmar comprobantes de pago.")

# --- FUNCIONES DE AUTENTICACIÓN Y CARGA DE DATOS ---

# La función load_credentials_from_file ya no es necesaria, ya que las credenciales se cargarán directamente desde st.secrets.
# @st.cache_resource
# def load_credentials_from_file(file_path):
#    try:
#        with open(file_path, 'r') as f:
#            creds = json.load(f)
#        return creds
#    except FileNotFoundError:
#        st.error(f"❌ Error: El archivo de credenciales '{file_path}' no fue encontrado. Asegúrate de que el nombre sea correcto y esté en la misma carpeta que 'app_admin.py'.")
#        st.stop()
#    except json.JSONDecodeError:
#        st.error(f"❌ Error: El archivo de credenciales '{file_path}' no es un JSON válido o está corrupto. Revisa el formato del archivo.")
#        st.stop()
#    except Exception as e:
#        st.error(f"❌ Error al leer el archivo de credenciales '{file_path}': {e}")
#        st.stop()

@st.cache_resource
def get_gspread_client(credentials_json):
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials_json, scope)
    client = gspread.authorize(creds)
    return client

@st.cache_resource
def get_s3_client():
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

def find_pedido_subfolder_prefix(s3_client, parent_prefix, folder_name):
    if not s3_client:
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
            response = s3_client.list_objects_v2(
                Bucket=S3_BUCKET_NAME,
                Prefix=pedido_prefix,
                MaxKeys=1
            )
            
            if 'Contents' in response and response['Contents']:
                return pedido_prefix
            
        except Exception:
            continue
    
    try:
        response = s3_client.list_objects_v2(
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

def get_files_in_s3_prefix(s3_client, prefix):
    if not s3_client or not prefix:
        return []
    
    try:
        response = s3_client.list_objects_v2(
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

def get_s3_file_download_url(s3_client, object_key):
    if not s3_client or not object_key:
        return "#"
    
    try:
        url = s3_client.generate_presigned_url(
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
    # Cargar credenciales de Google Sheets directamente desde st.secrets
    gc = get_gspread_client(st.secrets["google_credentials"])
    s3_client = get_s3_client()
    
    if not s3_client:
        st.error("❌ No se pudo inicializar el cliente de AWS S3.")
        st.stop()
    
except Exception as e:
    st.error(f"❌ Error al autenticarse o inicializar clientes de Google Sheets/AWS S3: {e}")
    st.info("ℹ️ Asegúrate de que:")
    st.info("- Las APIs de Drive/Sheets estén habilitadas en Google Cloud")
    st.info("- La cuenta de servicio de Google tenga permisos en el Sheet")
    st.info("- Tus credenciales de AWS S3 (aws_access_key_id, aws_secret_access_key, aws_region) y el s3_bucket_name sean correctos.")
    st.info("- La cuenta de AWS tenga permisos de lectura en el bucket S3.")
    st.stop()

# --- INTERFAZ PRINCIPAL ---

st.header("💳 Comprobantes de Pago Pendientes de Confirmación")

df_pedidos = pd.DataFrame()
try:
    spreadsheet = gc.open_by_key(GOOGLE_SHEET_ID)
    worksheet = spreadsheet.worksheet('datos_pedidos')
    headers = worksheet.row_values(1)
    if headers:
        df_pedidos = pd.DataFrame(worksheet.get_all_records())
    else:
        st.warning("No se pudieron cargar los encabezados del Google Sheet.")
        st.stop()
except Exception as e:
    st.error(f"❌ Error al cargar pedidos desde Google Sheet: {e}")
    st.stop()

if df_pedidos.empty:
    st.info("No hay pedidos registrados.")
else:
    if 'Estado_Pago' in df_pedidos.columns and 'Comprobante_Confirmado' in df_pedidos.columns:
        pedidos_pagados_no_confirmados = df_pedidos[
            (df_pedidos['Estado_Pago'] == '✅ Pagado') &
            (df_pedidos['Comprobante_Confirmado'] != 'Sí')
        ].copy()
    else:
        st.warning("Las columnas 'Estado_Pago' o 'Comprobante_Confirmado' no se encontraron en el Google Sheet.")
        pedidos_pagados_no_confirmados = pd.DataFrame()

    if pedidos_pagados_no_confirmados.empty:
        st.success("🎉 ¡No hay comprobantes pendientes de confirmación!")
        st.info("Todos los pedidos pagados han sido confirmados o no hay pedidos pagados.")
    else:
        st.warning(f"📋 Hay {len(pedidos_pagados_no_confirmados)} comprobantes pendientes de confirmación.")
        
        # Modificación: Eliminar 'Comprobante_Confirmado' y cambiar 'ID_Pedido' por 'Folio_Factura'
        columns_to_show = [
            'Folio_Factura', 'Cliente', 'Vendedor_Registro', 'Tipo_Envio', 
            'Fecha_Entrega', 'Estado', 'Estado_Pago'
        ]
        
        existing_columns = [col for col in columns_to_show if col in pedidos_pagados_no_confirmados.columns]
        
        if existing_columns:
            st.dataframe(
                pedidos_pagados_no_confirmados[existing_columns].sort_values(by='Fecha_Entrega'), 
                use_container_width=True, 
                hide_index=True
            )
        else:
            st.warning("No se encontraron las columnas esperadas para mostrar el resumen de pedidos.")
        
        st.markdown("---")
        st.subheader("🔍 Revisar Comprobante de Pago")
        
        # Modificación: Usar 'Folio_Factura' para el 'display_label' en el selectbox
        if 'Folio_Factura' in pedidos_pagados_no_confirmados.columns:
            pedidos_pagados_no_confirmados['display_label'] = (
                pedidos_pagados_no_confirmados['Folio_Factura'] + " - " +
                pedidos_pagados_no_confirmados.get('Cliente', 'N/A') + " - " +
                pedidos_pagados_no_confirmados.get('Vendedor_Registro', 'N/A') + " (ID: " + 
                pedidos_pagados_no_confirmados.get('ID_Pedido', 'N/A') + ")" # Mantener ID_Pedido para referencia visual
            )
        else:
            st.warning("La columna 'Folio_Factura' no se encontró en el Google Sheet. Usando 'ID_Pedido' en el selector.")
            pedidos_pagados_no_confirmados['display_label'] = (
                pedidos_pagados_no_confirmados.get('ID_Pedido', 'N/A') + " - " +
                pedidos_pagados_no_confirmados.get('Cliente', 'N/A') + " - " +
                pedidos_pagados_no_confirmados.get('Vendedor_Registro', 'N/A')
            )
            
        selected_pedido_display = st.selectbox(
            "📝 Seleccionar Pedido para Revisar Comprobante",
            pedidos_pagados_no_confirmados['display_label'].tolist(),
            key="select_pedido_comprobante"
        )
        
        if selected_pedido_display:
            selected_pedido_data = pedidos_pagados_no_confirmados[
                pedidos_pagados_no_confirmados['display_label'] == selected_pedido_display
            ].iloc[0]
            
            selected_pedido_id_for_s3_search = selected_pedido_data.get('ID_Pedido', 'N/A')

            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("📋 Información del Pedido")
                st.write(f"**Folio Factura:** {selected_pedido_data.get('Folio_Factura', 'N/A')}")
                st.write(f"**ID Pedido (interno):** {selected_pedido_data.get('ID_Pedido', 'N/A')}") # Se muestra como referencia interna
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
                    
                    if pedido_folder_prefix:
                        files_in_folder = get_files_in_s3_prefix(s3_client, pedido_folder_prefix)
                        
                        if files_in_folder:
                            comprobantes_encontrados = []
                            otros_archivos = []
                            
                            for file in files_in_folder:
                                if 'comprobante' in file['title'].lower():
                                    comprobantes_encontrados.append(file)
                                else:
                                    otros_archivos.append(file)
                            
                            if comprobantes_encontrados:
                                st.write("**🧾 Comprobantes de Pago:**")
                                for comp in comprobantes_encontrados:
                                    file_url = get_s3_file_download_url(s3_client, comp['key'])
                                    
                                    # Lógica para limpiar el nombre del archivo para mostrar
                                    display_name = comp['title']
                                    if selected_pedido_id_for_s3_search in display_name:
                                        display_name = display_name.replace(selected_pedido_id_for_s3_search, "")
                                        display_name = display_name.replace("__", "_").replace("_-", "_").replace("-_", "_").strip('_').strip('-')

                                    st.markdown(f"- 📄 **{display_name}** ({comp['size']} bytes) [🔗 Ver/Descargar]({file_url})")
                            else:
                                st.warning("⚠️ No se encontraron comprobantes en la carpeta del pedido en S3.")
                            
                            if otros_archivos:
                                with st.expander("📂 Otros archivos del pedido"):
                                    for file in otros_archivos:
                                        file_url = get_s3_file_download_url(s3_client, file['key'])
                                        st.markdown(f"- 📄 **{file['title']}** ({file['size']} bytes) [🔗 Ver/Descargar]({file_url})")
                            else:
                                st.info("No se encontraron otros archivos en la carpeta del pedido en S3.")
                        else:
                            st.info("No se encontraron archivos en la carpeta del pedido en S3.")
                    else:
                        st.error(f"❌ No se encontró la carpeta (prefijo S3) del pedido '{selected_pedido_id_for_s3_search}'.")
                else:
                    st.warning("⚠️ No se puede acceder a los archivos de AWS S3 en este momento.")
                    st.info("Verifica la configuración de autenticación y permisos de AWS.")
            
            st.markdown("---")
            
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

            col_payment_details = st.columns(4)
            with col_payment_details[0]:
                fecha_pago = st.date_input("Fecha Pago Comprobante", value=st.session_state.fecha_pago, key="date_input_payment")
            
            with col_payment_details[1]:
                forma_pago = st.selectbox(
                    "Forma de Pago", 
                    ["Transferencia", "Depósito en Efectivo", "Tarjeta de Débito", "Tarjeta de Crédito", "Cheque"], 
                    index=["Transferencia", "Depósito en Efectivo", "Tarjeta de Débito", "Tarjeta de Crédito", "Cheque"].index(st.session_state.forma_pago) if st.session_state.forma_pago in ["Transferencia", "Depósito en Efectivo", "Tarjeta de Débito", "Tarjeta de Crédito", "Cheque"] else 0, 
                    key="payment_method_select_payment"
                )
            
            with col_payment_details[2]:
                if forma_pago in ["Tarjeta de Débito", "Tarjeta de Crédito"]:
                    terminal = st.selectbox(
                        "Terminal", 
                        ["BANORTE", "AFIRME", "VELPAY", "CLIP", "PAYPAL"], 
                        index=["BANORTE", "AFIRME", "VELPAY", "CLIP", "PAYPAL"].index(st.session_state.terminal) if st.session_state.terminal in ["BANORTE", "AFIRME", "VELPAY", "CLIP", "PAYPAL"] else 0,
                        key="terminal_select_payment"
                    )
                    banco_destino_pago = ""
                else:
                    banco_destino_pago = st.selectbox(
                        "Banco de Destino", 
                        ["BANORTE", "BANAMEX", "AFIRME", "BANCOMER OP", "BANCOMER CURSOS"], 
                        index=["BANORTE", "BANAMEX", "AFIRME", "BANCOMER OP", "BANCOMER CURSOS"].index(st.session_state.banco_destino_pago) if st.session_state.banco_destino_pago in ["BANORTE", "BANAMEX", "AFIRME", "BANCOMER OP", "BANCOMER CURSOS"] else 0,
                        key="bank_select_payment"
                    )
                    terminal = ""
            
            with col_payment_details[3]:
                monto_pago = st.number_input("Monto", min_value=0.0, format="%.2f", value=st.session_state.monto_pago, key="amount_input_payment")
            
            referencia_pago = st.text_input("Referencia/Opcional", value=st.session_state.referencia_pago, key="reference_input_payment")

            col1, col2, col3 = st.columns([2, 1, 1])
            
            with col1:
                st.info("👆 Revisa el comprobante de pago haciendo clic en los enlaces de arriba.")
            
            with col2:
                if st.button("✅ Confirmar Comprobante", type="primary", use_container_width=True):
                    required_fields = [fecha_pago, forma_pago, monto_pago is not None]
                    
                    if forma_pago in ["Tarjeta de Débito", "Tarjeta de Crédito"]:
                        required_fields.append(terminal)
                    else:
                        required_fields.append(banco_destino_pago)
                    
                    if not all(required_fields):
                        st.error("Por favor, rellena todos los campos obligatorios antes de confirmar.")
                    else:
                        try:
                            df_row_index = df_pedidos[df_pedidos['ID_Pedido'] == selected_pedido_id_for_s3_search].index[0]
                            gsheet_row_index = df_row_index + 2
                            
                            updates = {
                                'Comprobante_Confirmado': 'Sí',
                                'Fecha_Pago_Comprobante': str(fecha_pago),
                                'Forma_Pago_Comprobante': forma_pago,
                                'Monto_Comprobante': monto_pago,
                                'Referencia_Comprobante': referencia_pago
                            }
                            
                            if forma_pago in ["Tarjeta de Débito", "Tarjeta de Crédito"]:
                                updates['Terminal'] = terminal
                                updates['Banco_Destino_Pago'] = ""
                            else:
                                updates['Banco_Destino_Pago'] = banco_destino_pago
                                updates['Terminal'] = ""

                            for col_name, value in updates.items():
                                if col_name in headers:
                                    col_idx = headers.index(col_name) + 1
                                    worksheet.update_cell(gsheet_row_index, col_idx, value)
                                else:
                                    st.warning(f"La columna '{col_name}' no se encontró en el Google Sheet y no se pudo actualizar.")
                            
                            st.success(f"🎉 Comprobante del pedido `{selected_pedido_id_for_s3_search}` confirmado exitosamente!")
                            st.balloons()

                            st.session_state.fecha_pago = None
                            st.session_state.banco_destino_pago = "BANORTE"
                            st.session_state.terminal = "BANORTE"
                            st.session_state.forma_pago = "Transferencia"
                            st.session_state.monto_pago = 0.0
                            st.session_state.referencia_pago = ""
                            
                            time.sleep(1)
                            st.rerun()
                            
                        except Exception as e:
                            st.error(f"❌ Error al confirmar el comprobante: {e}")
            
            with col3:
                if st.button("❌ Rechazar Comprobante", type="secondary", use_container_width=True):
                    st.warning("⚠️ Funcionalidad de rechazo pendiente de implementar.")

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
