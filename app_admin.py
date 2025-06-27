#app_admin.py
import streamlit as st
import json
import time
import pandas as pd
import boto3
import gspread
from oauth2client.service_account import ServiceAccountCredentials

st.set_page_config(page_title="App Admin TD", layout="wide")

# --- CONFIGURACIÓN DE GOOGLE SHEETS ---
GOOGLE_SHEET_ID = '1aWkSelodaz0nWfQx7FZAysGnIYGQFJxAN7RO3YgCiZY'

@st.cache_resource
def get_google_sheets_client():
    """
    Función para obtener el cliente de gspread usando credenciales de Streamlit secrets.
    """
    try:
        credentials_json_str = st.secrets["google_credentials"]
        creds_dict = json.loads(credentials_json_str)
        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        return gspread.authorize(creds)
    except KeyError:
        st.error("❌ Error: Las credenciales de Google Sheets no se encontraron en Streamlit secrets. Asegúrate de que estén configuradas correctamente como 'google_credentials'.")
        st.stop()
    except json.JSONDecodeError:
        st.error("❌ Error: Las credenciales de Google Sheets en Streamlit secrets no son un JSON válido.")
        st.stop()
    except Exception as e:
        st.error(f"❌ Error al cargar credenciales de Google Sheets: {e}")
        st.stop()

# --- CONFIGURACIÓN DE AWS S3 ---
# MODIFICADO: Ahora accede a las credenciales bajo la sección 'aws' para que coincida con secrets.toml
try:
    AWS_ACCESS_KEY_ID = st.secrets["aws"]["aws_access_key_id"]
    AWS_SECRET_ACCESS_KEY = st.secrets["aws"]["aws_secret_access_key"]
    AWS_REGION_NAME = st.secrets["aws"]["aws_region"] # Mantengo el nombre de variable original, pero accedo via "aws"
    S3_BUCKET_NAME = st.secrets["aws"]["s3_bucket_name"]
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

@st.cache_resource
def get_s3_client():
    """Initializes and returns an S3 client."""
    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION_NAME
        )
        return s3
    except Exception as e:
        st.error(f"❌ Error al inicializar el cliente S3: {e}")
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
    gc = get_google_sheets_client()
    s3_client = get_s3_client()
    if not s3_client:
        st.error("❌ No se pudo inicializar el cliente de AWS S3. Deteniendo la ejecución.")
        st.stop()
except Exception as e:
    st.error(f"❌ Error al autenticarse o inicializar clientes: {e}")
    st.info("ℹ️ Asegúrate de que:")
    st.info("- Las credenciales de Google Sheets estén en Streamlit secrets bajo la clave 'google_credentials' y sean un JSON válido.")
    st.info("- Las APIs de Drive y Sheets estén habilitadas en Google Cloud para la cuenta de servicio.")
    st.info("- La cuenta de servicio de Google tenga permisos de lectura/escritura en el Google Sheet.")
    st.info("- Tus credenciales de AWS S3 (aws_access_key_id, aws_secret_access_key, aws_region) y el s3_bucket_name estén bajo la sección [aws] en secrets.toml y sean correctos.")
    st.info("- La cuenta de AWS tenga permisos de lectura/escritura en el bucket S3.")
    st.stop()


# --- Cargar datos desde Google Sheets ---
@st.cache_data(ttl=60)
def load_data():
    try:
        spreadsheet = gc.open_by_key(GOOGLE_SHEET_ID)
        worksheet = spreadsheet.worksheet('datos_pedidos')
        data = worksheet.get_all_records()
        df = pd.DataFrame(data)

        date_columns = ['Fecha_Entrega', 'Fecha_Completado', 'Hora_Registro']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        if 'Adjuntos' not in df.columns:
            df['Adjuntos'] = ''
        df['Adjuntos'] = df['Adjuntos'].fillna('')

        if 'Comprobante_Confirmado' not in df.columns:
            df['Comprobante_Confirmado'] = 'No'
        df['Comprobante_Confirmado'] = df['Comprobante_Confirmado'].fillna('No')
        
        return df
    except Exception as e:
        st.error(f"❌ Error al cargar datos del Google Sheet: {e}")
        st.info("ℹ️ Asegúrate de que el Google Sheet ID y los permisos de la cuenta de servicio sean correctos.")
        return pd.DataFrame()

df_pedidos = load_data()

# --- INTERFAZ PRINCIPAL ---
st.header("💳 Comprobantes de Pago Pendientes de Confirmar")

comprobantes_pendientes_df = df_pedidos[
    (df_pedidos['Estado_Pago'] == '✅ Pagado') &
    (df_pedidos['Comprobante_Confirmado'] == 'No')
].copy()

if comprobantes_pendientes_df.empty:
    st.info("🎉 No hay comprobantes de pago pendientes de confirmar en este momento.")
else:
    st.write(f"Se encontraron {len(comprobantes_pendientes_df)} comprobante(s) pendiente(s).")
    st.dataframe(comprobantes_pendientes_df[[
        'ID_Pedido', 'Folio_Factura', 'Cliente', 'Vendedor', 'Estado_Pago', 'Comprobante_Confirmado'
    ]].sort_values(by='ID_Pedido', ascending=False), use_container_width=True)

    st.markdown("---")
    st.subheader("🔍 Confirmar o Rechazar Comprobante por ID de Pedido")

    if 'referencia_pago' not in st.session_state:
        st.session_state.referencia_pago = ""

    referencia_pago = st.text_input(
        "ID del Pedido o Folio de Factura del comprobante a confirmar/rechazar",
        value=st.session_state.referencia_pago,
        key="referencia_pago_input"
    )

    if referencia_pago:
        pedido_a_gestionar_df = df_pedidos[
            (df_pedidos['ID_Pedido'] == referencia_pago) | 
            (df_pedidos['Folio_Factura'] == referencia_pago)
        ]

        if pedido_a_gestionar_df.empty:
            st.warning("⚠️ No se encontró ningún pedido con ese ID o Folio de Factura.")
        elif len(pedido_a_gestionar_df) > 1:
            st.warning("⚠️ Múltiples pedidos encontrados con ese Folio de Factura. Por favor, usa el ID de Pedido para mayor precisión.")
            st.dataframe(pedido_a_gestionar_df)
        else:
            pedido_gestionar = pedido_a_gestionar_df.iloc[0]
            st.write("---")
            st.subheader(f"Detalles del Pedido: `{pedido_gestionar['ID_Pedido']}`")
            st.json(pedido_gestionar.to_dict())

            st.markdown("---")
            st.subheader("📎 Archivos Adjuntos del Pedido (S3)")
            adjuntos_del_pedido_str = pedido_gestionar.get('Adjuntos', '')
            adjuntos_urls = [url.strip() for url in adjuntos_del_pedido_str.split(',') if url.strip()]
            
            if adjuntos_urls:
                st.write("Archivos adjuntos encontrados en S3:")
                for url in adjuntos_urls:
                    file_name = url.split('/')[-1]
                    st.markdown(f"- [{file_name}]({url})")
            else:
                st.info("ℹ️ No hay archivos adjuntos en S3 para este pedido.")

            col1, col2, col3 = st.columns(3)

            with col1:
                if st.button("✅ Confirmar Comprobante", type="primary", use_container_width=True):
                    if pedido_gestionar['Estado_Pago'] == '✅ Pagado' and pedido_gestionar['Comprobante_Confirmado'] == 'No':
                        try:
                            all_data = gc.open_by_key(GOOGLE_SHEET_ID).worksheet('datos_pedidos').get_all_values()
                            headers = all_data[0]
                            data_rows = all_data[1:]

                            target_row_index = -1
                            for i, row in enumerate(data_rows):
                                row_dict = dict(zip(headers, row))
                                if row_dict.get('ID_Pedido') == pedido_gestionar['ID_Pedido']:
                                    target_row_index = i + 2
                                    break
                            
                            if target_row_index != -1:
                                worksheet = gc.open_by_key(GOOGLE_SHEET_ID).worksheet('datos_pedidos')
                                
                                col_index = headers.index('Comprobante_Confirmado') + 1
                                worksheet.update_cell(target_row_index, col_index, 'Sí')
                                st.success(f"✔️ Comprobante del pedido `{pedido_gestionar['ID_Pedido']}` confirmado con éxito.")
                                
                                st.session_state.referencia_pago = ""
                                st.cache_data.clear()
                                time.sleep(1)
                                st.rerun()
                            else:
                                st.error("❌ Error: No se pudo encontrar la fila del pedido en Google Sheets.")
                        except Exception as e:
                            st.error(f"❌ Error al confirmar el comprobante: {e}")
                    else:
                        st.info("ℹ️ Este pedido ya ha sido confirmado o no está marcado como 'Pagado' con un comprobante pendiente.")

            with col3:
                if st.button("❌ Rechazar Comprobante", type="secondary", use_container_width=True):
                    if pedido_gestionar['Estado_Pago'] == '✅ Pagado' and pedido_gestionar['Comprobante_Confirmado'] == 'No':
                        try:
                            all_data = gc.open_by_key(GOOGLE_SHEET_ID).worksheet('datos_pedidos').get_all_values()
                            headers = all_data[0]
                            data_rows = all_data[1:]

                            target_row_index = -1
                            for i, row in enumerate(data_rows):
                                row_dict = dict(zip(headers, row))
                                if row_dict.get('ID_Pedido') == pedido_gestionar['ID_Pedido']:
                                    target_row_index = i + 2
                                    break
                            
                            if target_row_index != -1:
                                worksheet = gc.open_by_key(GOOGLE_SHEET_ID).worksheet('datos_pedidos')
                                
                                col_index_confirmado = headers.index('Comprobante_Confirmado') + 1
                                worksheet.update_cell(target_row_index, col_index_confirmado, 'No')

                                col_index_estado_pago = headers.index('Estado_Pago') + 1
                                worksheet.update_cell(target_row_index, col_index_estado_pago, '🔴 No Pagado')

                                st.success(f"✔️ Comprobante del pedido `{pedido_gestionar['ID_Pedido']}` rechazado y pedido marcado como 'No Pagado'.")
                                
                                st.session_state.referencia_pago = ""
                                st.cache_data.clear()
                                time.sleep(1)
                                st.rerun()
                            else:
                                st.error("❌ Error: No se pudo encontrar la fila del pedido en Google Sheets.")
                        except Exception as e:
                            st.error(f"❌ Error al rechazar el comprobante: {e}")
                    else:
                        st.info("ℹ️ Este pedido no tiene un comprobante 'Pagado' y pendiente de confirmar para rechazar.")


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
        pedidos_pendientes_confirmacion = len(comprobantes_pendientes_df) if 'comprobantes_pendientes_df' in locals() else 0
        st.metric("Pendientes Confirmación", pedidos_pendientes_confirmacion)
