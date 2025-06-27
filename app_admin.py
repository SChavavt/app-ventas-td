import streamlit as st
import json # Asegúrate de que esta línea esté al principio del archivo
import time
import pandas as pd
import boto3
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import timedelta # Asegúrate de que timedelta también esté importado

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

# --- Funciones de Google Sheets ---
@st.cache_resource
def get_gspread_client(credentials_json):
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials_json, scope)
    client = gspread.authorize(creds)
    return client

@st.cache_data(ttl=600) # Cachear datos por 10 minutos
def load_data_from_sheet():
    try:
        sheet = gc.open_by_id(GOOGLE_SHEET_ID).worksheet('pedidos')
        df = pd.DataFrame(sheet.get_all_records())
        # Asegurarse de que las columnas de fecha sean tipo datetime
        date_columns = ['Fecha_Pedido', 'Fecha_Entrega'] # Añade aquí todas tus columnas de fecha
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        return df
    except Exception as e:
        st.error(f"❌ Error al cargar datos del Sheet: {e}")
        st.info("ℹ️ Verifica que el ID del Sheet sea correcto y que la cuenta de servicio tenga acceso de lectura.")
        st.stop()
        return pd.DataFrame() # Retorna un DataFrame vacío en caso de error

# --- Funciones de AWS S3 ---
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
        st.error(f"❌ Error al inicializar cliente S3: {e}")
        return None

def upload_file_to_s3(file_content, file_name, folder='adjuntos'):
    try:
        object_name = f"{folder}/{file_name}"
        s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=object_name, Body=file_content)
        return f"https://{S3_BUCKET_NAME}.s3.{AWS_REGION_NAME}.amazonaws.com/{object_name}"
    except Exception as e:
        st.error(f"❌ Error al subir archivo a S3: {e}")
        return None

def generate_s3_presigned_url(object_name, expiration=3600):
    try:
        response = s3_client.generate_presigned_url('get_object',
                                                    Params={'Bucket': S3_BUCKET_NAME,
                                                            'Key': object_name},
                                                    ExpiresIn=expiration)
        return response
    except Exception as e:
        st.error(f"❌ Error al generar URL pre-firmada: {e}")
        return None

# --- Inicializar clientes de Gspread y S3 ---
try:
    # MODIFICACIÓN CLAVE AQUÍ: Usar json.loads() para parsear la cadena JSON
    google_credentials_dict = json.loads(st.secrets["google_credentials"])
    gc = get_gspread_client(google_credentials_dict) # Pasar el diccionario parseado
    
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

# --- Cargar datos ---
df_pedidos = load_data_from_sheet()

st.title("Admin de Pedidos")
st.markdown("---")

# --- FILTROS Y BÚSQUEDA ---
st.header("🔍 Buscar y Filtrar Pedidos")

col_search, col_status, col_payment, col_date = st.columns([2, 1, 1, 1])

with col_search:
    search_query = st.text_input("Buscar por ID de Pedido, Cliente o Vendedor", "")

with col_status:
    estados_disponibles = ["Todos"] + df_pedidos['Estado'].unique().tolist()
    selected_status = st.selectbox("Filtrar por Estado", estados_disponibles)

with col_payment:
    estados_pago_disponibles = ["Todos"] + df_pedidos['Estado_Pago'].unique().tolist()
    selected_payment_status = st.selectbox("Filtrar por Estado de Pago", estados_pago_disponibles)

with col_date:
    date_filter_option = st.selectbox("Filtrar por Fecha", ["Todos", "Hoy", "Últimos 7 días", "Últimos 30 días", "Rango Personalizado"])
    
filtered_df = df_pedidos.copy()

if search_query:
    filtered_df = filtered_df[
        filtered_df.apply(lambda row: search_query.lower() in str(row.get('ID_Pedido', '')).lower() or
                                     search_query.lower() in str(row.get('Cliente', '')).lower() or
                                     search_query.lower() in str(row.get('Vendedor_Registro', '')).lower(), axis=1)
    ]

if selected_status != "Todos":
    filtered_df = filtered_df[filtered_df['Estado'] == selected_status]

if selected_payment_status != "Todos":
    filtered_df = filtered_df[filtered_df.get('Estado_Pago') == selected_payment_status]

# Aplicar filtro de fecha
if date_filter_option == "Hoy":
    today = pd.Timestamp.now().normalize()
    filtered_df = filtered_df[filtered_df['Fecha_Pedido'].dt.normalize() == today]
elif date_filter_option == "Últimos 7 días":
    seven_days_ago = pd.Timestamp.now().normalize() - timedelta(days=7)
    filtered_df = filtered_df[filtered_df['Fecha_Pedido'].dt.normalize() >= seven_days_ago]
elif date_filter_option == "Últimos 30 días":
    thirty_days_ago = pd.Timestamp.now().normalize() - timedelta(days=30)
    filtered_df = filtered_df[filtered_df['Fecha_Pedido'].dt.normalize() >= thirty_days_ago]
elif date_filter_option == "Rango Personalizado":
    col_start, col_end = st.columns(2)
    with col_start:
        start_date = st.date_input("Fecha de inicio", value=pd.Timestamp.now().normalize() - timedelta(days=7))
    with col_end:
        end_date = st.date_input("Fecha de fin", value=pd.Timestamp.now().normalize())
    
    if start_date and end_date:
        filtered_df = filtered_df[(filtered_df['Fecha_Pedido'].dt.normalize() >= pd.Timestamp(start_date).normalize()) & 
                                  (filtered_df['Fecha_Pedido'].dt.normalize() <= pd.Timestamp(end_date).normalize())]


st.subheader(f"Pedidos Encontrados ({len(filtered_df)})")
st.dataframe(filtered_df, use_container_width=True, hide_index=True)


# --- GESTIÓN DE PEDIDOS ---
st.markdown("---")
st.header("⚙️ Gestión de Pedidos")

if 'selected_row_id' not in st.session_state:
    st.session_state.selected_row_id = None
if 'confirmacion_confirmada' not in st.session_state:
    st.session_state.confirmacion_confirmada = False
if 'referencia_pago' not in st.session_state:
    st.session_state.referencia_pago = ""

with st.expander("Modificar Estado y Comprobantes"):
    st.info("Selecciona un pedido de la tabla de arriba para modificarlo.")
    
    if not filtered_df.empty:
        selected_index = st.number_input("Introduce el índice del pedido a modificar (primer pedido es 0)", min_value=0, max_value=len(filtered_df)-1, value=0, key="index_input")
        
        if st.button("Seleccionar Pedido por Índice"):
            if 0 <= selected_index < len(filtered_df):
                st.session_state.selected_row_id = filtered_df.iloc[selected_index]['ID_Pedido']
                st.session_state.confirmacion_confirmada = False # Reset confirmation
                st.session_state.referencia_pago = "" # Reset reference
                st.experimental_rerun()
            else:
                st.error("Índice fuera de rango.")

    if st.session_state.selected_row_id:
        selected_pedido = df_pedidos[df_pedidos['ID_Pedido'] == st.session_state.selected_row_id].iloc[0]
        st.write(f"**Pedido Seleccionado:** ID: {selected_pedido['ID_Pedido']} | Cliente: {selected_pedido['Cliente']}")
        
        col1, col2 = st.columns(2)
        with col1:
            current_status = selected_pedido['Estado']
            new_status = st.selectbox("Cambiar Estado del Pedido", df_pedidos['Estado'].unique().tolist(), index=df_pedidos['Estado'].unique().tolist().index(current_status))
        
        with col2:
            current_payment_status = selected_pedido.get('Estado_Pago', 'Pendiente')
            new_payment_status = st.selectbox("Cambiar Estado de Pago", ["Pendiente", "✅ Pagado", "❌ No Pagado"], index=["Pendiente", "✅ Pagado", "❌ No Pagado"].index(current_payment_status))
        
        if st.button("Actualizar Estado"):
            try:
                sheet = gc.open_by_id(GOOGLE_SHEET_ID).worksheet('pedidos')
                # Encuentra la fila por ID_Pedido y actualiza
                cell = sheet.find(selected_pedido['ID_Pedido'])
                sheet.update_cell(cell.row, df_pedidos.columns.get_loc('Estado') + 1, new_status)
                sheet.update_cell(cell.row, df_pedidos.columns.get_loc('Estado_Pago') + 1, new_payment_status)
                st.success(f"✅ Estado del pedido {selected_pedido['ID_Pedido']} actualizado a '{new_status}' y Estado de Pago a '{new_payment_status}'.")
                st.cache_data.clear() # Limpiar cache para recargar datos
                time.sleep(1)
                st.experimental_rerun()
            except Exception as e:
                st.error(f"❌ Error al actualizar estado: {e}")

        st.markdown("---")
        st.subheader("Comprobantes de Pago")
        
        comprobante_url = selected_pedido.get('URL_Comprobante', None)
        comprobante_confirmado = selected_pedido.get('Comprobante_Confirmado', 'No')

        if comprobante_url and comprobante_url != "N/A":
            if "s3.amazonaws.com" in comprobante_url:
                # Extraer la clave del objeto S3 de la URL completa
                # Ejemplo URL: https://app-pedidos-adjuntos-svt.s3.us-east-2.amazonaws.com/adjuntos/mi_comprobante.pdf
                # Extraer: adjuntos/mi_comprobante.pdf
                object_key_s3 = comprobante_url.split(f"https://{S3_BUCKET_NAME}.s3.{AWS_REGION_NAME}.amazonaws.com/")[1]
                presigned_url = generate_s3_presigned_url(object_key_s3)
                if presigned_url:
                    st.markdown(f"**Comprobante Adjunto:** [Ver Comprobante]({presigned_url}) (Caduca en 1 hora)")
                else:
                    st.warning("⚠️ No se pudo generar URL pre-firmada para el comprobante.")
            else:
                st.markdown(f"**Comprobante Adjunto (URL Directa):** [Ver Comprobante]({comprobante_url})")
            
            st.write(f"**Confirmación de Comprobante:** {comprobante_confirmado}")
            
            if comprobante_confirmado == 'No':
                col1, col2, col3 = st.columns(3)
                with col1:
                    if st.button("✅ Confirmar Comprobante", type="primary", use_container_width=True, key="confirm_btn"):
                        st.session_state.confirmacion_confirmada = True
                        st.experimental_rerun() # Trigger rerun to show input for reference
            
                if st.session_state.confirmacion_confirmada:
                    with col2:
                        st.session_state.referencia_pago = st.text_input("Referencia de Pago/Notas:", value=st.session_state.referencia_pago)
                        if st.button("Guardar Confirmación y Referencia", use_container_width=True, key="save_confirm_btn"):
                            try:
                                sheet = gc.open_by_id(GOOGLE_SHEET_ID).worksheet('pedidos')
                                cell = sheet.find(selected_pedido['ID_Pedido'])
                                sheet.update_cell(cell.row, df_pedidos.columns.get_loc('Comprobante_Confirmado') + 1, 'Sí')
                                sheet.update_cell(cell.row, df_pedidos.columns.get_loc('Referencia_Pago') + 1, st.session_state.referencia_pago) # Actualizar referencia
                                st.success(f"✅ Comprobante del pedido {selected_pedido['ID_Pedido']} confirmado y referencia guardada.")
                                st.cache_data.clear() # Limpiar cache para recargar datos
                                st.session_state.confirmacion_confirmada = False
                                st.session_state.referencia_pago = ""
                                
                                time.sleep(1)
                                st.rerun()
                                
                            except Exception as e:
                                st.error(f"❌ Error al confirmar el comprobante: {e}")
                
                with col3:
                    if st.button("❌ Rechazar Comprobante", type="secondary", use_container_width=True):
                        st.warning("⚠️ Funcionalidad de rechazo pendiente de implementar.")

        else:
            st.info("Este pedido no tiene un comprobante de pago adjunto.")

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
        pedidos_pendientes_confirmar = len(df_pedidos[(df_pedidos.get('Estado_Pago') == '✅ Pagado') & (df_pedidos.get('Comprobante_Confirmado') == 'No')]) if 'Estado_Pago' in df_pedidos.columns and 'Comprobante_Confirmado' in df_pedidos.columns else 0
        st.metric("Pendientes Confirmación", pedidos_pendientes_confirmar)
