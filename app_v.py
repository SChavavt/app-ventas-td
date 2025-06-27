import streamlit as st
import os
from datetime import datetime
import json # Necesario para parsear el JSON de secrets
import uuid
import pandas as pd
from io import BytesIO
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# NEW: Import boto3 for AWS S3
import boto3

# --- STREAMLIT CONFIGURATION ---
st.set_page_config(page_title="App Vendedores TD", layout="wide")


# --- GOOGLE SHEETS CONFIGURATION ---
# Ya no necesitamos SERVICE_ACCOUNT_FILE, porque leemos de secrets
GOOGLE_SHEET_ID = '1aWkSelodaz0nWfQx7FZAysGnIYGQFJxAN7RO3YgCiZY'

def get_google_sheets_client():
    """
    Función para obtener el cliente de gspread usando credenciales de Streamlit secrets.
    """
    try:
        # Cargar las credenciales desde Streamlit secrets
        # Asumimos que el secret se llamará "google_credentials"
        credentials_json_str = st.secrets["google_credentials"]
        
        # Convertir la cadena JSON a un diccionario de Python
        creds_dict = json.loads(credentials_json_str)
        
        # Definir el alcance de la API de Google Sheets
        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive']
        
        # Autorizar con las credenciales del servicio
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        
        # Retornar el cliente de gspread
        return gspread.authorize(creds)
    except KeyError:
        st.error("❌ Error: Las credenciales de Google Sheets no se encontraron en Streamlit secrets. Asegúrate de que estén configuradas correctamente como 'google_credentials'.")
        st.stop() # Detiene la ejecución de la app si las credenciales no están
    except json.JSONDecodeError:
        st.error("❌ Error: Las credenciales de Google Sheets en Streamlit secrets no son un JSON válido.")
        st.stop()
    except Exception as e:
        st.error(f"❌ Error al cargar credenciales de Google Sheets: {e}")
        st.stop()

# --- AWS S3 CONFIGURATION (NEW) ---
# Load AWS credentials from Streamlit secrets
try:
    AWS_ACCESS_KEY_ID = st.secrets["aws"]["aws_access_key_id"]
    AWS_SECRET_ACCESS_KEY = st.secrets["aws"]["aws_secret_access_key"]
    AWS_REGION = st.secrets["aws"]["aws_region"]
    S3_BUCKET_NAME = st.secrets["aws"]["s3_bucket_name"]
except KeyError as e:
    st.error(f"❌ Error: AWS S3 credentials not found in Streamlit secrets. Make sure your .streamlit/secrets.toml file is correctly configured. Missing key: {e}")
    st.stop()


st.title("🛒 **App Vendedores TD**")
st.markdown("Bienvenido al panel de gestión de pedidos.")

# Inicializar el cliente de Google Sheets
# Llama a la función que creamos para obtener el cliente de gspread
client_gs = get_google_sheets_client()

# Inicializar cliente S3
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)


# --- FUNCIONES DE CARGA DE DATOS ---
@st.cache_data(ttl=60) # Cache de datos por 60 segundos
def load_data_from_gsheet(sheet_id):
    try:
        # Usar el cliente 'client_gs' que ya fue inicializado
        spreadsheet = client_gs.open_by_id(sheet_id)
        worksheet = spreadsheet.worksheet("Pedidos") # Nombre de la hoja
        data = worksheet.get_all_records()
        df = pd.DataFrame(data)

        # Convertir 'Fecha_Registro' a datetime si existe
        if 'Fecha_Registro' in df.columns:
            df['Fecha_Registro'] = pd.to_datetime(df['Fecha_Registro'], errors='coerce')
        # Convertir 'Fecha_Entrega' a datetime si existe
        if 'Fecha_Entrega' in df.columns:
            df['Fecha_Entrega'] = pd.to_datetime(df['Fecha_Entrega'], errors='coerce')

        # Asegurar que las columnas numéricas sean tipo numérico
        for col in ['ID_Pedido', 'Subtotal', 'IVA', 'Total_Factura', 'Monto_Comprobante']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # Llenar nulos en 'Comprobante_Confirmado' con 'No'
        if 'Comprobante_Confirmado' in df.columns:
            df['Comprobante_Confirmado'] = df['Comprobante_Confirmado'].fillna('No')
            df['Comprobante_Confirmado'] = df['Comprobante_Confirmado'].astype(str) # Asegurar que sea string

        # Asegurar que 'Estado_Pago' no tenga nulos y sea string
        if 'Estado_Pago' in df.columns:
            df['Estado_Pago'] = df['Estado_Pago'].fillna('Pendiente')
            df['Estado_Pago'] = df['Estado_Pago'].astype(str) # Asegurar que sea string

        # Asegurar que 'Ref_Pago_Interna' no tenga nulos y sea string
        if 'Ref_Pago_Interna' in df.columns:
            df['Ref_Pago_Interna'] = df['Ref_Pago_Interna'].fillna('')
            df['Ref_Pago_Interna'] = df['Ref_Pago_Interna'].astype(str)

        # Asegurar que 'URL_Comprobante' no tenga nulos y sea string
        if 'URL_Comprobante' in df.columns:
            df['URL_Comprobante'] = df['URL_Comprobante'].fillna('')
            df['URL_Comprobante'] = df['URL_Comprobante'].astype(str)


        return df
    except gspread.exceptions.SpreadsheetNotFound:
        st.error(f"❌ Error: Hoja de cálculo de Google con ID '{sheet_id}' no encontrada. Verifica el GOOGLE_SHEET_ID.")
        st.stop()
    except gspread.exceptions.APIError as e:
        st.error(f"❌ Error de API de Google Sheets: {e}. Asegúrate de que la cuenta de servicio tenga permisos de acceso a la hoja.")
        st.stop()
    except Exception as e:
        st.error(f"❌ Error al cargar datos de Google Sheets: {e}")
        st.stop()

# --- FUNCIONES DE ESCRITURA DE DATOS ---
def update_data_to_gsheet(df_to_update, sheet_id):
    try:
        # Usar el cliente 'client_gs'
        spreadsheet = client_gs.open_by_id(sheet_id)
        worksheet = spreadsheet.worksheet("Pedidos")

        # Asegurarse de que el DataFrame no incluya la columna de Streamlit si se añadió
        if '_index' in df_to_update.columns:
            df_to_update = df_to_update.drop(columns=['_index'])
        
        # Convertir columnas de fecha a string para gspread
        for col in ['Fecha_Registro', 'Fecha_Entrega']:
            if col in df_to_update.columns:
                df_to_update[col] = df_to_update[col].dt.strftime('%Y-%m-%d')
        
        # Eliminar las columnas no deseadas antes de la actualización
        columns_to_drop = [col for col in ['Fecha de Creación del Pedido', 'Fecha de Entrega Estimada'] if col in df_to_update.columns]
        df_to_update = df_to_update.drop(columns=columns_to_drop)

        # Limpiar el contenido existente y luego actualizar
        worksheet.clear() # Limpia todo el contenido de la hoja
        worksheet.update([df_to_update.columns.values.tolist()] + df_to_update.values.tolist())
        st.success("✅ Datos actualizados en Google Sheets.")
    except Exception as e:
        st.error(f"❌ Error al actualizar datos en Google Sheets: {e}")


# --- CARGAR DATOS ---
df_pedidos = load_data_from_gsheet(GOOGLE_SHEET_ID)

# --- ESTADO DE LA APLICACIÓN ---
if 'nuevo_pedido' not in st.session_state:
    st.session_state.nuevo_pedido = {}
if 'confirmado' not in st.session_state:
    st.session_state.confirmado = False
if 'show_form' not in st.session_state:
    st.session_state.show_form = False
if 'comprobante_cargado' not in st.session_state:
    st.session_state.comprobante_cargado = False
if 'referencia_pago' not in st.session_state:
    st.session_state.referencia_pago = ""


# --- FORMULARIO DE NUEVO PEDIDO ---
st.subheader("📝 Registrar Nuevo Pedido")

with st.expander("Haz clic para registrar un nuevo pedido"):
    with st.form("nuevo_pedido_form", clear_on_submit=True):
        col1, col2 = st.columns(2)
        with col1:
            cliente = st.text_input("Cliente", key="cliente_input")
            tipo_envio = st.selectbox("Tipo de Envío", ["Local", "Nacional", "Internacional"], key="tipo_envio_select")
            vendedor_registro = st.text_input("Vendedor de Registro", key="vendedor_registro_input")
            
        with col2:
            estado = st.selectbox("Estado del Pedido", ["Pendiente", "En Proceso", "Completado", "Cancelado"], key="estado_select")
            fecha_entrega = st.date_input("Fecha de Entrega Estimada", min_value=datetime.today().date(), key="fecha_entrega_input")

        st.subheader("Detalles de la Factura (Opcional)")
        col_fact1, col_fact2 = st.columns(2)
        with col_fact1:
            folio_factura = st.text_input("Folio de Factura", key="folio_factura_input")
            subtotal = st.number_input("Subtotal", min_value=0.0, format="%.2f", key="subtotal_input")
        with col_fact2:
            iva = st.number_input("IVA", min_value=0.0, format="%.2f", key="iva_input")
            total_factura = st.number_input("Total Factura", min_value=0.0, format="%.2f", key="total_factura_input")

        # Carga de Comprobante de Pago y Referencia
        st.subheader("Carga de Comprobante de Pago")
        uploaded_file = st.file_uploader("Sube el comprobante de pago (PDF, JPG, PNG)", type=["pdf", "jpg", "png"], key="comprobante_uploader")
        
        st.session_state.referencia_pago = st.text_input("Referencia de Pago", key="referencia_pago_input")

        submit_button = st.form_submit_button("Registrar Pedido")

        if submit_button:
            if not all([cliente, tipo_envio, vendedor_registro, estado, fecha_entrega]):
                st.warning("⚠️ Por favor, completa todos los campos obligatorios del pedido.")
            else:
                # Generar un ID de pedido único
                id_pedido = str(uuid.uuid4())[:8] # Usar los primeros 8 caracteres del UUID
                fecha_registro = datetime.now()

                # Subir comprobante a S3 si existe
                url_comprobante = ""
                if uploaded_file is not None:
                    try:
                        file_extension = os.path.splitext(uploaded_file.name)[1]
                        s3_file_name = f"comprobantes/{id_pedido}_{datetime.now().strftime('%Y%m%d%H%M%S')}{file_extension}"
                        s3_client.upload_fileobj(uploaded_file, S3_BUCKET_NAME, s3_file_name)
                        url_comprobante = f"https://{S3_BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{s3_file_name}"
                        st.session_state.comprobante_cargado = True
                        st.success("✅ Comprobante subido a S3 con éxito.")
                    except Exception as e:
                        st.error(f"❌ Error al subir el comprobante a S3: {e}")
                        url_comprobante = "" # Asegurarse de que no haya una URL parcial

                # Crear nuevo registro
                nuevo_registro = pd.DataFrame([{
                    'ID_Pedido': id_pedido,
                    'Cliente': cliente,
                    'Tipo_Envio': tipo_envio,
                    'Vendedor_Registro': vendedor_registro,
                    'Estado': estado,
                    'Fecha_Registro': fecha_registro.strftime('%Y-%m-%d %H:%M:%S'),
                    'Fecha_Entrega': fecha_entrega.strftime('%Y-%m-%d'),
                    'Folio_Factura': folio_factura,
                    'Subtotal': subtotal,
                    'IVA': iva,
                    'Total_Factura': total_factura,
                    'URL_Comprobante': url_comprobante,
                    'Estado_Pago': 'Pendiente', # Estado inicial del pago
                    'Monto_Comprobante': 0.0, # Se llenará en la app admin
                    'Ref_Pago_Interna': st.session_state.referencia_pago, # Referencia de pago interna
                    'Comprobante_Confirmado': 'No' # Estado inicial del comprobante
                }])

                # Asegurarse de que todas las columnas existan en el df_pedidos original
                # y añadir las nuevas columnas si no existen antes de concatenar
                for col in nuevo_registro.columns:
                    if col not in df_pedidos.columns:
                        df_pedidos[col] = pd.NA # O un valor por defecto adecuado

                # Concatenar el nuevo registro al DataFrame existente
                df_pedidos = pd.concat([df_pedidos, nuevo_registro], ignore_index=True)

                # Actualizar Google Sheets
                update_data_to_gsheet(df_pedidos, GOOGLE_SHEET_ID)

                st.session_state.confirmado = True
                st.session_state.nuevo_pedido = nuevo_registro.iloc[0].to_dict() # Guardar el pedido para mostrarlo

                # Limpiar el estado de la sesión para el formulario
                st.session_state.cliente_input = ""
                st.session_state.tipo_envio_select = "Local"
                st.session_state.vendedor_registro_input = ""
                st.session_state.estado_select = "Pendiente"
                st.session_state.fecha_entrega_input = datetime.today().date()
                st.session_state.folio_factura_input = ""
                st.session_state.subtotal_input = 0.0
                st.session_state.iva_input = 0.0
                st.session_state.total_factura_input = 0.0
                st.session_state.comprobante_uploader = None
                st.session_state.referencia_pago = ""
                st.session_state.comprobante_cargado = False # Resetear estado de carga

                st.success("🎉 ¡Pedido registrado con éxito y comprobante subido (si aplica)!")
                st.rerun() # Recargar la app para limpiar el formulario y mostrar el nuevo pedido
if st.session_state.confirmado:
    st.subheader("✅ Pedido Confirmado")
    st.json(st.session_state.nuevo_pedido)
    st.button("Registrar Otro Pedido", on_click=lambda: st.session_state.update(confirmado=False, show_form=True))

st.markdown("---")


# --- FILTROS DE BÚSQUEDA ---
st.subheader("🔍 Buscar Pedidos")
col_search1, col_search2, col_search3 = st.columns(3)

with col_search1:
    search_id = st.text_input("Buscar por ID de Pedido", key="search_id_input")
with col_search2:
    search_cliente = st.text_input("Buscar por Cliente", key="search_cliente_input")
with col_search3:
    search_vendedor = st.text_input("Buscar por Vendedor", key="search_vendedor_input")

estado_filter = st.multiselect("Filtrar por Estado", df_pedidos['Estado'].unique(), key="estado_filter_select")
fecha_registro_start = st.date_input("Fecha de Registro - Desde", value=None, key="fecha_registro_start_input")
fecha_registro_end = st.date_input("Fecha de Registro - Hasta", value=None, key="fecha_registro_end_input")


filtered_df = df_pedidos.copy()

if search_id:
    filtered_df = filtered_df[filtered_df['ID_Pedido'].str.contains(search_id, case=False, na=False)]
if search_cliente:
    filtered_df = filtered_df[filtered_df['Cliente'].str.contains(search_cliente, case=False, na=False)]
if search_vendedor:
    filtered_df = filtered_df[filtered_df['Vendedor_Registro'].str.contains(search_vendedor, case=False, na=False)]
if estado_filter:
    filtered_df = filtered_df[filtered_df['Estado'].isin(estado_filter)]
if fecha_registro_start:
    # Asegurarse de que 'Fecha_Registro' sea datetime para la comparación
    if 'Fecha_Registro' in filtered_df.columns:
        filtered_df = filtered_df[pd.to_datetime(filtered_df['Fecha_Registro'], errors='coerce').dt.date >= fecha_registro_start]
if fecha_registro_end:
    if 'Fecha_Registro' in filtered_df.columns:
        filtered_df = filtered_df[pd.to_datetime(filtered_df['Fecha_Registro'], errors='coerce').dt.date <= fecha_registro_end]

st.dataframe(filtered_df, use_container_width=True, hide_index=True)


# --- EXPORTAR DATOS ---
st.subheader("📥 Exportar Datos Filtrados")
if not filtered_df.empty:
    filtered_df_download = filtered_df.copy()
    
    # Asegurarse de que solo las columnas requeridas para el Excel están presentes
    # y formatear fechas para Excel
    display_df = filtered_df_download[['Folio_Factura', 'ID_Pedido', 'Cliente', 'Estado', 'Vendedor_Registro', 'Tipo_Envio', 'Fecha_Entrega']].copy()
    if 'Fecha_Entrega' in display_df.columns:
        display_df['Fecha_Entrega'] = display_df['Fecha_Entrega'].dt.strftime('%Y-%m-%d')

    st.dataframe(display_df, use_container_width=True, hide_index=True)

    if not filtered_df_download.empty:
        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            # MODIFICATION 3: Ensure Fecha_Entrega is formatted as date string in Excel
            excel_df = filtered_df_download.copy()
            if 'Fecha_Entrega' in excel_df.columns:
                excel_df['Fecha_Entrega'] = excel_df['Fecha_Entrega'].dt.strftime('%Y-%m-%d')
            excel_df.to_excel(writer, index=False, sheet_name='Pedidos_Filtrados')
        processed_data = output.getvalue()

        st.download_button(
            label="📥 Descargar Excel Filtrado",
            data=processed_data,
            file_name=f"pedidos_filtrados_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            help="Descarga los datos filtrados en formato Excel."
        )
