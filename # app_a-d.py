# app_a.py
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import boto3
import re
import os
import gspread.utils
import requests
import uuid

st.set_page_config(page_title="Recepción de Pedidos TD", layout="wide")

st.title("📬 Bandeja de Pedidos TD")

# --- Google Sheets Configuration ---
# Eliminamos la línea SERVICE_ACCOUNT_FILE ya que leeremos de secrets
GOOGLE_SHEET_ID = '1aWkSelodaz0nWfQx7FZAysGnIYGQFJxAN7RO3YgCiZY' # Asegúrate de que este ID sea correcto
GOOGLE_SHEET_WORKSHEET_NAME = 'datos_pedidos' # Asegúrate de que este nombre sea correcto

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
        st.error("❌ Error: Las credenciales de Google Sheets no se encontraron en Streamlit secrets. Asegúrate de que 'google_credentials' esté en tus secretos de Streamlit.")
        st.stop()
    except json.JSONDecodeError:
        st.error("❌ Error: Las credenciales de Google Sheets en Streamlit secrets no son un JSON válido. Revisa el formato.")
        st.stop()
    except Exception as e:
        st.error(f"❌ Error al autenticar con Google Sheets: {e}")
        st.info("ℹ️ Verifica que las APIs de Google Sheets y Drive estén habilitadas para tu proyecto de Google Cloud y que tu archivo de credenciales sea válido.")
        st.stop()

# --- AWS S3 Configuration ---
try:
    AWS_ACCESS_KEY_ID = st.secrets["aws_access_key_id"]
    AWS_SECRET_ACCESS_KEY = st.secrets["aws_secret_access_key"]
    AWS_REGION = st.secrets["aws_region"]
    S3_BUCKET_NAME = st.secrets["s3_bucket_name"]
except KeyError as e:
    st.error(f"❌ Error: Las credenciales de AWS S3 no se encontraron en Streamlit secrets. Asegúrate de que las claves 'aws_access_key_id', 'aws_secret_access_key', 'aws_region' y 's3_bucket_name' estén directamente en tus secretos de Streamlit. Falta la clave: {e}")
    st.stop()

S3_ATTACHMENT_PREFIX = 'adjuntos_pedidos/'

# --- Initialize Session State for tab persistence ---
if "active_main_tab_index" not in st.session_state:
    st.session_state["active_main_tab_index"] = 0 # Default to the first tab

if "active_subtab_local_index" not in st.session_state:
    st.session_state["active_subtab_local_index"] = 0

if "active_date_tab_m_index" not in st.session_state:
    st.session_state["active_date_tab_m_index"] = 0 # Será dinámico

if "active_date_tab_t_index" not in st.session_state:
    st.session_state["active_date_tab_t_index"] = 0 # Será dinámico

if "expanded_attachments" not in st.session_state:
    st.session_state["expanded_attachments"] = {}


# --- Cached Clients for Google Sheets and AWS S3 ---

@st.cache_resource
def get_s3_client():
    """
    Inicializa y retorna un cliente de S3, usando credenciales globales.
    """
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
        st.info("ℹ️ Revisa tus credenciales de AWS en `st.secrets` y la configuración de la región.")
        st.stop()

# Initialize clients globally
try:
    gc = get_google_sheets_client()
    s3_client = get_s3_client()
except Exception as e:
    st.error(f"❌ Error general al autenticarse o inicializar clientes: {e}")
    st.info("ℹ️ Asegúrate de que las APIs de Google Sheets y Drive estén habilitadas para tu proyecto de Google Cloud y que tus secretos estén configurados correctamente.")
    st.stop()


# --- Data Loading from Google Sheets (Cached) ---
@st.cache_resource(ttl=60) # Carga cada 60 segundos o cuando se invalide la caché
def load_data_from_gsheets(sheet_id, worksheet_name):
    """
    Carga todos los datos de una hoja de cálculo de Google Sheets en un DataFrame de Pandas
    y añade el índice de fila de la hoja de cálculo.
    Retorna el DataFrame, el objeto worksheet y los encabezados.
    """
    try:
        spreadsheet = gc.open_by_id(sheet_id) # Usamos gc que ya está inicializado globalmente
        worksheet = spreadsheet.worksheet(worksheet_name)

        # Obtener todos los valores incluyendo los encabezados para poder calcular el índice de fila
        all_data = worksheet.get_all_values()
        if not all_data:
            return pd.DataFrame(), worksheet, [] # Devolver también los encabezados vacíos

        headers = all_data[0]
        data_rows = all_data[1:]

        df = pd.DataFrame(data_rows, columns=headers)

        # Añadir el índice de fila de Google Sheet (basado en 1)
        # Asumiendo que el encabezado está en la fila 1, la primera fila de datos es la fila 2.
        df['_gsheet_row_index'] = df.index + 2

        # Define las columnas esperadas y asegúrate de que existan
        expected_columns = [
            'ID_Pedido', 'Folio_Factura', 'Hora_Registro', 'Vendedor_Registro', 'Cliente',
            'Tipo_Envio', 'Fecha_Entrega', 'Comentario', 'Notas', 'Modificacion_Surtido',
            'Adjuntos', 'Adjuntos_Surtido', 'Estado', 'Estado_Pago', 'Fecha_Completado',
            'Hora_Proceso', 'Turno', 'Surtidor'
        ]

        for col in expected_columns:
            if col not in df.columns:
                df[col] = '' # Inicializa columnas faltantes como cadena vacía

        # Asegura que las columnas de fecha/hora se manejen correctamente
        df['Fecha_Entrega'] = df['Fecha_Entrega'].apply(
            lambda x: str(x) if pd.notna(x) and str(x).strip() != '' else ''
        )

        df['Hora_Registro'] = pd.to_datetime(df['Hora_Registro'], errors='coerce')
        df['Fecha_Completado'] = pd.to_datetime(df['Fecha_Completado'], errors='coerce')
        df['Hora_Proceso'] = pd.to_datetime(df['Hora_Proceso'], errors='coerce') # Ensure Hora_Proceso is datetime

        # IMPORTANT: Strip whitespace from key columns to ensure correct filtering and finding
        df['ID_Pedido'] = df['ID_Pedido'].astype(str).str.strip()
        df['Tipo_Envio'] = df['Tipo_Envio'].astype(str).str.strip()
        df['Turno'] = df['Turno'].astype(str).str.strip()
        df['Estado'] = df['Estado'].astype(str).str.strip()

        return df, worksheet, headers # Devolver también los encabezados

    except gspread.exceptions.SpreadsheetNotFound:
        st.error(f"❌ Error: La hoja de cálculo con ID '{sheet_id}' no se encontró. Verifica el ID.")
        st.stop()
    except gspread.exceptions.WorksheetNotFound:
        st.error(f"❌ Error: La pestaña '{worksheet_name}' no se encontró en la hoja de cálculo. Verifica el nombre de la pestaña.")
        st.stop()
    except Exception as e:
        st.error(f"❌ Error al cargar los datos desde Google Sheets: {e}")
        st.stop()

# --- Data Saving/Updating to Google Sheets ---
def update_gsheet_cell(worksheet, headers, row_index, col_name, value):
    """
    Actualiza una celda específica en Google Sheets.
    `row_index` es el índice de fila de gspread (base 1).
    `col_name` es el nombre de la columna.
    `headers` es la lista de encabezados obtenida previamente.
    """
    try:
        if col_name not in headers:
            st.error(f"❌ Error: La columna '{col_name}' no se encontró en Google Sheets para la actualización. Verifica los encabezados.")
            return False
        col_index = headers.index(col_name) + 1 # Convertir a índice base 1 de gspread
        worksheet.update_cell(row_index, col_index, value)
        # Invalida la caché de recursos para que la próxima carga sea fresca
        st.cache_resource.clear()
        return True
    except Exception as e:
        st.error(f"❌ Error al actualizar la celda ({row_index}, {col_name}) en Google Sheets: {e}")
        return False

def batch_update_gsheet_cells(worksheet, updates_list):
    """
    Realiza múltiples actualizaciones de celdas en una sola solicitud por lotes a Google Sheets utilizando worksheet.update_cells().
    updates_list: Lista de diccionarios, cada uno con las claves 'range' y 'values'. Ej: [{'range': 'A1', 'values': [['nuevo_valor']]}, ...]
    """
    try:
        if not updates_list:
            return False
        cell_list = []
        for update_item in updates_list:
            range_str = update_item['range']
            value = update_item['values'][0][0] # Asumiendo un único valor como [['valor']]
            # Convertir la notación A1 (ej. 'A1') a índice de fila y columna (base 1)
            row, col = gspread.utils.a1_to_rowcol(range_str)
            # Crear un objeto Cell y añadirlo a la lista
            cell_list.append(gspread.Cell(row=row, col=col, value=value))
        
        if cell_list:
            worksheet.update_cells(cell_list)
            st.cache_resource.clear() # Limpiar la caché después de una actualización
            return True
        return False
    except Exception as e:
        st.error(f"❌ Error al realizar la actualización por lotes en Google Sheets: {e}")
        return False

# --- Helper Functions ---
def get_s3_file_download_url(s3_client_instance, object_key):
    """Genera una URL de pre-firma para descargar un archivo de S3."""
    try:
        url = s3_client_instance.generate_presigned_url(
            ClientMethod='get_object',
            Params={'Bucket': S3_BUCKET_NAME, 'Key': object_key},
            ExpiresIn=3600 # URL válida por 1 hora
        )
        return url
    except Exception as e:
        st.error(f"❌ Error al generar URL de descarga para '{object_key}': {e}")
        return None

def find_pedido_subfolder_prefix(s3_client_instance, parent_prefix, folder_name):
    """
    Intenta encontrar el prefijo correcto de una subcarpeta de pedido en S3.
    Considera varias posibilidades para la estructura de carpetas.
    """
    if not s3_client_instance:
        return None
    
    # Lista de posibles prefijos para probar
    possible_prefixes = [
        f"{parent_prefix}{folder_name}/", # Ej: adjuntos_pedidos/PED-20231026123456-ABCD/
        f"{parent_prefix}{folder_name}",   # Ej: adjuntos_pedidos/PED-20231026123456-ABCD (sin barra al final)
        f"{folder_name}/",                 # Si la carpeta del pedido es directamente la raíz del bucket
        folder_name                        # Si la carpeta del pedido es directamente la raíz del bucket sin barra
    ]
    
    for pedido_prefix in possible_prefixes:
        try:
            # Intentar listar objetos con el prefijo, limitando a 1 para verificar existencia
            response = s3_client_instance.list_objects_v2(
                Bucket=S3_BUCKET_NAME,
                Prefix=pedido_prefix,
                MaxKeys=1
            )
            
            # Si hay contenido, significa que el prefijo es válido
            if 'Contents' in response and response['Contents']:
                return pedido_prefix
            
        except Exception:
            # Si hay un error (ej. prefijo inválido en S3, aunque poco probable con list_objects), ignorar y probar el siguiente
            continue
    
    # Si no se encuentra nada con los prefijos directos, hacer una búsqueda más general
    # Esto es más lento y solo se debería ejecutar si los intentos directos fallan
    try:
        response = s3_client_instance.list_objects_v2(
            Bucket=S3_BUCKET_NAME,
            MaxKeys=1000 # Limitar para evitar una lista excesivamente grande
        )
        
        if 'Contents' in response:
            for obj in response['Contents']:
                # Buscar si el nombre de la carpeta (ID_Pedido) está en la clave del objeto
                if folder_name in obj['Key']:
                    # Extraer el prefijo de la carpeta del pedido
                    if '/' in obj['Key']:
                        prefix_parts = obj['Key'].split('/')[:-1] # Obtener todas las partes excepto el nombre del archivo
                        return '/'.join(prefix_parts) + '/' # Reconstruir el prefijo con la barra al final
                    else:
                        # Si es un archivo directamente en la raíz con el nombre del pedido, esto es un caso límite.
                        # Mejor devolver el propio folder_name si es un archivo suelto, o None si no es una "carpeta".
                        # Para este contexto de subcarpetas, si no hay '/', no es una subcarpeta.
                        return None
        return None # No se encontró ninguna subcarpeta que coincida
    except Exception as e:
        st.warning(f"⚠️ Advertencia: Error durante la búsqueda general de prefijo de S3 para '{folder_name}': {e}")
        return None


def display_attachments(s3_client_instance, attachment_urls, pedido_id_for_prefix):
    """Muestra adjuntos con botones de descarga y miniaturas para imágenes."""
    if not attachment_urls:
        st.info("No hay adjuntos para este pedido.")
        return

    # Limpiar y obtener los nombres de los archivos para mostrar
    clean_attachment_info = []
    for url in attachment_urls:
        if url and isinstance(url, str):
            # Asume que el formato de URL es .../bucket_name/prefix/pedido_id/filename
            match = re.search(r'/(?:[a-zA-Z0-9_-]+\.)+[a-zA-Z]{2,6}/(?:.+/)*(.+)', url)
            file_name = match.group(1) if match else "Archivo Desconocido"
            
            # Intentar obtener la clave de S3 de la URL
            s3_key_match = re.search(r'\.amazonaws\.com/([^?]+)', url)
            s3_key = s3_key_match.group(1) if s3_key_match else None

            if s3_key:
                clean_attachment_info.append({'name': file_name, 's3_key': s3_key, 'url': url})
            else:
                # Si no podemos extraer la clave S3, al menos permitir la descarga directa de la URL original
                clean_attachment_info.append({'name': file_name, 'url': url, 's3_key': None})


    # Usar st.session_state para controlar la expansión
    if st.session_state["expanded_attachments"].get(pedido_id_for_prefix, False):
        if st.button("Contraer Adjuntos", key=f"collapse_att_{pedido_id_for_prefix}"):
            st.session_state["expanded_attachments"][pedido_id_for_prefix] = False
            st.rerun() # Recargar para aplicar el cambio
        
        cols = st.columns(3) # Para organizar los archivos en columnas
        col_idx = 0

        for att_info in clean_attachment_info:
            file_name = att_info['name']
            s3_key = att_info['s3_key']
            original_url = att_info['url'] # URL original del GSheet
            
            with cols[col_idx]:
                st.markdown(f"**{file_name}**")
                
                # Determinar si es una imagen para mostrar miniatura
                is_image = file_name.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.webp'))
                
                if s3_key and s3_client_instance:
                    # Generar URL de descarga firmada si tenemos la clave S3 y el cliente S3
                    download_url = get_s3_file_download_url(s3_client_instance, s3_key)
                    if download_url:
                        if is_image:
                            st.image(download_url, caption=file_name, width=150) # Miniatura
                        st.download_button(
                            label=f"Descargar {file_name}",
                            data=requests.get(download_url).content, # Descargar el contenido para el botón
                            file_name=file_name,
                            key=f"download_{s3_key}",
                            use_container_width=True
                        )
                    else:
                        st.warning(f"No se pudo generar URL de descarga para {file_name}.")
                else:
                    # Si no tenemos S3_key, intentamos usar la URL original directamente
                    if is_image:
                        st.image(original_url, caption=file_name, width=150) # Miniatura
                    st.markdown(f"[Descargar {file_name}]({original_url})", unsafe_allow_html=True) # Enlace directo

            col_idx = (col_idx + 1) % 3 # Mover a la siguiente columna

    else:
        if st.button(f"Ver {len(clean_attachment_info)} Adjuntos", key=f"expand_att_{pedido_id_for_prefix}"):
            st.session_state["expanded_attachments"][pedido_id_for_prefix] = True
            st.rerun() # Recargar para expandir
        

def get_current_week_dates():
    """Retorna las fechas de Lunes a Domingo de la semana actual."""
    today = datetime.now().date()
    start_of_week = today - timedelta(days=today.weekday()) # Lunes
    dates = [start_of_week + timedelta(days=i) for i in range(7)]
    return dates

def get_next_week_dates():
    """Retorna las fechas de Lunes a Domingo de la próxima semana."""
    today = datetime.now().date()
    start_of_current_week = today - timedelta(days=today.weekday()) # Lunes de esta semana
    start_of_next_week = start_of_current_week + timedelta(days=7) # Lunes de la próxima semana
    dates = [start_of_next_week + timedelta(days=i) for i in range(7)]
    return dates

def ordenar_pedidos_custom(df):
    """
    Ordena un DataFrame de pedidos según el tipo de envío y la fecha de entrega.
    """
    if df.empty:
        return df

    # Asegurarse de que 'Fecha_Entrega' es de tipo fecha
    df['Fecha_Entrega_dt'] = pd.to_datetime(df['Fecha_Entrega'], errors='coerce')

    # Definir el orden personalizado para 'Tipo_Envio'
    orden_tipo_envio = {
        "📍 Pedido Local": 0,
        "🚚 Pedido Foráneo": 1,
        "🛠 Garantía": 2,
        "🔁 Devolución": 3,
        "📬 Solicitud de guía": 4
    }
    df['Tipo_Envio_Orden'] = df['Tipo_Envio'].map(orden_tipo_envio)

    # Ordenar por Tipo_Envio_Orden y luego por Fecha_Entrega_dt (ascendente)
    df_sorted = df.sort_values(by=['Tipo_Envio_Orden', 'Fecha_Entrega_dt'], ascending=[True, True])

    # Eliminar columnas temporales
    df_sorted = df_sorted.drop(columns=['Fecha_Entrega_dt', 'Tipo_Envio_Orden'])

    return df_sorted

def mostrar_pedido(df_main, idx, row, orden, categoria, icono, worksheet, headers):
    """
    Muestra un pedido individual con sus detalles y botones de acción.
    """
    id_pedido = row['ID_Pedido']
    folio_factura = row['Folio_Factura']
    cliente = row['Cliente']
    estado = row['Estado']
    vendedor_registro = row['Vendedor_Registro']
    tipo_envio = row['Tipo_Envio']
    fecha_entrega = row['Fecha_Entrega']
    comentario = row['Comentario']
    notas = row['Notas']
    modificacion_surtido = row['Modificacion_Surtido']
    adjuntos = row['Adjuntos']
    adjuntos_surtido = row['Adjuntos_Surtido']
    estado_pago = row['Estado_Pago']
    turno = row['Turno']
    surtidor = row['Surtidor']

    st.markdown("---")
    st.markdown(f"#### {icono} Pedido #{orden}: {id_pedido} - Cliente: {cliente} {f'(Folio: {folio_factura})' if folio_factura else ''}")
    
    col1, col2, col3 = st.columns([1, 1, 1])

    with col1:
        st.write(f"**Vendedor:** {vendedor_registro}")
        st.write(f"**Tipo de Envío:** {tipo_envio}")
        st.write(f"**Fecha de Entrega:** {fecha_entrega}")
        if tipo_envio == "📍 Pedido Local":
            st.write(f"**Turno:** {turno if turno else 'N/A'}")
        
    with col2:
        st.write(f"**Estado General:** **`{estado}`**")
        st.write(f"**Estado de Pago:** **`{estado_pago}`**")
        st.write(f"**Surtidor Asignado:** {surtidor if surtidor else 'N/A'}")

    with col3:
        st.write(f"**Comentario:** {comentario if comentario else 'N/A'}")
        st.write(f"**Notas:** {notas if notas else 'N/A'}")
        st.write(f"**Modificación Surtido:** {modificacion_surtido if modificacion_surtido else 'N/A'}")
        
    # Sección de Adjuntos
    if adjuntos:
        st.markdown("**Adjuntos del Pedido:**")
        adjuntos_list = [url.strip() for url in adjuntos.split(',') if url.strip()]
        display_attachments(s3_client, adjuntos_list, id_pedido)

    if adjuntos_surtido:
        st.markdown("**Adjuntos de Surtido:**")
        adjuntos_surtido_list = [url.strip() for url in adjuntos_surtido.split(',') if url.strip()]
        display_attachments(s3_client, adjuntos_surtido_list, id_pedido)


    # --- Acciones de Estatus ---
    st.markdown("##### Acciones de Estatus:")
    col_acciones = st.columns(4)

    # Buscar el índice de la fila real en la hoja de Google Sheets
    gsheet_row_index = row['_gsheet_row_index']

    # Asignar Surtidor
    with col_acciones[0]:
        vendedores_surtidores_list = [""] + sorted(list(df_main['Vendedor_Registro'].unique())) # Incluye vacío y vendedores únicos
        current_surtidor_index = vendedores_surtidores_list.index(surtidor) if surtidor in vendedores_surtidores_list else 0
        new_surtidor = st.selectbox(
            "Asignar Surtidor",
            options=vendedores_surtidores_list,
            index=current_surtidor_index,
            key=f"surtidor_select_{id_pedido}"
        )
        if st.button("Asignar", key=f"assign_surtidor_btn_{id_pedido}"):
            if update_gsheet_cell(worksheet, headers, gsheet_row_index, 'Surtidor', new_surtidor):
                st.success(f"Surtidor '{new_surtidor}' asignado al pedido {id_pedido}.")
                st.rerun()

    # Actualizar Estado
    with col_acciones[1]:
        # Opciones de estado permitidas según el estado actual
        estado_options = [
            "🔴 Pendiente",
            "🟡 En Proceso",
            "✅ Completado",
            "❌ Cancelado"
        ]
        
        try:
            current_estado_index = estado_options.index(estado)
        except ValueError:
            current_estado_index = 0 # Default si el estado actual no está en las opciones

        new_estado = st.selectbox(
            "Actualizar Estado",
            options=estado_options,
            index=current_estado_index,
            key=f"estado_select_{id_pedido}"
        )
        if st.button("Actualizar", key=f"update_status_btn_{id_pedido}"):
            updates = []
            if new_estado != estado:
                updates.append({
                    'range': gspread.utils.rowcol_to_a1(gsheet_row_index, headers.index('Estado') + 1),
                    'values': [[new_estado]]
                })
                # Si el estado cambia a "Completado", registrar Fecha_Completado y Hora_Proceso
                if new_estado == "✅ Completado":
                    current_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    updates.append({
                        'range': gspread.utils.rowcol_to_a1(gsheet_row_index, headers.index('Fecha_Completado') + 1),
                        'values': [[current_time_str.split(' ')[0]]] # Solo la fecha
                    })
                    updates.append({
                        'range': gspread.utils.rowcol_to_a1(gsheet_row_index, headers.index('Hora_Proceso') + 1),
                        'values': [[current_time_str]] # Fecha y hora completas para Hora_Proceso
                    })
            
            if batch_update_gsheet_cells(worksheet, updates):
                st.success(f"Estado del pedido {id_pedido} actualizado a '{new_estado}'.")
                st.rerun()


    # Actualizar Notas
    with col_acciones[2]:
        new_notas = st.text_area("Notas Adicionales", value=notas, key=f"notas_text_{id_pedido}", height=50)
        if st.button("Guardar Notas", key=f"save_notas_btn_{id_pedido}"):
            if update_gsheet_cell(worksheet, headers, gsheet_row_index, 'Notas', new_notas):
                st.success(f"Notas del pedido {id_pedido} actualizadas.")
                st.rerun()
    
    # Subir Adjunto de Surtido
    with col_acciones[3]:
        uploaded_surtido_file = st.file_uploader(
            "Adjuntar de Surtido",
            type=["pdf", "jpg", "jpeg", "png", "xlsx", "docx"],
            key=f"surtido_file_uploader_{id_pedido}"
        )
        if uploaded_surtido_file:
            if st.button("Subir Adjunto Surtido", key=f"upload_surtido_btn_{id_pedido}"):
                file_extension = os.path.splitext(uploaded_surtido_file.name)[1]
                # Crear una clave única para S3
                s3_key = f"{S3_ATTACHMENT_PREFIX}{id_pedido}/{uploaded_surtido_file.name.replace(' ', '_').replace(file_extension, '')}_{uuid.uuid4().hex[:4]}{file_extension}"
                success, file_url = upload_file_to_s3(s3_client, S3_BUCKET_NAME, uploaded_surtido_file, s3_key)
                
                if success:
                    # Añadir la nueva URL a la lista existente de adjuntos de surtido
                    current_adjuntos_surtido = adjuntos_surtido.split(',') if adjuntos_surtido else []
                    current_adjuntos_surtido.append(file_url)
                    updated_adjuntos_surtido_str = ','.join([url.strip() for url in current_adjuntos_surtido if url.strip()])
                    
                    if update_gsheet_cell(worksheet, headers, gsheet_row_index, 'Adjuntos_Surtido', updated_adjuntos_surtido_str):
                        st.success(f"Adjunto de surtido para pedido {id_pedido} subido exitosamente.")
                        st.rerun()
                else:
                    st.error(f"❌ Falló la subida del adjunto de surtido para pedido {id_pedido}.")

def upload_file_to_s3(s3_client_instance, bucket_name, file_obj, s3_key):
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
        file_obj.seek(0) # Asegúrate de que el puntero del archivo esté al principio
        s3_client_instance.upload_fileobj(file_obj, bucket_name, s3_key)
        # Generar la URL pública (o de acceso)
        file_url = f"https://{bucket_name}.s3.{AWS_REGION}.amazonaws.com/{s3_key}"
        return True, file_url
    except Exception as e:
        st.error(f"❌ Error al subir el archivo '{s3_key}' a S3: {e}")
        return False, None


# --- Main Application Logic ---
df_main, worksheet_main, headers_main = load_data_from_gsheets(GOOGLE_SHEET_ID, GOOGLE_SHEET_WORKSHEET_NAME)

if not df_main.empty:
    # FILTRADO Y PROCESAMIENTO DE DATOS
    df_main['Fecha_Entrega_dt'] = pd.to_datetime(df_main['Fecha_Entrega'], errors='coerce')
    
    # Pedidos pendientes (que no están Completados o Cancelados)
    df_pendientes = df_main[~df_main['Estado'].isin(['✅ Completado', '❌ Cancelado'])].copy()

    # Pedidos completados para el historial (últimos 30 días)
    # Definir la fecha de hace 30 días
    thirty_days_ago = datetime.now() - timedelta(days=30)
    df_completados_historial = df_main[
        (df_main['Estado'] == '✅ Completado') & 
        (df_main['Fecha_Completado'] >= thirty_days_ago) # Filtrar por Fecha_Completado
    ].sort_values(by='Fecha_Completado', ascending=False).copy()
    
    # Filtros para "Pendientes Hoy/Mañana"
    today = datetime.now().date()
    tomorrow = today + timedelta(days=1)

    df_pendientes_hoy = df_pendientes[
        (df_pendientes['Fecha_Entrega_dt'].dt.date == today)
    ].copy()
    
    df_pendientes_manana = df_pendientes[
        (df_pendientes['Fecha_Entrega_dt'].dt.date == tomorrow)
    ].copy()

    # Filtros para "Pendientes Pasados"
    df_pendientes_pasados = df_pendientes[
        (df_pendientes['Fecha_Entrega_dt'].dt.date < today)
    ].copy()
    
    # Filtros para "En Proceso"
    df_en_proceso = df_pendientes[
        (df_pendientes['Estado'] == '🟡 En Proceso')
    ].copy()

    # Filtros para "Pendientes de Proceso" (Todo lo que no es Completado/Cancelado/En Proceso)
    df_pendientes_proceso = df_pendientes[
        ~df_pendientes['Estado'].isin(['🟡 En Proceso'])
    ].copy()

    # Pestañas principales
    main_tabs = st.tabs([
        f"⏳ Pendientes Hoy ({len(df_pendientes_hoy)})",
        f"➡️ Pendientes Mañana ({len(df_pendientes_manana)})",
        f"⏰ Pendientes Pasados ({len(df_pendientes_pasados)})",
        f"⚙️ En Proceso ({len(df_en_proceso)})",
        f"📦 Pendientes de Proceso ({len(df_pendientes_proceso)})",
        f"✅ Historial Completados ({len(df_completados_historial)})"
    ], key="main_tabs_app_a", on_change=lambda: st.session_state.update(active_main_tab_index=main_tabs.index(st.session_state.main_tabs_app_a)))

    with main_tabs[0]: # ⏳ Pendientes Hoy
        st.markdown("### Pedidos Pendientes para HOY")
        
        # Filtrar por Tipo de Envío para "Pendientes Hoy"
        tipo_envio_hoy = st.selectbox(
            "Filtrar por Tipo de Envío (Hoy)",
            ["Todos", "📍 Pedido Local", "🚚 Pedido Foráneo", "🛠 Garantía", "🔁 Devolución", "📬 Solicitud de guía"],
            key="filtro_tipo_envio_hoy"
        )
        if tipo_envio_hoy != "Todos":
            df_pendientes_hoy = df_pendientes_hoy[df_pendientes_hoy['Tipo_Envio'] == tipo_envio_hoy].copy()

        if not df_pendientes_hoy.empty:
            df_pendientes_hoy_sorted = ordenar_pedidos_custom(df_pendientes_hoy)
            # Organizar por Turno
            turnos_hoy = ["☀️ Local Mañana", "🌙 Local Tarde", "🌵 Saltillo", "📦 Pasa a Bodega", "N/A"] # N/A para foráneos/garantías etc.
            tab_titles_hoy = [f"{t} ({len(df_pendientes_hoy_sorted[df_pendientes_hoy_sorted['Turno'] == t])})" for t in turnos_hoy]
            tabs_hoy = st.tabs(tab_titles_hoy, key="tabs_pendientes_hoy") # Usar un key único

            for i, turno_val in enumerate(turnos_hoy):
                with tabs_hoy[i]:
                    pedidos_por_turno = df_pendientes_hoy_sorted[df_pendientes_hoy_sorted['Turno'] == turno_val]
                    if not pedidos_por_turno.empty:
                        for orden, (idx, row) in enumerate(pedidos_por_turno.iterrows(), start=1):
                            icono = "☀️" if "Mañana" in turno_val else "🌙" if "Tarde" in turno_val else "🌵" if "Saltillo" in turno_val else "📦" if "Bodega" in turno_val else "🚚" # Icono más genérico para N/A
                            mostrar_pedido(df_main, idx, row, orden, f"Pendientes Hoy - {turno_val}", icono, worksheet_main, headers_main)
                    else:
                        st.info(f"No hay pedidos pendientes para HOY en el turno: {turno_val}")
        else:
            st.info("No hay pedidos pendientes para HOY.")

    with main_tabs[1]: # ➡️ Pendientes Mañana
        st.markdown("### Pedidos Pendientes para MAÑANA")
        
        # Filtrar por Tipo de Envío para "Pendientes Mañana"
        tipo_envio_manana = st.selectbox(
            "Filtrar por Tipo de Envío (Mañana)",
            ["Todos", "📍 Pedido Local", "🚚 Pedido Foráneo", "🛠 Garantía", "🔁 Devolución", "📬 Solicitud de guía"],
            key="filtro_tipo_envio_manana"
        )
        if tipo_envio_manana != "Todos":
            df_pendientes_manana = df_pendientes_manana[df_pendientes_manana['Tipo_Envio'] == tipo_envio_manana].copy()

        if not df_pendientes_manana.empty:
            df_pendientes_manana_sorted = ordenar_pedidos_custom(df_pendientes_manana)
            turnos_manana = ["☀️ Local Mañana", "🌙 Local Tarde", "🌵 Saltillo", "📦 Pasa a Bodega", "N/A"] # N/A para foráneos/garantías etc.
            tab_titles_manana = [f"{t} ({len(df_pendientes_manana_sorted[df_pendientes_manana_sorted['Turno'] == t])})" for t in turnos_manana]
            tabs_manana = st.tabs(tab_titles_manana, key="tabs_pendientes_manana") # Usar un key único

            for i, turno_val in enumerate(turnos_manana):
                with tabs_manana[i]:
                    pedidos_por_turno = df_pendientes_manana_sorted[df_pendientes_manana_sorted['Turno'] == turno_val]
                    if not pedidos_por_turno.empty:
                        for orden, (idx, row) in enumerate(pedidos_por_turno.iterrows(), start=1):
                            icono = "☀️" if "Mañana" in turno_val else "🌙" if "Tarde" in turno_val else "🌵" if "Saltillo" in turno_val else "📦" if "Bodega" in turno_val else "🚚"
                            mostrar_pedido(df_main, idx, row, orden, f"Pendientes Mañana - {turno_val}", icono, worksheet_main, headers_main)
                    else:
                        st.info(f"No hay pedidos pendientes para MAÑANA en el turno: {turno_val}")
        else:
            st.info("No hay pedidos pendientes para MAÑANA.")

    with main_tabs[2]: # ⏰ Pendientes Pasados
        st.markdown("### Pedidos Pendientes con Fecha de Entrega Pasada")
        
        # Filtrar por Tipo de Envío para "Pendientes Pasados"
        tipo_envio_pasados = st.selectbox(
            "Filtrar por Tipo de Envío (Pasados)",
            ["Todos", "📍 Pedido Local", "🚚 Pedido Foráneo", "🛠 Garantía", "🔁 Devolución", "📬 Solicitud de guía"],
            key="filtro_tipo_envio_pasados"
        )
        if tipo_envio_pasados != "Todos":
            df_pendientes_pasados = df_pendientes_pasados[df_pendientes_pasados['Tipo_Envio'] == tipo_envio_pasados].copy()

        if not df_pendientes_pasados.empty:
            df_pendientes_pasados_sorted = ordenar_pedidos_custom(df_pendientes_pasados)
            for orden, (idx, row) in enumerate(df_pendientes_pasados_sorted.iterrows(), start=1):
                mostrar_pedido(df_main, idx, row, orden, "Pendientes Pasados", "⏰", worksheet_main, headers_main)
        else:
            st.info("No hay pedidos pendientes con fecha de entrega pasada.")

    with main_tabs[3]: # ⚙️ En Proceso
        st.markdown("### Pedidos Actualmente EN PROCESO")
        
        # Filtrar por Tipo de Envío para "En Proceso"
        tipo_envio_en_proceso = st.selectbox(
            "Filtrar por Tipo de Envío (En Proceso)",
            ["Todos", "📍 Pedido Local", "🚚 Pedido Foráneo", "🛠 Garantía", "🔁 Devolución", "📬 Solicitud de guía"],
            key="filtro_tipo_envio_en_proceso"
        )
        if tipo_envio_en_proceso != "Todos":
            df_en_proceso = df_en_proceso[df_en_proceso['Tipo_Envio'] == tipo_envio_en_proceso].copy()

        if not df_en_proceso.empty:
            df_en_proceso_sorted = ordenar_pedidos_custom(df_en_proceso)
            for orden, (idx, row) in enumerate(df_en_proceso_sorted.iterrows(), start=1):
                mostrar_pedido(df_main, idx, row, orden, "En Proceso", "⚙️", worksheet_main, headers_main)
        else:
            st.info("No hay pedidos actualmente en proceso.")

    with main_tabs[4]: # 📦 Pendientes de Proceso (Todo lo demás)
        st.markdown("### Pedidos Pendientes de Ser Procesados (General)")
        st.info("Esta sección muestra todos los pedidos que no están 'Completados', 'Cancelados' ni 'En Proceso'.")

        # Filtrar por Tipo de Envío para "Pendientes de Proceso"
        tipo_envio_pendientes_proceso = st.selectbox(
            "Filtrar por Tipo de Envío (Pendientes de Proceso)",
            ["Todos", "📍 Pedido Local", "🚚 Pedido Foráneo", "🛠 Garantía", "🔁 Devolución", "📬 Solicitud de guía"],
            key="filtro_tipo_envio_pendientes_proceso"
        )
        if tipo_envio_pendientes_proceso != "Todos":
            df_pendientes_proceso = df_pendientes_proceso[df_pendientes_proceso['Tipo_Envio'] == tipo_envio_pendientes_proceso].copy()

        if not df_pendientes_proceso.empty:
            df_pendientes_proceso_sorted = ordenar_pedidos_custom(df_pendientes_proceso)
            # Mostrar primero los pedidos locales por turno
            st.subheader("Pedidos Locales")
            turnos_proceso = ["☀️ Local Mañana", "🌙 Local Tarde", "🌵 Saltillo", "📦 Pasa a Bodega", "N/A"]
            
            for turno_val in turnos_proceso:
                pedidos_local_turno = df_pendientes_proceso_sorted[
                    (df_pendientes_proceso_sorted['Tipo_Envio'] == "📍 Pedido Local") & 
                    (df_pendientes_proceso_sorted['Turno'] == turno_val)
                ].copy()
                
                if not pedidos_local_turno.empty:
                    st.markdown(f"##### {turno_val} ({len(pedidos_local_turno)} pedidos)")
                    for orden, (idx, row) in enumerate(pedidos_local_turno.iterrows(), start=1):
                        icono = "☀️" if "Mañana" in turno_val else "🌙" if "Tarde" in turno_val else "🌵" if "Saltillo" in turno_val else "📦" if "Bodega" in turno_val else ""
                        mostrar_pedido(df_main, idx, row, orden, "Pedido Local", icono, worksheet_main, headers_main)
                # else:
                #    st.info(f"No hay pedidos locales pendientes para el turno: {turno_val}")

            # Luego, el resto de los tipos de envío (Foráneos, Garantías, Devoluciones, Solicitudes de guía)
            st.subheader("Otros Tipos de Envío")
            
            foraneo_display = df_pendientes_proceso_sorted[(df_pendientes_proceso_sorted["Tipo_Envio"] == "🚚 Pedido Foráneo")].copy()
            if not foraneo_display.empty:
                for orden, (idx, row) in enumerate(foraneo_display.iterrows(), start=1):
                    mostrar_pedido(df_main, idx, row, orden, "Pedido Foráneo", "🚚", worksheet_main, headers_main)
            else:
                st.info("No hay pedidos foráneos pendientes.")

            garantias_display = df_pendientes_proceso_sorted[(df_pendientes_proceso_sorted["Tipo_Envio"] == "🛠 Garantía")].copy()
            if not garantias_display.empty:
                for orden, (idx, row) in enumerate(garantias_display.iterrows(), start=1):
                    mostrar_pedido(df_main, idx, row, orden, "Garantía", "🛠", worksheet_main, headers_main)
            else:
                st.info("No hay garantías pendientes.")
            
            devoluciones_display = df_pendientes_proceso_sorted[(df_pendientes_proceso_sorted["Tipo_Envio"] == "🔁 Devolución")].copy()
            if not devoluciones_display.empty:
                for orden, (idx, row) in enumerate(devoluciones_display.iterrows(), start=1):
                    mostrar_pedido(df_main, idx, row, orden, "Devolución", "🔁", worksheet_main, headers_main)
            else:
                st.info("No hay devoluciones pendientes.")

            solicitudes_display = df_pendientes_proceso_sorted[(df_pendientes_proceso_sorted["Tipo_Envio"] == "📬 Solicitud de guía")].copy()
            if not solicitudes_display.empty:
                for orden, (idx, row) in enumerate(solicitudes_display.iterrows(), start=1):
                    mostrar_pedido(df_main, idx, row, orden, "Solicitud de Guía", "📬", worksheet_main, headers_main)
            else:
                st.info("No hay solicitudes de guía.")

        else:
            st.info("No hay pedidos pendientes de proceso.")

    with main_tabs[5]: # ✅ Historial Completados
        st.markdown("### Historial de Pedidos Completados")
        if not df_completados_historial.empty:
            st.dataframe(
                df_completados_historial[[
                    'ID_Pedido', 'Folio_Factura', 'Cliente', 'Estado', 'Vendedor_Registro',
                    'Tipo_Envio', 'Fecha_Entrega', 'Fecha_Completado', 'Notas', 'Modificacion_Surtido',
                    'Adjuntos', 'Adjuntos_Surtido', 'Turno'
                ]].head(50),
                use_container_width=True, hide_index=True
            )
            st.info("Mostrando los 50 pedidos completados más recientes. Puedes ajustar este límite si es necesario.")
        else:
            st.info("No hay pedidos completados en el historial.")

else:
    st.info("No se encontraron datos de pedidos en la hoja de Google Sheets. Asegúrate de que los datos se están subiendo correctamente y que el ID de la hoja y el nombre de la pestaña son correctos.")
