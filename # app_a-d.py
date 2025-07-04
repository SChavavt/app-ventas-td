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

# Configuración de la página de Streamlit
st.set_page_config(page_title="Recepción de Pedidos TD", layout="wide")

st.title("📬 Bandeja de Pedidos TD")

# --- Google Sheets Configuration ---
# NO se usa un archivo SERVICE_ACCOUNT_FILE local. Las credenciales se cargan desde st.secrets.
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
# Las credenciales de AWS S3 deben estar directamente en st.secrets, no anidadas.
try:
    AWS_ACCESS_KEY_ID = st.secrets["aws_access_key_id"]
    AWS_SECRET_ACCESS_KEY = st.secrets["aws_secret_access_key"]
    AWS_REGION = st.secrets["aws_region"]
    S3_BUCKET_NAME = st.secrets["s3_bucket_name"]
    S3_ATTACHMENT_PREFIX = 'adjuntos_pedidos/' # Prefijo para la subcarpeta de adjuntos
except KeyError as e:
    st.error(f"❌ Error: Las credenciales de AWS S3 no se encontraron en Streamlit secrets. Asegúrate de que las claves 'aws_access_key_id', 'aws_secret_access_key', 'aws_region' y 's3_bucket_name' estén directamente en tus secretos de Streamlit. Clave faltante: {e}")
    st.stop()
except Exception as e:
    st.error(f"❌ Error al cargar la configuración de AWS S3: {e}")
    st.stop()


@st.cache_resource
def get_s3_client():
    """
    Inicializa y retorna un cliente de S3, usando credenciales de Streamlit secrets.
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


# --- Inicialización de clientes globales ---
# Aquí se inicializan los clientes para Google Sheets y S3
try:
    gc = get_google_sheets_client()
    # Abrir la hoja de cálculo y seleccionar la pestaña principal
    spreadsheet = gc.open_by_id(GOOGLE_SHEET_ID)
    worksheet_main = spreadsheet.worksheet(GOOGLE_SHEET_WORKSHEET_NAME)
    s3_client = get_s3_client()
except Exception as e:
    st.error(f"❌ Error general al autenticarse o inicializar clientes: {e}")
    st.info("ℹ️ Asegúrate de que las APIs de Google Sheets y Drive estén habilitadas para tu proyecto de Google Cloud y que tus secretos estén configurados correctamente.")
    st.stop()


# --- Data Loading from Google Sheets (Cached) ---
@st.cache_data(ttl=60) # Carga cada 60 segundos o cuando se invalide la caché
def load_data_from_gsheets(sheet_id, worksheet_name):
    """
    Carga todos los datos de una hoja de cálculo de Google Sheets en un DataFrame de Pandas
    y añade el índice de fila de la hoja de cálculo.
    Retorna el DataFrame, el objeto worksheet y los encabezados.
    """
    try:
        # Asegurarse de que el worksheet se pasa correctamente
        # Dado que gc y worksheet_main son variables globales (o cacheadas), las usamos directamente
        
        all_data = worksheet_main.get_all_values() # Usamos worksheet_main directamente aquí
        if not all_data:
            return pd.DataFrame(), worksheet_main, [] # Devolver también los encabezados vacíos

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
        # Se asume formato DD/MM/YYYY o YYYY-MM-DD
        df['Fecha_Entrega'] = pd.to_datetime(df['Fecha_Entrega'], errors='coerce', dayfirst=True)
        df['Hora_Registro'] = pd.to_datetime(df['Hora_Registro'], errors='coerce')
        df['Fecha_Completado'] = pd.to_datetime(df['Fecha_Completado'], errors='coerce')
        df['Hora_Proceso'] = pd.to_datetime(df['Hora_Proceso'], errors='coerce') # Ensure Hora_Proceso is datetime

        # IMPORTANT: Strip whitespace from key columns to ensure correct filtering and finding
        df['ID_Pedido'] = df['ID_Pedido'].astype(str).str.strip()
        df['Tipo_Envio'] = df['Tipo_Envio'].astype(str).str.strip()
        df['Turno'] = df['Turno'].astype(str).str.strip()
        df['Estado'] = df['Estado'].astype(str).str.strip()

        return df, worksheet_main, headers # Devolver también los encabezados

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
        # Invalida la caché de datos para que la próxima carga sea fresca
        load_data_from_gsheets.clear()
        return True
    except Exception as e:
        st.error(f"❌ Error al actualizar la celda ({row_index}, {col_name}) en Google Sheets: {e}")
        return False

def batch_update_gsheet_cells(worksheet, updates_list):
    """
    Realiza múltiples actualizaciones de celdas en una sola solicitud por lotes a Google Sheets
    utilizando worksheet.update_cells().
    updates_list: Lista de diccionarios, cada uno con las claves 'range' y 'values'.
                  Ej: [{'range': 'A1', 'values': [['nuevo_valor']]}, ...]
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
            worksheet.update_cells(cell_list) # Este es el método correcto para batch update en el worksheet
            # Invalida la caché de datos para que la próxima carga sea fresca
            load_data_from_gsheets.clear()
            return True
        return False
    except Exception as e:
        st.error(f"❌ Error al realizar la actualización por lotes en Google Sheets: {e}")
        return False

# --- AWS S3 Helper Functions ---

def find_pedido_subfolder_prefix(s3_client_param, parent_prefix, folder_name):
    if not s3_client_param:
        return None

    # Normalizamos el folder_name para que coincida con el formato en S3
    normalized_folder_name = folder_name.strip('/') # Eliminar barras iniciales/finales

    # Intentamos prefijos específicos que podrían existir
    possible_prefixes = [
        f"{parent_prefix}{normalized_folder_name}/",
        f"{normalized_folder_name}/",
        # Considerar si los adjuntos están directamente bajo el bucket sin prefijo general
        f"{normalized_folder_name}" # Para casos donde el archivo está directamente en la raíz de la "subcarpeta"
    ]

    for pedido_prefix_attempt in possible_prefixes:
        try:
            response = s3_client_param.list_objects_v2(
                Bucket=S3_BUCKET_NAME,
                Prefix=pedido_prefix_attempt,
                MaxKeys=1
            )
            if 'Contents' in response and response['Contents']:
                return pedido_prefix_attempt
        except Exception:
            continue # Ignorar errores de lista de objetos para probar el siguiente prefijo

    # Si no se encuentra un prefijo directo, intentar una búsqueda más amplia
    # Esto es útil si la estructura es inconsistente
    try:
        response = s3_client_param.list_objects_v2(
            Bucket=S3_BUCKET_NAME,
            Prefix=parent_prefix, # Buscar solo dentro del prefijo general 'adjuntos_pedidos/'
            MaxKeys=1000 # Un límite razonable para evitar búsquedas muy grandes
        )
        if 'Contents' in response:
            for obj in response['Contents']:
                # Buscar si el nombre de la carpeta (ID_Pedido) está en la clave del objeto
                if f"/{normalized_folder_name}/" in obj['Key'] or obj['Key'].startswith(f"{normalized_folder_name}/"):
                    # Si se encuentra, extraer el prefijo de la carpeta
                    key_parts = obj['Key'].split('/')
                    # Asume que el ID_Pedido es la parte inmediatamente después de S3_ATTACHMENT_PREFIX
                    # o la primera parte si no hay S3_ATTACHMENT_PREFIX
                    if key_parts[0] == parent_prefix.strip('/'):
                        return f"{parent_prefix}{key_parts[1]}/"
                    else:
                        return f"{key_parts[0]}/" # Si el ID_Pedido es la carpeta raíz
    except Exception:
        pass # Ignorar errores en la búsqueda amplia

    return None # Si no se encontró ningún prefijo

def get_files_in_s3_prefix(s3_client_param, prefix):
    if not s3_client_param or not prefix:
        return []

    try:
        response = s3_client_param.list_objects_v2(
            Bucket=S3_BUCKET_NAME,
            Prefix=prefix,
            MaxKeys=100
        )

        files = []
        if 'Contents' in response:
            for item in response['Contents']:
                if not item['Key'].endswith('/'): # Asegurarse de que no sea una "carpeta"
                    file_name = item['Key'].split('/')[-1]
                    if file_name:
                        files.append({
                            'title': file_name,
                            'key': item['Key'], # La clave completa del objeto en S3
                            'size': item['Size'],
                            'last_modified': item['LastModified']
                        })
        return files

    except Exception as e:
        st.error(f"❌ Error al obtener archivos del prefijo S3 '{prefix}': {e}")
        return []

def get_s3_file_download_url(s3_client_param, object_key):
    if not s3_client_param or not object_key:
        return "#"

    try:
        url = s3_client_param.generate_presigned_url(
            'get_object',
            Params={'Bucket': S3_BUCKET_NAME, 'Key': object_key},
            ExpiresIn=7200 # URL válida por 2 horas
        )
        return url
    except Exception as e:
        st.error(f"❌ Error al generar URL pre-firmada para '{object_key}': {e}")
        return "#"

def upload_file_to_s3(file_object, file_name, pedido_id, client_name):
    """Sube un archivo a S3 dentro de una subcarpeta específica para el pedido y retorna su URL."""
    try:
        # Sanitizar el ID de pedido para usar en la ruta del archivo
        sanitized_pedido_id = re.sub(r'[^a-zA-Z0-9_.-]', '_', pedido_id)

        # Crear la ruta completa del objeto en S3: adjuntos_pedidos/ID_Pedido/nombre_archivo
        s3_object_key = f"{S3_ATTACHMENT_PREFIX}{sanitized_pedido_id}/{file_name}"

        s3_client.upload_fileobj(file_object, S3_BUCKET_NAME, s3_object_key)
        
        file_url = f"https://{S3_BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{s3_object_key}"
        return file_url
    except Exception as e:
        st.error(f"❌ Error al subir archivo a S3: {e}")
        return None

def delete_file_from_s3(file_url):
    """Elimina un archivo de S3 dada su URL."""
    try:
        # Extraer la clave del objeto de la URL
        key = file_url.split(f"https://{S3_BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/")[-1]
        s3_client.delete_object(Bucket=S3_BUCKET_NAME, Key=key)
        st.success(f"🗑️ Archivo '{key}' eliminado de S3.")
    except Exception as e:
        st.error(f"❌ Error al eliminar archivo de S3: {e}")


# --- Helper Functions (existentes en app.py) ---

def ordenar_pedidos_custom(df_pedidos_filtrados):
    """
    Ordena los pedidos con un orden de prioridad específico.
    Los estados críticos (Demorado, Pendiente, En Proceso) primero, luego el resto por Fecha_Entrega.
    """
    if df_pedidos_filtrados.empty:
        return df_pedidos_filtrados

    estado_orden = {
        '🔴 Demorado': 0,
        '🟡 Pendiente': 1,
        '🔵 En Proceso': 2,
        '📦 Surtido': 3,
        '🟣 Completado (Recepción)': 4,
        '✅ Entregado': 5,
        '🟢 Completado': 6, # Este es el estado final de 'Completado'
        '❌ Cancelado': 7
    }
    
    # Asegurarse de que 'Fecha_Entrega' sea datetime para el ordenamiento
    df_pedidos_filtrados['Fecha_Entrega_dt_sort'] = pd.to_datetime(df_pedidos_filtrados['Fecha_Entrega'], errors='coerce')
    # Usar Hora_Registro para desempate si la fecha de entrega es la misma o nula
    df_pedidos_filtrados['Hora_Registro_dt_sort'] = pd.to_datetime(df_pedidos_filtrados['Hora_Registro'], errors='coerce')


    df_pedidos_filtrados['Orden_Estado'] = df_pedidos_filtrados['Estado'].map(estado_orden).fillna(99) # Asegurar que estados no mapeados vayan al final
    
    # Ordenar por el número de orden de estado, luego por Fecha_Entrega_dt_sort (ascendente),
    # y finalmente por Hora_Registro_dt_sort (ascendente) para desempate.
    df_sorted = df_pedidos_filtrados.sort_values(
        by=['Orden_Estado', 'Fecha_Entrega_dt_sort', 'Hora_Registro_dt_sort'],
        ascending=[True, True, True]
    )
    
    # Eliminar las columnas temporales de orden
    df_sorted = df_sorted.drop(columns=['Orden_Estado', 'Fecha_Entrega_dt_sort', 'Hora_Registro_dt_sort'])
    return df_sorted


def check_and_update_demorados(df_to_check, worksheet, headers): # Añadir 'headers'
    """
    Checks for orders in 'En Proceso' status that have exceeded 1 hour and
    updates their status to 'Demorado' in the DataFrame and Google Sheets.
    Utiliza actualización por lotes para mayor eficiencia.
    """
    updates_to_perform = []
    current_time = datetime.now()
    one_hour_ago = current_time - timedelta(hours=1)

    try:
        estado_col_index = headers.index('Estado') + 1
    except ValueError:
        st.error("❌ Error interno: Columna 'Estado' o 'Hora_Proceso' no encontrada en los encabezados de Google Sheets.")
        return df_to_check, False
    
    for idx, row in df_to_check.iterrows():
        # Procesar solo si el estado es 'En Proceso' y Hora_Proceso no es nula
        if row['Estado'] == "🔵 En Proceso" and pd.notna(row['Hora_Proceso']):
            hora_proceso_dt = pd.to_datetime(row['Hora_Proceso'], errors='coerce')

            if pd.notna(hora_proceso_dt) and hora_proceso_dt < one_hour_ago:
                gsheet_row_index = row.get('_gsheet_row_index') # Usar el índice pre-calculado

                if gsheet_row_index is not None:
                    # Preparar la actualización para el estado
                    updates_to_perform.append({
                        'range': gspread.utils.rowcol_to_a1(gsheet_row_index, estado_col_index),
                        'values': [["🔴 Demorado"]]
                    })
                    # Actualizar el DataFrame localmente
                    df_to_check.loc[idx, "Estado"] = "🔴 Demorado"
                else:
                    st.warning(f"⚠️ ID_Pedido '{row['ID_Pedido']}' no tiene '_gsheet_row_index'. No se pudo actualizar el estado a 'Demorado'.")

    if updates_to_perform:
        # Realizar la actualización por lotes si hay cambios pendientes
        if batch_update_gsheet_cells(worksheet, updates_to_perform):
            st.toast(f"✅ Se actualizaron {len(updates_to_perform)} pedidos a 'Demorado'.", icon="✅")
            load_data_from_gsheets.clear() # Invalidar caché después de la actualización por lotes
            return df_to_check, True
        else:
            st.error("Falló la actualización por lotes de estados 'Demorado'.")
            return df_to_check, False

    return df_to_check, False

def get_unique_id(df):
    """Genera un ID único para un nuevo pedido."""
    # Asegúrate de que la columna 'ID_Pedido' exista y sea de tipo string para el regex
    if 'ID_Pedido' not in df.columns or df['ID_Pedido'].empty:
        return 'P0001'
    
    # Extraer los números de los ID_Pedido existentes, ignorando los errores de formato
    numeric_ids = df['ID_Pedido'].astype(str).str.extract(r'P(\d+)').dropna().astype(int)
    
    if numeric_ids.empty:
        return 'P0001'
    
    last_id_num = numeric_ids.max()
    new_id_num = int(last_id_num) + 1
    return f'P{new_id_num:04d}'


def mostrar_pedido(df, idx, row, orden, origen_tab, current_main_tab_label, worksheet, headers): # Añadir 'headers'
    """
    Muestra los detalles de un pedido y permite acciones.
    """
    gsheet_row_index = row.get('_gsheet_row_index') # Obtener el índice de fila de GSheet del DataFrame
    if gsheet_row_index is None:
        st.error(f"❌ Error interno: No se pudo obtener el índice de fila de Google Sheets para el pedido '{row['ID_Pedido']}'. No se puede actualizar este pedido.")
        return

    with st.container():
        st.markdown("---")
        tiene_modificacion = row.get("Modificacion_Surtido") and pd.notna(row["Modificacion_Surtido"]) and str(row["Modificacion_Surtido"]).strip() != ''
        if tiene_modificacion:
            st.warning(f"⚠ ¡MODIFICACIÓN DE SURTIDO DETECTADA! Pedido #{orden}")

        # --- Sección "Cambiar Fecha y Turno" ---
        # Se muestra si el estado no es Completado Y (es Pedido Local O es Pedido Foráneo)
        if row['Estado'] != "🟢 Completado" and \
           (row.get("Tipo_Envio") == "📍 Pedido Local" or row.get("Tipo_Envio") == "🚚 Pedido Foráneo"):
            st.markdown("##### 📅 Cambiar Fecha y Turno")
            col_current_info_date, col_current_info_turno, col_inputs = st.columns([1, 1, 2])

            fecha_actual_dt = pd.to_datetime(row.get("Fecha_Entrega"), errors='coerce')
            fecha_mostrar = fecha_actual_dt.strftime('%d/%m/%Y') if pd.notna(fecha_actual_dt) else "Sin fecha"
            col_current_info_date.info(f"**Fecha de envío actual:** {fecha_mostrar}")

            # Mostrar el turno actual solo si es un Pedido Local
            current_turno = row.get("Turno", "") # Obtener el turno actual para uso posterior
            if row.get("Tipo_Envio") == "📍 Pedido Local":
                col_current_info_turno.info(f"**Turno actual:** {current_turno}")
            else: # Para foráneos, esta columna no es relevante para el "turno"
                col_current_info_turno.empty() # O podrías poner un mensaje como "No aplica"


            today = datetime.now().date()
            date_input_value = today
            if pd.notna(fecha_actual_dt) and fecha_actual_dt.date() >= today:
                date_input_value = fecha_actual_dt.date()

            new_fecha_entrega_dt = col_inputs.date_input(
                "Nueva fecha de envío:",
                value=date_input_value,
                key=f"new_date_{row['ID_Pedido']}_{origen_tab}",
                disabled=(row['Estado'] == "🟢 Completado")
            )

            # Inicializar new_turno con el valor actual por defecto
            new_turno = current_turno

            # Mostrar el selector de turno solo para Pedidos Locales (Mañana/Tarde/Saltillo/Bodega)
            if row.get("Tipo_Envio") == "📍 Pedido Local":
                turno_options = ["", "☀️ Local Mañana", "🌙 Local Tarde", "🌵 Saltillo", "📦 Pasa a Bodega"]
                try:
                    default_index_turno = turno_options.index(current_turno)
                except ValueError:
                    default_index_turno = 0 # Si el turno actual no está en las opciones, seleccionar la primera

                new_turno = col_inputs.selectbox(
                    "Clasificar Turno como:",
                    options=turno_options,
                    index=default_index_turno,
                    key=f"new_turno_{row['ID_Pedido']}_{origen_tab}",
                    disabled=(row['Estado'] == "🟢 Completado")
                )
            # Para Foráneos, el new_turno ya se inicializó con el current_turno
            # y no se mostrará un selectbox para modificarlo.

            if st.button("✅ Aplicar Cambios de Fecha/Turno", key=f"apply_changes_{row['ID_Pedido']}_{origen_tab}", disabled=(row['Estado'] == "🟢 Completado")):
                changes_made = False

                new_fecha_entrega_str = new_fecha_entrega_dt.strftime('%d/%m/%Y') # Guardar como DD/MM/YYYY
                current_fecha_entrega_str = fecha_actual_dt.strftime('%d/%m/%Y') if pd.notna(fecha_actual_dt) else ""

                if new_fecha_entrega_str != current_fecha_entrega_str:
                    if update_gsheet_cell(worksheet, headers, gsheet_row_index, "Fecha_Entrega", new_fecha_entrega_str):
                        df.loc[idx, "Fecha_Entrega"] = new_fecha_entrega_dt # Actualizar el DF con datetime
                        changes_made = True
                    else:
                        st.error("Falló la actualización de la fecha de entrega.")

                # Solo intentar actualizar el turno si el selector de turno fue visible y su valor ha cambiado
                if row.get("Tipo_Envio") == "📍 Pedido Local" and new_turno != current_turno:
                    if update_gsheet_cell(worksheet, headers, gsheet_row_index, "Turno", new_turno):
                        df.loc[idx, "Turno"] = new_turno
                        changes_made = True
                    else:
                        st.error("Falló la actualización del turno.")

                if changes_made:
                    st.success(f"✅ Cambios aplicados para el pedido {row['ID_Pedido']}!")
                    st.rerun() # Rerun para reflejar los cambios en el filtro de pestañas
                else:
                    st.info("No se realizaron cambios en la fecha o turno.")

        st.markdown("---")

        # --- Layout Principal del Pedido (como en la imagen original) ---
        disabled_if_completed = (row['Estado'] == "🟢 Completado")

        col_order_num, col_client, col_time, col_status, col_surtidor, col_print_btn, col_complete_btn = st.columns([0.5, 2, 1.5, 1, 1.2, 1, 1])

        col_order_num.write(f"**{orden}**")
        col_client.write(f"**{row['Cliente']}**")

        hora_registro_dt = pd.to_datetime(row['Hora_Registro'], errors='coerce')
        if pd.notna(hora_registro_dt):
            col_time.write(f"🕒 {hora_registro_dt.strftime('%d/%m/%Y %H:%M:%S')}") # Formato más amigable
        else:
            col_time.write("")

        col_status.write(f"{row['Estado']}")

        surtidor_current = row.get("Surtidor", "")
        # Usamos on_change para manejar la actualización del surtidor
        def update_surtidor_callback(current_gsheet_row_index, current_surtidor_key):
            new_surtidor_val = st.session_state[current_surtidor_key]
            # No necesitamos comparar con surtidor_current aquí, porque ya se maneja en el input
            if update_gsheet_cell(worksheet, headers, current_gsheet_row_index, "Surtidor", new_surtidor_val):
                # La actualización del DF local no es estrictamente necesaria aquí si se hace un rerun,
                # pero ayuda a la consistencia si se usa df en otras partes antes del rerun.
                df.loc[df['_gsheet_row_index'] == current_gsheet_row_index, "Surtidor"] = new_surtidor_val
                st.toast("Surtidor actualizado", icon="✅")
            else:
                st.error("Falló la actualización del surtidor.")


        surtidor_key = f"surtidor_{row['ID_Pedido']}_{origen_tab}"
        col_surtidor.text_input(
            "Surtidor",
            value=surtidor_current,
            label_visibility="collapsed",
            placeholder="Surtidor",
            key=surtidor_key,
            disabled=disabled_if_completed,
            on_change=update_surtidor_callback,
            args=(gsheet_row_index, surtidor_key) # Pasamos el gsheet_row_index
        )


        # Imprimir/Ver Adjuntos and change to "En Proceso"
        if col_print_btn.button("🖨 Imprimir", key=f"print_button_{row['ID_Pedido']}_{origen_tab}", disabled=disabled_if_completed):
            updates_for_print_button = []
            
            # Actualizar estado a "En Proceso" si no lo está
            if row['Estado'] != "🔵 En Proceso":
                estado_col_idx = headers.index('Estado') + 1
                updates_for_print_button.append({
                    'range': gspread.utils.rowcol_to_a1(gsheet_row_index, estado_col_idx),
                    'values': [["🔵 En Proceso"]]
                })
                # Actualizar el DataFrame local para reflejar el cambio inmediatamente
                df.loc[df['_gsheet_row_index'] == gsheet_row_index, "Estado"] = "🔵 En Proceso"
                
                # Registrar Hora_Proceso
                hora_proceso_col_idx = headers.index('Hora_Proceso') + 1
                current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                updates_for_print_button.append({
                    'range': gspread.utils.rowcol_to_a1(gsheet_row_index, hora_proceso_col_idx),
                    'values': [[current_timestamp]]
                })
                df.loc[df['_gsheet_row_index'] == gsheet_row_index, "Hora_Proceso"] = current_timestamp
                
                # Asegurarse de que Fecha_Completado esté vacío si no aplica
                fecha_completado_col_idx = headers.index('Fecha_Completado') + 1
                updates_for_print_button.append({
                    'range': gspread.utils.rowcol_to_a1(gsheet_row_index, fecha_completado_col_idx),
                    'values': [[""]]
                })
                df.loc[df['_gsheet_row_index'] == gsheet_row_index, "Fecha_Completado"] = pd.NaT # Resetear en DF

            if batch_update_gsheet_cells(worksheet, updates_for_print_button):
                st.toast(f"✅ Pedido {orden} marcado como 'En Proceso' y adjuntos desplegados.", icon="✅")
                # No necesitamos load_data_from_gsheets.clear() aquí, batch_update_gsheet_cells ya lo hace
            else:
                st.error("Falló la actualización del estado a 'En Proceso' al imprimir.")

            # Alternar el estado de expansión de adjuntos
            st.session_state["expanded_attachments"][row['ID_Pedido']] = not st.session_state["expanded_attachments"].get(row['ID_Pedido'], False)
            st.rerun() # Se necesita rerun para que los cambios de estado y expansión se reflejen


        # Completar
        if col_complete_btn.button("🟢 Completar", key=f"done_{row['ID_Pedido']}_{origen_tab}", disabled=disabled_if_completed):
            surtidor_final = row.get("Surtidor", "").strip()
            if surtidor_final:
                updates_for_complete_button = []

                # Actualizar estado a "🟢 Completado"
                estado_col_idx = headers.index('Estado') + 1
                updates_for_complete_button.append({
                    'range': gspread.utils.rowcol_to_a1(gsheet_row_index, estado_col_idx),
                    'values': [["🟢 Completado"]]
                })
                df.loc[df['_gsheet_row_index'] == gsheet_row_index, "Estado"] = "🟢 Completado"

                # Registrar Fecha_Completado
                fecha_completado_col_idx = headers.index('Fecha_Completado') + 1
                current_timestamp_complete = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                updates_for_complete_button.append({
                    'range': gspread.utils.rowcol_to_a1(gsheet_row_index, fecha_completado_col_idx),
                    'values': [[current_timestamp_complete]]
                })
                df.loc[df['_gsheet_row_index'] == gsheet_row_index, "Fecha_Completado"] = current_timestamp_complete

                # Asegurarse de que Hora_Proceso esté vacío
                hora_proceso_col_idx = headers.index('Hora_Proceso') + 1
                updates_for_complete_button.append({
                    'range': gspread.utils.rowcol_to_a1(gsheet_row_index, hora_proceso_col_idx),
                    'values': [[""]]
                })
                df.loc[df['_gsheet_row_index'] == gsheet_row_index, "Hora_Proceso"] = pd.NaT # Resetear en DF

                if batch_update_gsheet_cells(worksheet, updates_for_complete_button):
                    st.toast(f"✅ Pedido {orden} marcado como completado", icon="✅")
                    if row['ID_Pedido'] in st.session_state["expanded_attachments"]:
                        del st.session_state["expanded_attachments"][row['ID_Pedido']]
                    st.rerun() # Rerun para que el pedido se mueva a la pestaña de completados
                else:
                    st.error("Falló la actualización del estado a 'Completado'.")
            else:
                st.warning("⚠ Por favor, ingrese el Surtidor antes de completar el pedido.")

        # --- Adjuntos desplegados (if expanded) ---
        if st.session_state["expanded_attachments"].get(row['ID_Pedido'], False):
            st.markdown(f"##### Adjuntos para ID: {row['ID_Pedido']}")

            # Buscar la carpeta del pedido en S3
            pedido_folder_prefix = find_pedido_subfolder_prefix(s3_client, S3_ATTACHMENT_PREFIX, row['ID_Pedido'])

            if pedido_folder_prefix:
                files_in_folder = get_files_in_s3_prefix(s3_client, pedido_folder_prefix)

                if files_in_folder:
                    filtered_files_to_display = [
                        f for f in files_in_folder
                        if "comprobante" not in f['title'].lower() and "surtido" not in f['title'].lower()
                    ]

                    if filtered_files_to_display:
                        for file_info in filtered_files_to_display:
                            file_url = get_s3_file_download_url(s3_client, file_info['key'])
                            display_name = file_info['title']
                            # Limpiar el nombre para mostrar si contiene el ID_Pedido o prefijos comunes
                            display_name = display_name.replace(row['ID_Pedido'], "").strip('_-')
                            # Remover prefijos de timestamp si existen
                            display_name = re.sub(r'_\d{14}(_\d+)?', '', display_name).strip('_-')
                            
                            if display_name: # Asegurarse de que no esté vacío después de la limpieza
                                st.markdown(f"- 📄 **{display_name}** ([🔗 Ver/Descargar]({file_url}))")
                            else: # Si el nombre queda vacío (ej. solo era el ID_Pedido), mostrar el nombre original
                                st.markdown(f"- 📄 **{file_info['title']}** ([🔗 Ver/Descargar]({file_url}))")
                    else:
                        st.info("No hay adjuntos para mostrar (excluyendo comprobantes y surtidos).")
                else:
                    st.info("No se encontraron archivos en la carpeta del pedido en S3.")
            else:
                st.error(f"❌ No se encontró la carpeta (prefijo S3) del pedido '{row['ID_Pedido']}'.")


        # --- Campo de Notas editable y Comentario ---
        st.markdown("---")
        info_text_comment = row.get("Comentario")
        if pd.notna(info_text_comment) and str(info_text_comment).strip() != '':
            st.info(f"💬 Comentario: {info_text_comment}")

        current_notas = row.get("Notas", "")
        # Usamos on_change para manejar la actualización de las notas
        def update_notas_callback(current_gsheet_row_index, current_notas_key):
            new_notas_val = st.session_state[current_notas_key]
            # No necesitamos comparar con current_notas aquí, porque ya se maneja en el input
            if update_gsheet_cell(worksheet, headers, current_gsheet_row_index, "Notas", new_notas_val):
                df.loc[df['_gsheet_row_index'] == current_gsheet_row_index, "Notas"] = new_notas_val
                st.toast("Notas actualizadas", icon="✅")
            else:
                st.error("Falló la actualización de las notas.")

        notas_key = f"notas_edit_{row['ID_Pedido']}_{origen_tab}"
        st.text_area(
            "📝 Notas (editable)",
            value=current_notas,
            key=notas_key,
            height=70,
            disabled=disabled_if_completed,
            on_change=update_notas_callback,
            args=(gsheet_row_index, notas_key)
        )

        if tiene_modificacion:
            st.warning(f"🟡 Modificación de Surtido:\n{row['Modificacion_Surtido']}")

            # Extraer nombres de archivos mencionados en Modificacion_Surtido
            mod_surtido_archivos_mencionados_raw = []
            for linea in str(row['Modificacion_Surtido']).split('\n'):
                match = re.search(r'\(Adjunto: (.+?)\)', linea)
                if match:
                    mod_surtido_archivos_mencionados_raw.extend([f.strip() for f in match.group(1).split(',')])

            all_surtido_related_files_display = []
            archivos_ya_mostrados_para_mod = set()

            # Asegurarse de que tenemos el prefijo de la carpeta del pedido
            pedido_folder_prefix = find_pedido_subfolder_prefix(s3_client, S3_ATTACHMENT_PREFIX, row['ID_Pedido'])
            
            if pedido_folder_prefix:
                # Obtener todos los archivos en la carpeta del pedido en S3
                all_files_in_folder = get_files_in_s3_prefix(s3_client, pedido_folder_prefix)

                # 1. Añadir archivos que contengan "surtido" en su título desde S3
                for s_file in all_files_in_folder:
                    if "surtido" in s_file['title'].lower() and s_file['title'] not in archivos_ya_mostrados_para_mod:
                        all_surtido_related_files_display.append(s_file)
                        archivos_ya_mostrados_para_mod.add(s_file['title'])
                
                # 2. Añadir archivos mencionados en Modificacion_Surtido (si aún no están)
                for f_name in mod_surtido_archivos_mencionados_raw:
                    if f_name not in archivos_ya_mostrados_para_mod:
                        # Necesitamos la clave completa para S3, no solo el nombre del archivo
                        object_key_from_name = f"{pedido_folder_prefix}{f_name}"
                        all_surtido_related_files_display.append({
                            'title': f_name,
                            'key': object_key_from_name # Asumimos la ruta completa
                        })
                        archivos_ya_mostrados_para_mod.add(f_name)

            if all_surtido_related_files_display:
                st.markdown("Adjuntos de Modificación (Surtido/Relacionados):")
                for file_info in all_surtido_related_files_display:
                    file_name_to_display = file_info['title']
                    object_key_to_download = file_info['key']

                    try:
                        presigned_url = get_s3_file_download_url(s3_client, object_key_to_download)
                        if presigned_url:
                            st.markdown(f"- 📄 [{file_name_to_display}]({presigned_url})")
                        else:
                            st.warning(f"⚠️ No se pudo generar el enlace para: {file_name_to_display}")
                    except Exception as e:
                        st.warning(f"⚠️ Error al procesar adjunto de modificación '{file_name_to_display}': {e}")
            else:
                st.info("No hay adjuntos específicos para esta modificación de surtido.")


# --- Main Application Logic ---

# Carga de datos inicial y aplicación de la lógica de demorados
df_main, worksheet_main, headers_main = load_data_from_gsheets(GOOGLE_SHEET_ID, GOOGLE_SHEET_WORKSHEET_NAME)

if not df_main.empty:
    df_main, changes_made_by_demorado_check = check_and_update_demorados(df_main, worksheet_main, headers_main)
    if changes_made_by_demorado_check:
        # Si se realizaron cambios por la verificación de demorados, hacer un rerun para reflejarlo en las pestañas
        st.rerun()

    # Filtros y búsqueda
    st.sidebar.header("Filtros de Búsqueda")
    search_term = st.sidebar.text_input("Buscar por ID de Pedido, Cliente o Vendedor")
    selected_status = st.sidebar.multiselect("Filtrar por Estado", df_main['Estado'].unique())
    selected_delivery_type = st.sidebar.multiselect("Filtrar por Tipo de Envío", df_main['Tipo_Envio'].unique())

    df_filtered_by_sidebar = df_main.copy() # Usar una copia para aplicar filtros de sidebar

    if search_term:
        df_filtered_by_sidebar = df_filtered_by_sidebar[
            df_filtered_by_sidebar.apply(lambda row: search_term.lower() in str(row['ID_Pedido']).lower() or \
                                                       search_term.lower() in str(row['Cliente']).lower() or \
                                                       search_term.lower() in str(row['Vendedor_Registro']).lower(), axis=1)
        ]
    if selected_status:
        df_filtered_by_sidebar = df_filtered_by_sidebar[df_filtered_by_sidebar['Estado'].isin(selected_status)]
    if selected_delivery_type:
        df_filtered_by_sidebar = df_filtered_by_sidebar[df_filtered_by_sidebar['Tipo_Envio'].isin(selected_delivery_type)]

    # División de pedidos por estado para las pestañas
    # Ahora estas divisiones usan df_filtered_by_sidebar
    df_pendientes_proceso_demorado = df_filtered_by_sidebar[
        df_filtered_by_sidebar["Estado"].isin(["🟡 Pendiente", "🔵 En Proceso", "🔴 Demorado"])
    ].copy()
    df_completados_historial = df_filtered_by_sidebar[df_filtered_by_sidebar["Estado"] == "🟢 Completado"].copy()
    
    st.markdown("### 📊 Resumen de Estados")

    estado_counts = df_filtered_by_sidebar['Estado'].astype(str).value_counts().reindex([
        '🟡 Pendiente', '🔵 En Proceso', '🔴 Demorado', '🟢 Completado', # Aseguramos el orden
        '📦 Surtido', '🟣 Completado (Recepción)', '✅ Entregado', '❌ Cancelado',
        '📍 Pedido Local', '🚚 Pedido Foráneo', '🛠 Garantía', '🔁 Devolución', '📬 Solicitud de guía'
    ], fill_value=0) # Incluir todos los estados posibles

    # Mostrar solo los estados más relevantes en el resumen
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("🟡 Pendientes", estado_counts.get('🟡 Pendiente', 0))
    col2.metric("🔵 En Proceso", estado_counts.get('🔵 En Proceso', 0))
    col3.metric("🔴 Demorados", estado_counts.get('🔴 Demorado', 0))
    col4.metric("🟢 Completados", estado_counts.get('🟢 Completado', 0))

    # --- Implementación de Pestañas con st.tabs ---
    tab_options = [
        "📍 Pedidos Locales", "🚚 Pedidos Foráneos", "🛠 Garantías",
        "🔁 Devoluciones", "📬 Solicitud de Guía", "➕ Nuevo Pedido",
        "✅ Historial Completados"
    ]

    main_tabs = st.tabs(tab_options)

    with main_tabs[0]: # 📍 Pedidos Locales
        st.markdown("### 📋 Pedidos Locales")
        subtab_options_local = ["🌅 Mañana", "🌇 Tarde", "⛰️ Saltillo", "📦 En Bodega"]
        
        subtabs_local = st.tabs(subtab_options_local)

        with subtabs_local[0]: # 🌅 Mañana
            pedidos_m_display = df_pendientes_proceso_demorado[
                (df_pendientes_proceso_demorado["Tipo_Envio"] == "📍 Pedido Local") &
                (df_pendientes_proceso_demorado["Turno"] == "☀️ Local Mañana")
            ].copy()
            if not pedidos_m_display.empty:
                pedidos_m_display['Fecha_Entrega_dt'] = pd.to_datetime(pedidos_m_display['Fecha_Entrega'], errors='coerce')
                fechas_unicas_dt = sorted(pedidos_m_display["Fecha_Entrega_dt"].dropna().unique())

                if fechas_unicas_dt:
                    date_tab_labels = [f"📅 {pd.to_datetime(fecha).strftime('%d/%m/%Y')}" for fecha in fechas_unicas_dt]
                    
                    date_tabs_m = st.tabs(date_tab_labels)
                    
                    for i, date_label in enumerate(date_tab_labels):
                        with date_tabs_m[i]:
                            current_selected_date_dt = pd.to_datetime(date_label.replace("📅 ", ""), format='%d/%m/%Y')
                            
                            pedidos_fecha = pedidos_m_display[pedidos_m_display["Fecha_Entrega_dt"] == current_selected_date_dt].copy()
                            pedidos_fecha = ordenar_pedidos_custom(pedidos_fecha)
                            st.markdown(f"#### 🌅 Pedidos Locales - Mañana - {date_label}")
                            for orden, (idx, row) in enumerate(pedidos_fecha.iterrows(), start=1):
                                mostrar_pedido(df_main, idx, row, orden, "Mañana", "📍 Pedidos Locales", worksheet_main, headers_main)
                else:
                    st.info("No hay pedidos para el turno mañana.")
            else:
                st.info("No hay pedidos para el turno mañana.")

        with subtabs_local[1]: # 🌇 Tarde
            pedidos_t_display = df_pendientes_proceso_demorado[
                (df_pendientes_proceso_demorado["Tipo_Envio"] == "📍 Pedido Local") &
                (df_pendientes_proceso_demorado["Turno"] == "🌙 Local Tarde")
            ].copy()
            if not pedidos_t_display.empty:
                pedidos_t_display['Fecha_Entrega_dt'] = pd.to_datetime(pedidos_t_display['Fecha_Entrega'], errors='coerce')
                fechas_unicas_dt = sorted(pedidos_t_display["Fecha_Entrega_dt"].dropna().unique())

                if fechas_unicas_dt:
                    date_tab_labels = [f"📅 {pd.to_datetime(fecha).strftime('%d/%m/%Y')}" for fecha in fechas_unicas_dt]
                    
                    date_tabs_t = st.tabs(date_tab_labels)
                    for i, date_label in enumerate(date_tab_labels):
                        with date_tabs_t[i]:
                            current_selected_date_dt = pd.to_datetime(date_label.replace("📅 ", ""), format='%d/%m/%Y')
                            
                            pedidos_fecha = pedidos_t_display[pedidos_t_display["Fecha_Entrega_dt"] == current_selected_date_dt].copy()
                            pedidos_fecha = ordenar_pedidos_custom(pedidos_fecha)
                            st.markdown(f"#### 🌇 Pedidos Locales - Tarde - {date_label}")
                            for orden, (idx, row) in enumerate(pedidos_fecha.iterrows(), start=1):
                                mostrar_pedido(df_main, idx, row, orden, "Tarde", "📍 Pedidos Locales", worksheet_main, headers_main)
                else:
                    st.info("No hay pedidos para el turno tarde.")
            else:
                st.info("No hay pedidos para el turno tarde.")

        with subtabs_local[2]: # ⛰️ Saltillo
            pedidos_s_display = df_pendientes_proceso_demorado[
                (df_pendientes_proceso_demorado["Tipo_Envio"] == "📍 Pedido Local") &
                (df_pendientes_proceso_demorado["Turno"] == "🌵 Saltillo")
            ].copy()
            if not pedidos_s_display.empty:
                pedidos_s_display = ordenar_pedidos_custom(pedidos_s_display)
                st.markdown("#### ⛰️ Pedidos Locales - Saltillo")
                for orden, (idx, row) in enumerate(pedidos_s_display.iterrows(), start=1):
                    mostrar_pedido(df_main, idx, row, orden, "Saltillo", "📍 Pedidos Locales", worksheet_main, headers_main)
            else:
                st.info("No hay pedidos para Saltillo.")

        with subtabs_local[3]: # 📦 En Bodega
            pedidos_b_display = df_pendientes_proceso_demorado[
                (df_pendientes_proceso_demorado["Tipo_Envio"] == "📍 Pedido Local") &
                (df_pendientes_proceso_demorado["Turno"] == "📦 Pasa a Bodega")
            ].copy()
            if not pedidos_b_display.empty:
                pedidos_b_display = ordenar_pedidos_custom(pedidos_b_display)
                st.markdown("#### 📦 Pedidos Locales - En Bodega")
                for orden, (idx, row) in enumerate(pedidos_b_display.iterrows(), start=1):
                    mostrar_pedido(df_main, idx, row, orden, "Pasa a Bodega", "📍 Pedidos Locales", worksheet_main, headers_main)
            else:
                st.info("No hay pedidos para pasar a bodega.")

    with main_tabs[1]: # 🚚 Pedidos Foráneos
        pedidos_foraneos_display = df_pendientes_proceso_demorado[
            (df_pendientes_proceso_demorado["Tipo_Envio"] == "🚚 Pedido Foráneo")
        ].copy()
        if not pedidos_foraneos_display.empty:
            pedidos_foraneos_display = ordenar_pedidos_custom(pedidos_foraneos_display)
            for orden, (idx, row) in enumerate(pedidos_foraneos_display.iterrows(), start=1):
                mostrar_pedido(df_main, idx, row, orden, "Foráneo", "🚚 Pedidos Foráneos", worksheet_main, headers_main)
        else:
            st.info("No hay pedidos foráneos.")

    with main_tabs[2]: # 🛠 Garantías
        garantias_display = df_pendientes_proceso_demorado[(df_pendientes_proceso_demorado["Tipo_Envio"] == "🛠 Garantía")].copy()
        if not garantias_display.empty:
            garantias_display = ordenar_pedidos_custom(garantias_display)
            for orden, (idx, row) in enumerate(garantias_display.iterrows(), start=1):
                mostrar_pedido(df_main, idx, row, orden, "Garantía", "🛠 Garantías", worksheet_main, headers_main)
        else:
            st.info("No hay garantías.")

    with main_tabs[3]: # 🔁 Devoluciones
        devoluciones_display = df_pendientes_proceso_demorado[(df_pendientes_proceso_demorado["Tipo_Envio"] == "🔁 Devolución")].copy()
        if not devoluciones_display.empty:
            devoluciones_display = ordenar_pedidos_custom(devoluciones_display)
            for orden, (idx, row) in enumerate(devoluciones_display.iterrows(), start=1):
                mostrar_pedido(df_main, idx, row, orden, "Devolución", "🔁 Devoluciones", worksheet_main, headers_main)
        else:
            st.info("No hay devoluciones.")

    with main_tabs[4]: # 📬 Solicitud de Guía
        solicitudes_display = df_pendientes_proceso_demorado[(df_pendientes_proceso_demorado["Tipo_Envio"] == "📬 Solicitud de guía")].copy()
        if not solicitudes_display.empty:
            solicitudes_display = ordenar_pedidos_custom(solicitudes_display)
            for orden, (idx, row) in enumerate(solicitudes_display.iterrows(), start=1):
                mostrar_pedido(df_main, idx, row, orden, "Solicitud de Guía", "📬 Solicitud de Guía", worksheet_main, headers_main)
        else:
            st.info("No hay solicitudes de guía.")

    with main_tabs[5]: # ➕ Nuevo Pedido
        st.markdown("### Registrar Nuevo Pedido")

        # Obtener el siguiente ID_Pedido único
        next_id_pedido = get_unique_id(df_main)
        st.info(f"El próximo ID de Pedido será: `{next_id_pedido}`")

        with st.form("new_order_form"):
            col_form1, col_form2 = st.columns(2)

            with col_form1:
                folio_factura = st.text_input("Folio Factura", key="new_folio_factura")
                cliente = st.text_input("Cliente", key="new_cliente")
                vendedor_registro = st.text_input("Vendedor Registro", key="new_vendedor_registro")
                tipo_envio = st.selectbox("Tipo de Envío", ["📍 Pedido Local", "🚚 Pedido Foráneo", "🛠 Garantía", "🔁 Devolución", "📬 Solicitud de guía"], key="new_tipo_envio")
                turno = st.selectbox("Turno", ["", "☀️ Local Mañana", "🌙 Local Tarde", "🌵 Saltillo", "📦 Pasa a Bodega"], key="new_turno")
                
            with col_form2:
                fecha_entrega = st.date_input("Fecha de Entrega", value=datetime.now().date() + timedelta(days=2), key="new_fecha_entrega")
                estado_inicial = st.selectbox("Estado Inicial", ["🟡 Pendiente", "🔵 En Proceso", "🔴 Demorado"], key="new_estado_inicial")
                notas = st.text_area("Notas", key="new_notas")
                comentario = st.text_area("Comentario", key="new_comentario")


            # Uploader para adjuntos iniciales
            initial_uploaded_files = st.file_uploader("Subir adjuntos iniciales (opcional)", accept_multiple_files=True, key="new_adjuntos")
            
            submitted = st.form_submit_button("Registrar Pedido")

            if submitted:
                # Validación simple
                if not cliente or not vendedor_registro:
                    st.error("Por favor, completa los campos obligatorios: Cliente y Vendedor Registro.")
                else:
                    # Preparar los adjuntos para la nueva fila
                    uploaded_urls = []
                    if initial_uploaded_files:
                        # Sanitizar el nombre del cliente para usarlo en el nombre del archivo
                        sanitized_client_name = re.sub(r'[^a-zA-Z0-9_.-]', '_', cliente)
                        for i, file in enumerate(initial_uploaded_files):
                            file_extension = os.path.splitext(file.name)[1]
                            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
                            # El nombre del archivo en S3 incluirá el ID del pedido y el nombre del cliente para mejor organización
                            new_file_name_in_s3 = f"{sanitized_client_name}_{timestamp}_{i}{file_extension}"
                            file_url = upload_file_to_s3(file, new_file_name_in_s3, next_id_pedido, cliente)
                            if file_url:
                                uploaded_urls.append(file_url)

                    new_row_data = {
                        'ID_Pedido': next_id_pedido,
                        'Folio_Factura': folio_factura,
                        'Cliente': cliente,
                        'Estado': estado_inicial,
                        'Vendedor_Registro': vendedor_registro,
                        'Tipo_Envio': tipo_envio,
                        'Fecha_Registro': datetime.now().strftime("%d/%m/%Y"), # Guardar fecha como string DD/MM/YYYY
                        'Hora_Registro': datetime.now().strftime("%Y-%m-%d %H:%M:%S"), # Guardar hora como string
                        'Fecha_Entrega': fecha_entrega.strftime("%d/%m/%Y"), # Guardar fecha como string DD/MM/YYYY
                        'Comentario': comentario,
                        'Notas': notas,
                        'Modificacion_Surtido': '', # Campo vacío por defecto
                        'Adjuntos': json.dumps(uploaded_urls), # Guardar la lista de URLs como string JSON
                        'Adjuntos_Surtido': '', # Campo vacío por defecto (vacío si no hay surtido)
                        'Estado_Pago': '', # Campo vacío por defecto
                        'Fecha_Completado': '', # Campo vacío por defecto
                        'Hora_Proceso': '', # Campo vacío por defecto
                        'Turno': turno,
                        'Surtidor': '' # Campo vacío por defecto
                    }

                    # Asegurarse de que el nuevo DataFrame tenga todas las columnas existentes en headers_main
                    row_to_append = [new_row_data.get(header, '') for header in headers_main]
                    
                    # Añadir la nueva fila a Google Sheets
                    try:
                        worksheet_main.append_row(row_to_append)
                        st.success(f"📦 Pedido {next_id_pedido} registrado exitosamente en Google Sheets.")
                        load_data_from_gsheets.clear() # Invalidar caché para recargar datos frescos
                        st.rerun() # Rerun para mostrar el nuevo pedido
                    except Exception as e:
                        st.error(f"❌ Error al registrar el pedido en Google Sheets: {e}")

    with main_tabs[6]: # ✅ Historial Completados
        st.markdown("### Historial de Pedidos Completados")
        if not df_completados_historial.empty:
            df_completados_historial['Fecha_Completado_dt'] = pd.to_datetime(df_completados_historial['Fecha_Completado'], errors='coerce')
            df_completados_historial_sorted = df_completados_historial.sort_values(by='Fecha_Completado_dt', ascending=False)
            st.dataframe(
                df_completados_historial_sorted[[
                    'ID_Pedido', 'Folio_Factura', 'Cliente', 'Estado', 'Vendedor_Registro',
                    'Tipo_Envio', 'Fecha_Entrega', 'Fecha_Completado', 'Notas', 'Modificacion_Surtido',
                    'Turno' # No mostrar adjuntos directamente en el dataframe para evitar URLs largas
                ]].head(50),
                use_container_width=True, hide_index=True
            )
            st.info("Mostrando los 50 pedidos completados más recientes. Puedes ajustar este límite si es necesario.")
        else:
            st.info("No hay pedidos completados en el historial.")

else:
    st.info("No se encontraron datos de pedidos en la hoja de Google Sheets. Asegúrate de que los datos se están subiendo correctamente desde la aplicación de Vendedores o que el ID de la hoja y el nombre de la pestaña son correctos.")
