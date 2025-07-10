import streamlit as st
import os
from datetime import datetime, timedelta
import json
import uuid
import pandas as pd
from io import BytesIO
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pytz # Import pytz for timezone handling

# NEW: Import boto3 for AWS S3
import boto3

# --- STREAMLIT CONFIGURATION ---
st.set_page_config(page_title="App Administrador TD", layout="wide")

# --- GOOGLE SHEETS CONFIGURATION ---
GOOGLE_SHEET_ID = '1aWkSelodaz0nWfQx7FZAysGnIYGQFJxAN7RO3YgCiZY' # Replace with your actual Google Sheet ID

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

# --- AWS S3 CONFIGURATION (NEW) ---
# Load AWS credentials from Streamlit secrets
try:
    AWS_ACCESS_KEY_ID = st.secrets["aws_access_key_id"]
    AWS_SECRET_ACCESS_KEY = st.secrets["aws_secret_access_key"]
    AWS_REGION = st.secrets["aws_region"]
    S3_BUCKET_NAME = st.secrets["s3_bucket_name"]
except KeyError as e:
    st.error(f"❌ Error: AWS S3 credentials not found in Streamlit secrets. Make sure your .streamlit/secrets.toml file is correctly configured. Missing key: {e}")
    st.stop()

st.title("👨‍💼 App de Administrador TD")
st.write("Gestiona y actualiza el estado de los pedidos.")

@st.cache_resource
def get_s3_client():
    """Initializes and returns an S3 client."""
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
        st.stop()

def upload_file_to_s3(s3_client, bucket_name, file_obj, s3_key):
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
        file_obj.seek(0)
        s3_client.upload_fileobj(file_obj, bucket_name, s3_key)
        file_url = f"https://{bucket_name}.s3.{AWS_REGION}.amazonaws.com/{s3_key}"
        return True, file_url
    except Exception as e:
        st.error(f"❌ Error al subir el archivo '{s3_key}' a S3: {e}")
        return False, None

# --- Initialize Gspread Client and S3 Client ---
g_spread_client = get_google_sheets_client()
s3_client = get_s3_client() # Initialize S3 client

# --- List of Surtidores (reusable and explicitly alphabetically sorted) ---
SURTIDORES_LIST = sorted([
    "BODEGA 1",
    "BODEGA 2",
    "CEDIS",
    "PAQUETERÍA",
    "S/N", # Sin surtidor
])

# --- List of Vendors (reusable and explicitly alphabetically sorted) ---
VENDEDORES_LIST = sorted([
    "ANA KAREN ORTEGA MAHUAD",
    "DANIELA LOPEZ RAMIREZ",
    "EDGAR ORLANDO GOMEZ VILLAGRAN",
    "GLORIA MICHELLE GARCIA TORRES",
    "GRISELDA CAROLINA SANCHEZ GARCIA",
    "HECTOR DEL ANGEL AREVALO ALCALA",
    "JOSELIN TRUJILLO PATRACA",
    "NORA ALEJANDRA MARTINEZ MORENO",
    "PAULINA TREJO"
])


# --- Tab Definition ---
tab1, tab2 = st.tabs(["📊 Monitoreo y Actualización de Pedidos", "📈 Historial de Pedidos"])

# --- TAB 1: ORDER MONITORING AND UPDATE ---
with tab1:
    st.header("📋 Monitoreo y Actualización de Pedidos")
    message_placeholder_tab1 = st.empty()

    df_pedidos = pd.DataFrame()
    try:
        spreadsheet = g_spread_client.open_by_key(GOOGLE_SHEET_ID)
        worksheet = spreadsheet.worksheet('datos_pedidos')
        headers = worksheet.row_values(1)
        if headers:
            df_pedidos = pd.DataFrame(worksheet.get_all_records())

            # AÑADIDO: Filtrar filas donde 'Folio_Factura' y 'ID_Pedido' son ambos vacíos
            df_pedidos = df_pedidos.dropna(subset=['Folio_Factura', 'ID_Pedido'], how='all')

            if 'Folio_Factura' in df_pedidos.columns:
                df_pedidos['Folio_Factura'] = df_pedidos['Folio_Factura'].astype(str).replace('nan', '')
            if 'Vendedor_Registro' in df_pedidos.columns:
                df_pedidos['Vendedor_Registro'] = df_pedidos['Vendedor_Registro'].apply(
                    lambda x: x if x in VENDEDORES_LIST else 'Otro/Desconocido' if pd.notna(x) and str(x).strip() != '' else 'N/A'
                ).astype(str)
            if 'Surtidor' in df_pedidos.columns:
                df_pedidos['Surtidor'] = df_pedidos['Surtidor'].apply(
                    lambda x: x if x in SURTIDORES_LIST else 'Otro/Desconocido' if pd.notna(x) and str(x).strip() != '' else 'N/A'
                ).astype(str)
            if 'Fecha_Entrega' in df_pedidos.columns:
                df_pedidos['Fecha_Entrega'] = pd.to_datetime(df_pedidos['Fecha_Entrega'], errors='coerce')

        else:
            message_placeholder_tab1.warning("No se pudieron cargar los encabezados del Google Sheet. Asegúrate de que la primera fila no esté vacía.")

    except Exception as e:
        message_placeholder_tab1.error(f"❌ Error al cargar pedidos: {e}")
        message_placeholder_tab1.info("Asegúrate de que la primera fila de tu Google Sheet contiene los encabezados esperados.")

    selected_order_id = None
    selected_row_data = None
    current_estado_value = "🟡 Pendiente"
    current_surtidor_value = ""
    current_modificacion_surtido_value = ""
    current_notas_value = ""
    current_adjuntos_list = []
    current_adjuntos_surtido_list = []

    if df_pedidos.empty:
        message_placeholder_tab1.info("No hay pedidos registrados para mostrar. ¡Todo en orden!")
    else:
        df_pedidos['Filtro_Envio_Combinado'] = df_pedidos.apply(
            lambda row: row['Turno'] if row['Tipo_Envio'] == "📍 Pedido Local" and pd.notna(row['Turno']) and row['Turno'] else row['Tipo_Envio'],
            axis=1
        )

        # Filtros
        col1, col2, col3 = st.columns(3)
        with col1:
            unique_estados_filter = ["Todos"] + sorted(df_pedidos['Estado'].unique().tolist())
            estado_filter = st.selectbox(
                "Filtrar por Estado:",
                options=unique_estados_filter,
                key="estado_filter_admin"
            )
        with col2:
            unique_vendedores_filter = ["Todos"] + sorted(df_pedidos['Vendedor_Registro'].unique().tolist())
            vendedor_filter = st.selectbox(
                "Filtrar por Vendedor:",
                options=unique_vendedores_filter,
                key="vendedor_filter_admin"
            )
        with col3:
            unique_tipos_envio_filter = ["Todos"] + df_pedidos['Filtro_Envio_Combinado'].unique().tolist()
            tipo_envio_filter = st.selectbox(
                "Filtrar por Tipo de Envío:",
                options=unique_tipos_envio_filter,
                key="tipo_envio_filter_admin"
            )

        filtered_orders = df_pedidos.copy()
        if estado_filter != "Todos":
            filtered_orders = filtered_orders[filtered_orders['Estado'] == estado_filter]
        if vendedor_filter != "Todos":
            filtered_orders = filtered_orders[filtered_orders['Vendedor_Registro'] == vendedor_filter]
        if tipo_envio_filter != "Todos":
            filtered_orders = filtered_orders[filtered_orders['Filtro_Envio_Combinado'] == tipo_envio_filter]


        if filtered_orders.empty:
            st.warning("No hay pedidos que coincidan con los filtros seleccionados.")
        else:
            # Sort for display
            # Asegurar que las columnas existan antes de usarlas en el sort_values
            sort_columns = []
            if 'Fecha_Entrega' in filtered_orders.columns:
                sort_columns.append('Fecha_Entrega')
            if 'Folio_Factura' in filtered_orders.columns:
                sort_columns.append('Folio_Factura')
            if 'ID_Pedido' in filtered_orders.columns:
                sort_columns.append('ID_Pedido')

            if sort_columns:
                filtered_orders = filtered_orders.sort_values(
                    by=sort_columns,
                    ascending=[True, True, True], # Ordenar por fecha_entrega ascendente, luego folio, luego ID
                    na_position='last'
                )

            # Preparar la etiqueta de visualización
            filtered_orders['display_label'] = filtered_orders.apply(lambda row:
                f"📄 {row.get('Folio_Factura', '') if row.get('Folio_Factura', '') != '' else row.get('ID_Pedido', 'N/A')} - "
                f"Cliente: {row.get('Cliente', 'N/A')} - Estado: {row.get('Estado', 'N/A')} - Envío: {row.get('Tipo_Envio', 'N/A')}", axis=1
            )

            selected_order_display = st.selectbox(
                "📝 Seleccionar Pedido para Monitorear/Actualizar",
                filtered_orders['display_label'].tolist(),
                key="select_order_to_monitor"
            )

            if selected_order_display:
                selected_order_id = filtered_orders[filtered_orders['display_label'] == selected_order_display]['ID_Pedido'].iloc[0]
                selected_row_data = filtered_orders[filtered_orders['ID_Pedido'] == selected_order_id].iloc[0]

                st.subheader(f"Detalles del Pedido: Folio `{selected_row_data.get('Folio_Factura', 'N/A')}` (ID `{selected_order_id}`)")
                st.write(f"**Vendedor:** {selected_row_data.get('Vendedor', selected_row_data.get('Vendedor_Registro', 'No especificado'))}")
                st.write(f"**Cliente:** {selected_row_data.get('Cliente', 'N/A')}")
                st.write(f"**Folio de Factura:** {selected_row_data.get('Folio_Factura', 'N/A')}")
                st.write(f"**Tipo de Envío:** {selected_row_data.get('Tipo_Envio', 'N/A')}")
                if selected_row_data.get('Tipo_Envio') == "📍 Pedido Local":
                    st.write(f"**Turno Local:** {selected_row_data.get('Turno', 'N/A')}")
                st.write(f"**Fecha de Entrega Requerida:** {selected_row_data.get('Fecha_Entrega', 'N/A').strftime('%Y-%m-%d') if pd.notna(selected_row_data.get('Fecha_Entrega')) else 'N/A'}")
                st.write(f"**Comentario Original:** {selected_row_data.get('Comentario', 'N/A')}")
                st.write(f"**Estado de Pago:** {selected_row_data.get('Estado_Pago', '🔴 No Pagado')}")

                current_estado_value = selected_row_data.get('Estado', '🟡 Pendiente')
                current_surtidor_value = selected_row_data.get('Surtidor', '')
                current_modificacion_surtido_value = selected_row_data.get('Modificacion_Surtido', '')
                current_notas_value = selected_row_data.get('Notas', '')

                current_adjuntos_str = selected_row_data.get('Adjuntos', '')
                current_adjuntos_list = [f.strip() for f in current_adjuntos_str.split(',') if f.strip()]

                current_adjuntos_surtido_str = selected_row_data.get('Adjuntos_Surtido', '')
                current_adjuntos_surtido_list = [f.strip() for f in current_adjuntos_surtido_str.split(',') if f.strip()]

                if current_adjuntos_list:
                    st.write("**Adjuntos Originales:**")
                    for adj in current_adjuntos_list:
                        st.markdown(f"- [{os.path.basename(adj)}]({adj})")
                else:
                    st.write("**Adjuntos Originales:** Ninguno")

                if current_adjuntos_surtido_list:
                    st.write("**Adjuntos de Modificación/Surtido:**")
                    for adj_surtido in current_adjuntos_surtido_list:
                        st.markdown(f"- [{os.path.basename(adj_surtido)}]({adj_surtido})")
                else:
                    st.write("**Adjuntos de Modificación/Surtido:** Ninguno")


                st.markdown("---")
                st.subheader("Actualizar Estado y Asignación")

                with st.form(key="update_pedido_form_inner", clear_on_submit=False):
                    col_estado, col_surtidor = st.columns(2)
                    with col_estado:
                        new_estado = st.selectbox(
                            "Estado del Pedido",
                            ["🟡 Pendiente", "🟢 Completado", "🔵 En Proceso", "🟠 Retenido", "⚫ Cancelado"],
                            index=["🟡 Pendiente", "🟢 Completado", "🔵 En Proceso", "🟠 Retenido", "⚫ Cancelado"].index(current_estado_value) if current_estado_value in ["🟡 Pendiente", "🟢 Completado", "🔵 En Proceso", "🟠 Retenido", "⚫ Cancelado"] else 0,
                            key="new_estado_selector"
                        )
                    with col_surtidor:
                        try:
                            initial_surtidor_index = SURTIDORES_LIST.index(current_surtidor_value)
                        except ValueError:
                            initial_surtidor_index = 0 # Default to the first if not found

                        new_surtidor = st.selectbox(
                            "Asignar Surtidor",
                            options=SURTIDORES_LIST,
                            index=initial_surtidor_index,
                            key="new_surtidor_selector"
                        )

                    new_modificacion_surtido_input = st.text_area(
                        "✍️ Notas de Modificación/Surtido (del vendedor)",
                        value=current_modificacion_surtido_value,
                        height=100,
                        key="new_modificacion_surtido_input_admin"
                    )
                    new_notas_input = st.text_area(
                        "✍️ Notas de Almacén",
                        value=current_notas_value,
                        height=100,
                        key="new_notas_input_admin"
                    )

                    uploaded_files_surtido_admin = st.file_uploader(
                        "📎 Subir Archivos para Surtido (desde administrador)",
                        type=["pdf", "jpg", "jpeg", "png", "xlsx", "docx"],
                        accept_multiple_files=True,
                        key="uploaded_files_surtido_admin"
                    )

                    update_button = st.form_submit_button("💾 Guardar Actualización")

                    if update_button:
                        message_placeholder_tab1.empty()
                        try:
                            headers = worksheet.row_values(1)

                            gsheet_row_index = df_pedidos[df_pedidos['ID_Pedido'] == selected_order_id].index[0] + 2

                            estado_col_idx = headers.index('Estado') + 1
                            surtidor_col_idx = headers.index('Surtidor') + 1
                            modificacion_surtido_col_idx = headers.index('Modificacion_Surtido') + 1
                            notas_col_idx = headers.index('Notas') + 1
                            fecha_completado_col_idx = headers.index('Fecha_Completado') + 1
                            hora_proceso_col_idx = headers.index('Hora_Proceso') + 1
                            fecha_completado_dt_col_idx = headers.index('Fecha_Completado_dt') + 1
                            adjuntos_surtido_col_idx = headers.index('Adjuntos_Surtido') + 1

                            changes_made = False

                            if new_estado != current_estado_value:
                                worksheet.update_cell(gsheet_row_index, estado_col_idx, new_estado)
                                changes_made = True
                                if new_estado == "🟢 Completado":
                                    mexico_city_tz = pytz.timezone('America/Mexico_City')
                                    now_mx = datetime.now(mexico_city_tz)
                                    fecha_completado_str = now_mx.strftime('%Y-%m-%d')
                                    hora_proceso_str = now_mx.strftime('%H:%M:%S')
                                    worksheet.update_cell(gsheet_row_index, fecha_completado_col_idx, fecha_completado_str)
                                    worksheet.update_cell(gsheet_row_index, hora_proceso_col_idx, hora_proceso_str)
                                    # Update Fecha_Completado_dt with full datetime for accurate date comparisons later
                                    worksheet.update_cell(gsheet_row_index, fecha_completado_dt_col_idx, now_mx.isoformat())
                                else:
                                    worksheet.update_cell(gsheet_row_index, fecha_completado_col_idx, "")
                                    worksheet.update_cell(gsheet_row_index, hora_proceso_col_idx, "")
                                    worksheet.update_cell(gsheet_row_index, fecha_completado_dt_col_idx, "")


                            if new_surtidor != current_surtidor_value:
                                worksheet.update_cell(gsheet_row_index, surtidor_col_idx, new_surtidor)
                                changes_made = True

                            if new_modificacion_surtido_input != current_modificacion_surtido_value:
                                worksheet.update_cell(gsheet_row_index, modificacion_surtido_col_idx, new_modificacion_surtido_input)
                                changes_made = True

                            if new_notas_input != current_notas_value:
                                worksheet.update_cell(gsheet_row_index, notas_col_idx, new_notas_input)
                                changes_made = True

                            # Handle S3 upload for 'Adjuntos_Surtido'
                            new_adjuntos_surtido_urls = []
                            if uploaded_files_surtido_admin:
                                for uploaded_file in uploaded_files_surtido_admin:
                                    file_extension = os.path.splitext(uploaded_file.name)[1]
                                    s3_key = f"{selected_order_id}/admin_surtido_{uploaded_file.name.replace(' ', '_').replace(file_extension, '')}_{uuid.uuid4().hex[:4]}{file_extension}"

                                    success, file_url = upload_file_to_s3(s3_client, S3_BUCKET_NAME, uploaded_file, s3_key)
                                    if success:
                                        new_adjuntos_surtido_urls.append(file_url)
                                        changes_made = True
                                    else:
                                        message_placeholder_tab1.warning(f"⚠️ Falló la subida de '{uploaded_file.name}' para surtido. Continuará con otros cambios.")

                            if new_adjuntos_surtido_urls:
                                updated_adjuntos_surtido_list = current_adjuntos_surtido_list + new_adjuntos_surtido_urls
                                updated_adjuntos_surtido_str = ", ".join(updated_adjuntos_surtido_list)
                                worksheet.update_cell(gsheet_row_index, adjuntos_surtido_col_idx, updated_adjuntos_surtido_str)
                                changes_made = True
                                message_placeholder_tab1.info(f"📎 Nuevos archivos para Surtido subidos a S3: {', '.join([os.path.basename(url) for url in new_adjuntos_surtido_urls])}")


                            if changes_made:
                                message_placeholder_tab1.success(f"✅ Pedido `{selected_order_id}` actualizado con éxito.")
                                st.session_state.show_success_message_admin = True
                                st.session_state.last_updated_order_id_admin = selected_order_id
                            else:
                                message_placeholder_tab1.info("ℹ️ No se detectaron cambios para guardar.")
                                st.session_state.show_success_message_admin = False

                            st.rerun()

                        except Exception as e:
                            message_placeholder_tab1.error(f"❌ Error al guardar los cambios en el Google Sheet: {e}")
                            message_placeholder_tab1.info("ℹ️ Verifica que la cuenta de servicio tenga permisos de escritura en la hoja y que las columnas sean correctas.")

    if 'show_success_message_admin' in st.session_state and st.session_state.show_success_message_admin:
        message_placeholder_tab1.success(f"✅ Pedido `{st.session_state.last_updated_order_id_admin}` actualizado con éxito.")
        del st.session_state.show_success_message_admin
        del st.session_state.last_updated_order_id_admin


# --- TAB 2: ORDER HISTORY ---
with tab2:
    st.header("📈 Historial de Pedidos")

    df_historial = pd.DataFrame()
    try:
        spreadsheet = g_spread_client.open_by_key(GOOGLE_SHEET_ID)
        worksheet = spreadsheet.worksheet('datos_pedidos')
        headers = worksheet.row_values(1)
        if headers:
            df_historial = pd.DataFrame(worksheet.get_all_records())

            # AÑADIDO: Filtrar filas donde 'Folio_Factura' y 'ID_Pedido' son ambos vacíos
            df_historial = df_historial.dropna(subset=['Folio_Factura', 'ID_Pedido'], how='all')

            if 'Fecha_Entrega' in df_historial.columns:
                df_historial['Fecha_Entrega'] = pd.to_datetime(df_historial['Fecha_Entrega'], errors='coerce')
            if 'Fecha_Completado' in df_historial.columns:
                df_historial['Fecha_Completado'] = pd.to_datetime(df_historial['Fecha_Completado'], errors='coerce')
            if 'Hora_Registro' in df_historial.columns:
                # Convert 'Hora_Registro' to datetime, assuming it includes date
                df_historial['Hora_Registro_dt'] = pd.to_datetime(df_historial['Hora_Registro'], errors='coerce')
            if 'Fecha_Completado_dt' in df_historial.columns:
                df_historial['Fecha_Completado_dt'] = pd.to_datetime(df_historial['Fecha_Completado_dt'], errors='coerce')

            if 'Vendedor_Registro' in df_historial.columns:
                df_historial['Vendedor_Registro'] = df_historial['Vendedor_Registro'].apply(
                    lambda x: x if x in VENDEDORES_LIST else 'Otro/Desconocido' if pd.notna(x) and str(x).strip() != '' else 'N/A'
                ).astype(str)
            if 'Surtidor' in df_historial.columns:
                df_historial['Surtidor'] = df_historial['Surtidor'].apply(
                    lambda x: x if x in SURTIDORES_LIST else 'Otro/Desconocido' if pd.notna(x) and str(x).strip() != '' else 'N/A'
                ).astype(str)
            if 'Folio_Factura' in df_historial.columns:
                df_historial['Folio_Factura'] = df_historial['Folio_Factura'].astype(str).replace('nan', '')

        else:
            st.warning("No se pudieron cargar los encabezados del Google Sheet para el historial. Asegúrate de que la primera fila no esté vacía.")

    except Exception as e:
        st.error(f"❌ Error al cargar el historial de pedidos: {e}")
        st.info("Asegúrate de que la primera fila de tu Google Sheet contiene los encabezados esperados y que la API de Google Sheets está habilitada.")

    if df_historial.empty:
        st.info("No hay datos de pedidos en el historial para mostrar.")
    else:
        st.markdown("---")
        st.subheader("Filtros para el Historial")

        col1_h, col2_h, col3_h, col4_h = st.columns(4)

        with col1_h:
            time_filter_hist = st.radio(
                "Rango de Tiempo:",
                ("Últimos 7 días", "Últimos 30 días", "Todos los datos"),
                key="hist_time_filter"
            )

        filtered_hist_df = df_historial.copy()
        current_time_hist = datetime.now()
        mexico_city_tz = pytz.timezone('America/Mexico_City')
        current_time_mx_hist = datetime.now(mexico_city_tz)

        # Apply time filter based on 'Fecha_Completado_dt' for accuracy
        if 'Fecha_Completado_dt' in filtered_hist_df.columns:
            if time_filter_hist == "Últimos 7 días":
                start_date_hist = current_time_mx_hist - timedelta(days=7)
                filtered_hist_df = filtered_hist_df[filtered_hist_df['Fecha_Completado_dt'] >= start_date_hist]
            elif time_filter_hist == "Últimos 30 días":
                start_date_hist = current_time_mx_hist - timedelta(days=30)
                filtered_hist_df = filtered_hist_df[filtered_hist_df['Fecha_Completado_dt'] >= start_date_hist]
        else:
            st.warning("La columna 'Fecha_Completado_dt' no se encontró para aplicar el filtro de tiempo. Asegúrate de que existe y se completa al 'Completar' un pedido.")


        with col2_h:
            if 'Vendedor_Registro' in filtered_hist_df.columns:
                unique_vendedores_hist = ["Todos"] + sorted(filtered_hist_df['Vendedor_Registro'].unique().tolist())
                selected_vendedor_hist = st.selectbox(
                    "Filtrar por Vendedor:",
                    options=unique_vendedores_hist,
                    key="hist_vendedor_filter"
                )
                if selected_vendedor_hist != "Todos":
                    filtered_hist_df = filtered_hist_df[filtered_hist_df['Vendedor_Registro'] == selected_vendedor_hist]
            else:
                st.warning("La columna 'Vendedor_Registro' no se encontró para aplicar el filtro de vendedor.")

        with col3_h:
            if 'Tipo_Envio' in filtered_hist_df.columns:
                unique_tipos_envio_hist = ["Todos", "📍 Pedido Local", "🚚 Pedido Foráneo", "🛠 Garantía", "🔁 Devolución", "📬 Solicitud de guía"]
                selected_tipo_envio_hist = st.selectbox(
                    "Filtrar por Tipo de Envío:",
                    options=unique_tipos_envio_hist,
                    key="hist_tipo_envio_filter"
                )
                if selected_tipo_envio_hist != "Todos":
                    filtered_hist_df = filtered_hist_df[filtered_hist_df['Tipo_Envio'] == selected_tipo_envio_hist]
            else:
                st.warning("La columna 'Tipo_Envio' no se encontró para aplicar el filtro de tipo de envío.")

        with col4_h:
            if 'Estado' in filtered_hist_df.columns:
                unique_estados_hist = ["Todos"] + sorted(filtered_hist_df['Estado'].dropna().unique().tolist())
                selected_estado_hist = st.selectbox("Filtrar por Estado:", unique_estados_hist, key="hist_estado_filter")
                if selected_estado_hist != "Todos":
                    filtered_hist_df = filtered_hist_df[filtered_hist_df['Estado'] == selected_estado_hist]


        st.markdown("---")
        st.subheader("Vista Previa del Historial")

        if not filtered_hist_df.empty:
            # Seleccionar y reordenar columnas para la visualización del historial
            display_columns_hist = [
                'ID_Pedido', 'Folio_Factura', 'Cliente', 'Estado', 'Vendedor_Registro',
                'Tipo_Envio', 'Turno', 'Fecha_Entrega', 'Estado_Pago', 'Surtidor',
                'Modificacion_Surtido', 'Notas', 'Adjuntos', 'Adjuntos_Surtido',
                'Hora_Registro', 'Fecha_Completado', 'Hora_Proceso'
            ]
            # Solo mantener las columnas que realmente existen en el DataFrame
            existing_display_columns_hist = [col for col in display_columns_hist if col in filtered_hist_df.columns]

            # Formatear columnas de fecha para visualización
            display_hist_df = filtered_hist_df[existing_display_columns_hist].copy()
            if 'Fecha_Entrega' in display_hist_df.columns:
                display_hist_df['Fecha_Entrega'] = display_hist_df['Fecha_Entrega'].dt.strftime('%Y-%m-%d').fillna('')
            if 'Fecha_Completado' in display_hist_df.columns:
                display_hist_df['Fecha_Completado'] = display_hist_df['Fecha_Completado'].dt.strftime('%Y-%m-%d').fillna('')
            if 'Hora_Registro' in display_hist_df.columns:
                # Asegurarse de que Hora_Registro sea un string válido o vacío
                display_hist_df['Hora_Registro'] = display_hist_df['Hora_Registro'].astype(str).replace('NaT', '').fillna('')


            st.dataframe(display_hist_df, use_container_width=True, hide_index=True)

            output_hist = BytesIO()
            with pd.ExcelWriter(output_hist, engine='xlsxwriter') as writer:
                # Formatear fechas para Excel
                excel_hist_df = filtered_hist_df.copy()
                if 'Fecha_Entrega' in excel_hist_df.columns:
                    excel_hist_df['Fecha_Entrega'] = excel_hist_df['Fecha_Entrega'].dt.strftime('%Y-%m-%d').fillna('')
                if 'Fecha_Completado' in excel_hist_df.columns:
                    excel_hist_df['Fecha_Completado'] = excel_hist_df['Fecha_Completado'].dt.strftime('%Y-%m-%d').fillna('')
                if 'Hora_Registro_dt' in excel_hist_df.columns: # Usar la columna datetime para el formato
                    excel_hist_df['Hora_Registro'] = excel_hist_df['Hora_Registro_dt'].dt.strftime('%Y-%m-%d %H:%M:%S').fillna('')
                # Eliminar columnas temporales o de ayuda antes de exportar
                excel_hist_df = excel_hist_df.drop(columns=['Hora_Registro_dt', 'Fecha_Completado_dt', 'Filtro_Envio_Combinado'], errors='ignore')

                excel_hist_df.to_excel(writer, index=False, sheet_name='Historial_Pedidos')
            processed_data_hist = output_hist.getvalue()

            st.download_button(
                label="📥 Descargar Historial Excel",
                data=processed_data_hist,
                file_name=f"historial_pedidos_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                help="Haz clic para descargar los datos del historial mostrados arriba en formato Excel."
            )
        else:
            st.info("No hay datos en el historial que coincidan con los filtros seleccionados para descargar.")
