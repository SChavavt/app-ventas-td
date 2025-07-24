# --- NUEVA PESTAÑA: DESCARGA DE COMPROBANTES CONFIRMADOS ---
st.markdown("---")
mostrar_descarga_confirmados = st.toggle("🔽 Mostrar/Descargar Pedidos Confirmados", value=False)

if mostrar_descarga_confirmados:
    st.markdown("### 📥 Pedidos Confirmados - Comprobantes de Pago")
    with st.spinner("Cargando comprobantes confirmados..."):

        if (
            'Estado_Pago' in df_pedidos.columns and
            'Comprobante_Confirmado' in df_pedidos.columns and
            not df_pedidos.empty
        ):
            df_confirmados_actual = df_pedidos[
                (df_pedidos['Estado_Pago'] == '✅ Pagado') &
                (df_pedidos['Comprobante_Confirmado'] == 'Sí')
            ].copy()

            # Convertir fechas
            for col in ['Fecha_Entrega', 'Fecha_Pago_Comprobante']:
                if col in df_confirmados_actual.columns:
                    df_confirmados_actual[col] = pd.to_datetime(df_confirmados_actual[col], errors='coerce')

            # Ordenar por Fecha_Pago
            df_confirmados_actual = df_confirmados_actual.sort_values(by='Fecha_Pago_Comprobante', ascending=False)

            # Cargar enlaces previos si existen
            df_cache = st.session_state.get("df_confirmados_cache", pd.DataFrame())
            ids_previos = set(st.session_state.get("confirmados_anteriores_ids", []))

            nuevos_rows = []
            nuevos_ids = []

            for _, row in df_confirmados_actual.iterrows():
                pedido_id = row.get("ID_Pedido")
                if pedido_id in ids_previos:
                    continue  # Ya procesado antes

                comprobante_url = ""
                factura_url = ""

                if pedido_id:
                    prefix = f"{S3_ATTACHMENT_PREFIX}{pedido_id}/"
                    files = get_files_in_s3_prefix(s3_client, prefix)

                    if not files:
                        prefix = find_pedido_subfolder_prefix(s3_client, S3_ATTACHMENT_PREFIX, pedido_id)
                        files = get_files_in_s3_prefix(s3_client, prefix) if prefix else []

                    comprobantes = [f for f in files if "comprobante" in f["title"].lower()]
                    if comprobantes:
                        key = comprobantes[0]['key']
                        comprobante_url = f"https://{S3_BUCKET_NAME}.s3.{AWS_REGION_NAME}.amazonaws.com/{key}"

                    facturas = [f for f in files if "factura" in f["title"].lower()]
                    if facturas:
                        key = facturas[0]['key']
                        factura_url = f"https://{S3_BUCKET_NAME}.s3.{AWS_REGION_NAME}.amazonaws.com/{key}"

                row["Link_Comprobante"] = comprobante_url
                row["Link_Factura"] = factura_url
                nuevos_rows.append(row)
                nuevos_ids.append(pedido_id)

            # Unir con caché anterior
            if not df_cache.empty:
                df_confirmados_final = pd.concat([df_cache, pd.DataFrame(nuevos_rows)], ignore_index=True)
            else:
                df_confirmados_final = pd.DataFrame(nuevos_rows)

            # Guardar nueva caché
            st.session_state.df_confirmados_cache = df_confirmados_final
            st.session_state.confirmados_anteriores_ids = list(set(ids_previos).union(nuevos_ids))

            # Mostrar
            columnas_a_mostrar = [
                'Folio_Factura', 'Cliente', 'Vendedor_Registro', 'Tipo_Envio', 'Fecha_Entrega',
                'Estado', 'Estado_Pago', 'Forma_Pago_Comprobante', 'Monto_Comprobante',
                'Fecha_Pago_Comprobante', 'Banco_Destino_Pago', 'Terminal', 'Referencia_Comprobante',
                'Link_Comprobante', 'Link_Factura'
            ]

            columnas_existentes = [col for col in columnas_a_mostrar if col in df_confirmados_final.columns]
            df_vista = df_confirmados_final[columnas_existentes].copy()

            st.dataframe(df_vista, use_container_width=True, hide_index=True)

            # Botón de descarga
            output_confirmados = BytesIO()
            with pd.ExcelWriter(output_confirmados, engine='xlsxwriter') as writer:
                df_vista.to_excel(writer, index=False, sheet_name='Confirmados')
            data_xlsx = output_confirmados.getvalue()

            st.download_button(
                label="📤 Descargar Excel de Confirmados",
                data=data_xlsx,
                file_name=f"pedidos_confirmados_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
