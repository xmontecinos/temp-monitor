import streamlit as st
import pandas as pd
import os
import re
import plotly.express as px
import gc

# 1. Corrección de sintaxis y configuración limpia
st.set_page_config(page_title="Monitor Red - Full Histórico", layout="wide")

UMBRAL_CRITICO = 65 
FOLDER_PATH = 'Temperatura'

def extraer_datos_masivo(path):
    """Escaneo profundo por bloques de sitio."""
    rows = []
    try:
        with open(path, 'r', encoding='latin-1', errors='ignore') as f:
            content = f.read()
            
            ts_match = re.search(r'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})', content)
            if not ts_match: return []
            ts = pd.to_datetime(f"{ts_match.group(1)} {ts_match.group(2)}")

            # División por sitio para precisión total
            bloques = re.split(r'NE Name\s*:\s*', content)
            
            for bloque in bloques[1:]:
                lineas = bloque.split('\n')
                if not lineas: continue
                nombre_sitio = lineas[0].strip().split()[0]
                
                # Regex corregida: Captura Subrack, Slot y Temperatura
                filas = re.findall(r'^\s*\d+\s+(\d+)\s+(\d+)\s+(\d+)', bloque, re.MULTILINE)
                
                for r in filas:
                    rows.append({
                        "Timestamp": ts,
                        "Sitio": nombre_sitio,
                        "Slot": int(r[1]),
                        "Temp": int(r[2]),
                        "ID_Full": f"{nombre_sitio} (S:{r[0]}-L:{r[1]})"
                    })
    except Exception: pass
    return rows

@st.cache_data(ttl=60)
def listar_archivos(folder):
    if not os.path.exists(folder): return []
    fs = [os.path.join(folder, f) for f in os.listdir(folder) if ".txt" in f]
    # Ordenar por fecha extraída del nombre del archivo
    fs.sort(key=lambda x: "".join(re.findall(r'\d+', x)), reverse=True)
    return fs

# --- INTERFAZ ---
archivos_lista = listar_archivos(FOLDER_PATH)

if archivos_lista:
    # Sidebar para control de memoria
    if st.sidebar.button("♻️ Forzar Limpieza de RAM"):
        st.cache_data.clear()
        st.session_state.clear()
        st.rerun()

    if "df_now" not in st.session_state:
        st.session_state["df_now"] = pd.DataFrame(extraer_datos_masivo(archivos_lista[0]))
    
    df_actual = st.session_state["df_now"]
    tab1, tab2, tab3 = st.tabs(["🚨 ALERTAS ACTUALES", "🔍 BUSCADOR", "📈 HISTÓRICO"])

    with tab1:
        if not df_actual.empty:
            st.write(f"**Reporte:** {os.path.basename(archivos_lista[0])} | **Sitios:** {df_actual['Sitio'].nunique()}")
            
            slots = sorted(df_actual['Slot'].unique())
            sel_slots = st.multiselect("Filtrar por Slots:", slots, default=slots)
            
            criticos = df_actual[(df_actual['Temp'] >= UMBRAL_CRITICO) & (df_actual['Slot'].isin(sel_slots))]
            if not criticos.empty:
                cols = st.columns(4)
                for i, (_, r) in enumerate(criticos.sort_values('Temp', ascending=False).iterrows()):
                    with cols[i % 4]:
                        st.markdown(f"""<div style="background-color:#fee2e2; border:1px solid #dc2626; padding:10px; border-radius:8px; margin-bottom:10px; text-align:center;">
                            <strong style="color:#991b1b;">{r['Sitio']}</strong><br>
                            <span style="font-size:24px; font-weight:bold; color:#dc2626;">{r['Temp']}°C</span><br>
                            <small>Slot: {r['Slot']}</small></div>""", unsafe_allow_html=True)
            else: st.success("✅ No hay alertas en este momento.")

    with tab2:
        if not df_actual.empty:
            sitio_busq = st.selectbox("Buscar Sitio:", sorted(df_actual['Sitio'].unique()))
            st.dataframe(df_actual[df_actual['Sitio'] == sitio_busq], use_container_width=True)

    with tab3:
        st.subheader(f"Tendencia (Disponibles: {len(archivos_lista)} reportes)")
        num_reportes = st.slider("Reportes a procesar:", 10, min(180, len(archivos_lista)), 100)
        
        if st.button(f"📊 Cargar {num_reportes} Horas"):
            all_data = []
            progress = st.progress(0)
            status = st.empty()
            
            for i, p in enumerate(archivos_lista[:num_reportes]):
                all_data.extend(extraer_datos_masivo(p))
                if (i + 1) % 10 == 0:
                    status.text(f"Procesando {i+1}/{num_reportes}...")
                    progress.progress((i + 1) / num_reportes)
                    gc.collect() 

            if all_data:
                df_h = pd.DataFrame(all_data)
                # Agrupamos por hora, sitio e ID_Full para el histórico
                st.session_state["df_full"] = df_h.groupby([df_h['Timestamp'].dt.floor('h'), 'Sitio', 'ID_Full'])['Temp'].max().reset_index()
                status.success(f"✅ ¡{num_reportes} horas cargadas!")
            else:
                status.error("No se encontraron datos.")

        # --- AQUÍ EMPIEZA LA NUEVA LÓGICA DE SELECCIÓN ---
        if "df_full" in st.session_state:
            df_p = st.session_state["df_full"]
            
            # 1. Filtro de Sitio (necesario para acotar los IDs)
            sitio_sel = st.selectbox("Seleccione sitio para graficar:", sorted(df_p['Sitio'].unique()))
            
            # 2. Obtener los IDs únicos de ese sitio en particular
            df_sitio = df_p[df_p['Sitio'] == sitio_sel]
            ids_unicos = sorted(df_sitio['ID_Full'].unique())

            # 3. Botones para selección masiva
            st.write("Selección rápida de IDs:")
            col_b1, col_b2 = st.columns(2)
            
            if col_b1.button("✅ Seleccionar Todos", use_container_width=True):
                st.session_state["ids_seleccionados"] = ids_unicos
            
            if col_b2.button("❌ Limpiar Selección", use_container_width=True):
                st.session_state["ids_seleccionados"] = []

            # 4. El Multiselect conectado al estado de sesión
            seleccion_final = st.multiselect(
                "Elija los Slots/IDs a visualizar:",
                options=ids_unicos,
                default=st.session_state.get("ids_seleccionados", ids_unicos)
            )

            # 5. Graficar solo lo seleccionado
            if seleccion_final:
                df_grafico = df_sitio[df_sitio['ID_Full'].isin(seleccion_final)]
                fig = px.line(
                    df_grafico, 
                    x='Timestamp', 
                    y='Temp', 
                    color='ID_Full', 
                    markers=True,
                    title=f"Evolución de Temperatura - {sitio_sel}"
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("💡 Por favor, selecciona al menos un ID para generar la gráfica.")
else:
    st.error("No se detectaron archivos en la carpeta 'Temperatura'.")
