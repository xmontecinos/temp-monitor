import streamlit as st
import pandas as pd
import os
import re
import plotly.express as px
import gc

st.set_page_config(page_title="Monitor Red - Fix 100h", layout="wide")

# --- CONFIGURACIÓN ---
UMBRAL_CRITICO = 65 
FOLDER_PATH = 'Temperatura'

if st.sidebar.button("♻️ Reiniciar Aplicación"):
    st.cache_data.clear()
    st.session_state.clear()
    st.rerun()

def extraer_datos_ultra_rapido(path):
    """Lectura de alto rendimiento línea por línea."""
    rows = []
    try:
        # Abrimos el archivo ignorando errores de codificación para mayor velocidad
        with open(path, 'r', encoding='latin-1', errors='ignore') as f:
            sitio = "Desconocido"
            ts = None
            for line in f:
                if "NE Name:" in line:
                    sitio = line.split(":")[-1].strip()
                if "REPORT" in line and not ts:
                    m = re.search(r'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})', line)
                    if m: ts = pd.to_datetime(f"{m.group(1)} {m.group(2)}")
                
                # Buscamos filas de datos: Subrack Slot Temp
                match = re.match(r'^\s*(\d+)\s+(\d+)\s+(\d+)\s+(\d+)', line)
                if match and ts:
                    rows.append({
                        "Timestamp": ts, "Sitio": sitio,
                        "Slot": int(match.group(3)),
                        "Temp": int(match.group(4)),
                        "ID_Full": f"{sitio} (S:{match.group(2)}-L:{match.group(3)})"
                    })
    except: pass
    return rows

@st.cache_data(ttl=60)
def listar_archivos_todos(folder):
    """Busca archivos .txt y .gz.txt en la carpeta."""
    if not os.path.exists(folder): return []
    # CORRECCIÓN: Detecta archivos que terminen en .txt o contengan .gz.txt
    fs = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".txt") or ".gz" in f]
    # Ordenar por el número de fecha en el nombre (más reciente arriba)
    fs.sort(key=lambda x: "".join(re.findall(r'\d+', x)), reverse=True)
    return fs

# --- FLUJO DE LA APP ---
archivos = listar_archivos_todos(FOLDER_PATH)

if archivos:
    # Carga rápida del reporte actual (Pestaña 1 y 2)
    if "df_now" not in st.session_state:
        st.session_state["df_now"] = pd.DataFrame(extraer_datos_ultra_rapido(archivos[0]))
    
    df_actual = st.session_state["df_now"]
    tab1, tab2, tab3 = st.tabs(["🚨 ALERTAS", "🔍 BUSCADOR", "📈 HISTÓRICO 100H"])

    with tab1:
        if not df_actual.empty:
            st.info(f"Reporte Actual: {os.path.basename(archivos[0])}")
            slots_disp = sorted(df_actual['Slot'].unique())
            sel_slots = st.multiselect("Filtrar Slots:", slots_disp, default=slots_disp)
            
            criticos = df_actual[(df_actual['Temp'] >= UMBRAL_CRITICO) & (df_actual['Slot'].isin(sel_slots))]
            
            if not criticos.empty:
                cols = st.columns(4)
                for i, (_, r) in enumerate(criticos.sort_values('Temp', ascending=False).iterrows()):
                    with cols[i % 4]:
                        st.markdown(f"""<div style="background-color:#FFC7CE; padding:10px; border-radius:10px; border:2px solid #9C0006; margin-bottom:10px; text-align:center;">
                            <b style="color:#9C0006;">{r['Sitio']}</b><br><span style="font-size:24px; color:#9C0006;">{r['Temp']}°C</span><br>
                            <small style="color:#9C0006;">Slot: {r['Slot']}</small></div>""", unsafe_allow_html=True)
            else: st.success("✅ Todo normal en los slots seleccionados.")

    with tab2:
        if not df_actual.empty:
            s_sel = st.selectbox("Sitio:", sorted(df_actual['Sitio'].unique()))
            st.dataframe(df_actual[df_actual['Sitio'] == s_sel], use_container_width=True)

    with tab3:
        st.subheader("Tendencia de 100 Reportes")
        if st.button("🚀 Cargar Histórico (4 días)"):
            all_data = []
            bar = st.progress(0)
            status = st.empty()
            
            # Procesar hasta 100 archivos (aprox 100 horas de datos)
            limite = archivos[:100]
            for i, p in enumerate(limite):
                status.text(f"Analizando reporte {i+1} de {len(limite)}...")
                all_data.extend(extraer_datos_ultra_rapido(p))
                bar.progress((i + 1) / len(limite))
                # Limpieza de RAM agresiva cada 10 archivos para evitar el error "Failed to fetch"
                if i % 10 == 0: gc.collect()
            
            if all_data:
                df_h = pd.DataFrame(all_data)
                # Agrupamos por hora para que la gráfica sea ligera
                df_h['Hora'] = df_h['Timestamp'].dt.floor('h')
                st.session_state["df_100h"] = df_h.groupby(['Hora', 'Sitio', 'ID_Full'])['Temp'].max().reset_index()
                status.success("✅ Histórico cargado con éxito.")
            else:
                status.error("No se encontraron datos dentro de los archivos .gz.txt")

        if "df_100h" in st.session_state:
            df_p = st.session_state["df_100h"]
            sitio_h = st.selectbox("Seleccione Sitio:", sorted(df_p['Sitio'].unique()), key="h100")
            fig = px.line(df_p[df_p['Sitio'] == sitio_h], x='Hora', y='Temp', color='ID_Full', markers=True)
            st.plotly_chart(fig, use_container_width=True)
else:
    st.error("No se encontraron archivos en la carpeta 'Temperatura'.")
