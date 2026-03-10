import streamlit as st
import pandas as pd
import os
import re
import plotly.express as px
import gc

st.set_page_config(page_title="Monitor Red - 100h Fix", layout="wide")

# --- CONFIGURACIÓN ---
UMBRAL_CRITICO = 65 
FOLDER_PATH = 'Temperatura'

if st.sidebar.button("♻️ Reiniciar Aplicación"):
    st.cache_data.clear()
    st.session_state.clear()
    st.rerun()

def extraer_datos_ultra_rapido(path):
    """Lectura optimizada línea por línea para archivos .gz.txt."""
    rows = []
    try:
        with open(path, 'r', encoding='latin-1', errors='ignore') as f:
            sitio = "Desconocido"
            ts = None
            for line in f:
                if "NE Name:" in line:
                    sitio = line.split(":")[-1].strip()
                if "REPORT" in line and not ts:
                    m = re.search(r'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})', line)
                    if m: ts = pd.to_datetime(f"{m.group(1)} {m.group(2)}")
                
                # Captura la tabla de temperaturas (Subrack, Slot, Temperatura)
                match = re.match(r'^\s*(\d+)\s+(\d+)\s+(\d+)\s+(\d+)', line)
                if match and ts:
                    rows.append({
                        "Timestamp": ts, "Sitio": sitio,
                        "Subrack": match.group(2), "Slot": int(match.group(3)),
                        "Temp": int(match.group(4)),
                        "ID_Full": f"{sitio} (S:{match.group(2)}-L:{match.group(3)})"
                    })
    except Exception:
        pass
    return rows

@st.cache_data(ttl=60)
def listar_archivos_gz(folder):
    """Busca archivos .txt y .gz.txt en la carpeta."""
    if not os.path.exists(folder): return []
    # CORRECCIÓN: Filtra archivos que contengan .txt (incluyendo .gz.txt)
    fs = [os.path.join(folder, f) for f in os.listdir(folder) if ".txt" in f]
    # Ordenar por fecha numérica en el nombre para tener el más reciente primero
    fs.sort(key=lambda x: "".join(re.findall(r'\d+', x)), reverse=True)
    return fs

# --- PROCESAMIENTO ---
archivos = listar_archivos_gz(FOLDER_PATH)

if archivos:
    # 1. Carga inmediata del reporte más nuevo para Alertas
    if "df_actual" not in st.session_state:
        st.session_state["df_actual"] = pd.DataFrame(extraer_datos_ultra_rapido(archivos[0]))
    
    df_actual = st.session_state["df_actual"]
    tab1, tab2, tab3 = st.tabs(["🚨 ALERTAS", "🔍 BUSCADOR", "📈 HISTÓRICO 100H"])

    with tab1:
        if not df_actual.empty:
            st.info(f"Reporte Actual: {os.path.basename(archivos[0])}")
            # Recuperamos los filtros de Slot
            slots_disponibles = sorted(df_actual['Slot'].unique())
            sel_slots = st.multiselect("Filtrar Slots:", slots_disponibles, default=slots_disponibles)
            
            criticos = df_actual[(df_actual['Temp'] >= UMBRAL_CRITICO) & (df_actual['Slot'].isin(sel_slots))]
            
            if not criticos.empty:
                cols = st.columns(4)
                for i, (_, r) in enumerate(criticos.sort_values('Temp', ascending=False).iterrows()):
                    with cols[i % 4]:
                        st.markdown(f"""<div style="background-color:#FFC7CE; padding:15px; border-radius:10px; border:2px solid #9C0006; margin-bottom:10px; text-align:center;">
                            <b style="color:#9C0006; font-size:18px;">{r['Sitio']}</b><br>
                            <span style="font-size:32px; font-weight:bold; color:#9C0006;">{r['Temp']}°C</span><br>
                            <small style="color:#9C0006;">S:{r['Subrack']} | L:{r['Slot']}</small></div>""", unsafe_allow_html=True)
            else:
                st.success("✅ No hay temperaturas críticas en los slots seleccionados.")

    with tab2:
        if not df_actual.empty:
            s_sel = st.selectbox("Seleccione Sitio:", sorted(df_actual['Sitio'].unique()))
            st.dataframe(df_actual[df_actual['Sitio'] == s_sel], use_container_width=True)

    with tab3:
        st.subheader("Tendencia de los últimos 100 reportes")
        if st.button("📊 Procesar Histórico Completo"):
            all_rows = []
            p_bar = st.progress(0)
            status = st.empty()
            
            # Tomamos hasta los últimos 100 archivos detectados
            limite_archivos = archivos[:100]
            for i, path in enumerate(limite_archivos):
                status.text(f"Analizando reporte {i+1} de {len(limite_archivos)}...")
                all_rows.extend(extraer_datos_ultra_rapido(path))
                p_bar.progress((i + 1) / len(limite_archivos))
                if i % 20 == 0: gc.collect() # Liberar RAM
            
            if all_rows:
                df_h = pd.DataFrame(all_rows)
                # Agrupamos por hora para que la gráfica sea fluida
                df_h['Hora'] = df_h['Timestamp'].dt.floor('h')
                st.session_state["df_100h"] = df_h.groupby(['Hora', 'Sitio', 'ID_Full'])['Temp'].max().reset_index()
                status.success(f"✅ ¡Histórico de {len(limite_archivos)} reportes listo!")
            else:
                status.error("No se pudieron extraer datos de los archivos.")

        if "df_100h" in st.session_state:
            df_p = st.session_state["df_100h"]
            s_h = st.selectbox("Filtrar Gráfica por Sitio:", sorted(df_p['Sitio'].unique()), key="graph_site")
            fig = px.line(df_p[df_p['Sitio'] == s_h], x='Hora', y='Temp', color='ID_Full', markers=True)
            fig.update_layout(xaxis_title="Fecha/Hora", yaxis_title="Temperatura (°C)")
            st.plotly_chart(fig, use_container_width=True)
else:
    st.error("No se encontraron archivos .txt o .gz.txt en la carpeta 'Temperatura'.")
