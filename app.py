import streamlit as st
import pandas as pd
import re
import os
import plotly.express as px
import gc

st.set_page_config(page_title="Monitor Red - 100h Ultra Rápido", layout="wide")

# --- CONFIGURACIÓN ---
UMBRAL_CRITICO = 65 
FOLDER_PATH = 'Temperatura'

st.sidebar.header("🛡️ Control de Red")
if st.sidebar.button("♻️ Forzar Recarga Total"):
    st.cache_data.clear()
    st.session_state.clear()
    st.rerun()

def extraer_datos_rapido(path):
    """Lectura ultra-eficiente línea por línea para no saturar la RAM."""
    res = []
    try:
        with open(path, 'r', encoding='latin-1', errors='ignore') as f:
            ne_name = "Desconocido"
            fecha_hora = None
            for line in f:
                # Detectar nombre del sitio
                if "NE Name:" in line:
                    ne_name = line.split(":")[-1].strip()
                # Detectar Timestamp
                if "---    REPORT" in line:
                    match = re.search(r'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})', line)
                    if match:
                        fecha_hora = pd.to_datetime(f"{match.group(1)} {match.group(2)}")
                
                # Detectar filas de datos (Cab Sub Slot Temp)
                # Buscamos líneas que empiecen con números (ej: 0  0  1  55)
                row_match = re.match(r'^\s*(\d+)\s+(\d+)\s+(\d+)\s+(\d+)', line)
                if row_match and fecha_hora:
                    res.append({
                        "Timestamp": fecha_hora,
                        "Sitio": ne_name,
                        "Subrack": row_match.group(2),
                        "Slot": int(row_match.group(3)),
                        "Temp": int(row_match.group(4)),
                        "ID_Full": f"{ne_name} (S:{row_match.group(2)}-L:{row_match.group(3)})"
                    })
        return res
    except:
        return []

@st.cache_data(ttl=60)
def listar_archivos_ordenados(folder):
    if not os.path.exists(folder): return []
    archivos = [f for f in os.listdir(folder) if f.endswith(".txt")]
    # Ordenar por el nombre numérico (el más reciente primero)
    archivos.sort(key=lambda x: "".join(re.findall(r'\d+', x)), reverse=True)
    return [os.path.join(folder, f) for f in archivos]

# --- LÓGICA PRINCIPAL ---
archivos_total = listar_archivos_ordenados(FOLDER_PATH)

if archivos_total:
    # 1. CARGA REPORTE ACTUAL (Instantáneo)
    if "df_actual" not in st.session_state:
        st.session_state["df_actual"] = pd.DataFrame(extraer_datos_rapido(archivos_total[0]))
    
    df_actual = st.session_state["df_actual"]
    
    tab1, tab2, tab3 = st.tabs(["🚨 ALERTA ÚLTIMA", "📍 BUSCADOR", "📈 HISTÓRICO 100H"])

    with tab1:
        st.subheader(f"Reporte: {os.path.basename(archivos_total[0])}")
        if not df_actual.empty:
            slots_disp = sorted(df_actual['Slot'].unique())
            sel_slots = st.multiselect("Filtrar Slots:", slots_disp, default=slots_disp)
            
            criticos = df_actual[(df_actual['Temp'] >= UMBRAL_CRITICO) & (df_actual['Slot'].isin(sel_slots))]
            
            if not criticos.empty:
                cols = st.columns(4)
                for i, (_, r) in enumerate(criticos.sort_values('Temp', ascending=False).iterrows()):
                    with cols[i % 4]:
                        st.markdown(f"""<div style="background-color:#FFC7CE; padding:10px; border-radius:10px; border:2px solid #9C0006; margin-bottom:10px; text-align:center;">
                            <b style="color:#9C0006;">{r['Sitio']}</b><br><span style="font-size:24px; color:#9C0006;">{r['Temp']}°C</span><br>
                            <small style="color:#9C0006;">S:{r['Subrack']} | L:{r['Slot']}</small></div>""", unsafe_allow_html=True)
            else: st.success("✅ Todo normal en los slots seleccionados.")

    with tab2:
        if not df_actual.empty:
            s_sel = st.selectbox("Sitio:", sorted(df_actual['Sitio'].unique()))
            st.dataframe(df_actual[df_actual['Sitio'] == s_sel], use_container_width=True)

    with tab3:
        st.subheader("Tendencia de las últimas 100 horas")
        
        # Botón de carga con gestión de memoria agresiva
        if st.button("🚀 Cargar Histórico Completo"):
            all_data = []
            bar = st.progress(0)
            status = st.empty()
            
            # Tomamos 100 archivos (aprox 4 días)
            archivos_h = archivos_total[:100]
            
            for idx, p in enumerate(archivos_h):
                status.text(f"Procesando {idx+1}/{len(archivos_h)} reportes...")
                all_data.extend(extraer_datos_rapido(p))
                bar.progress((idx + 1) / len(archivos_h))
                
                # Liberar RAM cada 20 archivos
                if idx % 20 == 0:
                    gc.collect()
            
            df_h = pd.DataFrame(all_data)
            if not df_h.empty:
                # Agrupamos por hora para que el gráfico no pese nada
                df_h['Hora'] = df_h['Timestamp'].dt.floor('h')
                st.session_state["df_h_100"] = df_h.groupby(['Hora', 'Sitio', 'ID_Full'])['Temp'].max().reset_index()
                status.success("✅ ¡Datos listos!")
            else: status.error("No hay datos.")

        if "df_h_100" in st.session_state:
            df_plot = st.session_state["df_h_100"]
            sitio_h = st.selectbox("Seleccione Sitio:", sorted(df_plot['Sitio'].unique()), key="h100")
            fig = px.line(df_plot[df_plot['Sitio'] == sitio_h], x='Hora', y='Temp', color='ID_Full', markers=True)
            st.plotly_chart(fig, use_container_width=True)
else:
    st.error("No se encontraron archivos .txt en la carpeta 'Temperatura'.")
