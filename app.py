import streamlit as st
import pandas as pd
import re
import os
import plotly.express as px
import gc

st.set_page_config(page_title="Monitor Red - Histórico 100h", layout="wide")

# --- CONFIGURACIÓN ---
UMBRAL_CRITICO = 65 
FOLDER_PATH = 'Temperatura'

st.sidebar.header("🛡️ Control de Red")
if st.sidebar.button("♻️ Forzar Recarga Total"):
    st.cache_data.clear()
    st.session_state.clear()
    st.rerun()

def extraer_datos_archivo(path):
    """Extrae datos optimizando el uso de memoria."""
    try:
        with open(path, 'r', encoding='latin-1', errors='ignore') as f:
            content = f.read()
            blocks = re.findall(r'NE Name:\s+([^\n\r]+).*?(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2}).*?Display Board Temperature(.*?)\n---    END', content, re.DOTALL)
            res = []
            for ne_name, fecha, hora, table in blocks:
                rows = re.findall(r'(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(?:\d+|NULL)', table)
                for r in rows:
                    res.append({
                        "Timestamp": pd.to_datetime(f"{fecha} {hora}"),
                        "Sitio": ne_name.strip(), 
                        "Subrack": r[1], "Slot": int(r[2]),
                        "Temp": int(r[3]), 
                        "ID_Full": f"{ne_name.strip()} (S:{r[1]}-L:{r[2]})"
                    })
            return res
    except: return []

@st.cache_data(ttl=30)
def listar_archivos(folder):
    if not os.path.exists(folder): return []
    archivos = [f for f in os.listdir(folder) if f.endswith(".txt")]
    archivos.sort(key=lambda x: "".join(re.findall(r'\d+', x)), reverse=True)
    return [os.path.join(folder, f) for f in archivos]

# --- FLUJO PRINCIPAL ---
archivos_total = listar_archivos(FOLDER_PATH)

if archivos_total:
    # 1. CARGA REPORTE ACTUAL (Pestaña 1 y 2)
    df_actual = pd.DataFrame(extraer_datos_archivo(archivos_total[0]))
    
    tab1, tab2, tab3 = st.tabs(["🚨 ALERTA ÚLTIMA", "📍 BUSCADOR", "📈 HISTÓRICO 7D"])

    with tab1:
        st.subheader(f"Reporte: {os.path.basename(archivos_total[0])}")
        if not df_actual.empty:
            # --- RECUPERADO: Selección de Slots ---
            slots_disponibles = sorted(df_actual['Slot'].unique())
            sel_slots = st.multiselect("Filtrar Slots:", slots_disponibles, default=slots_disponibles)
            
            criticos = df_actual[(df_actual['Temp'] >= UMBRAL_CRITICO) & (df_actual['Slot'].isin(sel_slots))]
            
            if not criticos.empty:
                cols = st.columns(4)
                for i, (_, r) in enumerate(criticos.sort_values('Temp', ascending=False).iterrows()):
                    with cols[i % 4]:
                        st.markdown(f"""
                            <div style="background-color:#FFC7CE; padding:10px; border-radius:5px; border:1px solid #9C0006; margin-bottom:5px;">
                                <b style="color:#9C0006;">{r['Sitio']}</b><br>
                                <span style="font-size:24px; font-weight:bold; color:#9C0006;">{r['Temp']}°C</span><br>
                                <small style="color:#9C0006;">S:{r['Subrack']} | L:{r['Slot']}</small>
                            </div>
                        """, unsafe_allow_html=True)
            else: st.success("✅ Sin alertas críticas en los slots seleccionados.")

    with tab2:
        if not df_actual.empty:
            sitio_sel = st.selectbox("Sitio:", sorted(df_actual['Sitio'].unique()))
            st.dataframe(df_actual[df_actual['Sitio'] == sitio_sel], width=1200)

    with tab3:
        # --- NUEVO: Histórico de 100 horas ---
        st.subheader("Tendencia Histórica (100 Reportes)")
        
        if st.button("📊 Cargar Histórico Extendido (100h)"):
            data_acumulada = []
            progreso = st.progress(0)
            status = st.empty()
            
            # Subimos el límite a 100 archivos
            archivos_a_procesar = archivos_total[:100]
            
            for idx, p in enumerate(archivos_a_procesar):
                status.text(f"Procesando reporte {idx+1}/{len(archivos_a_procesar)}...")
                data_acumulada.extend(extraer_datos_archivo(p))
                progreso.progress((idx + 1) / len(archivos_a_procesar))
                
                # Liberación de memoria cada 10 archivos para evitar cuelgues
                if idx % 10 == 0:
                    gc.collect()
            
            df_h = pd.DataFrame(data_acumulada)
            if not df_h.empty:
                df_h['Hora'] = df_h['Timestamp'].dt.floor('h')
                # Agrupamos para que la gráfica no sea pesada para el navegador
                st.session_state["df_h_100"] = df_h.groupby(['Hora', 'Sitio', 'ID_Full'])['Temp'].max().reset_index()
                status.success(f"✅ ¡Histórico de {len(archivos_a_procesar)} reportes cargado!")
            else:
                status.error("No se encontraron datos.")

        if "df_h_100" in st.session_state:
            df_plot = st.session_state["df_h_100"]
            sitio_h = st.selectbox("Elegir Sitio:", sorted(df_plot['Sitio'].unique()), key="h_site_100")
            
            fig = px.line(df_plot[df_plot['Sitio'] == sitio_h], 
                         x='Hora', y='Temp', color='ID_Full', markers=True,
                         title=f"Tendencia 100h: {sitio_h}")
            fig.update_layout(xaxis_title="Fecha y Hora", yaxis_title="Temperatura °C")
            st.plotly_chart(fig, width=1200)
else:
    st.info("No se encontró la carpeta 'Temperatura'.")
