import streamlit as st
import pandas as pd
import re
import os
import plotly.express as px
from datetime import datetime, timedelta

st.set_page_config(page_title="Monitor Red Profesional", layout="wide")

# --- CONFIGURACIÓN ---
UMBRAL_CRITICO = 79 
FOLDER_PATH = 'Temperatura'

st.sidebar.header("🛡️ Control de Red")
if st.sidebar.button("♻️ Forzar Recarga Total"):
    st.cache_data.clear()
    st.rerun()

def extraer_datos(content, ne_name, fecha, hora):
    """Función auxiliar para procesar el texto de los archivos."""
    rows_list = []
    # Captura: Cab, Sub, Slot, Temp
    rows = re.findall(r'(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(?:\d+|NULL)', content)
    for r in rows:
        rows_list.append({
            "Timestamp": pd.to_datetime(f"{fecha} {hora}"),
            "Sitio": ne_name.strip(),
            "Subrack": r[1],
            "Slot": int(r[2]),
            "Temp": int(r[3]),
            "ID_Full": f"{ne_name.strip()} (S:{r[1]}-L:{r[2]})"
        })
    return rows_list

@st.cache_data(ttl=60)
def cargar_datos_monitor(folder):
    if not os.path.exists(folder): return None, None
    archivos = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".txt")]
    if not archivos: return None, None
    archivos.sort(key=os.path.getmtime, reverse=True)

    # 1. PROCESAR ÚLTIMO REPORTE (Alertas y Buscador)
    ultimo_archivo = archivos[0]
    data_ultima = []
    try:
        with open(ultimo_archivo, 'r', encoding='latin-1', errors='ignore') as f:
            content = f.read()
            blocks = re.findall(r'NE Name:\s+([^\n\r]+).*?(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2}).*?Display Board Temperature(.*?)\n---    END', content, re.DOTALL)
            for ne_name, fecha, hora, table_text in blocks:
                data_ultima.extend(extraer_datos(table_text, ne_name, fecha, hora))
    except: pass
    df_ultima = pd.DataFrame(data_ultima)

    # 2. PROCESAR HISTÓRICO (Últimos 7 días)
    data_hist = []
    limite_7d = datetime.now() - timedelta(days=7)
    # Leemos hasta 100 archivos para asegurar cubrir la semana
    for path in archivos[:100]: 
        if datetime.fromtimestamp(os.path.getmtime(path)) < limite_7d: break
        try:
            with open(path, 'r', encoding='latin-1', errors='ignore') as f:
                content = f.read()
                blocks = re.findall(r'NE Name:\s+([^\n\r]+).*?(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2}).*?Display Board Temperature(.*?)\n---    END', content, re.DOTALL)
                for ne_name, fecha, hora, table_text in blocks:
                    data_hist.extend(extraer_datos(table_text, ne_name, fecha, hora))
        except: continue
    
    df_hist = pd.DataFrame(data_hist)
    if not df_hist.empty:
        # Agrupar por hora para limpiar el gráfico
        df_hist['Fecha_Hora'] = df_hist['Timestamp'].dt.floor('h')
        df_hist = df_hist.groupby(['Fecha_Hora', 'Sitio', 'ID_Full', 'Slot'])['Temp'].max().reset_index()

    return df_ultima, df_hist

# --- EJECUCIÓN ---
with st.spinner('Actualizando Monitor...'):
    df_ultima, df_hist = cargar_datos_monitor(FOLDER_PATH)

if df_ultima is not None and not df_ultima.empty:
    tab1, tab2, tab3 = st.tabs(["🚨 ALERTAS CRÍTICAS", "📍 BUSCADOR POR SITIO", "📈 HISTÓRICO (7 DÍAS)"])

    with tab1:
        st.subheader(f"Alertas del Último Reporte (>= {UMBRAL_CRITICO}°C)")
        # Selección de Slots
        slots_disp = sorted(df_ultima['Slot'].unique())
        slots_sel = st.multiselect("Filtrar por Slot:", slots_disp, default=slots_disp)
        
        criticos = df_ultima[(df_ultima['Temp'] >= UMBRAL_CRITICO) & (df_ultima['Slot'].isin(slots_sel))]
        
        if not criticos.empty:
            cols = st.columns(4)
            for i, (_, row) in enumerate(criticos.sort_values('Temp', ascending=False).iterrows()):
                with cols[i % 4]:
                    st.markdown(f"""
                        <div style="background-color:#FFC7CE; padding:15px; border-radius:10px; text-align:center; border:2px solid #9C0006; margin-bottom:10px;">
                            <p style="margin:0; font-weight:bold; color:#9C0006;">{row['Sitio']}</p>
                            <h2 style="margin:5px 0; color:#9C0006;">{row['Temp']}°C</h2>
                            <p style="margin:0; font-size:12px; color:#9C0006;">SUB {row['Subrack']} | SLOT {row['Slot']}</p>
                        </div>
                    """, unsafe_allow_html=True)
        else:
            st.success("✅ Sin alertas críticas en los slots seleccionados.")

    with tab2:
        st.subheader("Buscador: Estado en el Último Reporte")
        sitio_sel = st.selectbox("Seleccione Sitio:", sorted(df_ultima['Sitio'].unique()))
        st.dataframe(df_ultima[df_ultima['Sitio'] == sitio_sel][['Subrack', 'Slot', 'Temp', 'Timestamp']], use_container_width=True, hide_index=True)

    with tab3:
        if df_hist is not None and not df_hist.empty:
            st.subheader("Tendencia Semanal por Hora")
            sitio_h = st.selectbox("Sitio para Histórico:", sorted(df_hist['Sitio'].unique()))
            df_plot = df_hist[df_hist['Sitio'] == sitio_h]
            
            fig = px.line(df_plot, x='Fecha_Hora', y='Temp', color='ID_Full', markers=True,
                         title=f"Evolución 7 días: {sitio_h}")
            fig.update_xaxes(dtick="H24", tickformat="%d %b\n%H:%M")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No hay datos históricos suficientes para los últimos 7 días.")
else:
    st.warning("No se encontraron reportes recientes.")
