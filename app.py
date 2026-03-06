import streamlit as st
import pandas as pd
import re
import os
import plotly.express as px
from datetime import datetime, timedelta

st.set_page_config(page_title="Monitor Red - Versión Final", layout="wide")

# --- CONFIGURACIÓN ---
UMBRAL_CRITICO = 75 
FOLDER_PATH = 'Temperatura'

st.sidebar.header("🛡️ Control de Red")
if st.sidebar.button("♻️ Forzar Recarga Total"):
    st.cache_data.clear()
    st.rerun()

def extraer_datos_archivo(path):
    """Procesa un archivo de texto de forma ultra eficiente."""
    try:
        with open(path, 'r', encoding='latin-1', errors='ignore') as f:
            content = f.read()
            # Busca bloques de cada NE (Sitio)
            blocks = re.findall(r'NE Name:\s+([^\n\r]+).*?(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2}).*?Display Board Temperature(.*?)\n---    END', content, re.DOTALL)
            res = []
            for ne_name, fecha, hora, table in blocks:
                # Extrae columnas: Cab, Sub, Slot, Temp
                rows = re.findall(r'(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(?:\d+|NULL)', table)
                for r in rows:
                    res.append({
                        "Timestamp": pd.to_datetime(f"{fecha} {hora}"),
                        "Sitio": ne_name.strip(), 
                        "Subrack": r[1], 
                        "Slot": int(r[2]),
                        "Temp": int(r[3]), 
                        "ID_Full": f"{ne_name.strip()} (S:{r[1]}-L:{r[2]})"
                    })
            return res
    except: return []

@st.cache_data(ttl=10)
def obtener_archivos_blindado(folder):
    """Ordena archivos por el número más alto en su nombre (Cronología real)."""
    if not os.path.exists(folder): return []
    archivos = [f for f in os.listdir(folder) if f.endswith(".txt")]
    if not archivos: return []

    # Convierte el nombre en un número para comparar (ej: 20260306150005)
    def llave_numerica(nombre):
        nums = "".join(re.findall(r'\d+', nombre))
        return int(nums) if nums else 0

    archivos.sort(key=llave_numerica, reverse=True)
    return [os.path.join(folder, f) for f in archivos]

# --- PROCESAMIENTO ---
lista_archivos = obtener_archivos_blindado(FOLDER_PATH)

if lista_archivos:
    # EL PRIMERO ES SIEMPRE EL ÚLTIMO CARGADO (POR NOMBRE)
    archivo_actual = lista_archivos[0]
    df_ultima = pd.DataFrame(extraer_datos_archivo(archivo_actual))
    
    tab1, tab2, tab3 = st.tabs(["🚨 ALERTA ÚLTIMA", "📍 BUSCADOR", "📈 HISTÓRICO 7D"])

    with tab1:
        st.subheader(f"Reporte Actual: {os.path.basename(archivo_actual)}")
        
        # Filtro de Slots
        slots_f = sorted(df_ultima['Slot'].unique())
        sel_s = st.multiselect("Filtrar Slots:", slots_f, default=slots_f)
        
        # Lógica de Alerta Crítica (>= 65°C)
        criticos = df_ultima[(df_ultima['Temp'] >= UMBRAL_CRITICO) & (df_ultima['Slot'].isin(sel_s))]
        
        if not criticos.empty:
            cols = st.columns(4)
            for i, (_, r) in enumerate(criticos.sort_values('Temp', ascending=False).iterrows()):
                with cols[i % 4]:
                    st.markdown(f"""<div style="background-color:#FFC7CE; padding:15px; border-radius:10px; text-align:center; border:2px solid #9C0006; margin-bottom:10px;">
                        <h4 style="margin:0; color:#9C0006;">{r['Sitio']}</h4>
                        <h1 style="margin:5px 0; color:#9C0006;">{r['Temp']}°C</h1>
                        <small style="color:#9C0006;">S:{r['Subrack']} | L:{r['Slot']}</small>
                        </div>""", unsafe_allow_html=True)
        else:
            st.success("✅ Todo normal en el reporte más reciente.")

    with tab2:
        sitio_sel = st.selectbox("Seleccione Sitio (Reporte Actual):", sorted(df_ultima['Sitio'].unique()))
        st.dataframe(df_ultima[df_ultima['Sitio'] == sitio_sel][['Subrack', 'Slot', 'Temp', 'Timestamp']], use_container_width=True, hide_index=True)

    with tab3:
        st.subheader("Tendencia Semanal (Últimos 25 reportes)")
        if st.button("📊 Cargar/Actualizar Histórico"):
            with st.spinner('Procesando datos...'):
                data_h = []
                # Limitamos a 25 archivos para evitar bloqueos del servidor
                for p in lista_archivos[:25]:
                    data_h.extend(extraer_datos_archivo(p))
                
                df_h = pd.DataFrame(data_h)
                if not df_h.empty:
                    df_h['Hora'] = df_h['Timestamp'].dt.floor('h')
                    # Agrupar por hora para limpiar el gráfico
                    df_h = df_h.groupby(['Hora', 'Sitio', 'ID_Full'])['Temp'].max().reset_index()
                    
                    s_h = st.selectbox("Sitio para tendencia:", sorted(df_h['Sitio'].unique()))
                    fig = px.line(df_h[df_h['Sitio'] == s_h], x='Hora', y='Temp', color='ID_Full', markers=True)
                    fig.update_xaxes(dtick="H24", tickformat="%d %b")
                    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No se encontraron archivos en la carpeta 'Temperatura'.")
