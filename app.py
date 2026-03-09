import streamlit as st
import pandas as pd
import re
import os
import plotly.express as px
from datetime import datetime

st.set_page_config(page_title="Monitor Red - Ultra Rápido", layout="wide")

# --- CONFIGURACIÓN ---
UMBRAL_CRITICO = 78 
FOLDER_PATH = 'Temperatura'

st.sidebar.header("🛡️ Control de Red")
if st.sidebar.button("♻️ Forzar Recarga Total"):
    st.cache_data.clear()
    st.rerun()

def extraer_datos_archivo(path):
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

# --- CACHE MAESTRO PARA EL HISTÓRICO ---
# Esta función guarda los datos en memoria para que NO se lean otra vez al cambiar de sitio
@st.cache_data(ttl=300) # Guarda los datos por 5 minutos
def procesar_todo_el_historico(lista_rutas):
    all_data = []
    for p in lista_rutas:
        all_data.extend(extraer_datos_archivo(p))
    df = pd.DataFrame(all_data)
    if not df.empty:
        df['Hora'] = df['Timestamp'].dt.floor('h')
        # Reducimos el tamaño del archivo final agrupando
        df = df.groupby(['Hora', 'Sitio', 'ID_Full'])['Temp'].max().reset_index()
    return df

@st.cache_data(ttl=30)
def listar_archivos(folder):
    if not os.path.exists(folder): return []
    archivos = [f for f in os.listdir(folder) if f.endswith(".txt")]
    archivos.sort(key=lambda x: "".join(re.findall(r'\d+', x)), reverse=True)
    return [os.path.join(folder, f) for f in archivos]

# --- LÓGICA PRINCIPAL ---
archivos_total = listar_archivos(FOLDER_PATH)

if archivos_total:
    # Carga rápida del reporte actual
    df_actual = pd.DataFrame(extraer_datos_archivo(archivos_total[0]))
    
    tab1, tab2, tab3 = st.tabs(["🚨 ALERTA ÚLTIMA", "📍 BUSCADOR", "📈 HISTÓRICO 7D"])

    with tab1:
        st.subheader(f"Último Reporte: {os.path.basename(archivos_total[0])}")
        if not df_actual.empty:
            slots_f = sorted(df_actual['Slot'].unique())
            sel_s = st.multiselect("Filtrar Slots:", slots_f, default=slots_f)
            criticos = df_actual[(df_actual['Temp'] >= UMBRAL_CRITICO) & (df_actual['Slot'].isin(sel_s))]
            if not criticos.empty:
                cols = st.columns(4)
                for i, (_, r) in enumerate(criticos.sort_values('Temp', ascending=False).iterrows()):
                    with cols[i % 4]:
                        st.error(f"**{r['Sitio']}** \n{r['Temp']}°C (S:{r['Subrack']} L:{r['Slot']})")
            else: st.success("✅ Sin alertas.")

    with tab2:
        if not df_actual.empty:
            sitio_sel = st.selectbox("Sitio:", sorted(df_actual['Sitio'].unique()), key="buscador_sitio")
            st.dataframe(df_actual[df_actual['Sitio'] == sitio_sel], use_container_width=True)

    with tab3:
        st.subheader("Tendencia Semanal (Carga Inteligente)")
        
        # El botón solo sirve para arrancar el proceso la primera vez
        if "datos_cargados" not in st.session_state:
            if st.button("📊 Cargar Datos Históricos (Una sola vez)"):
                with st.spinner("Procesando reportes... esto solo pasará una vez."):
                    # Guardamos el resultado en el cache maestro
                    st.session_state["historico_df"] = procesar_todo_el_historico(archivos_total[:170])
                    st.session_state["datos_cargados"] = True
                    st.rerun()
        
        # Si los datos ya están en el cache o en el estado de sesión, mostrar el gráfico al instante
        if st.session_state.get("datos_cargados"):
            df_h = st.session_state["historico_df"]
            sitios_h = sorted(df_h['Sitio'].unique())
            
            # Al cambiar este selectbox, la app NO volverá a leer los archivos .txt
            s_h = st.selectbox("Elegir Sitio para tendencia:", sitios_h, key="historico_sitio")
            
            fig = px.line(df_h[df_h['Sitio'] == s_h], x='Hora', y='Temp', color='ID_Full', markers=True)
            fig.update_layout(hovermode="x unified")
            st.plotly_chart(fig, use_container_width=True)
            
            if st.button("🗑️ Limpiar Memoria Histórica"):
                del st.session_state["datos_cargados"]
                st.cache_data.clear()
                st.rerun()
else:
    st.info("No hay archivos.")
