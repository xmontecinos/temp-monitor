import streamlit as st
import pandas as pd
import re
import os
import plotly.express as px
from datetime import datetime

st.set_page_config(page_title="Monitor Red - Carga Ultra Rápida", layout="wide")

# --- CONFIGURACIÓN ---
UMBRAL_CRITICO = 65 
FOLDER_PATH = 'Temperatura'

st.sidebar.header("🛡️ Control de Red")
if st.sidebar.button("♻️ Forzar Recarga Total"):
    st.cache_data.clear()
    st.session_state.clear()
    st.rerun()

def extraer_datos_archivo(path):
    """Extracción optimizada."""
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

# --- CACHE DE PROCESAMIENTO ---
@st.cache_data(ttl=600, show_spinner=False)
def procesar_historico_optimizado(lista_rutas):
    """Lee archivos y devuelve un DataFrame agrupado muy ligero."""
    data_acumulada = []
    # Usamos un contenedor de progreso visual
    for p in lista_rutas:
        data_acumulada.extend(extraer_datos_archivo(p))
    
    df = pd.DataFrame(data_acumulada)
    if not df.empty:
        # IMPORTANTE: Reducimos precisión a la hora para que la gráfica no pese
        df['Hora'] = df['Timestamp'].dt.floor('h')
        # Solo guardamos el valor máximo por hora para cada slot
        df = df.groupby(['Hora', 'Sitio', 'ID_Full'])['Temp'].max().reset_index()
    return df

@st.cache_data(ttl=30)
def listar_archivos(folder):
    if not os.path.exists(folder): return []
    archivos = [f for f in os.listdir(folder) if f.endswith(".txt")]
    archivos.sort(key=lambda x: "".join(re.findall(r'\d+', x)), reverse=True)
    return [os.path.join(folder, f) for f in archivos]

# --- FLUJO PRINCIPAL ---
archivos_total = listar_archivos(FOLDER_PATH)

if archivos_total:
    # Carga instantánea del reporte para Pestaña 1 y 2
    df_actual = pd.DataFrame(extraer_datos_archivo(archivos_total[0]))
    
    tab1, tab2, tab3 = st.tabs(["🚨 ALERTA ÚLTIMA", "📍 BUSCADOR", "📈 HISTÓRICO 7D"])

    with tab1:
        st.subheader(f"Último Reporte: {os.path.basename(archivos_total[0])}")
        # (Lógica de tarjetas rojas se mantiene igual)
        if not df_actual.empty:
            criticos = df_actual[df_actual['Temp'] >= UMBRAL_CRITICO]
            if not criticos.empty:
                cols = st.columns(4)
                for i, (_, r) in enumerate(criticos.sort_values('Temp', ascending=False).iterrows()):
                    with cols[i % 4]:
                        st.error(f"**{r['Sitio']}** \n{r['Temp']}°C")
            else: st.success("✅ Sin alertas.")

    with tab2:
        if not df_actual.empty:
            sitio_sel = st.selectbox("Sitio:", sorted(df_actual['Sitio'].unique()))
            st.dataframe(df_actual[df_actual['Sitio'] == sitio_sel], use_container_width=True)

    with tab3:
        st.subheader("Tendencia Semanal (Carga Inteligente)")
        
        # Botón para disparar la carga pesada al cache
        if st.button("📊 Cargar/Actualizar Gráfica"):
            # Limitamos a los últimos 25 para cubrir la semana (1 reporte x hora aprox)
            # Esto evita que el sistema se quede "Running" para siempre
            df_h = procesar_historico_optimizado(archivos_total[:25])
            st.session_state["df_historico"] = df_h
            st.success("¡Datos procesados! Ya puedes filtrar por sitio abajo.")

        # Si los datos están en el estado de sesión, mostrar buscador y gráfica
        if "df_historico" in st.session_state:
            df_plot_base = st.session_state["df_historico"]
            sitio_h = st.selectbox("Elegir Sitio para tendencia:", sorted(df_plot_base['Sitio'].unique()), key="h_site")
            
            df_filtrado = df_plot_base[df_plot_base['Sitio'] == sitio_h]
            
            # Gráfica optimizada
            fig = px.line(df_filtrado, x='Hora', y='Temp', color='ID_Full', markers=True, 
                         template="plotly_white")
            fig.update_layout(height=500, margin=dict(l=0, r=0, t=30, b=0))
            st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No hay archivos.")
