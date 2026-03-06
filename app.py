import streamlit as st
import pandas as pd
import re
import os
import plotly.express as px
from datetime import datetime, timedelta

st.set_page_config(page_title="Monitor Red - 7 Días", layout="wide")

st.title("🌡️ Monitor de Temperaturas: Histórico 7 Días")

# --- CONFIGURACIÓN ---
st.sidebar.header("🛡️ Control de Red")
UMBRAL_CRITICO = 65 

if st.sidebar.button("♻️ Forzar Recarga"):
    st.cache_data.clear()
    st.rerun()

FOLDER_PATH = 'Temperatura'

@st.cache_data(ttl=60)
def procesar_red_7dias(folder):
    rows_list = []
    if not os.path.exists(folder): return None
    
    archivos = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".txt")]
    archivos.sort(key=os.path.getmtime, reverse=True)
    
    # Procesamos los últimos 50 archivos para obtener profundidad de tiempo
    for path in archivos[:50]:
        try:
            with open(path, 'r', encoding='latin-1', errors='ignore') as f:
                content = f.read()
                blocks = re.findall(r'NE Name:\s+([^\n\r]+).*?(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2}).*?Display Board Temperature(.*?)\n---    END', content, re.DOTALL)
                for ne_name, fecha, hora, table_text in blocks:
                    rows = re.findall(r'(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(?:\d+|NULL)', table_text)
                    for r in rows:
                        rows_list.append({
                            "Timestamp": pd.to_datetime(f"{fecha} {hora}"),
                            "Sitio": ne_name.strip(),
                            "Subrack": r[1],
                            "Slot": int(r[2]),
                            "Temp": int(r[3]),
                            "ID_Full": f"{ne_name.strip()} (S:{r[1]}-L:{r[2]})"
                        })
        except: continue
    
    if not rows_list: return None
    
    df_raw = pd.DataFrame(rows_list)
    
    # --- FILTRO DE 7 DÍAS ---
    fecha_limite = datetime.now() - timedelta(days=7)
    df_7d = df_raw[df_raw['Timestamp'] >= fecha_limite].copy()
    
    # --- AGRUPACIÓN POR HORA ---
    # Esto asegura que si hay múltiples lecturas en una hora, veamos el promedio o máximo
    df_7d['Fecha_Hora'] = df_7d['Timestamp'].dt.floor('h') 
    
    return df_7d

# --- EJECUCIÓN ---
with st.spinner('Calculando histórico de los últimos 7 días...'):
    df = procesar_red_7dias(FOLDER_PATH)

if df is not None and not df.empty:
    # Último estado para el semáforo
    df_actual = df.sort_values('Timestamp').groupby(['Sitio', 'Subrack', 'Slot']).last().reset_index()
    
    tab1, tab2, tab3 = st.tabs(["🚨 ALERTAS CRÍTICAS", "📍 BUSCADOR SITIOS", "📈 HISTÓRICO (7 DÍAS/HORA)"])

    with tab1:
        st.subheader(f"Componentes Críticos (>= {UMBRAL_CRITICO}°C)")
        criticos = df_actual[df_actual['Temp'] >= UMBRAL_CRITICO].sort_values('Temp', ascending=False)
        
        if not criticos.empty:
            cols = st.columns(4)
            for i, (_, row) in enumerate(criticos.iterrows()):
                with cols[i % 4]:
                    st.markdown(f"""
                        <div style="background-color:#FFC7CE; padding:15px; border-radius:10px; text-align:center; border:2px solid #9C0006; margin-bottom:10px;">
                            <p style="margin:0; font-weight:bold; color:#9C0006;">{row['Sitio']}</p>
                            <h2 style="margin:5px 0; color:#9C0006;">{row['Temp']}°C</h2>
                            <p style="margin:0; font-size:12px; color:#9C0006;">SUB {row['Subrack']} | SLOT {row['Slot']}</p>
                        </div>
                    """, unsafe_allow_html=True)
        else:
            st.success("✅ Red Estable bajo los 65°C.")

    with tab2:
        lista_s = sorted(df_actual['Sitio'].unique())
        s_sel = st.selectbox("Elegir Sitio", lista_s)
        st.dataframe(df_actual[df_actual['Sitio'] == s_sel][['Subrack', 'Slot', 'Temp', 'Timestamp']], use_container_width=True, hide_index=True)

    with tab3:
        st.subheader("Tendencia Horaria - Última Semana")
        s_hist = st.selectbox("Seleccione Sitio para ver Tendencia", lista_s, key="shist")
        
        # Filtrar datos del sitio
        df_h = df[df['Sitio'] == s_hist].copy()
        
        # Agrupar por Hora e ID para que el gráfico sea por hora exacta
        df_grafico = df_h.groupby(['Fecha_Hora', 'ID_Full'])['Temp'].max().reset_index()
        
        if not df_grafico.empty:
            fig = px.line(
                df_grafico, 
                x='Fecha_Hora', 
                y='Temp', 
                color='ID_Full', 
                markers=True,
                title=f"Evolución por Hora: {s_hist}",
                labels={'Fecha_Hora': 'Tiempo (Hora)', 'Temp': 'Temperatura Max (°C)'}
            )
            # Ajustar el eje X para mostrar mejor los días
            fig.update_xaxes(dtick="H24", tickformat="%d %b\n%H:%M") 
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No hay datos suficientes para los últimos 7 días en este sitio.")

else:
    st.warning("No se encontraron datos en el rango de los últimos 7 días.")
