import streamlit as st
import pandas as pd
import re
import os
import plotly.express as px

st.set_page_config(page_title="Monitor Pro", layout="wide")

# --- BARRA LATERAL (FILTROS) ---
st.sidebar.header("⚙️ Configuración y Filtros")

if st.sidebar.button("🗑️ Limpiar Caché"):
    st.cache_data.clear()
    st.rerun()

FOLDER_PATH = 'Temperatura'

@st.cache_data(ttl=600)
def procesar_datos_completos(folder):
    rows_list = []
    if not os.path.exists(folder): return None
    
    archivos = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".txt")]
    archivos.sort(key=os.path.getmtime)
    
    # Procesamos los últimos 25 archivos para balancear historial y velocidad
    for path in archivos[-25:]:
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
                            "ID_Unico": f"Sub:{r[1]}-Slot:{r[2]}",
                            "Temp": int(r[3])
                        })
        except: continue
    return pd.DataFrame(rows_list)

# --- CARGA DE DATOS ---
df = procesar_datos_completos(FOLDER_PATH)

if df is not None and not df.empty:
    # 1. Selector de Sitio
    lista_sitios = sorted(df['Sitio'].unique())
    sitio_sel = st.sidebar.selectbox("📍 Seleccione Sitio", lista_sitios)
    
    # Filtrar datos por sitio
    df_sitio = df[df['Sitio'] == sitio_sel].copy()
    
    # 2. Selector de Slots (Agregar/Quitar)
    todos_los_slots = sorted(df_sitio['Slot'].unique())
    slots_visibles = st.sidebar.multiselect(
        "🔌 Seleccionar Slots a visualizar", 
        options=todos_los_slots, 
        default=todos_los_slots
    )
    
    # Aplicar filtro de slots
    df_filtrado = df_sitio[df_sitio['Slot'].isin(slots_visibles)]

    st.title(f"🌡️ Monitor: {sitio_sel}")

    # --- PESTAÑAS ---
    tab1, tab2 = st.tabs(["🚦 Semáforo de Estado", "📈 Histórico de Tendencias"])

    with tab1:
        # Obtener la última lectura de los slots seleccionados
        if not df_filtrado.empty:
            ultimo_ts = df_filtrado['Timestamp'].max()
            df_actual = df_filtrado[df_filtrado['Timestamp'] == ultimo_ts].sort_values(['Subrack', 'Slot'])
            
            st.subheader(f"Estado Actual ({ultimo_ts})")
            
            # Layout de tarjetas (semáforo)
            cols = st.columns(4)
            for i, (_, row) in enumerate(df_actual.iterrows()):
                t = row['Temp']
                # Lógica de colores del semáforo
                if t < 45: 
                    bg_color, txt_color, estado = "#C6EFCE", "#006100", "Normal 🟢"
                elif t < 65: 
                    bg_color, txt_color, estado = "#FFEB9C", "#9C6500", "Precaución 🟡"
                else: 
                    bg_color, txt_color, estado = "#FFC7CE", "#9C0006", "ALTA TEMP 🔴"

                with cols[i % 4]:
                    st.markdown(f"""
                        <div style="background-color:{bg_color}; padding:20px; border-radius:15px; text-align:center; border:2px solid {txt_color}50; margin-bottom:15px;">
                            <p style="margin:0; font-size:13px; font-weight:bold; color:{txt_color};">SUB {row['Subrack']} | SLOT {row['Slot']}</p>
                            <h1 style="margin:10px 0; color:{txt_color}; font-size:35px;">{t}°C</h1>
                            <p style="margin:0; font-weight:bold; color:{txt_color};">{estado}</p>
                        </div>
                    """, unsafe_allow_html=True)
        else:
            st.info("Seleccione al menos un Slot en la barra lateral.")

    with tab2:
        st.subheader("Evolución de Temperatura")
        if not df_filtrado.empty:
            fig = px.line(
                df_filtrado, 
                x='Timestamp', 
                y='Temp', 
                color='ID_Unico',
                markers=True,
                labels={'ID_Unico': 'Ubicación', 'Temp': 'Temperatura °C'},
                color_discrete_sequence=px.colors.qualitative.Safe
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No hay datos para graficar con los filtros actuales.")

else:
    st.error("No se encontraron datos en la carpeta 'Temperatura'.")
