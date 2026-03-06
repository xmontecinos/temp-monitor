import streamlit as st
import pandas as pd
import re
import os
import plotly.express as px

st.set_page_config(page_title="Monitor de Red - 50 Archivos", layout="wide")

st.title("🚦 Monitor de Red: Alertas Críticas (Top 50)")

# --- CONFIGURACIÓN ---
st.sidebar.header("🛡️ Control de Red")
UMBRAL_CRITICO = 65 

if st.sidebar.button("♻️ Forzar Recarga"):
    st.cache_data.clear()
    st.rerun()

FOLDER_PATH = 'Temperatura'

@st.cache_data(ttl=60)
def procesar_red_50(folder):
    rows_list = []
    if not os.path.exists(folder): return None
    
    # Obtener archivos y ordenar por fecha de modificación (más recientes primero)
    archivos = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".txt")]
    archivos.sort(key=os.path.getmtime, reverse=True)
    
    # LIMITAR A LOS ÚLTIMOS 50 ARCHIVOS
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
    return pd.DataFrame(rows_list) if rows_list else None

# --- EJECUCIÓN ---
with st.spinner('Procesando los últimos 50 reportes...'):
    df = procesar_red_50(FOLDER_PATH)

if df is not None and not df.empty:
    # Obtener el último estado reportado de cada componente
    df_actual = df.sort_values('Timestamp').groupby(['Sitio', 'Subrack', 'Slot']).last().reset_index()
    
    tab1, tab2, tab3 = st.tabs(["🚨 ALERTAS CRÍTICAS", "📍 BUSCADOR SITIOS", "📈 TENDENCIAS"])

    with tab1:
        st.subheader(f"Componentes Críticos (>= {UMBRAL_CRITICO}°C)")
        
        # Filtro por Slot dentro de la pestaña de alertas
        todos_slots_criticos = sorted(df_actual[df_actual['Temp'] >= UMBRAL_CRITICO]['Slot'].unique())
        
        if todos_slots_criticos:
            slots_alertas = st.multiselect(
                "Filtrar alertas por número de Slot:", 
                options=todos_slots_criticos, 
                default=todos_slots_criticos
            )
            
            # Aplicar filtros: Temperatura >= 65 Y Slot seleccionado
            criticos_filtrados = df_actual[
                (df_actual['Temp'] >= UMBRAL_CRITICO) & 
                (df_actual['Slot'].isin(slots_alertas))
            ].sort_values('Temp', ascending=False)
            
            if not criticos_filtrados.empty:
                cols = st.columns(4)
                for i, (_, row) in enumerate(criticos_filtrados.iterrows()):
                    with cols[i % 4]:
                        st.markdown(f"""
                            <div style="background-color:#FFC7CE; padding:15px; border-radius:10px; text-align:center; border:2px solid #9C0006; margin-bottom:10px;">
                                <p style="margin:0; font-weight:bold; color:#9C0006;">{row['Sitio']}</p>
                                <h2 style="margin:5px 0; color:#9C0006;">{row['Temp']}°C</h2>
                                <p style="margin:0; font-size:12px; color:#9C0006;">SUB {row['Subrack']} | SLOT {row['Slot']}</p>
                                <b style="color:#9C0006;">🔴 CRÍTICO</b>
                            </div>
                        """, unsafe_allow_html=True)
            else:
                st.info("No hay alertas para los slots seleccionados.")
        else:
            st.success("✅ Red Estable. No hay componentes por encima de 65°C.")

    with tab2:
        lista_s = sorted(df_actual['Sitio'].unique())
        s_sel = st.selectbox("Elegir Sitio para detalle completo", lista_s)
        df_s = df_actual[df_actual['Sitio'] == s_sel].sort_values(['Subrack', 'Slot'])
        st.dataframe(df_s[['Subrack', 'Slot', 'Temp', 'Timestamp']], use_container_width=True, hide_index=True)

    with tab3:
        # Histórico basado en los 50 archivos leídos
        s_hist = st.selectbox("Elegir sitio para ver tendencias (Últimos 50 archivos)", lista_s, key="shist")
        df_h = df[df['Sitio'] == s_hist]
        fig = px.line(df_h, x='Timestamp', y='Temp', color='ID_Full', markers=True)
        st.plotly_chart(fig, use_container_width=True)

else:
    st.warning("No se encontraron datos. Verifique los archivos en la carpeta 'Temperatura'.")
