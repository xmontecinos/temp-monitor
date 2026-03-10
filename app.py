import streamlit as st
import pandas as pd
import os
import re
import plotly.express as px
import gc

st.set_page_config(page_title="Monitor Red - Final Fix", layout="wide")

# --- CONFIGURACIÓN ---
UMBRAL_CRITICO = 65 
FOLDER_PATH = 'Temperatura'

def limpiar_texto(texto):
    """Elimina caracteres no imprimibles que bloquean la lectura en archivos .gz.txt"""
    return "".join(char for char in texto if char.isprintable() or char in '\n\r\t')

def extraer_datos_robusto(path):
    """Lectura ultra-robusta ignorando errores de formato binario."""
    rows = []
    try:
        with open(path, 'r', encoding='latin-1', errors='ignore') as f:
            # Leemos el archivo completo pero lo limpiamos de caracteres binarios
            content = f.read()
            clean_content = limpiar_texto(content)
            
            # Buscamos el sitio y la fecha con Regex flexible
            ne_match = re.search(r'NE Name:\s*([^\n\r]+)', clean_content)
            sitio = ne_match.group(1).strip() if ne_match else "Desconocido"
            
            ts_match = re.search(r'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})', clean_content)
            ts = pd.to_datetime(f"{ts_match.group(1)} {ts_match.group(2)}") if ts_match else None
            
            # Extraer todas las filas de la tabla de temperatura
            # Formato: Subrack Slot Temperatura
            table_rows = re.findall(r'^\s*\d+\s+(\d+)\s+(\d+)\s+(\d+)', clean_content, re.MULTILINE)
            
            for r in table_rows:
                if ts:
                    rows.append({
                        "Timestamp": ts, "Sitio": sitio,
                        "Subrack": r[0], "Slot": int(r[1]),
                        "Temp": int(r[2]),
                        "ID_Full": f"{sitio} (S:{r[0]}-L:{r[1]})"
                    })
    except Exception as e:
        print(f"Error en {path}: {e}")
    return rows

@st.cache_data(ttl=60)
def listar_archivos(folder):
    if not os.path.exists(folder): return []
    # Buscamos cualquier archivo que tenga la extensión .txt o .gz
    fs = [os.path.join(folder, f) for f in os.listdir(folder) if ".txt" in f or ".gz" in f]
    fs.sort(key=lambda x: "".join(re.findall(r'\d+', x)), reverse=True)
    return fs

# --- APP ---
archivos = listar_archivos(FOLDER_PATH)

if archivos:
    # 1. Carga del reporte más reciente
    if "df_now" not in st.session_state or st.sidebar.button("♻️ Forzar Recarga"):
        st.session_state["df_now"] = pd.DataFrame(extraer_datos_robusto(archivos[0]))
    
    df_actual = st.session_state["df_now"]
    tab1, tab2, tab3 = st.tabs(["🚨 ALERTAS", "🔍 BUSCADOR", "📈 HISTÓRICO 100H"])

    with tab1:
        if not df_actual.empty:
            st.info(f"Reporte: {os.path.basename(archivos[0])}")
            slots = sorted(df_actual['Slot'].unique())
            sel_slots = st.multiselect("Filtrar Slots:", slots, default=slots)
            
            criticos = df_actual[(df_actual['Temp'] >= UMBRAL_CRITICO) & (df_actual['Slot'].isin(sel_slots))]
            if not criticos.empty:
                cols = st.columns(4)
                for i, (_, r) in enumerate(criticos.sort_values('Temp', ascending=False).iterrows()):
                    with cols[i % 4]:
                        st.error(f"**{r['Sitio']}**\n\n# {r['Temp']}°C\nSlot: {r['Slot']}")
            else: st.success("✅ Sin alertas.")
        else:
            st.warning("No se pudieron extraer datos del último archivo. Verifica el formato.")

    with tab3:
        st.subheader("Histórico de 100 Horas")
        if st.button("🚀 Iniciar Carga Pesada"):
            all_data = []
            bar = st.progress(0)
            # Procesamos 100 archivos
            limite = archivos[:100]
            for i, p in enumerate(limite):
                all_data.extend(extraer_datos_robusto(p))
                bar.progress((i + 1) / len(limite))
                if i % 20 == 0: gc.collect()
            
            if all_data:
                df_h = pd.DataFrame(all_data)
                df_h['Hora'] = df_h['Timestamp'].dt.floor('h')
                st.session_state["df_h"] = df_h.groupby(['Hora', 'Sitio', 'ID_Full'])['Temp'].max().reset_index()
                st.success("✅ Datos cargados correctamente.")
            else:
                st.error("Error crítico: Los archivos no contienen el formato esperado.")

        if "df_h" in st.session_state:
            df_p = st.session_state["df_h"]
            sitio = st.selectbox("Sitio:", sorted(df_p['Sitio'].unique()))
            fig = px.line(df_p[df_p['Sitio'] == sitio], x='Hora', y='Temp', color='ID_Full', markers=True)
            st.plotly_chart(fig, use_container_width=True)
else:
    st.error("Carpeta 'Temperatura' no encontrada o vacía.")
