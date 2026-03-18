import streamlit as st
import pandas as pd
import os
import re
import plotly.express as px
import gc
from io import BytesIO

# 1. CONFIGURACIÓN DE PÁGINA
st.set_page_config(page_title="Monitor Red - Huawei", layout="wide")

# UMBRALES
UMBRAL_CRITICO = 78 
UMBRAL_PREVENTIVO = 65
FOLDER_PATH = 'Temperatura'
PARQUET_FILE = 'base_historica.parquet'

# --- FUNCIONES DE EXTRACCIÓN ---
def extraer_datos_masivo(path):
    rows = []
    try:
        with open(path, 'r', encoding='latin-1', errors='ignore') as f:
            content = f.read()
            ts_match = re.search(r'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})', content)
            if not ts_match: return []
            ts = pd.to_datetime(f"{ts_match.group(1)} {ts_match.group(2)}")
            
            bloques = re.split(r'NE Name\s*:\s*', content)
            for bloque in bloques[1:]:
                lineas = bloque.split('\n')
                if not lineas: continue
                nombre_sitio = lineas[0].strip().split()[0]
                
                # Captura Subrack, Slot y Temp
                filas = re.findall(r'^\s*\d+\s+(\d+)\s+(\d+)\s+(\d+)', bloque, re.MULTILINE)
                for r in filas:
                    rows.append({
                        "Timestamp": ts, 
                        "Sitio": nombre_sitio, 
                        "Slot": int(r[1]),
                        "Temp": int(r[2]), 
                        "ID_Full": f"{nombre_sitio} (S:{r[0]}-L:{r[1]})"
                    })
    except Exception: pass
    return rows

@st.cache_data(ttl=60)
def listar_archivos(folder):
    if not os.path.exists(folder): 
        os.makedirs(folder, exist_ok=True)
        return []
    fs = [os.path.join(folder, f) for f in os.listdir(folder) if ".txt" in f]
    fs.sort(key=lambda x: "".join(re.findall(r'\d+', x)), reverse=True)
    return fs

# --- PROCESAMIENTO INICIAL ---
archivos_lista = listar_archivos(FOLDER_PATH)

if archivos_lista:
    if "df_now" not in st.session_state:
        st.session_state["df_now"] = pd.DataFrame(extraer_datos_masivo(archivos_lista[0]))
    
    df_actual = st.session_state["df_now"]

    tab_dash, tab_alertas, tab_busq, tab_hist, tab_red_slots = st.tabs([
        "📊 DASHBOARD", "🚨 ALERTAS", "🔍 BUSCADOR", "📈 HISTÓRICO", "🎰 COMPARATIVA SLOTS"
    ])

    # --- PESTAÑA HISTÓRICO (GESTIÓN DE DATOS) ---
    with tab_hist:
        st.subheader("⚙️ Gestión de Datos Históricos")
        c1, c2 = st.columns(2)
        with c1:
            num_reportes = st.slider("Cantidad de reportes a procesar:", 1, len(archivos_lista), min(50, len(archivos_lista)))
            if st.button("🔥 Generar Base Parquet"):
                all_dfs = []
                progreso = st.progress(0)
                for i, p in enumerate(archivos_lista[:num_reportes]):
                    progreso.progress((i + 1) / num_reportes)
                    data = extraer_datos_masivo(p)
                    if data:
                        temp_df = pd.DataFrame(data)
                        # Consolidar por hora para no saturar memoria
                        temp_df = temp_df.groupby([temp_df['Timestamp'].dt.floor('h'), 'Sitio', 'Slot'])['Temp'].max().reset_index()
                        all_dfs.append(temp_df)
                if all_dfs:
                    df_final = pd.concat(all_dfs, ignore_index=True)
                    df_final.to_parquet(PARQUET_FILE, index=False)
                    st.session_state["df_full"] = df_final
                    st.success("✅ Base histórica actualizada.")

        with c2:
            if st.button("📂 Cargar Datos"):
                if os.path.exists(PARQUET_FILE):
                    st.session_state["df_full"] = pd.read_parquet(PARQUET_FILE)
                    st.success("✅ Datos cargados correctamente.")

    # --- PESTAÑA 4: COMPARATIVA POR SLOTS (VISIÓN RED) ---
    with tab_red_slots:
        st.subheader("🌐 Análisis de Temperatura por Slot (Toda la Red)")
        
        if "df_full" in st.session_state:
            df_p = st.session_state["df_full"]
            
            # Filtrar solo slots del 0 al 7 como solicitaste
            df_07 = df_p[df_p['Slot'].isin(range(0, 8))].copy()
            df_07['Slot_Label'] = "Slot " + df_07['Slot'].astype(str).str.zfill(2)

            # 1. Gráfico de Barras: Máximas Históricas por Slot
            st.write("### 🔥 Temperaturas Máximas por Slot (Red Completa)")
            df_max_slots = df_07.groupby('Slot_Label')['Temp'].max().reset_index()
            
            fig_bar = px.bar(
                df_max_slots, 
                x='Slot_Label', 
                y='Temp', 
                color='Temp',
                color_continuous_scale='Reds',
                text_auto=True,
                title="Picos de Temperatura detectados por cada Slot"
            )
            fig_bar.add_hline(y=UMBRAL_CRITICO, line_dash="dash", line_color="red", annotation_text="CRÍTICO")
            st.plotly_chart(fig_bar, use_container_width=True)

            # 2. Gráfico de Líneas: Evolución del Promedio de la Red por Slot
            st.write("### 📈 Evolución Promedio de la Red (Comparativa Slots 00-07)")
            df_evolucion = df_07.groupby(['Timestamp', 'Slot_Label'])['Temp'].mean().reset_index()
            
            fig_line = px.line(
                df_evolucion, 
                x='Timestamp', 
                y='Temp', 
                color='Slot_Label',
                title="Tendencia Térmica Promedio: ¿Qué slot se calienta más en el tiempo?"
            )
            st.plotly_chart(fig_line, use_container_width=True)

            # 3. Detalle de Sitios Críticos para los Slots 0-7
            st.divider()
            st.write("### 🚨 Sitios con mayor temperatura en estos Slots")
            
            resumen_slots = df_07.groupby(['Sitio', 'Slot_Label'])['Temp'].max().reset_index()
            # Filtrar solo lo que está cerca o sobre el preventivo para limpiar la vista
            resumen_slots = resumen_slots[resumen_slots['Temp'] >= UMBRAL_PREVENTIVO].sort_values('Temp', ascending=False)
            
            st.dataframe(
                resumen_slots.style.background_gradient(cmap='OrRd', subset=['Temp']),
                use_container_width=True,
                hide_index=True
            )
            
        else:
            st.info("💡 Por favor, ve a la pestaña '📈 HISTÓRICO' y presiona 'Cargar Datos' o 'Generar Base'.")

    # --- RESTO DE PESTAÑAS (DASHBOARD ACTUAL) ---
    with tab_dash:
        if not df_actual.empty:
            st.title(f"📊 Estado Actual - {df_actual['Timestamp'].max()}")
            m1, m2 = st.columns(2)
            m1.metric("Sitios Reportando", df_actual['Sitio'].nunique())
            m2.metric("Alertas Críticas", len(df_actual[df_actual['Temp'] >= UMBRAL_CRITICO]))
            st.dataframe(df_actual.sort_values('Temp', ascending=False).head(10), use_container_width=True)

    with tab_alertas:
        criticos = df_actual[df_actual['Temp'] >= UMBRAL_CRITICO]
        if not criticos.empty:
            st.error(f"Se detectaron {len(criticos)} tarjetas en estado crítico.")
            st.table(criticos[['Sitio', 'Slot', 'Temp']])
        else:
            st.success("No hay alertas críticas en el último reporte.")

    with tab_busq:
        busqueda = st.text_input("Ingrese nombre del Sitio:")
        if busqueda:
            st.dataframe(df_actual[df_actual['Sitio'].str.contains(busqueda, case=False)], use_container_width=True)

else:
    st.warning(f"⚠️ No se encontraron archivos .txt en '{FOLDER_PATH}'.")
