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

def to_excel(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Datos')
    return output.getvalue()

# --- PROCESAMIENTO INICIAL ---
archivos_lista = listar_archivos(FOLDER_PATH)

if archivos_lista:
    if "df_now" not in st.session_state:
        st.session_state["df_now"] = pd.DataFrame(extraer_datos_masivo(archivos_lista[0]))
    
    df_actual = st.session_state["df_now"]

    tab_dash, tab_alertas, tab_busq, tab_hist, tab_slot = st.tabs([
        "📊 DASHBOARD", "🚨 ALERTAS ACTUALES", "🔍 BUSCADOR", "📈 HISTÓRICO SITIO", "🌐 VISTA RED POR SLOT"
    ])

    # --- PESTAÑA 0: DASHBOARD ---
    with tab_dash:
        if not df_actual.empty:
            ultima_hora = df_actual['Timestamp'].max().strftime('%d/%m/%Y %H:%M:%S')
            total_sitios_red = df_actual['Sitio'].nunique()
            st.title("📊 Monitor de Salud de Red")
            c_info1, c_info2 = st.columns(2)
            c_info1.info(f"🕒 **Reporte:** {ultima_hora}")
            c_info2.success(f"📍 **Sitios:** {total_sitios_red}")

            df_sitios_max = df_actual.groupby('Sitio')['Temp'].max().reset_index()
            s_crit = df_sitios_max[df_sitios_max['Temp'] >= UMBRAL_CRITICO]
            s_prev = df_sitios_max[(df_sitios_max['Temp'] >= UMBRAL_PREVENTIVO) & (df_sitios_max['Temp'] < UMBRAL_CRITICO)]
            
            t_crit = df_actual[df_actual['Temp'] >= UMBRAL_CRITICO]
            t_prev = df_actual[(df_actual['Temp'] >= UMBRAL_PREVENTIVO) & (df_actual['Temp'] < UMBRAL_CRITICO)]
            t_ok = df_actual[df_actual['Temp'] < UMBRAL_PREVENTIVO]

            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Total Tarjetas", f"{len(df_actual):,}")
            with m2: st.error(f"CRÍTICO: {len(t_crit)}")
            with m3: st.warning(f"PREV: {len(t_prev)}")
            with m4: st.success(f"OK: {len(t_ok)}")

    # --- PESTAÑA 3: HISTÓRICO SITIO ---
    with tab_hist:
        st.subheader("⚙️ Configuración de Base de Datos")
        c1, c2 = st.columns(2)
        with c1:
            num_reportes = st.slider("Archivos para procesar:", 1, len(archivos_lista), min(50, len(archivos_lista)))
            if st.button("🔥 Actualizar Parquet"):
                all_dfs = []
                progreso = st.progress(0)
                for i, p in enumerate(archivos_lista[:num_reportes]):
                    progreso.progress((i + 1) / num_reportes)
                    data = extraer_datos_masivo(p)
                    if data:
                        temp_df = pd.DataFrame(data)
                        temp_df = temp_df.groupby([temp_df['Timestamp'].dt.floor('h'), 'Sitio', 'Slot', 'ID_Full'])['Temp'].max().reset_index()
                        all_dfs.append(temp_df)
                if all_dfs:
                    df_final = pd.concat(all_dfs, ignore_index=True)
                    df_final.to_parquet(PARQUET_FILE, index=False)
                    st.session_state["df_full"] = df_final
                    st.success("Base lista.")

        with c2:
            if st.button("📂 Cargar Histórico"):
                if os.path.exists(PARQUET_FILE):
                    st.session_state["df_full"] = pd.read_parquet(PARQUET_FILE)
                    st.success("Datos cargados.")

    # --- PESTAÑA 4: VISTA RED POR SLOT (OPTIMIZADA PARA TODA LA RED) ---
    with tab_slot:
        st.subheader("🌐 Análisis Global de Hardware (Toda la Red)")
        
        if "df_full" in st.session_state:
            df_p = st.session_state["df_full"]
            
            # Filtros de Red
            col_f1, col_f2 = st.columns(2)
            with col_f1:
                slots_disponibles = sorted(df_p['Slot'].unique())
                slot_sel = st.selectbox("Analizar Comportamiento Global del Slot No.:", slots_disponibles)
            
            with col_f2:
                umbral_ver = st.slider("Mostrar solo sitios con Temp mayor a:", 30, 85, 60)

            # Filtrar datos de toda la red para ese slot
            df_red_slot = df_p[df_p['Slot'] == slot_sel]
            
            # 1. Gráfico de Tendencia (Top 20 sitios más calientes en ese slot)
            sitios_calientes = df_red_slot.groupby('Sitio')['Temp'].max().sort_values(ascending=False).head(20).index
            df_plot_red = df_red_slot[df_red_slot['Sitio'].isin(sitios_calientes)]
            
            st.write(f"### 🔥 Evolución Térmica: Top 20 Sitios con Slot {slot_sel} más caliente")
            fig_red = px.line(
                df_plot_red, 
                x='Timestamp', 
                y='Temp', 
                color='Sitio',
                title=f"Comportamiento del Slot {slot_sel} en la Red (Filtro: Top Sitios)",
                labels={'Temp': 'Grados Celsius'}
            )
            fig_red.add_hline(y=UMBRAL_CRITICO, line_dash="dash", line_color="red")
            st.plotly_chart(fig_red, use_container_width=True)

            # 2. Resumen General de la Red para ese Slot
            st.divider()
            st.write(f"### 📊 Estado de todos los Slot {slot_sel} en la red (> {umbral_ver}°C)")
            
            # Agrupar datos para ver el peor caso por sitio
            resumen_red = df_red_slot.groupby('Sitio').agg({
                'Temp': ['max', 'mean'],
                'Timestamp': 'max'
            }).reset_index()
            resumen_red.columns = ['Sitio', 'Temp Máx', 'Temp Promedio', 'Último Registro']
            
            # Filtrar por el umbral del slider
            resumen_filtrado = resumen_red[resumen_red['Temp Máx'] >= umbral_ver].sort_values('Temp Máx', ascending=False)
            
            st.dataframe(
                resumen_filtrado.style.background_gradient(subset=['Temp Máx'], cmap='YlOrRd'),
                use_container_width=True, 
                hide_index=True
            )
            
            # Botón de descarga para toda la red
            csv_red = resumen_filtrado.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Descargar Reporte de Red (Slot seleccionado)", csv_red, f"reporte_red_slot_{slot_sel}.csv", "text/csv")

        else:
            st.warning("⚠️ Carga el histórico en la pestaña anterior para ver la vista de red.")

    # --- PESTAÑAS RESTANTES ---
    with tab_alertas:
        crit_all = df_actual[df_actual['Temp'] >= UMBRAL_CRITICO]
        if not crit_all.empty:
            for _, r in crit_all.iterrows(): st.error(f"⚠️ {r['Sitio']} - Slot {r['Slot']}: {r['Temp']}°C")
    
    with tab_busq:
        s = st.selectbox("Buscar Sitio:", sorted(df_actual['Sitio'].unique()))
        st.dataframe(df_actual[df_actual['Sitio'] == s], use_container_width=True)
else:
    st.warning(f"⚠️ No hay archivos en '{FOLDER_PATH}'.")
