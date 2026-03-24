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

    tab_dash, tab_alertas, tab_busq, tab_hist = st.tabs([
        "📊 DASHBOARD", "🚨 ALERTAS ACTUALES", "🔍 BUSCADOR", "📈 HISTÓRICO"
    ])

    # --- PESTAÑA 0: DASHBOARD ---
    with tab_dash:
        if not df_actual.empty:
            ultima_hora = df_actual['Timestamp'].max().strftime('%d/%m/%Y %H:%M:%S')
            total_sitios_red = df_actual['Sitio'].nunique()
            st.title("📊 Monitor de Salud de Red")
            
            c_info1, c_info2 = st.columns(2)
            c_info1.info(f"🕒 **Reporte Actual:** {ultima_hora}")
            c_info2.success(f"📍 **Sitios en Red:** {total_sitios_red}")

            df_sitios_max = df_actual.groupby('Sitio')['Temp'].max().reset_index()
            s_crit = df_sitios_max[df_sitios_max['Temp'] >= UMBRAL_CRITICO]
            s_prev = df_sitios_max[(df_sitios_max['Temp'] >= UMBRAL_PREVENTIVO) & (df_sitios_max['Temp'] < UMBRAL_CRITICO)]
            
            t_crit = df_actual[df_actual['Temp'] >= UMBRAL_CRITICO]
            t_prev = df_actual[(df_actual['Temp'] >= UMBRAL_PREVENTIVO) & (df_actual['Temp'] < UMBRAL_CRITICO)]
            t_ok = df_actual[df_actual['Temp'] < UMBRAL_PREVENTIVO]

            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Total Tarjetas", f"{len(df_actual):,}")
            
            with m2:
                st.markdown(f'<div style="background-color:#fee2e2; border:2px solid #dc2626; padding:15px; border-radius:10px; text-align:center;"><h4 style="color:#991b1b; margin:0;">CRÍTICO</h4><h1 style="color:#dc2626; margin:5px 0;">{len(t_crit)}</h1><small>En {len(s_crit)} sitios</small></div>', unsafe_allow_html=True)
            with m3:
                st.markdown(f'<div style="background-color:#fef9c3; border:2px solid #ca8a04; padding:15px; border-radius:10px; text-align:center;"><h4 style="color:#854d0e; margin:0;">PREVENTIVO</h4><h1 style="color:#ca8a04; margin:5px 0;">{len(t_prev)}</h1><small>En {len(s_prev)} sitios</small></div>', unsafe_allow_html=True)
            with m4:
                st.markdown(f'<div style="background-color:#dcfce7; border:2px solid #16a34a; padding:15px; border-radius:10px; text-align:center;"><h4 style="color:#166534; margin:0;">ÓPTIMO</h4><h1 style="color:#166534; margin:5px 0;">{len(t_ok)}</h1><small>Sistema Estable</small></div>', unsafe_allow_html=True)

            if not t_crit.empty:
                st.divider()
                st.subheader("🔝 Top Slots Críticos")
                res_slots = t_crit.groupby('Slot').size().reset_index(name='Cant').sort_values('Cant', ascending=False).head(10)
                res_slots['Slot_Label'] = "Slot " + res_slots['Slot'].astype(str)
                st.plotly_chart(px.bar(res_slots, x='Slot_Label', y='Cant', color='Cant', color_continuous_scale='Reds'), use_container_width=True)

    # --- PESTAÑA 3: HISTÓRICO (OPTIMIZADA) ---
    with tab_hist:
        st.subheader("📈 Gestión Histórica de Gran Volumen")
        c1, c2 = st.columns(2)
        
        with c1:
            num_reportes = st.slider("Cantidad de archivos a procesar:", 1, len(archivos_lista), min(100, len(archivos_lista)))
            if st.button("🔥 Reconstruir Base Parquet (Modo Seguro)"):
                progreso_bar = st.progress(0)
                texto_estado = st.empty()
                
                # Eliminar base anterior para evitar duplicados si se reconstruye
                if os.path.exists(PARQUET_FILE):
                    os.remove(PARQUET_FILE)

                for i, p in enumerate(archivos_lista[:num_reportes]):
                    texto_estado.text(f"Procesando {i+1}/{num_reportes}: {os.path.basename(p)}")
                    progreso_bar.progress((i + 1) / num_reportes)
                    
                    data = extraer_datos_masivo(p)
                    if data:
                        temp_df = pd.DataFrame(data)
                        
                        # Optimización 1: Reducir tipos de datos para ahorrar RAM
                        temp_df['Slot'] = temp_df['Slot'].astype('int16')
                        temp_df['Temp'] = temp_df['Temp'].astype('int16')
                        
                        # Optimización 2: Agrupar por hora ANTES de guardar (reduce filas drásticamente)
                        temp_df = temp_df.groupby([temp_df['Timestamp'].dt.floor('h'), 'Sitio', 'ID_Full'])[['Temp', 'Slot']].max().reset_index()
                        
                        # Optimización 3: Escritura incremental (Append)
                        if not os.path.exists(PARQUET_FILE):
                            temp_df.to_parquet(PARQUET_FILE, engine='pyarrow', index=False)
                        else:
                            temp_df.to_parquet(PARQUET_FILE, engine='pyarrow', index=False, append=True)
                    
                    # Optimización 4: Forzar limpieza de memoria
                    if i % 10 == 0:
                        gc.collect()
                
                texto_estado.success("✅ Base Parquet actualizada correctamente.")

        with c2:
            if st.button("📂 Cargar Datos desde Disco"):
                if os.path.exists(PARQUET_FILE):
                    # Solo cargamos las columnas necesarias para liberar RAM
                    st.session_state["df_full"] = pd.read_parquet(PARQUET_FILE)
                    st.success(f"✅ Cargados {len(st.session_state['df_full']):,} registros.")
                else: 
                    st.error("No existe el archivo. Genéralo primero.")

        if "df_full" in st.session_state:
            st.divider()
            df_p = st.session_state["df_full"]
            sitio_sel = st.selectbox("Seleccionar Sitio Histórico:", sorted(df_p['Sitio'].unique()))
            
            df_s = df_p[df_p['Sitio'] == sitio_sel].copy()
            ids = sorted(df_s['ID_Full'].unique())
            sel = st.multiselect("Comparar Slots:", ids, default=ids[:2] if ids else [])
            
            if sel:
                fig = px.line(df_s[df_s['ID_Full'].isin(sel)], 
                             x='Timestamp', y='Temp', color='ID_Full', markers=True,
                             title=f"Evolución Térmica: {sitio_sel}")
                fig.add_hline(y=UMBRAL_CRITICO, line_dash="dash", line_color="red", annotation_text="CRÍTICO")
                st.plotly_chart(fig, use_container_width=True)

    # --- PESTAÑA ALERTAS Y BUSCADOR ---
    with tab_alertas:
        crit_all = df_actual[df_actual['Temp'] >= UMBRAL_CRITICO]
        if not crit_all.empty:
            st.warning(f"Se detectaron {len(crit_all)} tarjetas por encima de {UMBRAL_CRITICO}°C")
            st.dataframe(crit_all[['Sitio', 'Slot', 'Temp']].sort_values('Temp', ascending=False), use_container_width=True)
        else: st.success("✅ No hay alertas críticas actuales.")

    with tab_busq:
        s = st.selectbox("Filtrar por nombre de sitio:", sorted(df_actual['Sitio'].unique()))
        st.dataframe(df_actual[df_actual['Sitio'] == s], use_container_width=True)

else:
    st.warning(f"⚠️ No se encontraron archivos .txt en la carpeta '{FOLDER_PATH}'.")
