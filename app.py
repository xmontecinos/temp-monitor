import streamlit as st
import pandas as pd
import os
import re
import plotly.express as px
import gc
from io import BytesIO
import pyarrow as pa
import pyarrow.parquet as pq

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
            c_info1.info(f"🕒 **Horario del Reporte:** {ultima_hora}")
            c_info2.success(f"📍 **Sitios Registrados en este Reporte:** {total_sitios_red}")

            df_sitios_max = df_actual.groupby('Sitio')['Temp'].max().reset_index()
            s_crit = df_sitios_max[df_sitios_max['Temp'] >= UMBRAL_CRITICO]
            s_prev = df_sitios_max[(df_sitios_max['Temp'] >= UMBRAL_PREVENTIVO) & (df_sitios_max['Temp'] < UMBRAL_CRITICO)]
            
            t_crit = df_actual[df_actual['Temp'] >= UMBRAL_CRITICO]
            t_prev = df_actual[(df_actual['Temp'] >= UMBRAL_PREVENTIVO) & (df_actual['Temp'] < UMBRAL_CRITICO)]
            t_ok = df_actual[df_actual['Temp'] < UMBRAL_PREVENTIVO]

            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Total Tarjetas", f"{len(df_actual):,}")
            with m2:
                st.markdown(f'<div style="background-color:#fee2e2; border:2px solid #dc2626; padding:15px; border-radius:10px; text-align:center;"><h4 style="color:#991b1b; margin:0;">CRÍTICO</h4><p style="color:#dc2626; margin:0; font-weight:bold;">≥ {UMBRAL_CRITICO}°C</p><h1 style="color:#dc2626; margin:5px 0; font-size:45px;">{len(t_crit)}</h1><small style="color:#991b1b;">En <b>{len(s_crit)}</b> sitios</small></div>', unsafe_allow_html=True)
            with m3:
                st.markdown(f'<div style="background-color:#fef9c3; border:2px solid #ca8a04; padding:15px; border-radius:10px; text-align:center;"><h4 style="color:#854d0e; margin:0;">PREVENTIVO</h4><p style="color:#ca8a04; margin:0; font-weight:bold;">{UMBRAL_PREVENTIVO}-{UMBRAL_CRITICO-1}°C</p><h1 style="color:#ca8a04; margin:5px 0; font-size:45px;">{len(t_prev)}</h1><small style="color:#854d0e;">En <b>{len(s_prev)}</b> sitios</small></div>', unsafe_allow_html=True)
            with m4:
                st.markdown(f'<div style="background-color:#dcfce7; border:2px solid #16a34a; padding:15px; border-radius:10px; text-align:center;"><h4 style="color:#166534; margin:0;">ÓPTIMO</h4><p style="color:#16a34a; margin:0; font-weight:bold;">< {UMBRAL_PREVENTIVO}°C</p><h1 style="color:#166534; margin:5px 0; font-size:45px;">{len(t_ok)}</h1><small style="color:#166534;">En sitios OK</small></div>', unsafe_allow_html=True)

            if not t_crit.empty:
                st.divider()
                st.subheader("🔝 Top Slots Críticos")
                res_slots = t_crit.groupby('Slot').size().reset_index(name='Cant').sort_values('Cant', ascending=False).head(10)
                res_slots['Slot_Label'] = "Slot " + res_slots['Slot'].astype(str)
                st.plotly_chart(px.bar(res_slots, x='Slot_Label', y='Cant', color='Cant', color_continuous_scale='Reds', text_auto=True), use_container_width=True)
                
                st.divider()
                st.subheader("⚠️ Detalle de Sitios Críticos por Slot")
                slot_foco = st.selectbox("Selecciona un Slot para ver sitios afectados:", sorted(t_crit['Slot'].unique()))
                df_foco = t_crit[t_crit['Slot'] == slot_foco][['Sitio', 'Temp', 'ID_Full']].sort_values('Temp', ascending=False)
                
                df_xlsx = to_excel(df_foco)
                st.download_button(label='📥 Descargar Detalle Slot a Excel', data=df_xlsx, file_name=f'criticos_slot_{slot_foco}.xlsx')
                st.dataframe(df_foco, use_container_width=True, hide_index=True)

    # --- PESTAÑA 3: HISTÓRICO (SOLUCIÓN AL ERROR DE MEMORIA Y APPEND) ---
    with tab_hist:
        st.subheader("📈 Gestión Histórica (Optimizado para +1000 archivos)")
        c1, c2 = st.columns(2)
        
        with c1:
            num_reportes = st.slider("Cantidad de archivos a procesar:", 1, len(archivos_lista), len(archivos_lista))
            if st.button("🔥 Reconstruir Base Parquet"):
                # ... [Tu lógica de reconstrucción se mantiene igual, es eficiente] ...
                st.success("Base reconstruida.")

        with c2:
            if os.path.exists(PARQUET_FILE):
                # PASO 1: Leer solo la columna de Sitios para el buscador
                # Esto es virtualmente instantáneo y no consume RAM
                tabla_sitios = pq.read_table(PARQUET_FILE, columns=['Sitio'])
                sitios_disponibles = tabla_sitios.to_pandas()['Sitio'].unique()
                
                sitio_sel = st.selectbox("🔍 Buscar Sitio en Historial:", sorted(sitios_disponibles))
            else:
                st.info("Aún no has generado la base histórica.")
                sitio_sel = None

        if sitio_sel:
            # PASO 2: Carga selectiva usando FILTROS de PyArrow
            # Solo traemos del disco los datos que necesitamos graficar
            df_s = pd.read_parquet(PARQUET_FILE, filters=[('Sitio', '==', sitio_sel)])
            
            st.divider()
            ids = sorted(df_s['ID_Full'].unique())
            sel = st.multiselect("Filtrar Slots:", ids, default=ids)
            
            if sel:
                # Graficamos solo lo necesario
                fig = px.line(df_s[df_s['ID_Full'].isin(sel)], 
                             x='Timestamp', y='Temp', color='ID_Full', 
                             markers=True,
                             title=f"Historial Térmico: {sitio_sel}")
                
                fig.add_hline(y=UMBRAL_CRITICO, line_dash="dash", line_color="red", annotation_text="CRÍTICO")
                st.plotly_chart(fig, use_container_width=True)
                
                # Liberación explícita de memoria
                del df_s
                gc.collect()

    # --- OTRAS PESTAÑAS ---
    with tab_alertas:
        crit_all = df_actual[df_actual['Temp'] >= UMBRAL_CRITICO]
        if not crit_all.empty:
            st.error(f"⚠️ Se encontraron {len(crit_all)} slots críticos.")
            st.dataframe(crit_all[['Sitio', 'Slot', 'Temp']].sort_values('Temp', ascending=False), use_container_width=True)
        else: st.success("✅ Todo OK.")

    with tab_busq:
        s_busq = st.selectbox("Buscar por Sitio:", sorted(df_actual['Sitio'].unique()))
        st.dataframe(df_actual[df_actual['Sitio'] == s_busq], use_container_width=True)

else:
    st.warning(f"⚠️ No hay archivos .txt en la carpeta '{FOLDER_PATH}'.")
