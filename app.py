import streamlit as st
import pandas as pd
import os
import re
import plotly.express as px

# 1. Configuración de página
st.set_page_config(page_title="Monitor Red", layout="wide")

UMBRAL_CRITICO = 78 
UMBRAL_PREVENTIVO = 60
FOLDER_PATH = 'Temperatura'

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
                        "Timestamp": ts, "Sitio": nombre_sitio, "Slot": int(r[1]),
                        "Temp": int(r[2]), "ID_Full": f"{nombre_sitio} (S:{r[0]}-L:{r[1]})"
                    })
    except Exception: pass
    return rows

@st.cache_data(ttl=60)
def listar_archivos(folder):
    if not os.path.exists(folder): return []
    fs = [os.path.join(folder, f) for f in os.listdir(folder) if ".txt" in f]
    fs.sort(key=lambda x: "".join(re.findall(r'\d+', x)), reverse=True)
    return fs

# --- PROCESAMIENTO ---
archivos_lista = listar_archivos(FOLDER_PATH)

if archivos_lista:
    if "df_now" not in st.session_state:
        st.session_state["df_now"] = pd.DataFrame(extraer_datos_masivo(archivos_lista[0]))
    
    df_actual = st.session_state["df_now"]

    tab_dash, tab_alertas, tab_busq, tab_hist = st.tabs([
        "📊 DASHBOARD", "🚨 ALERTAS ACTUALES", "🔍 BUSCADOR", "📈 HISTÓRICO"
    ])

    with tab_dash:
        if not df_actual.empty:
            # Metadatos
            ultima_hora = df_actual['Timestamp'].max().strftime('%d/%m/%Y %H:%M:%S')
            total_sitios_red = df_actual['Sitio'].nunique()
            
            st.title("📊 Monitor de Salud de Red")
            c_info1, c_info2 = st.columns(2)
            c_info1.info(f"🕒 **Último reporte:** {ultima_hora}")
            c_info2.info(f"📍 **Sitios únicos procesados:** {total_sitios_red}")

            # --- LÓGICA DE JERARQUÍA PARA QUE LOS SITIOS CUADREN ---
            # Sacamos la temperatura máxima por sitio
            df_sitios_max = df_actual.groupby('Sitio')['Temp'].max().reset_index()
            
            # Clasificamos sitios (Un sitio pertenece a UNA sola categoría)
            s_crit = df_sitios_max[df_sitios_max['Temp'] >= UMBRAL_CRITICO]
            s_prev = df_sitios_max[(df_sitios_max['Temp'] >= UMBRAL_PREVENTIVO) & (df_sitios_max['Temp'] < UMBRAL_CRITICO)]
            s_ok = df_sitios_max[df_sitios_max['Temp'] < UMBRAL_PREVENTIVO]

            # Clasificación de TARJETAS (conteo individual)
            t_crit = df_actual[df_actual['Temp'] >= UMBRAL_CRITICO]
            t_prev = df_actual[(df_actual['Temp'] >= UMBRAL_PREVENTIVO) & (df_actual['Temp'] < UMBRAL_CRITICO)]
            t_ok = df_actual[df_actual['Temp'] < UMBRAL_PREVENTIVO]

            # Semáforo
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Total Tarjetas", len(df_actual))
            
            with m2:
                st.markdown(f'<div style="background-color:#fee2e2; border:1px solid #dc2626; padding:15px; border-radius:10px; text-align:center;"><h4 style="color:#991b1b; margin:0;">CRÍTICO</h4><h1 style="color:#dc2626; margin:0;">{len(t_crit)}</h1><small>En <b>{len(s_crit)}</b> sitios</small></div>', unsafe_allow_html=True)
            with m3:
                st.markdown(f'<div style="background-color:#fef9c3; border:1px solid #ca8a04; padding:15px; border-radius:10px; text-align:center;"><h4 style="color:#854d0e; margin:0;">PREVENTIVO</h4><h1 style="color:#ca8a04; margin:0;">{len(t_prev)}</h1><small>En <b>{len(s_prev)}</b> sitios</small></div>', unsafe_allow_html=True)
            with m4:
                st.markdown(f'<div style="background-color:#dcfce7; border:1px solid #16a34a; padding:15px; border-radius:10px; text-align:center;"><h4 style="color:#166534; margin:0;">ÓPTIMO</h4><h1 style="color:#16a34a; margin:0;">{len(t_ok)}</h1><small>En <b>{len(s_ok)}</b> sitios</small></div>', unsafe_allow_html=True)

            st.divider()
            
            if not t_crit.empty:
                st.subheader("🔝 Top 10 Slots Críticos (Toda la Red)")
                res_slots = t_crit.groupby('Slot').size().reset_index(name='Cant').sort_values('Cant', ascending=False).head(10)
                res_slots['Slot_Label'] = "Slot " + res_slots['Slot'].astype(str)
                fig_bar = px.bar(res_slots, x='Slot_Label', y='Cant', text='Cant', color='Cant', color_continuous_scale='Reds')
                st.plotly_chart(fig_bar, use_container_width=True)

                st.subheader("🔍 Detalle de Sitios por Slot")
                slot_sel = st.selectbox("Seleccione un Slot:", res_slots['Slot'].tolist())
                if slot_sel:
                    sitios_det = t_crit[t_crit['Slot'] == slot_sel].sort_values('Temp', ascending=False)
                    st.dataframe(sitios_det[['Sitio', 'Temp']], hide_index=True, use_container_width=True)

    with tab_alertas:
        crit_all = df_actual[df_actual['Temp'] >= UMBRAL_CRITICO]
        if not crit_all.empty:
            cols = st.columns(4)
            for i, (_, r) in enumerate(crit_all.sort_values('Temp', ascending=False).iterrows()):
                with cols[i % 4]:
                    st.markdown(f'<div style="background-color:#fee2e2; border:1px solid #dc2626; padding:10px; border-radius:8px; margin-bottom:10px; text-align:center;"><strong style="color:#991b1b;">{r["Sitio"]}</strong><br><span style="font-size:24px; font-weight:bold; color:#dc2626;">{r["Temp"]}°C</span><br><small>Slot: {r["Slot"]}</small></div>', unsafe_allow_html=True)
        else: st.success("✅ Red estable.")

    with tab_busq:
        sitio_busq = st.selectbox("Buscar Sitio:", sorted(df_actual['Sitio'].unique()))
        st.dataframe(df_actual[df_actual['Sitio'] == sitio_busq], use_container_width=True)

    with tab_hist:
        num_reportes = st.slider("Reportes:", 10, min(180, len(archivos_lista)), 100)
        if st.button("📊 Cargar Histórico"):
            all_data = []
            for p in archivos_lista[:num_reportes]: all_data.extend(extraer_datos_masivo(p))
            if all_data:
                df_h = pd.DataFrame(all_data)
                st.session_state["df_full"] = df_h.groupby([df_h['Timestamp'].dt.floor('h'), 'Sitio', 'ID_Full'])['Temp'].max().reset_index()

        if "df_full" in st.session_state:
            df_p = st.session_state["df_full"]
            sitio_sel = st.selectbox("Sitio Histórico:", sorted(df_p['Sitio'].unique()))
            df_s = df_p[df_p['Sitio'] == sitio_sel]
            ids = sorted(df_s['ID_Full'].unique())
            sel = st.multiselect("ID:", ids, default=ids)
            if sel:
                fig_l = px.line(df_s[df_s['ID_Full'].isin(sel)], x='Timestamp', y='Temp', color='ID_Full')
                st.plotly_chart(fig_l, use_container_width=True)
else:
    st.error("No hay archivos en 'Temperatura'.")
