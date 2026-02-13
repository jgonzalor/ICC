# pages/03_manzanas_por_seccion.py
# 📌 Conteo de manzanas (cuadras) por Sección Electoral (INE x INEGI) + mapa con relieve/topo/calles
#
# ✅ Qué hace:
# 1) Subes 2 ZIPs (shapefile completo):
#    - INE: Secciones electorales (Base Geográfica Digital)
#    - INEGI: Manzanas (Marco Geoestadístico)
# 2) Hace un spatial join (intersect) y cuenta cuántas manzanas caen dentro de cada sección.
# 3) Mapa Folium con selector de basemap: Relieve (Esri) / Topográfico (OpenTopoMap) / Calles (OSM)
# 4) Tabla + export a CSV
#
# Reqs (mínimo):
#   streamlit
#   geopandas
#   shapely
#   folium
#   streamlit_folium
#   pandas

from __future__ import annotations

import os
import zipfile
import tempfile
from io import BytesIO
from typing import Optional, List, Tuple

import pandas as pd
import streamlit as st
import geopandas as gpd
import folium
from streamlit_folium import st_folium


# =========================
# Opcional: Integración Suite
# =========================
try:
    from guardian import login_guard
    from suite_nav import render_suite_sidebar

    login_guard()
    render_suite_sidebar()
except Exception:
    # Si no existe guardian/nav, corre standalone sin broncas
    pass


# =========================
# Config UI
# =========================
st.set_page_config(layout="wide", page_title="Manzanas por Sección (INE x INEGI)")
st.title("🧭 Manzanas (cuadras) por Sección Electoral — INE x INEGI")
st.caption(
    "Colega: sube 2 ZIPs (shapefiles completos). Se calcula cuántas manzanas (INEGI) hay dentro de cada sección (INE)."
)

# =========================
# Helpers
# =========================
def extract_zip_to_folder(uploaded_zip, folder: str) -> str:
    """Guarda el ZIP subido y lo extrae en folder."""
    zpath = os.path.join(folder, "input.zip")
    with open(zpath, "wb") as f:
        f.write(uploaded_zip.getbuffer())
    with zipfile.ZipFile(zpath, "r") as z:
        z.extractall(folder)
    return folder


def find_first_shp(folder: str) -> Optional[str]:
    """Busca el primer .shp dentro de un folder (recursivo)."""
    for root, _, files in os.walk(folder):
        for fn in files:
            if fn.lower().endswith(".shp"):
                return os.path.join(root, fn)
    return None


def pick_col(gdf: gpd.GeoDataFrame, candidates: List[str]) -> Optional[str]:
    """Detecta una columna por lista de candidatos (case-insensitive)."""
    cols_upper = {c.upper(): c for c in gdf.columns}
    for cand in candidates:
        if cand.upper() in cols_upper:
            return cols_upper[cand.upper()]
    return None


def ensure_crs(gdf: gpd.GeoDataFrame, default_epsg: int = 4326) -> gpd.GeoDataFrame:
    """Garantiza CRS; si falta, asume EPSG:4326; luego convierte a EPSG:4326."""
    if gdf.crs is None:
        st.warning(f"⚠️ La capa '{getattr(gdf, 'name', 'sin_nombre')}' no trae CRS. Asumiendo EPSG:{default_epsg}.")
        gdf = gdf.set_crs(epsg=default_epsg)
    return gdf.to_crs(epsg=4326)


def safe_center(gdf: gpd.GeoDataFrame) -> Tuple[float, float]:
    """Centro aproximado (lat, lon) usando centroid del bounds."""
    minx, miny, maxx, maxy = gdf.total_bounds
    cx = (minx + maxx) / 2
    cy = (miny + maxy) / 2
    return (cy, cx)


def add_basemap(m: folium.Map, basemap: str) -> None:
    """Agrega un basemap según selección."""
    if basemap == "Relieve (Esri)":
        folium.TileLayer(
            tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Shaded_Relief/MapServer/tile/{z}/{y}/{x}.jpg",
            name="Relieve (Esri)",
            attr="Tiles © Esri",
            overlay=False,
            control=True,
        ).add_to(m)

    elif basemap == "Topográfico (OpenTopoMap)":
        folium.TileLayer(
            tiles="https://tile.openmaps.fr/opentopomap/{z}/{x}/{y}.png",
            name="Topográfico (OpenTopoMap)",
            attr="© OpenTopoMap / © OpenStreetMap contributors",
            overlay=False,
            control=True,
        ).add_to(m)

    else:  # "Calles (OSM)"
        folium.TileLayer(
            tiles="https://tile.openstreetmap.org/{z}/{x}/{y}.png",
            name="Calles (OSM)",
            attr="© OpenStreetMap contributors",
            overlay=False,
            control=True,
        ).add_to(m)


def style_by_bins(v: int) -> dict:
    """Estilo simple por rangos (sin complicarnos con colormap)."""
    # Nota: Folium acepta colores HEX.
    if v == 0:
        return {"weight": 1, "fillOpacity": 0.08, "color": "#444444"}
    if v <= 20:
        return {"weight": 1, "fillOpacity": 0.18, "color": "#1f77b4"}
    if v <= 50:
        return {"weight": 1, "fillOpacity": 0.22, "color": "#ff7f0e"}
    return {"weight": 1, "fillOpacity": 0.26, "color": "#d62728"}


def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")


# =========================
# UI de carga
# =========================
c1, c2, c3 = st.columns([1, 1, 1])

with c1:
    ine_zip = st.file_uploader("1) ZIP INE — Secciones electorales", type=["zip"])

with c2:
    inegi_zip = st.file_uploader("2) ZIP INEGI — Manzanas (Marco Geoestadístico)", type=["zip"])

with c3:
    basemap = st.selectbox("Basemap", ["Relieve (Esri)", "Topográfico (OpenTopoMap)", "Calles (OSM)"], index=0)

if not (ine_zip and inegi_zip):
    st.info("Sube ambos ZIPs para calcular el conteo (INE secciones + INEGI manzanas).")
    st.stop()

# =========================
# Lectura de archivos
# =========================
with st.spinner("📦 Extrayendo ZIPs y leyendo shapefiles..."):
    with tempfile.TemporaryDirectory() as tmp_ine, tempfile.TemporaryDirectory() as tmp_inegi:
        extract_zip_to_folder(ine_zip, tmp_ine)
        extract_zip_to_folder(inegi_zip, tmp_inegi)

        shp_secc = find_first_shp(tmp_ine)
        shp_mza = find_first_shp(tmp_inegi)

        if not shp_secc or not shp_mza:
            st.error(
                "No encontré archivos .shp dentro de uno de los ZIPs.\n\n"
                "Asegúrate que el ZIP contenga shapefile completo: .shp, .dbf, .shx, .prj (y opcional .cpg)."
            )
            st.stop()

        secc = gpd.read_file(shp_secc)
        mza = gpd.read_file(shp_mza)

# Identificación de columnas típicas
col_seccion = pick_col(secc, ["SECCION", "SECC", "CVE_SECC", "SECCION_E", "ID_SECC"])
col_distr = pick_col(secc, ["DISTRITO", "DISTRITO_F", "DISTRITO_L", "DTO", "CVE_DIST", "CVE_DISTR"])
col_mza_id = pick_col(mza, ["CVEGEO", "CVE_MZA", "ID_MZA", "MANZANA", "MZA", "CVE_MANZ"])

if col_seccion is None:
    st.error("No detecté la columna de SECCIÓN en la capa del INE. Revisa cómo se llama el campo en tu shapefile.")
    st.write("Columnas INE:", list(secc.columns))
    st.stop()

# Si no hay ID de manzana, usamos índice
if col_mza_id is None:
    mza["_MZA_ID_"] = mza.index.astype(str)
    col_mza_id = "_MZA_ID_"

# CRS a WGS84
secc.name = "INE_Secciones"
mza.name = "INEGI_Manzanas"
secc = ensure_crs(secc)
mza = ensure_crs(mza)

# =========================
# Filtros (opcional)
# =========================
st.divider()
fc1, fc2, fc3 = st.columns([1, 1, 1])

with fc1:
    if col_distr is not None:
        distritos = sorted(pd.Series(secc[col_distr].dropna().unique()).astype(str).tolist())
        distrito_sel = st.selectbox("Filtrar por Distrito (opcional)", ["(Todos)"] + distritos, index=0)
    else:
        distrito_sel = "(No disponible)"

with fc2:
    join_pred = st.selectbox("Regla de cruce (spatial join)", ["intersects", "within"], index=0)
    st.caption("Tip: 'within' suele ser más estricto; 'intersects' perdona bordes.")

with fc3:
    show_labels = st.checkbox("Mostrar etiqueta de sección en el mapa", value=False)

# Aplicar filtro de distrito si existe
secc_f = secc.copy()
if col_distr is not None and distrito_sel not in ["(Todos)", "(No disponible)"]:
    secc_f = secc_f[secc_f[col_distr].astype(str) == str(distrito_sel)].copy()

if secc_f.empty:
    st.warning("El filtro dejó la capa de secciones vacía. Ajusta el distrito.")
    st.stop()

# Recortar manzanas por bounding box de las secciones (acelera muchísimo)
minx, miny, maxx, maxy = secc_f.total_bounds
mza_f = mza.cx[minx:maxx, miny:maxy].copy()

if mza_f.empty:
    st.warning("No quedaron manzanas dentro del bounding box de las secciones (revisa CRS o capas).")
    st.stop()

# =========================
# Spatial Join y conteo
# =========================
st.divider()
with st.spinner("🧠 Cruzando manzanas INEGI ↔ secciones INE y contando..."):
    # Solo columnas esenciales
    mza_min = mza_f[[col_mza_id, "geometry"]].copy()
    secc_min = secc_f[[col_seccion, "geometry"]].copy()

    # Geopandas sjoin
    joined = gpd.sjoin(mza_min, secc_min, how="inner", predicate=join_pred)

    counts = (
        joined.groupby(col_seccion)[col_mza_id]
        .nunique()
        .reset_index(name="manzanas")
        .sort_values("manzanas", ascending=False)
    )

    secc_out = secc_f.merge(counts, on=col_seccion, how="left")
    secc_out["manzanas"] = secc_out["manzanas"].fillna(0).astype(int)

# =========================
# Resultados + export
# =========================
rc1, rc2, rc3 = st.columns([1, 1, 1])

with rc1:
    st.metric("Secciones (filtradas)", int(len(secc_out)))

with rc2:
    st.metric("Manzanas (recortadas)", int(len(mza_f)))

with rc3:
    st.metric("Manzanas asignadas a sección", int(counts["manzanas"].sum() if not counts.empty else 0))

exp1, exp2 = st.columns([1, 1])
with exp1:
    st.download_button(
        "⬇️ Descargar conteo por sección (CSV)",
        data=df_to_csv_bytes(counts),
        file_name="conteo_manzanas_por_seccion.csv",
        mime="text/csv",
        use_container_width=True,
    )
with exp2:
    st.download_button(
        "⬇️ Descargar secciones con conteo (GeoJSON)",
        data=secc_out.to_json().encode("utf-8"),
        file_name="secciones_con_conteo.geojson",
        mime="application/geo+json",
        use_container_width=True,
    )

# =========================
# Mapa
# =========================
st.divider()
st.subheader("🗺️ Mapa (relieve/topo/calles) + tooltip por sección")

lat, lon = safe_center(secc_out)

m = folium.Map(location=[lat, lon], zoom_start=12, tiles=None, control_scale=True)
add_basemap(m, basemap)

# Tooltip con sección y manzanas
tooltip = folium.GeoJsonTooltip(
    fields=[col_seccion, "manzanas"],
    aliases=["Sección:", "Manzanas:"],
    localize=True,
    sticky=False,
)

def _style(feat):
    v = int(feat["properties"].get("manzanas", 0))
    return style_by_bins(v)

gj = folium.GeoJson(
    secc_out.to_json(),
    name="Secciones (INE) con conteo de manzanas (INEGI)",
    style_function=_style,
    tooltip=tooltip,
)
gj.add_to(m)

# Opcional: etiquetas (ligero; no es perfecto, pero ayuda)
if show_labels:
    # etiqueta en centroid (en WGS84 puede estar “fuera” en polígonos raros, pero suele servir)
    for _, row in secc_out.iterrows():
        try:
            geom = row.geometry
            c = geom.centroid
            folium.Marker(
                [c.y, c.x],
                icon=folium.DivIcon(
                    html=f"""
                    <div style="font-size:10px; font-weight:600; color:#111; background:rgba(255,255,255,0.65);
                                padding:2px 4px; border-radius:6px; border:1px solid rgba(0,0,0,0.2);">
                        {row[col_seccion]}
                    </div>
                    """
                ),
            ).add_to(m)
        except Exception:
            pass

folium.LayerControl(collapsed=False).add_to(m)

mc1, mc2 = st.columns([1.45, 1])

with mc1:
    st_folium(m, use_container_width=True, height=720)

with mc2:
    st.subheader("📊 Tabla: manzanas por sección")
    st.dataframe(counts, use_container_width=True, height=650)

st.success("Listo, colega. Ya puedes ver el mapa en relieve y el conteo de cuadra/manzana por sección.")
