# app_viewer_kmz.py
# ICC — Secciones INE + Manzanas INEGI (Marco Geoestadístico)
# - Mapa con estilo "relieve" (OpenTopoMap / Esri Shaded Relief)
# - Conteo de manzanas (cuadras) por sección
# - Descarga a Excel
#
# Uso:
# 1) Sube ZIP INE (Secciones electorales)
# 2) Sube ZIP INEGI (Manzanas del Marco Geoestadístico)
# 3) Filtra por campos (Entidad/Municipio/Distrito si aplica)
# 4) Calcula conteos y visualiza en mapa

from __future__ import annotations

import os
import zipfile
import tempfile
from io import BytesIO
from typing import Optional, Tuple, List

import pandas as pd
import streamlit as st

import geopandas as gpd
from shapely.geometry import mapping

import folium
from folium.features import GeoJsonTooltip
from streamlit_folium import st_folium


# =========================
#   CONFIG
# =========================
st.set_page_config(
    page_title="ICC — Manzanas por Sección (INE + INEGI)",
    page_icon="🗺️",
    layout="wide",
    menu_items={"Get Help": None, "Report a bug": None, "About": None},
)

st.markdown(
    """
<style>
#MainMenu {visibility: hidden;}
header {visibility: hidden;}
footer {visibility: hidden;}
</style>
""",
    unsafe_allow_html=True,
)

st.title("🗺️ ICC — Manzanas (cuadras) por Sección Electoral")
st.caption(
    "Sube los ZIP de **INE (Secciones)** y **INEGI (Manzanas)**. "
    "Luego contamos cuántas manzanas caen dentro de cada sección y lo vemos en mapa con estilo relieve."
)

with st.expander("📌 ¿De dónde salen los ZIP oficiales? (INE / INEGI)", expanded=False):
    st.markdown(
        """
**INE – Secciones electorales (Base Geográfica Digital / Marco Geoelectoral)**  
- Descarga desde el portal de cartografía del INE (bases cartográficas) o repositorios públicos oficiales.  

**INEGI – Manzanas (Marco Geoestadístico)**  
- Descarga desde la sección de descargas del Marco Geoestadístico (capas de manzana).

> Nota: el ZIP de INEGI trae **geometría**; para “casas por manzana” necesitas además **tabla censal** (viviendas) y luego se une por clave geoestadística.
"""
    )


# =========================
#   UTILIDADES
# =========================
def _extract_zip_to_temp(uploaded: BytesIO) -> str:
    """Extrae un ZIP de Streamlit a una carpeta temporal y regresa la ruta."""
    tmpdir = tempfile.mkdtemp(prefix="icc_zip_")
    with zipfile.ZipFile(uploaded) as z:
        z.extractall(tmpdir)
    return tmpdir


def _find_first_shp(folder: str) -> str:
    """Busca el primer .shp dentro de folder (recursivo)."""
    for root, _, files in os.walk(folder):
        for f in files:
            if f.lower().endswith(".shp"):
                return os.path.join(root, f)
    raise FileNotFoundError("No se encontró ningún archivo .shp dentro del ZIP.")


@st.cache_data(show_spinner=False)
def load_gdf_from_zip(uploaded_file) -> gpd.GeoDataFrame:
    """Carga un GeoDataFrame desde un ZIP subido en Streamlit."""
    # Streamlit UploadedFile soporta .getvalue()
    data = uploaded_file.getvalue()
    folder = _extract_zip_to_temp(BytesIO(data))
    shp_path = _find_first_shp(folder)

    # Lee shapefile
    gdf = gpd.read_file(shp_path)

    # Arreglos básicos de geometría
    gdf = gdf[gdf.geometry.notna()].copy()
    try:
        # intentamos corregir geometrías inválidas de forma segura
        gdf["geometry"] = gdf["geometry"].buffer(0)
    except Exception:
        pass

    return gdf


def to_epsg4326(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if gdf.crs is None:
        # Si viene sin CRS, lo más común es EPSG:4326 o EPSG:6372/6362 (INEGI).
        # Aquí no adivinamos: asumimos 4326 para que el mapa no truene, pero avisamos.
        st.warning("⚠️ El shapefile viene sin CRS. Asumiendo EPSG:4326 para visualización.")
        gdf = gdf.set_crs(epsg=4326, allow_override=True)
    if str(gdf.crs).lower() != "epsg:4326":
        gdf = gdf.to_crs(epsg=4326)
    return gdf


def pick_column(candidates: List[str], cols: List[str]) -> Optional[str]:
    """Devuelve el primer match por nombre (case-insensitive)."""
    cols_l = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand.lower() in cols_l:
            return cols_l[cand.lower()]
    return None


def safe_str_series(s: pd.Series) -> pd.Series:
    return s.astype(str).fillna("")


def guess_id_columns(gdf: gpd.GeoDataFrame) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    cols = list(gdf.columns)

    col_seccion = pick_column(["seccion", "sec", "cve_secc", "cve_sec", "id_secc"], cols)
    col_distrito = pick_column(["distrito", "dist", "dto", "cve_dist"], cols)
    col_municipio = pick_column(["municipio", "mun", "cve_mun", "nom_mun"], cols)

    return col_seccion, col_distrito, col_municipio


def spatial_count_manzanas_por_seccion(
    secciones: gpd.GeoDataFrame,
    manzanas: gpd.GeoDataFrame,
    seccion_id_col: str,
) -> pd.DataFrame:
    """Spatial join manzanas->secciones y cuenta manzanas por sección."""
    secc = secciones[[seccion_id_col, "geometry"]].copy()
    manz = manzanas[["geometry"]].copy()

    # Join: manzana intersecta sección
    joined = gpd.sjoin(manz, secc, predicate="intersects", how="left")

    counts = (
        joined.groupby(seccion_id_col, dropna=False)
        .size()
        .reset_index(name="manzanas_conteo")
    )

    # Limpieza de nulos
    counts[seccion_id_col] = counts[seccion_id_col].astype(str)

    return counts


def make_relief_map(
    secciones: gpd.GeoDataFrame,
    manzanas: Optional[gpd.GeoDataFrame] = None,
    tooltip_fields: Optional[List[str]] = None,
) -> folium.Map:
    # Centro aproximado
    bounds = secciones.total_bounds  # (minx, miny, maxx, maxy)
    center_lat = (bounds[1] + bounds[3]) / 2
    center_lon = (bounds[0] + bounds[2]) / 2

    m = folium.Map(location=[center_lat, center_lon], zoom_start=12, control_scale=True, tiles=None)

    # Base layers (relieve/topo)
    folium.TileLayer(
        tiles="https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png",
        attr="© OpenTopoMap (CC-BY-SA)",
        name="Relieve (OpenTopoMap)",
        overlay=False,
        control=True,
        max_zoom=17,
    ).add_to(m)

    folium.TileLayer(
        tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Shaded_Relief/MapServer/tile/{z}/{y}/{x}",
        attr="Tiles © Esri — World Shaded Relief",
        name="Relieve (Esri Shaded)",
        overlay=False,
        control=True,
        max_zoom=13,
    ).add_to(m)

    folium.TileLayer(
        tiles="OpenStreetMap",
        name="Calles (OSM)",
        overlay=False,
        control=True,
    ).add_to(m)

    # Secciones (relleno suave)
    tooltip_fields = tooltip_fields or []
    tooltip_fields = [f for f in tooltip_fields if f in secciones.columns]

    secc_json = folium.GeoJson(
        data=secciones.__geo_interface__,
        name="Secciones (INE)",
        style_function=lambda _: {
            "weight": 2,
            "fillOpacity": 0.08,
        },
        tooltip=GeoJsonTooltip(fields=tooltip_fields) if tooltip_fields else None,
    )
    secc_json.add_to(m)

    # Manzanas (borde)
    if manzanas is not None and len(manzanas) > 0:
        folium.GeoJson(
            data=manzanas.__geo_interface__,
            name="Manzanas (INEGI)",
            style_function=lambda _: {"weight": 1, "fillOpacity": 0.0},
        ).add_to(m)

    folium.LayerControl(collapsed=False).add_to(m)
    folium.FitBounds([[bounds[1], bounds[0]], [bounds[3], bounds[2]]]).add_to(m)

    return m


def df_to_excel_bytes(df: pd.DataFrame, sheet_name: str = "RESUMEN") -> bytes:
    out = BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    return out.getvalue()


# =========================
#   UI: CARGA DE ARCHIVOS
# =========================
col1, col2 = st.columns(2)

with col1:
    zip_ine = st.file_uploader(
        "ZIP INE — Secciones electorales",
        type=["zip"],
        accept_multiple_files=False,
    )

with col2:
    zip_inegi = st.file_uploader(
        "ZIP INEGI — Manzanas (Marco Geoestadístico)",
        type=["zip"],
        accept_multiple_files=False,
    )

if not zip_ine or not zip_inegi:
    st.info("👆 Sube ambos ZIP (INE y INEGI) para continuar.")
    st.stop()

with st.spinner("Cargando shapefiles..."):
    gdf_secciones = load_gdf_from_zip(zip_ine)
    gdf_manzanas = load_gdf_from_zip(zip_inegi)

gdf_secciones = to_epsg4326(gdf_secciones)
gdf_manzanas = to_epsg4326(gdf_manzanas)

st.success(
    f"Listo: INE(secciones)={len(gdf_secciones):,} registros | INEGI(manzanas)={len(gdf_manzanas):,} registros"
)

# =========================
#   FILTROS / CAMPOS
# =========================
st.subheader("🎛️ Filtros")

col_secc, col_dist, col_mun = guess_id_columns(gdf_secciones)

c1, c2, c3 = st.columns(3)

with c1:
    seccion_id_col = st.selectbox(
        "Campo de ID de Sección",
        options=list(gdf_secciones.columns),
        index=(list(gdf_secciones.columns).index(col_secc) if col_secc in gdf_secciones.columns else 0),
    )

with c2:
    distrito_col = st.selectbox(
        "Campo de Distrito (si existe)",
        options=["(no usar)"] + list(gdf_secciones.columns),
        index=(1 + list(gdf_secciones.columns).index(col_dist) if col_dist in gdf_secciones.columns else 0),
    )

with c3:
    municipio_col = st.selectbox(
        "Campo de Municipio (si existe)",
        options=["(no usar)"] + list(gdf_secciones.columns),
        index=(1 + list(gdf_secciones.columns).index(col_mun) if col_mun in gdf_secciones.columns else 0),
    )

gdf_secc_f = gdf_secciones.copy()

# Filtrar por Distrito
if distrito_col != "(no usar)":
    dist_vals = sorted(safe_str_series(gdf_secc_f[distrito_col]).unique().tolist())
    dist_sel = st.selectbox("Selecciona Distrito", options=dist_vals, index=0)
    gdf_secc_f = gdf_secc_f[safe_str_series(gdf_secc_f[distrito_col]) == str(dist_sel)].copy()

# Filtrar por Municipio
if municipio_col != "(no usar)":
    mun_vals = sorted(safe_str_series(gdf_secc_f[municipio_col]).unique().tolist())
    default_idx = 0
    # intento de “Ahome”
    for i, v in enumerate(mun_vals):
        if "ahome" in str(v).lower():
            default_idx = i
            break
    mun_sel = st.selectbox("Selecciona Municipio", options=mun_vals, index=default_idx)
    gdf_secc_f = gdf_secc_f[safe_str_series(gdf_secc_f[municipio_col]) == str(mun_sel)].copy()

st.write(f"Secciones filtradas: **{len(gdf_secc_f):,}**")

if len(gdf_secc_f) == 0:
    st.error("No quedaron secciones con esos filtros. Ajusta Distrito/Municipio.")
    st.stop()

# Clip opcional de manzanas a bbox de secciones filtradas (performance)
minx, miny, maxx, maxy = gdf_secc_f.total_bounds
bbox = gpd.GeoSeries.from_bbox((minx, miny, maxx, maxy), crs="EPSG:4326").iloc[0]
gdf_manz_f = gdf_manzanas[gdf_manzanas.intersects(bbox)].copy()

st.write(f"Manzanas dentro del bbox: **{len(gdf_manz_f):,}**")

# =========================
#   CÁLCULO
# =========================
st.subheader("📊 Conteo de manzanas (cuadras) por sección")

do_calc = st.button("🚀 Calcular conteos", use_container_width=True)

if do_calc:
    with st.spinner("Haciendo spatial join y conteo..."):
        df_counts = spatial_count_manzanas_por_seccion(
            secciones=gdf_secc_f,
            manzanas=gdf_manz_f,
            seccion_id_col=seccion_id_col,
        )

        # Unir a tabla de secciones para tooltip/mapa/descargas
        gdf_out = gdf_secc_f.copy()
        gdf_out[seccion_id_col] = gdf_out[seccion_id_col].astype(str)

        gdf_out = gdf_out.merge(df_counts, on=seccion_id_col, how="left")
        gdf_out["manzanas_conteo"] = gdf_out["manzanas_conteo"].fillna(0).astype(int)

        # Resumen
        resumen = (
            gdf_out[[seccion_id_col, "manzanas_conteo"]]
            .sort_values("manzanas_conteo", ascending=False)
            .reset_index(drop=True)
        )

    st.success("Conteo listo ✅")

    cA, cB, cC = st.columns(3)
    with cA:
        st.metric("Secciones", f"{len(gdf_out):,}")
    with cB:
        st.metric("Manzanas (cuadras) totales", f"{int(resumen['manzanas_conteo'].sum()):,}")
    with cC:
        st.metric("Promedio manzanas por sección", f"{resumen['manzanas_conteo'].mean():.2f}")

    st.dataframe(resumen, use_container_width=True)

    # Descarga Excel
    xls = df_to_excel_bytes(resumen, sheet_name="MANZANAS_X_SECCION")
    st.download_button(
        "⬇️ Descargar Excel (manzanas por sección)",
        data=xls,
        file_name="manzanas_por_seccion.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )

    # =========================
    #   MAPA (RELIEVE)
    # =========================
    st.subheader("🧭 Mapa (Relieve / Topo)")
    tooltip_fields = [seccion_id_col, "manzanas_conteo"]
    if distrito_col != "(no usar)":
        tooltip_fields.append(distrito_col)
    if municipio_col != "(no usar)":
        tooltip_fields.append(municipio_col)

    m = make_relief_map(gdf_out, gdf_manz_f, tooltip_fields=tooltip_fields)
    st_folium(m, height=650, use_container_width=True)

else:
    # Mapa preliminar (sin conteos)
    st.subheader("🧭 Mapa (Relieve / Topo)")
    m0 = make_relief_map(gdf_secc_f, gdf_manz_f, tooltip_fields=[seccion_id_col])
    st_folium(m0, height=650, use_container_width=True)
