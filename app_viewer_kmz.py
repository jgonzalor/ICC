# app_viewer_kmz.py
# Conteo de MANZANAS (cuadras) por SECCIÓN electoral (INE x INEGI)
# + mapa base con relieve/topo/calles
#
# Uso:
# 1) Sube ZIP INE (Secciones electorales)  -> SHP dentro del ZIP
# 2) Sube ZIP INEGI (Manzanas)            -> SHP dentro del ZIP
# 3) Elige columna de "SECCION" (y filtra por municipio/distrito si existen)
# 4) Calcula conteo de manzanas por sección (spatial join)
# 5) Mapa coloreado por conteo + tabla + descargas
#
# NOTA: Para que no truene la app, NO dibuja todas las manzanas.
#       Solo (opcional) dibuja manzanas cuando seleccionas una sección.

from __future__ import annotations

import io
import os
import zipfile
import tempfile
from typing import List, Optional, Tuple

import streamlit as st

st.set_page_config(page_title="INE x INEGI — Manzanas por Sección", page_icon="🗺️", layout="wide")

# --- imports pesados después de set_page_config ---
try:
    import geopandas as gpd
except ModuleNotFoundError:
    st.error("Falta instalar `geopandas`. Revisa tu requirements.txt (abajo te lo dejo).")
    st.stop()

import pandas as pd
import folium
from streamlit_folium import st_folium


# =========================
# Helpers ZIP / SHP
# =========================
def zip_list_shps(zip_bytes: bytes) -> List[str]:
    with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as z:
        return sorted([n for n in z.namelist() if n.lower().endswith(".shp")])


def unzip_to_temp(zip_bytes: bytes) -> str:
    tmpdir = tempfile.mkdtemp(prefix="shp_")
    with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as z:
        z.extractall(tmpdir)
    return tmpdir


def read_shp_from_zip(zip_bytes: bytes, shp_inside_zip: str) -> gpd.GeoDataFrame:
    tmpdir = unzip_to_temp(zip_bytes)
    shp_path = os.path.join(tmpdir, shp_inside_zip)

    if not os.path.exists(shp_path):
        # Algunas veces el ZIP trae rutas raras; buscamos por nombre base
        base = os.path.basename(shp_inside_zip).lower()
        found = None
        for root, _, files in os.walk(tmpdir):
            for f in files:
                if f.lower() == base and f.lower().endswith(".shp"):
                    found = os.path.join(root, f)
                    break
            if found:
                break
        if not found:
            raise FileNotFoundError("No encontré el .shp seleccionado dentro del ZIP.")
        shp_path = found

    gdf = gpd.read_file(shp_path)

    # limpieza mínima
    gdf = gdf[gdf.geometry.notna()].copy()
    gdf.columns = [str(c).strip().upper() for c in gdf.columns]

    # CRS -> WGS84 para mapa
    if gdf.crs is None:
        # si viene sin CRS, asumimos WGS84 para que no truene el mapa
        gdf = gdf.set_crs(epsg=4326, allow_override=True)
    gdf = gdf.to_crs(epsg=4326)

    # intentar arreglar geometrías inválidas (sin romper)
    try:
        gdf["geometry"] = gdf.geometry.buffer(0)
    except Exception:
        pass

    return gdf


def pick_col(cols: List[str], candidates: List[str]) -> Optional[str]:
    cols_u = {c.upper() for c in cols}
    for cand in candidates:
        if cand.upper() in cols_u:
            return cand.upper()
    return None


def safe_center(gdf: gpd.GeoDataFrame) -> Tuple[float, float]:
    minx, miny, maxx, maxy = gdf.total_bounds
    return ( (miny + maxy) / 2.0, (minx + maxx) / 2.0 )


def to_excel_bytes(df: pd.DataFrame, sheet="RESUMEN") -> bytes:
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        df.to_excel(w, index=False, sheet_name=sheet)
    return out.getvalue()


# =========================
# UI
# =========================
st.title("🗺️ Manzanas (cuadras) por Sección Electoral — INE x INEGI")
st.caption("Sube los ZIP oficiales (INE Secciones + INEGI Manzanas). El app cuenta cuántas manzanas caen dentro de cada sección.")

c1, c2, c3 = st.columns([1, 1, 1])

with c1:
    ine_zip_file = st.file_uploader("ZIP INE — Secciones electorales (SHP en ZIP)", type=["zip"])

with c2:
    inegi_zip_file = st.file_uploader("ZIP INEGI — Manzanas (Marco Geoestadístico) (SHP en ZIP)", type=["zip"])

with c3:
    basemap = st.selectbox("Basemap", ["Relieve (Esri)", "Topográfico (OpenTopoMap)", "Calles (OSM)"], index=0)

if not ine_zip_file or not inegi_zip_file:
    st.info("👆 Sube ambos ZIP para continuar.")
    st.stop()

# Leer bytes sin “consumir” el UploadedFile
ine_zip_bytes = ine_zip_file.getvalue()
inegi_zip_bytes = inegi_zip_file.getvalue()

ine_shps = zip_list_shps(ine_zip_bytes)
inegi_shps = zip_list_shps(inegi_zip_bytes)

if not ine_shps:
    st.error("El ZIP del INE no contiene .shp.")
    st.stop()
if not inegi_shps:
    st.error("El ZIP del INEGI no contiene .shp.")
    st.stop()

sel1, sel2 = st.columns(2)
with sel1:
    ine_shp_choice = st.selectbox("SHP dentro del ZIP INE (elige la capa de SECCIONES)", ine_shps, index=0)
with sel2:
    inegi_shp_choice = st.selectbox("SHP dentro del ZIP INEGI (elige la capa de MANZANAS)", inegi_shps, index=0)

with st.spinner("Leyendo shapefiles..."):
    secc = read_shp_from_zip(ine_zip_bytes, ine_shp_choice)
    mza = read_shp_from_zip(inegi_zip_bytes, inegi_shp_choice)

st.success(f"INE Secciones: {len(secc):,} | INEGI Manzanas: {len(mza):,}")

st.divider()
st.subheader("🎛️ Configuración / filtros")

# Detectores comunes
guess_seccion = pick_col(list(secc.columns), ["SECCION", "SECC", "CVE_SECC", "ID_SECC", "SECCION_E", "SECCION_I"])
guess_distrito = pick_col(list(secc.columns), ["DISTRITO", "DTO", "DIST", "DISTRITO_F", "DISTRITO_L", "CVE_DIST"])
guess_mun = pick_col(list(secc.columns), ["MUNICIPIO", "NOM_MUN", "NOM_MPIO", "CVE_MUN", "MUN"])

f1, f2, f3 = st.columns(3)

with f1:
    secc_id_col = st.selectbox(
        "Columna de SECCIÓN (INE)",
        options=sorted(secc.columns),
        index=(sorted(secc.columns).index(guess_seccion) if guess_seccion in secc.columns else 0),
    )

with f2:
    distrito_col = st.selectbox(
        "Columna de DISTRITO (opcional)",
        options=["(no usar)"] + sorted(secc.columns),
        index=(1 + sorted(secc.columns).index(guess_distrito) if guess_distrito in secc.columns else 0),
    )

with f3:
    mun_col = st.selectbox(
        "Columna de MUNICIPIO (opcional)",
        options=["(no usar)"] + sorted(secc.columns),
        index=(1 + sorted(secc.columns).index(guess_mun) if guess_mun in secc.columns else 0),
    )

secc_f = secc.copy()

# Filtro distrito
if distrito_col != "(no usar)":
    vals = sorted(secc_f[distrito_col].dropna().astype(str).unique().tolist())
    distrito_sel = st.selectbox("Filtrar distrito", ["(todos)"] + vals, index=0)
    if distrito_sel != "(todos)":
        secc_f = secc_f[secc_f[distrito_col].astype(str) == str(distrito_sel)].copy()

# Filtro municipio
if mun_col != "(no usar)":
    vals = sorted(secc_f[mun_col].dropna().astype(str).unique().tolist())
    # intento poner Ahome como default si aparece
    def_idx = 0
    for i, v in enumerate(vals):
        if "ahome" in str(v).lower():
            def_idx = i + 1  # +1 porque "(todos)"
            break
    mun_sel = st.selectbox("Filtrar municipio", ["(todos)"] + vals, index=def_idx)
    if mun_sel != "(todos)":
        secc_f = secc_f[secc_f[mun_col].astype(str) == str(mun_sel)].copy()

if secc_f.empty:
    st.error("Con esos filtros no quedó ninguna sección.")
    st.stop()

# Recorte rápido de manzanas a bbox de secciones filtradas (performance)
minx, miny, maxx, maxy = secc_f.total_bounds
mza_f = mza.cx[minx:maxx, miny:maxy].copy()
if mza_f.empty:
    st.warning("No quedaron manzanas dentro del bbox de las secciones filtradas (revisa CRS/capa).")
    st.stop()

st.write(f"Secciones filtradas: **{len(secc_f):,}** | Manzanas (bbox): **{len(mza_f):,}**")

st.divider()
st.subheader("📊 Conteo manzanas por sección")

pred = st.selectbox("Predicado espacial", ["intersects", "within"], index=0)
btn = st.button("🚀 Calcular", use_container_width=True)

if not btn:
    st.stop()

with st.spinner("Cruzando (spatial join) y contando..."):
    # Trabajamos en CRS proyectado para evitar warnings y mejorar performance
    secc_wgs = secc_f.copy()
    mza_wgs = mza_f.copy()

    secc_p = secc_wgs.to_crs(epsg=3857)
    mza_p = mza_wgs.to_crs(epsg=3857)

    left = mza_p[["geometry"]].copy()
    right = secc_p[[secc_id_col, "geometry"]].copy()

    joined = gpd.sjoin(left, right, how="inner", predicate=pred)

    counts = (
        joined.groupby(secc_id_col)
        .size()
        .reset_index(name="MANZANAS")
        .sort_values("MANZANAS", ascending=False)
    )

    # Merge a secciones (para mapa / tooltip)
    secc_out = secc_wgs.copy()
    secc_out[secc_id_col] = secc_out[secc_id_col].astype(str)
    counts[secc_id_col] = counts[secc_id_col].astype(str)

    secc_out = secc_out.merge(counts, on=secc_id_col, how="left")
    secc_out["MANZANAS"] = secc_out["MANZANAS"].fillna(0).astype(int)

st.success("Listo ✅")

m1, m2, m3 = st.columns(3)
m1.metric("Secciones", f"{len(secc_out):,}")
m2.metric("Manzanas contadas", f"{int(secc_out['MANZANAS'].sum()):,}")
m3.metric("Promedio por sección", f"{secc_out['MANZANAS'].mean():.2f}")

cT, cM = st.columns([1, 1.3])
with cT:
    st.dataframe(counts, use_container_width=True, height=520)

    st.download_button(
        "⬇️ Descargar CSV",
        data=counts.to_csv(index=False).encode("utf-8"),
        file_name="conteo_manzanas_por_seccion.csv",
        mime="text/csv",
        use_container_width=True,
    )

    st.download_button(
        "⬇️ Descargar Excel",
        data=to_excel_bytes(counts, sheet="MANZANAS_X_SECCION"),
        file_name="conteo_manzanas_por_seccion.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )

# =========================
# MAPA
# =========================
with cM:
    st.subheader("🗺️ Mapa (coloreado por conteo)")

    lat, lon = safe_center(secc_out)
    m = folium.Map(location=[lat, lon], zoom_start=12, tiles=None, control_scale=True)

    # basemaps
    if basemap == "Relieve (Esri)":
        folium.TileLayer(
            tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Shaded_Relief/MapServer/tile/{z}/{y}/{x}",
            name="Relieve (Esri)",
            attr="Tiles © Esri",
            overlay=False,
            control=True,
        ).add_to(m)
    elif basemap == "Topográfico (OpenTopoMap)":
        folium.TileLayer(
            tiles="https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png",
            name="Topográfico (OpenTopoMap)",
            attr="© OpenTopoMap / © OpenStreetMap contributors",
            overlay=False,
            control=True,
        ).add_to(m)
    else:
        folium.TileLayer("OpenStreetMap", name="Calles (OSM)", overlay=False, control=True).add_to(m)

    # estilo por rangos
    def style_fn(feat):
        v = int(feat["properties"].get("MANZANAS", 0))
        if v == 0:
            return {"weight": 1, "fillOpacity": 0.06, "color": "#444"}
        if v <= 20:
            return {"weight": 2, "fillOpacity": 0.12, "color": "#1f77b4"}
        if v <= 50:
            return {"weight": 2, "fillOpacity": 0.14, "color": "#ff7f0e"}
        return {"weight": 2, "fillOpacity": 0.16, "color": "#d62728"}

    tooltip_fields = [secc_id_col, "MANZANAS"]
    aliases = ["Sección:", "Manzanas:"]

    if distrito_col != "(no usar)":
        tooltip_fields.append(distrito_col)
        aliases.append("Distrito:")
    if mun_col != "(no usar)":
        tooltip_fields.append(mun_col)
        aliases.append("Municipio:")

    folium.GeoJson(
        secc_out.to_json(),
        name="Secciones (INE) + Conteo",
        style_function=style_fn,
        tooltip=folium.GeoJsonTooltip(fields=tooltip_fields, aliases=aliases, sticky=False),
    ).add_to(m)

    folium.LayerControl(collapsed=False).add_to(m)

    # zoom al bbox de secciones
    b = secc_out.total_bounds
    m.fit_bounds([[b[1], b[0]], [b[3], b[2]]])

    st_folium(m, use_container_width=True, height=580)

st.divider()
st.subheader("🔎 (Opcional) Ver manzanas SOLO de una sección")
sel_sec = st.selectbox("Elige una sección para dibujar sus manzanas (evita crasheos)", sorted(secc_out[secc_id_col].astype(str).unique().tolist()))
show_mza = st.checkbox("Dibujar manzanas de esa sección", value=False)

if show_mza:
    with st.spinner("Recortando manzanas por sección seleccionada..."):
        # recorte en CRS proyectado por performance
        sec_poly_wgs = secc_out[secc_out[secc_id_col].astype(str) == str(sel_sec)].copy()
        if sec_poly_wgs.empty:
            st.warning("No encontré esa sección.")
            st.stop()

        sec_poly_p = sec_poly_wgs.to_crs(epsg=3857)
        mza_p2 = mza_wgs.to_crs(epsg=3857)

        # recorte real
        clip = gpd.overlay(mza_p2, sec_poly_p[["geometry"]], how="intersection")
        clip_wgs = clip.to_crs(epsg=4326)

    st.write(f"Manzanas en la sección {sel_sec}: **{len(clip_wgs):,}**")

    lat2, lon2 = safe_center(sec_poly_wgs)
    m2 = folium.Map(location=[lat2, lon2], zoom_start=14, tiles=None, control_scale=True)

    # basemap replicado
    if basemap == "Relieve (Esri)":
        folium.TileLayer(
            tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Shaded_Relief/MapServer/tile/{z}/{y}/{x}",
            name="Relieve (Esri)",
            attr="Tiles © Esri",
            overlay=False,
            control=True,
        ).add_to(m2)
    elif basemap == "Topográfico (OpenTopoMap)":
        folium.TileLayer(
            tiles="https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png",
            name="Topográfico (OpenTopoMap)",
            attr="© OpenTopoMap / © OpenStreetMap contributors",
            overlay=False,
            control=True,
        ).add_to(m2)
    else:
        folium.TileLayer("OpenStreetMap", name="Calles (OSM)", overlay=False, control=True).add_to(m2)

    folium.GeoJson(sec_poly_wgs.to_json(), name="Sección", style_function=lambda f: {"weight": 4, "fillOpacity": 0.05}).add_to(m2)
    folium.GeoJson(clip_wgs.to_json(), name="Manzanas", style_function=lambda f: {"weight": 1, "fillOpacity": 0.0}).add_to(m2)
    folium.LayerControl(collapsed=False).add_to(m2)

    b2 = sec_poly_wgs.total_bounds
    m2.fit_bounds([[b2[1], b2[0]], [b2[3], b2[2]]])

    st_folium(m2, use_container_width=True, height=580)
