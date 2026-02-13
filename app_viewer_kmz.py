# app_mapas_sinaloa.py
# 🗺️ App de Mapas — Sinaloa (Secciones INE + Manzanas INEGI) + Conteo + (Opcional) Población por Sección
#
# - Carga 2 ZIP: (1) INE BGD (o zip mínimo con SECCION.*), (2) INEGI Marco (o zip mínimo con 25m.*)
# - Mapa base: Relieve / Topo / Calles / Satélite
# - Explora secciones, filtra por municipio/distrito si existen columnas
# - Cuenta manzanas por sección (sjoin) y exporta CSV/Excel
# - Población por sección (opcional): sube CSV/Excel con columnas (SECCION, POBTOT) o similares y se une
#
# Requisitos: ver requirements.txt (geopandas>=0.14, shapely>=2, pyogrio recomendado)

from __future__ import annotations

import io
import os
import re
import zipfile
import tempfile
import hashlib
from typing import List, Tuple, Optional

import pandas as pd
import streamlit as st
import geopandas as gpd

import folium
from streamlit_folium import st_folium


# -------------------------
# UI
# -------------------------
st.set_page_config(page_title="Mapas Sinaloa — INE/INEGI", page_icon="🗺️", layout="wide")
st.title("🗺️ Mapas Sinaloa — Secciones (INE) + Manzanas (INEGI)")
st.caption("Explora capas, mapa en relieve/topo/satélite y cuenta manzanas por sección. (Población por sección: opcional con tabla).")


# -------------------------
# Utils
# -------------------------
def md5_bytes(b: bytes) -> str:
    return hashlib.md5(b).hexdigest()


def safe_extract_zip_bytes(zip_bytes: bytes, out_dir: str) -> None:
    zpath = os.path.join(out_dir, "root.zip")
    with open(zpath, "wb") as f:
        f.write(zip_bytes)
    with zipfile.ZipFile(zpath, "r") as z:
        try:
            z.extractall(out_dir)
        except Exception:
            for n in z.namelist():
                try:
                    z.extract(n, out_dir)
                except Exception:
                    pass


def extract_nested_zips(base_dir: str, max_depth: int = 2) -> None:
    for _ in range(max_depth):
        nested = []
        for root, _, files in os.walk(base_dir):
            for fn in files:
                if fn.lower().endswith(".zip") and fn.lower() != "root.zip":
                    nested.append(os.path.join(root, fn))
        if not nested:
            return
        for z in nested:
            out = z + "_unzipped"
            if not os.path.exists(out):
                os.makedirs(out, exist_ok=True)
            try:
                with zipfile.ZipFile(z, "r") as zz:
                    zz.extractall(out)
            except Exception:
                pass


def list_shps(ws_dir: str) -> List[str]:
    shps = []
    for root, _, files in os.walk(ws_dir):
        for fn in files:
            if fn.lower().endswith(".shp"):
                shps.append(os.path.relpath(os.path.join(root, fn), ws_dir))
    return sorted(shps)


def prepare_workspace(zip_bytes: bytes, key: str) -> Tuple[str, List[str]]:
    h = md5_bytes(zip_bytes)
    ss_key = f"WS_{key}"
    if ss_key in st.session_state:
        item = st.session_state[ss_key]
        if item.get("hash") == h and os.path.exists(item.get("dir", "")):
            return item["dir"], item["shps"]

    ws = tempfile.mkdtemp(prefix=f"{key}_")
    safe_extract_zip_bytes(zip_bytes, ws)
    extract_nested_zips(ws, max_depth=2)
    shps = list_shps(ws)
    st.session_state[ss_key] = {"hash": h, "dir": ws, "shps": shps}
    return ws, shps


def filter_list(items: List[str], q: str) -> List[str]:
    q = (q or "").strip().lower()
    if not q:
        return items
    return [x for x in items if q in x.lower()]


def auto_pick(shps: List[str], kind: str) -> int:
    low = [s.lower() for s in shps]
    if kind == "seccion":
        for i, s in enumerate(low):
            if "seccion" in s or "secc" in s:
                return i
        return 0
    if kind == "manzana":
        for i, s in enumerate(low):
            if re.search(r"(^|/)\d{2}m\.shp$", s):
                return i
        for i, s in enumerate(low):
            if "manzana" in s or "manz" in s or "mza" in s:
                return i
        return 0
    return 0


def ensure_active_geometry(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    try:
        _ = gdf.geometry
    except Exception:
        for cand in ["geometry", "GEOMETRY", "Shape", "SHAPE"]:
            if cand in gdf.columns:
                gdf = gdf.set_geometry(cand)
                break

    geom_name = gdf.geometry.name
    if geom_name != "geometry":
        if "geometry" in gdf.columns and str(gdf["geometry"].dtype) != "geometry":
            gdf = gdf.rename(columns={"geometry": "GEOMETRY_OLD"})
        gdf = gdf.rename(columns={geom_name: "geometry"}).set_geometry("geometry")

    return gdf


def uppercase_non_geometry(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    rename = {}
    for c in gdf.columns:
        if c == "geometry":
            continue
        rename[c] = str(c).strip().upper()
    return gdf.rename(columns=rename)


def read_layer(ws_dir: str, shp_rel: str, label: str) -> gpd.GeoDataFrame:
    shp_path = os.path.join(ws_dir, shp_rel)
    if not os.path.exists(shp_path):
        raise FileNotFoundError(f"No existe el SHP: {shp_rel}")

    gdf = gpd.read_file(shp_path)
    gdf = ensure_active_geometry(gdf)
    gdf = uppercase_non_geometry(gdf)
    gdf = ensure_active_geometry(gdf)

    if gdf.crs is None:
        st.warning(f"⚠️ {label} sin CRS. Asumiendo EPSG:4326.")
        gdf = gdf.set_crs(epsg=4326, allow_override=True)

    try:
        gdf = gdf.to_crs(epsg=4326)
    except Exception:
        gdf = gdf.set_crs(epsg=4326, allow_override=True)

    gdf = gdf[gdf.geometry.notna()].copy()
    return gdf


def pick_col(cols: List[str], candidates: List[str]) -> Optional[str]:
    cols_u = {c.upper() for c in cols}
    for c in candidates:
        if c.upper() in cols_u:
            return c.upper()
    return None


def safe_center(gdf: gpd.GeoDataFrame) -> Tuple[float, float]:
    minx, miny, maxx, maxy = gdf.total_bounds
    return ((miny + maxy) / 2.0, (minx + maxx) / 2.0)


def to_excel_bytes(df: pd.DataFrame, sheet: str) -> bytes:
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        df.to_excel(w, index=False, sheet_name=sheet)
    return out.getvalue()


def to_geojson_small(gdf: gpd.GeoDataFrame, max_features: int = 6000) -> str:
    if len(gdf) > max_features:
        gdf = gdf.sample(max_features, random_state=7).copy()
    return gdf.to_json()


# -------------------------
# Inputs
# -------------------------
st.subheader("1) Carga de ZIPs")
c1, c2, c3 = st.columns([1, 1, 1])

with c1:
    zip_ine = st.file_uploader("ZIP 1 — INE BGD (o ZIP mínimo con SECCION.*)", type=["zip"])
with c2:
    zip_inegi = st.file_uploader("ZIP 2 — INEGI Marco (o ZIP mínimo con 25m.*)", type=["zip"])
with c3:
    basemap = st.selectbox("Mapa base", ["Relieve (Esri)", "Topográfico (OpenTopoMap)", "Calles (OSM)", "Satélite (Esri)"], index=0)

if not zip_ine or not zip_inegi:
    st.info("Sube los dos ZIP para continuar.")
    st.stop()

ine_bytes = zip_ine.getvalue()
inegi_bytes = zip_inegi.getvalue()

with st.spinner("📦 Preparando INE..."):
    ws_ine, shps_ine = prepare_workspace(ine_bytes, "INE")
with st.spinner("📦 Preparando INEGI..."):
    ws_inegi, shps_inegi = prepare_workspace(inegi_bytes, "INEGI")

if not shps_ine:
    st.error("No encontré .shp en el ZIP del INE.")
    st.stop()
if not shps_inegi:
    st.error("No encontré .shp en el ZIP del INEGI.")
    st.stop()

st.subheader("2) Selección de capas (SHP)")
f1, f2 = st.columns(2)
with f1:
    q_ine = st.text_input("Filtrar capas INE", value="seccion")
with f2:
    q_inegi = st.text_input("Filtrar capas INEGI", value="25m")

ine_view = filter_list(shps_ine, q_ine) or shps_ine
inegi_view = filter_list(shps_inegi, q_inegi) or shps_inegi

idx_secc = auto_pick(ine_view, "seccion")
idx_mza = auto_pick(inegi_view, "manzana")

a, b = st.columns(2)
with a:
    shp_secc = st.selectbox("INE: Secciones", ine_view, index=min(idx_secc, len(ine_view)-1))
with b:
    shp_mza = st.selectbox("INEGI: Manzanas", inegi_view, index=min(idx_mza, len(inegi_view)-1))

if re.search(r"(^|/)\d{2}a\.shp$", shp_mza.lower()):
    st.warning("⚠️ Elegiste ..a.shp (AGEB). Para manzana busca ..m.shp (ej: 25m.shp).")

with st.spinner("🧠 Leyendo capas seleccionadas..."):
    secc = read_layer(ws_ine, shp_secc, "INE Secciones")
    mza = read_layer(ws_inegi, shp_mza, "INEGI Manzanas")

st.success(f"Secciones: {len(secc):,} | Manzanas: {len(mza):,}")

tab1, tab2, tab3 = st.tabs(["🗺️ Mapa", "📊 Conteo manzanas x sección", "👥 Población por sección (opcional)"])


# -------------------------
# TAB 1
# -------------------------
with tab1:
    st.subheader("Mapa interactivo")

    guess_secc = pick_col(list(secc.columns), ["SECCION", "SECC", "CVE_SECC", "ID_SECC"])
    secc_id_col = st.selectbox("Columna ID de Sección", sorted(secc.columns),
                               index=sorted(secc.columns).index(guess_secc) if guess_secc in secc.columns else 0)

    distrito_col = pick_col(list(secc.columns), ["DISTRITO", "DTO", "DIST", "CVE_DIST"])
    mun_col = pick_col(list(secc.columns), ["MUNICIPIO", "NOM_MUN", "NOM_MPIO", "CVE_MUN", "MUN"])

    cfa, cfb, cfc = st.columns(3)
    with cfa:
        use_mun = st.checkbox("Filtrar por municipio (si existe)", value=bool(mun_col))
    with cfb:
        use_dist = st.checkbox("Filtrar por distrito (si existe)", value=bool(distrito_col))
    with cfc:
        show_manzanas = st.checkbox("Mostrar manzanas (recortadas) en el mapa", value=False)

    secc_f = secc.copy()

    if use_mun and mun_col:
        vals = sorted(secc_f[mun_col].dropna().astype(str).unique().tolist())
        mun_sel = st.selectbox("Municipio", ["(todos)"] + vals, index=0)
        if mun_sel != "(todos)":
            secc_f = secc_f[secc_f[mun_col].astype(str) == str(mun_sel)].copy()

    if use_dist and distrito_col:
        vals = sorted(secc_f[distrito_col].dropna().astype(str).unique().tolist())
        dist_sel = st.selectbox("Distrito", ["(todos)"] + vals, index=0)
        if dist_sel != "(todos)":
            secc_f = secc_f[secc_f[distrito_col].astype(str) == str(dist_sel)].copy()

    if secc_f.empty:
        st.error("Con esos filtros no quedó ninguna sección.")
        st.stop()

    lat, lon = safe_center(secc_f)
    m = folium.Map(location=[lat, lon], zoom_start=8, tiles=None, control_scale=True)

    if basemap == "Relieve (Esri)":
        folium.TileLayer(
            tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Shaded_Relief/MapServer/tile/{z}/{y}/{x}",
            name="Relieve (Esri)", attr="Tiles © Esri", overlay=False, control=True
        ).add_to(m)
    elif basemap == "Topográfico (OpenTopoMap)":
        folium.TileLayer(
            tiles="https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png",
            name="Topográfico (OpenTopoMap)",
            attr="© OpenTopoMap / © OpenStreetMap contributors",
            overlay=False, control=True
        ).add_to(m)
    elif basemap == "Satélite (Esri)":
        folium.TileLayer(
            tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
            name="Satélite (Esri)", attr="Tiles © Esri", overlay=False, control=True
        ).add_to(m)
    else:
        folium.TileLayer("OpenStreetMap", name="Calles (OSM)", overlay=False, control=True).add_to(m)

    tooltip_fields = [secc_id_col]
    aliases = ["Sección:"]
    if mun_col:
        tooltip_fields.append(mun_col); aliases.append("Municipio:")
    if distrito_col:
        tooltip_fields.append(distrito_col); aliases.append("Distrito:")

    folium.GeoJson(
        secc_f.to_json(),
        name="Secciones",
        style_function=lambda feat: {"weight": 1.6, "fillOpacity": 0.06},
        tooltip=folium.GeoJsonTooltip(fields=tooltip_fields, aliases=aliases, sticky=False),
    ).add_to(m)

    if show_manzanas:
        minx, miny, maxx, maxy = secc_f.total_bounds
        mza_f = mza.cx[minx:maxx, miny:maxy].copy()
        gj = to_geojson_small(mza_f, max_features=6000)
        folium.GeoJson(
            gj,
            name="Manzanas (muestra)",
            style_function=lambda feat: {"weight": 0.8, "fillOpacity": 0.02},
        ).add_to(m)

    folium.LayerControl(collapsed=False).add_to(m)
    bds = secc_f.total_bounds
    m.fit_bounds([[bds[1], bds[0]], [bds[3], bds[2]]])

    st_folium(m, use_container_width=True, height=650)


# -------------------------
# TAB 2
# -------------------------
with tab2:
    st.subheader("Conteo de manzanas por sección")

    guess_secc = pick_col(list(secc.columns), ["SECCION", "SECC", "CVE_SECC", "ID_SECC"])
    secc_id_col = st.selectbox("Columna ID de Sección (para conteo)", sorted(secc.columns),
                               index=sorted(secc.columns).index(guess_secc) if guess_secc in secc.columns else 0,
                               key="secc_id_for_count")

    pred = st.selectbox("Regla espacial", ["intersects", "within"], index=0)
    approx = st.checkbox("Modo rápido (centroid within) — más rápido, menos exacto en límites", value=True)

    if "COUNT_RESULT" not in st.session_state:
        st.session_state["COUNT_RESULT"] = None

    colA, colB = st.columns(2)
    if colA.button("🚀 Calcular (Sinaloa completo)", use_container_width=True):
        with st.spinner("Procesando (puede tardar)..."):
            secc_p = ensure_active_geometry(secc).to_crs(epsg=3857)
            mza_p = ensure_active_geometry(mza).to_crs(epsg=3857)

            if approx:
                pts = mza_p.copy()
                pts["geometry"] = pts.geometry.centroid
                left = gpd.GeoDataFrame(pts[["geometry"]].copy(), geometry="geometry", crs=pts.crs)
            else:
                left = gpd.GeoDataFrame(mza_p[["geometry"]].copy(), geometry="geometry", crs=mza_p.crs)

            right = gpd.GeoDataFrame(secc_p[[secc_id_col, "geometry"]].copy(), geometry="geometry", crs=secc_p.crs)

            if left.crs != right.crs:
                right = right.to_crs(left.crs)

            joined = gpd.sjoin(left, right, how="inner", predicate=pred)

            counts = (
                joined.groupby(secc_id_col)
                .size()
                .reset_index(name="MANZANAS")
                .sort_values("MANZANAS", ascending=False)
            )

            secc_out = ensure_active_geometry(secc).copy()
            secc_out[secc_id_col] = secc_out[secc_id_col].astype(str)
            counts[secc_id_col] = counts[secc_id_col].astype(str)
            secc_out = secc_out.merge(counts, on=secc_id_col, how="left")
            secc_out["MANZANAS"] = secc_out["MANZANAS"].fillna(0).astype(int)

            st.session_state["COUNT_RESULT"] = {"counts": counts, "secc_out": secc_out}

    if colB.button("🧹 Limpiar resultado", use_container_width=True):
        st.session_state["COUNT_RESULT"] = None

    res = st.session_state["COUNT_RESULT"]
    if res is None:
        st.info("Dale a **Calcular** para obtener el conteo y exportarlo.")
    else:
        counts = res["counts"]
        secc_out = res["secc_out"]

        m1, m2, m3 = st.columns(3)
        m1.metric("Secciones", f"{len(secc_out):,}")
        m2.metric("Manzanas contadas", f"{int(secc_out['MANZANAS'].sum()):,}")
        m3.metric("Promedio por sección", f"{secc_out['MANZANAS'].mean():.2f}")

        st.dataframe(counts, use_container_width=True, height=420)

        cdl1, cdl2 = st.columns(2)
        cdl1.download_button("⬇️ CSV", counts.to_csv(index=False).encode("utf-8"),
                             "conteo_manzanas_por_seccion.csv", "text/csv", use_container_width=True)
        cdl2.download_button("⬇️ Excel", to_excel_bytes(counts, "MANZANAS_X_SECCION"),
                             "conteo_manzanas_por_seccion.xlsx",
                             "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                             use_container_width=True)

        st.subheader("Mapa por conteo (secciones)")
        lat, lon = safe_center(secc_out)
        mm = folium.Map(location=[lat, lon], zoom_start=8, tiles=None, control_scale=True)

        if basemap == "Relieve (Esri)":
            folium.TileLayer(
                tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Shaded_Relief/MapServer/tile/{z}/{y}/{x}",
                name="Relieve (Esri)", attr="Tiles © Esri", overlay=False, control=True
            ).add_to(mm)
        elif basemap == "Topográfico (OpenTopoMap)":
            folium.TileLayer(
                tiles="https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png",
                name="Topográfico (OpenTopoMap)",
                attr="© OpenTopoMap / © OpenStreetMap contributors",
                overlay=False, control=True
            ).add_to(mm)
        elif basemap == "Satélite (Esri)":
            folium.TileLayer(
                tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
                name="Satélite (Esri)", attr="Tiles © Esri", overlay=False, control=True
            ).add_to(mm)
        else:
            folium.TileLayer("OpenStreetMap", name="Calles (OSM)", overlay=False, control=True).add_to(mm)

        def style_fn(feat):
            v = int(feat["properties"].get("MANZANAS", 0))
            if v == 0:
                return {"weight": 1, "fillOpacity": 0.03}
            if v <= 20:
                return {"weight": 1.6, "fillOpacity": 0.08}
            if v <= 50:
                return {"weight": 1.6, "fillOpacity": 0.10}
            return {"weight": 1.6, "fillOpacity": 0.12}

        folium.GeoJson(
            secc_out.to_json(),
            name="Secciones (conteo)",
            style_function=style_fn,
            tooltip=folium.GeoJsonTooltip(fields=[secc_id_col, "MANZANAS"], aliases=["Sección:", "Manzanas:"], sticky=False),
        ).add_to(mm)

        folium.LayerControl(collapsed=False).add_to(mm)
        bds = secc_out.total_bounds
        mm.fit_bounds([[bds[1], bds[0]], [bds[3], bds[2]]])

        st_folium(mm, use_container_width=True, height=650)


# -------------------------
# TAB 3
# -------------------------
with tab3:
    st.subheader("Población / Habitantes por Sección (opcional)")
    st.info(
        "Los ZIP de cartografía normalmente NO traen población. "
        "Para 'habitantes por sección' sube una tabla (CSV/Excel) con: "
        "columna de sección + columna de población (ej. POBTOT)."
    )

    pop_file = st.file_uploader("Sube CSV/Excel con población por sección", type=["csv", "xlsx", "xls"])
    if not pop_file:
        st.stop()

    if pop_file.name.lower().endswith(".csv"):
        pop_df = pd.read_csv(pop_file)
    else:
        pop_df = pd.read_excel(pop_file)

    pop_df.columns = [str(c).strip().upper() for c in pop_df.columns]

    guess_secc = pick_col(list(secc.columns), ["SECCION", "SECC", "CVE_SECC", "ID_SECC"])
    secc_id_col = st.selectbox("Columna de sección en el SHP", sorted(secc.columns),
                               index=sorted(secc.columns).index(guess_secc) if guess_secc in secc.columns else 0,
                               key="pop_secc_id")

    secc_col_in_table = st.selectbox("Columna de sección en tu tabla", sorted(pop_df.columns))
    pop_col_in_table = st.selectbox("Columna de población/habitantes en tu tabla", sorted(pop_df.columns))

    secc2 = ensure_active_geometry(secc).copy()
    secc2[secc_id_col] = secc2[secc_id_col].astype(str)

    pop2 = pop_df.copy()
    pop2[secc_col_in_table] = pop2[secc_col_in_table].astype(str)

    merged = secc2.merge(
        pop2[[secc_col_in_table, pop_col_in_table]].drop_duplicates(),
        left_on=secc_id_col,
        right_on=secc_col_in_table,
        how="left",
    )
    merged = merged.drop(columns=[secc_col_in_table], errors="ignore")
    merged[pop_col_in_table] = pd.to_numeric(merged[pop_col_in_table], errors="coerce")

    st.success("Unión lista ✅")
    st.dataframe(merged[[secc_id_col, pop_col_in_table]].head(50), use_container_width=True)

    st.subheader("Mapa (por población)")
    lat, lon = safe_center(merged)
    mp = folium.Map(location=[lat, lon], zoom_start=8, tiles=None, control_scale=True)

    if basemap == "Relieve (Esri)":
        folium.TileLayer(
            tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Shaded_Relief/MapServer/tile/{z}/{y}/{x}",
            name="Relieve (Esri)", attr="Tiles © Esri", overlay=False, control=True
        ).add_to(mp)
    elif basemap == "Topográfico (OpenTopoMap)":
        folium.TileLayer(
            tiles="https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png",
            name="Topográfico (OpenTopoMap)",
            attr="© OpenTopoMap / © OpenStreetMap contributors",
            overlay=False, control=True
        ).add_to(mp)
    elif basemap == "Satélite (Esri)":
        folium.TileLayer(
            tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
            name="Satélite (Esri)", attr="Tiles © Esri", overlay=False, control=True
        ).add_to(mp)
    else:
        folium.TileLayer("OpenStreetMap", name="Calles (OSM)", overlay=False, control=True).add_to(mp)

    def style_pop(feat):
        v = feat["properties"].get(pop_col_in_table)
        try:
            v = float(v) if v is not None else None
        except Exception:
            v = None
        if v is None or (v != v):
            return {"weight": 1, "fillOpacity": 0.02}
        if v <= 1000:
            return {"weight": 1.4, "fillOpacity": 0.06}
        if v <= 3000:
            return {"weight": 1.4, "fillOpacity": 0.09}
        return {"weight": 1.4, "fillOpacity": 0.12}

    folium.GeoJson(
        merged.to_json(),
        name="Población por Sección",
        style_function=style_pop,
        tooltip=folium.GeoJsonTooltip(fields=[secc_id_col, pop_col_in_table], aliases=["Sección:", "Población:"], sticky=False),
    ).add_to(mp)

    folium.LayerControl(collapsed=False).add_to(mp)
    bds = merged.total_bounds
    mp.fit_bounds([[bds[1], bds[0]], [bds[3], bds[2]]])

    st_folium(mp, use_container_width=True, height=650)
