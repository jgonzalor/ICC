# app_mapa_1zip_v2_kmz_editable.py
# 🗺️ ICC — Mapa con 1 ZIP (Secciones INE + Manzanas INEGI)
#
# Mejoras v2:
# ✅ KMZ agrupado por Distrito Local exportado
# ✅ Editor manual para los valores visibles del globo/popup del KMZ
# ✅ Alias editables para renombrar campos visibles del popup
# ✅ Popup KMZ personalizado con HTML limpio (en vez de depender solo de ExtendedData)

from __future__ import annotations

import io
import os
import re
import zipfile
import tempfile
import hashlib
import html
from typing import List, Tuple, Optional, Iterable, Dict

import pandas as pd
import streamlit as st

import geopandas as gpd
import folium
from streamlit_folium import st_folium


# -------------------------
# UI
# -------------------------
st.set_page_config(page_title="ICC — 1 ZIP Mapas", page_icon="🗺️", layout="wide")
st.title("🗺️ ICC — Mapas con 1 ZIP (Secciones + Manzanas)")
st.caption("Sube un ZIP que ya contenga Secciones (INE) y Manzanas (INEGI). Filtra por distrito y sección (multi).")


# -------------------------
# ZIP / workspace helpers
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
            # extracción "best effort"
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


def prepare_workspace(zip_bytes: bytes, key: str = "ONEZIP") -> Tuple[str, List[str]]:
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


# -------------------------
# Geo helpers
# -------------------------
def ensure_active_geometry(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Normaliza geometry activa a 'geometry'."""
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


def safe_center(gdf: gpd.GeoDataFrame) -> Tuple[float, float]:
    minx, miny, maxx, maxy = gdf.total_bounds
    return ((miny + maxy) / 2.0, (minx + maxx) / 2.0)


def pick_col(cols: List[str], candidates: List[str]) -> Optional[str]:
    cols_u = {c.upper() for c in cols}
    for c in candidates:
        if c.upper() in cols_u:
            return c.upper()
    return None


def to_excel_bytes(sheets: dict) -> bytes:
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        for name, df in sheets.items():
            df.to_excel(w, index=False, sheet_name=name[:31])
    return out.getvalue()


def safe_int_values(series: pd.Series) -> List[int]:
    vals = pd.to_numeric(series, errors="coerce").dropna()
    if vals.empty:
        return []
    return sorted(vals.astype(int).unique().tolist())


# -------------------------
# Editor helpers
# -------------------------
def df_fingerprint(df: pd.DataFrame) -> str:
    payload = f"{list(df.columns)}|{len(df)}|{df.head(50).to_json(orient='split', date_format='iso', default_handler=str)}"
    return hashlib.md5(payload.encode("utf-8")).hexdigest()


def ensure_editor_df(state_key: str, fp_key: str, source_df: pd.DataFrame) -> pd.DataFrame:
    fp = df_fingerprint(source_df)
    if fp_key not in st.session_state or st.session_state.get(fp_key) != fp:
        st.session_state[state_key] = source_df.copy()
        st.session_state[fp_key] = fp
    return st.session_state[state_key].copy()


def update_editor_df(state_key: str, edited_df: pd.DataFrame) -> None:
    st.session_state[state_key] = edited_df.copy()


def normalize_alias_df(selected_cols: List[str], alias_df: Optional[pd.DataFrame]) -> pd.DataFrame:
    base = pd.DataFrame({"CAMPO": selected_cols, "ETIQUETA": selected_cols})
    if alias_df is None or alias_df.empty:
        return base

    alias_df2 = alias_df.copy()
    if "CAMPO" not in alias_df2.columns or "ETIQUETA" not in alias_df2.columns:
        return base

    alias_map_prev = {}
    for _, row in alias_df2.iterrows():
        campo = str(row.get("CAMPO", "")).strip()
        if campo:
            alias_map_prev[campo] = str(row.get("ETIQUETA", campo)).strip() or campo

    base["ETIQUETA"] = base["CAMPO"].map(lambda c: alias_map_prev.get(c, c))
    return base


def alias_df_to_map(alias_df: pd.DataFrame) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if alias_df is None or alias_df.empty:
        return out
    for _, row in alias_df.iterrows():
        campo = str(row.get("CAMPO", "")).strip()
        etiqueta = str(row.get("ETIQUETA", campo)).strip() or campo
        if campo:
            out[campo] = etiqueta
    return out


# -------------------------
# Color helpers for sections
# -------------------------
def _hsl_to_rgb(h: float, s: float, l: float) -> Tuple[int, int, int]:
    """h in [0,360), s,l in [0,1]"""
    c = (1 - abs(2*l - 1)) * s
    hp = h / 60.0
    x = c * (1 - abs((hp % 2) - 1))
    if 0 <= hp < 1:
        r1, g1, b1 = c, x, 0
    elif 1 <= hp < 2:
        r1, g1, b1 = x, c, 0
    elif 2 <= hp < 3:
        r1, g1, b1 = 0, c, x
    elif 3 <= hp < 4:
        r1, g1, b1 = 0, x, c
    elif 4 <= hp < 5:
        r1, g1, b1 = x, 0, c
    else:
        r1, g1, b1 = c, 0, x
    m = l - c/2
    r, g, b = r1 + m, g1 + m, b1 + m
    return int(round(r*255)), int(round(g*255)), int(round(b*255))


def color_for_section(section_value) -> str:
    """Color determinístico por sección (hex #RRGGBB)."""
    try:
        v = int(float(section_value))
    except Exception:
        v = abs(hash(str(section_value))) % 100000
    h = (v * 47) % 360
    r, g, b = _hsl_to_rgb(h, 0.65, 0.55)
    return f"#{r:02x}{g:02x}{b:02x}"


# -------------------------
# KML / KMZ export helpers (con estilos + etiquetas + popup editable)
# -------------------------
def xml_escape(s: str) -> str:
    s = "" if s is None else str(s)
    return (s.replace("&", "&amp;")
             .replace("<", "&lt;")
             .replace(">", "&gt;")
             .replace('"', "&quot;")
             .replace("'", "&apos;"))


def kml_color_from_hex(hex_rgb: str, alpha: int = 140) -> str:
    """
    KML usa AABBGGRR (no RRGGBBAA).
    alpha: 0-255 (0 transparente, 255 opaco)
    """
    h = (hex_rgb or "#999999").lstrip("#")
    if len(h) != 6:
        h = "999999"
    rr = int(h[0:2], 16)
    gg = int(h[2:4], 16)
    bb = int(h[4:6], 16)
    return f"{alpha:02x}{bb:02x}{gg:02x}{rr:02x}"


def style_id_for(val) -> str:
    s = "S_" + re.sub(r"[^A-Za-z0-9_]+", "_", str(val))
    return s[:60]


def label_style_id_for(val) -> str:
    s = "LBL_" + re.sub(r"[^A-Za-z0-9_]+", "_", str(val))
    return s[:60]


def iter_polygons(geom) -> Iterable:
    if geom is None:
        return []
    gt = geom.geom_type
    if gt == "Polygon":
        return [geom]
    if gt == "MultiPolygon":
        return list(geom.geoms)
    if gt == "GeometryCollection":
        out = []
        for g in geom.geoms:
            out.extend(iter_polygons(g))
        return out
    return []


def ring_to_kml_coords(ring) -> str:
    coords = list(ring.coords)
    return " ".join([f"{x:.8f},{y:.8f},0" for x, y in coords])


def polygon_to_kml(poly) -> str:
    outer = ring_to_kml_coords(poly.exterior)
    inners = ""
    for interior in poly.interiors:
        inners += f"""
        <innerBoundaryIs>
          <LinearRing>
            <coordinates>{ring_to_kml_coords(interior)}</coordinates>
          </LinearRing>
        </innerBoundaryIs>"""
    return f"""
    <Polygon>
      <outerBoundaryIs>
        <LinearRing>
          <coordinates>{outer}</coordinates>
        </LinearRing>
      </outerBoundaryIs>
      {inners}
    </Polygon>"""


def geom_to_kml(geom) -> str:
    polys = iter_polygons(geom)
    if not polys:
        return ""
    if len(polys) == 1:
        return polygon_to_kml(polys[0])
    parts = "\n".join([polygon_to_kml(p) for p in polys])
    return f"<MultiGeometry>\n{parts}\n</MultiGeometry>"


def build_section_styles(
    secc_gdf: gpd.GeoDataFrame,
    section_col: Optional[str],
    alpha_fill: int = 140,
    label_scale: float = 1.2,
) -> Tuple[str, Dict[str, str], Dict[str, str]]:
    """
    Retorna:
      - styles_xml: estilos de polígonos + estilos de etiquetas (puntos)
      - value_to_poly_styleid: dict(str(section)->polyStyleId)
      - value_to_lbl_styleid: dict(str(section)->labelStyleId)
    """
    if not section_col or section_col not in secc_gdf.columns:
        return "", {}, {}

    values = sorted(secc_gdf[section_col].dropna().astype(str).unique().tolist())
    poly_map: Dict[str, str] = {}
    lbl_map: Dict[str, str] = {}
    style_chunks = []

    for v in values:
        poly_id = style_id_for(v)
        lbl_id = label_style_id_for(v)
        poly_map[v] = poly_id
        lbl_map[v] = lbl_id

        hex_rgb = color_for_section(v)
        poly_color = kml_color_from_hex(hex_rgb, alpha=alpha_fill)
        line_color = "ff000000"  # negro
        label_color = "ff111111"  # casi negro

        # Polígono
        style_chunks.append(f"""
  <Style id="{xml_escape(poly_id)}">
    <LineStyle><color>{line_color}</color><width>2</width></LineStyle>
    <PolyStyle><color>{poly_color}</color><fill>1</fill><outline>1</outline></PolyStyle>
    <LabelStyle><color>{label_color}</color><scale>{label_scale:.2f}</scale></LabelStyle>
  </Style>
        """)

        # Etiqueta como punto sin ícono (solo texto)
        style_chunks.append(f"""
  <Style id="{xml_escape(lbl_id)}">
    <IconStyle><scale>0</scale></IconStyle>
    <LabelStyle><color>{label_color}</color><scale>{label_scale:.2f}</scale></LabelStyle>
  </Style>
        """)

    return "\n".join(style_chunks), poly_map, lbl_map


def build_popup_description_html(
    row: pd.Series,
    popup_cols: List[str],
    popup_alias_map: Optional[Dict[str, str]] = None,
    title_value: Optional[str] = None,
) -> str:
    popup_alias_map = popup_alias_map or {}
    title_text = html.escape("" if title_value is None else str(title_value))

    rows_html = []
    for c in popup_cols:
        if c not in row.index:
            continue
        v = row.get(c)
        if pd.isna(v):
            v_txt = ""
        else:
            v_txt = str(v)
        label = popup_alias_map.get(c, c)
        rows_html.append(
            "<tr>"
            f"<td style='border:1px solid #999;padding:4px 6px;background:#f4f4f4;font-weight:600;'>{html.escape(str(label))}</td>"
            f"<td style='border:1px solid #999;padding:4px 6px;'>{html.escape(v_txt)}</td>"
            "</tr>"
        )

    body = "".join(rows_html) if rows_html else "<tr><td style='padding:6px;'>Sin datos</td></tr>"
    html_block = (
        "<div style='font-family:Arial,Helvetica,sans-serif;min-width:260px;'>"
        f"<div style='font-size:16px;font-weight:700;margin-bottom:8px;'>{title_text}</div>"
        "<table style='border-collapse:collapse;font-size:12px;width:100%;'>"
        f"{body}"
        "</table>"
        "</div>"
    )
    return html_block.replace("]]>", "]] ]>")


def get_row_title_value(row: pd.Series, preferred_cols: List[str]) -> str:
    for c in preferred_cols:
        if c in row.index:
            v = row.get(c)
            if v is not None and not pd.isna(v) and str(v).strip() != "":
                return str(v)
    return "Elemento"


def gdf_to_kml_folder(
    gdf: gpd.GeoDataFrame,
    folder_name: str,
    name_col: Optional[str],
    max_features: int,
    style_by_col: Optional[str] = None,
    style_map: Optional[Dict[str, str]] = None,
    popup_cols: Optional[List[str]] = None,
    popup_alias_map: Optional[Dict[str, str]] = None,
    include_extended_data: bool = False,
) -> str:
    gdf2 = gdf.copy()
    if len(gdf2) > max_features:
        gdf2 = gdf2.sample(max_features, random_state=7).copy()

    placemarks = []
    cols = [c for c in gdf2.columns if c != "geometry"]
    popup_cols = [c for c in (popup_cols or []) if c in gdf2.columns]

    for _, row in gdf2.iterrows():
        kml_geom = geom_to_kml(row.geometry)
        if not kml_geom:
            continue

        nm = ""
        if name_col and name_col in gdf2.columns:
            nm = str(row.get(name_col))
        else:
            nm = str(row.get("SECCION") or row.get("SECC") or "")

        style_url = ""
        if style_by_col and style_map and style_by_col in gdf2.columns:
            key = row.get(style_by_col)
            if key is not None and not pd.isna(key):
                sid = style_map.get(str(key))
                if sid:
                    style_url = f"<styleUrl>#{xml_escape(sid)}</styleUrl>"

        description_xml = ""
        if popup_cols:
            title_val = get_row_title_value(row, [name_col] if name_col else ["SECCION", "SECC", "ID"])
            desc_html = build_popup_description_html(
                row=row,
                popup_cols=popup_cols,
                popup_alias_map=popup_alias_map,
                title_value=title_val,
            )
            description_xml = f"<description><![CDATA[{desc_html}]]></description>"

        ext = ""
        if include_extended_data:
            data_items = []
            for c in cols:
                v = row.get(c)
                if pd.isna(v):
                    continue
                data_items.append(f'<Data name="{xml_escape(c)}"><value>{xml_escape(v)}</value></Data>')
            ext = f"<ExtendedData>{''.join(data_items)}</ExtendedData>" if data_items else ""

        placemarks.append(f"""
        <Placemark>
          <name>{xml_escape(nm)}</name>
          {style_url}
          {description_xml}
          {ext}
          {kml_geom}
        </Placemark>
        """)
    return f"<Folder><name>{xml_escape(folder_name)}</name>{''.join(placemarks)}</Folder>"


def gdf_to_kml_labels_folder(
    secc_gdf: gpd.GeoDataFrame,
    folder_name: str,
    section_col: str,
    label_style_map: Dict[str, str],
) -> str:
    placemarks = []
    for _, row in secc_gdf.iterrows():
        sec_val = row.get(section_col)
        if sec_val is None or pd.isna(sec_val):
            continue
        sec_key = str(sec_val)
        sid = label_style_map.get(sec_key)
        pt = row.geometry.representative_point()
        placemarks.append(f"""
        <Placemark>
          <name>{xml_escape(sec_key)}</name>
          {"<styleUrl>#"+xml_escape(sid)+"</styleUrl>" if sid else ""}
          <Point><coordinates>{pt.x:.8f},{pt.y:.8f},0</coordinates></Point>
        </Placemark>
        """)
    return f"<Folder><name>{xml_escape(folder_name)}</name>{''.join(placemarks)}</Folder>"


def kml_folder_wrap(folder_name: str, inner_xml: str) -> str:
    return f"<Folder><name>{xml_escape(folder_name)}</name>{inner_xml}</Folder>"


def folder_name_for_dl(value) -> str:
    if value is None or pd.isna(value):
        return "DISTRITO_LOCAL_SIN_VALOR"
    txt = str(value).strip()
    return f"DISTRITO_LOCAL_{txt}"


def subset_manzanas_for_group(
    mza_gdf: Optional[gpd.GeoDataFrame],
    secc_sub: gpd.GeoDataFrame,
    group_col: Optional[str],
    group_value,
    section_col_for_style: Optional[str],
    mza_section_col: Optional[str],
) -> Optional[gpd.GeoDataFrame]:
    if mza_gdf is None or len(mza_gdf) == 0:
        return None

    try:
        if group_col and group_col in mza_gdf.columns:
            return mza_gdf[mza_gdf[group_col].astype(str) == str(group_value)].copy()

        if section_col_for_style and mza_section_col and section_col_for_style in secc_sub.columns and mza_section_col in mza_gdf.columns:
            sec_values = secc_sub[section_col_for_style].dropna().astype(str).unique().tolist()
            if sec_values:
                return mza_gdf[mza_gdf[mza_section_col].astype(str).isin(sec_values)].copy()

        union_geom = secc_sub.geometry.unary_union
        if union_geom is None:
            return None
        mask = mza_gdf.geometry.intersects(union_geom)
        return mza_gdf[mask].copy()
    except Exception:
        return mza_gdf.copy()


def build_grouped_kml_folders(
    secc_gdf: gpd.GeoDataFrame,
    mza_gdf: Optional[gpd.GeoDataFrame],
    section_col_for_style: Optional[str],
    district_local_col: Optional[str],
    mza_section_col: Optional[str],
    name_col_secc: Optional[str],
    name_col_mza: Optional[str],
    max_manzanas: int,
    include_labels: bool,
    poly_style_map: Dict[str, str],
    lbl_style_map: Dict[str, str],
    popup_cols_secc: Optional[List[str]],
    popup_alias_map_secc: Optional[Dict[str, str]],
) -> str:
    folders = []

    if district_local_col and district_local_col in secc_gdf.columns:
        group_values = secc_gdf[district_local_col].dropna().astype(str).unique().tolist()
        group_values = sorted(group_values, key=lambda x: (len(x), x))

        for dl_val in group_values:
            secc_sub = secc_gdf[secc_gdf[district_local_col].astype(str) == str(dl_val)].copy()
            inner_parts = []
            inner_parts.append(
                gdf_to_kml_folder(
                    secc_sub,
                    "SECCIONES",
                    name_col_secc,
                    max_features=len(secc_sub),
                    style_by_col=section_col_for_style,
                    style_map=poly_style_map,
                    popup_cols=popup_cols_secc,
                    popup_alias_map=popup_alias_map_secc,
                    include_extended_data=False,
                )
            )
            if include_labels and section_col_for_style and lbl_style_map:
                inner_parts.append(
                    gdf_to_kml_labels_folder(secc_sub, "ETIQUETAS_SECCIONES", section_col_for_style, lbl_style_map)
                )

            mza_sub = subset_manzanas_for_group(
                mza_gdf=mza_gdf,
                secc_sub=secc_sub,
                group_col=district_local_col,
                group_value=dl_val,
                section_col_for_style=section_col_for_style,
                mza_section_col=mza_section_col,
            )
            if mza_sub is not None and len(mza_sub) > 0:
                inner_parts.append(
                    gdf_to_kml_folder(
                        mza_sub,
                        "MANZANAS",
                        name_col_mza,
                        max_features=max_manzanas,
                        include_extended_data=True,
                    )
                )

            folders.append(kml_folder_wrap(folder_name_for_dl(dl_val), "".join(inner_parts)))
    else:
        folders.append(
            gdf_to_kml_folder(
                secc_gdf,
                "SECCIONES",
                name_col_secc,
                max_features=len(secc_gdf),
                style_by_col=section_col_for_style,
                style_map=poly_style_map,
                popup_cols=popup_cols_secc,
                popup_alias_map=popup_alias_map_secc,
                include_extended_data=False,
            )
        )
        if include_labels and section_col_for_style and lbl_style_map:
            folders.append(gdf_to_kml_labels_folder(secc_gdf, "ETIQUETAS_SECCIONES", section_col_for_style, lbl_style_map))
        if mza_gdf is not None and len(mza_gdf) > 0:
            folders.append(
                gdf_to_kml_folder(
                    mza_gdf,
                    "MANZANAS",
                    name_col_mza,
                    max_features=max_manzanas,
                    include_extended_data=True,
                )
            )

    return "".join(folders)


def build_kml_document(
    secc_gdf: gpd.GeoDataFrame,
    mza_gdf: Optional[gpd.GeoDataFrame],
    section_col_for_style: Optional[str],
    district_local_col: Optional[str],
    mza_section_col: Optional[str],
    name_col_secc: Optional[str],
    name_col_mza: Optional[str],
    max_manzanas: int,
    alpha_fill: int = 140,
    label_scale: float = 1.2,
    include_labels: bool = True,
    popup_cols_secc: Optional[List[str]] = None,
    popup_alias_map_secc: Optional[Dict[str, str]] = None,
) -> str:
    styles_xml, poly_style_map, lbl_style_map = build_section_styles(
        secc_gdf, section_col_for_style, alpha_fill=alpha_fill, label_scale=label_scale
    )

    folders_xml = build_grouped_kml_folders(
        secc_gdf=secc_gdf,
        mza_gdf=mza_gdf,
        section_col_for_style=section_col_for_style,
        district_local_col=district_local_col,
        mza_section_col=mza_section_col,
        name_col_secc=name_col_secc,
        name_col_mza=name_col_mza,
        max_manzanas=max_manzanas,
        include_labels=include_labels,
        poly_style_map=poly_style_map,
        lbl_style_map=lbl_style_map,
        popup_cols_secc=popup_cols_secc,
        popup_alias_map_secc=popup_alias_map_secc,
    )

    kml = f"""<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2">
<Document>
  <name>ICC Export</name>
  {styles_xml}
  {folders_xml}
</Document>
</kml>"""
    return kml


def kml_to_kmz_bytes(kml_text: str, doc_name: str = "doc.kml") -> bytes:
    bio = io.BytesIO()
    with zipfile.ZipFile(bio, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr(doc_name, kml_text.encode("utf-8"))
    return bio.getvalue()


# -------------------------
# Auto-detect SHPs
# -------------------------
def auto_pick_secciones(shps: List[str]) -> Optional[str]:
    low = [s.lower() for s in shps]
    for i, s in enumerate(low):
        if "secciones" in s and s.endswith(".shp"):
            return shps[i]
    for i, s in enumerate(low):
        if "seccion" in s and s.endswith(".shp"):
            return shps[i]
    return None


def auto_pick_manzanas(shps: List[str]) -> Optional[str]:
    low = [s.lower() for s in shps]
    for i, s in enumerate(low):
        if "manzanas" in s and s.endswith(".shp"):
            return shps[i]
    for i, s in enumerate(low):
        if re.search(r"(^|/)\d{2}m\.shp$", s):
            return shps[i]
    for i, s in enumerate(low):
        if "mza" in s and s.endswith(".shp"):
            return shps[i]
    return None


# -------------------------
# Input: 1 ZIP
# -------------------------
zip_file = st.file_uploader("📦 Sube tu ZIP (Secciones + Manzanas)", type=["zip"])
basemap = st.selectbox("Mapa base", ["Relieve (Esri)", "Topográfico (OpenTopoMap)", "Calles (OSM)", "Satélite (Esri)"], index=0)

if not zip_file:
    st.info("Sube el ZIP para empezar.")
    st.stop()

zbytes = zip_file.getvalue()

with st.spinner("Preparando ZIP..."):
    ws, shps = prepare_workspace(zbytes, "ONEZIP")

if not shps:
    st.error("No encontré ningún .shp dentro del ZIP.")
    st.stop()

secc_guess = auto_pick_secciones(shps)
mza_guess = auto_pick_manzanas(shps)

st.subheader("🧩 Capas detectadas")
c1, c2 = st.columns(2)
with c1:
    secc_shp = st.selectbox("Capa de SECCIONES (INE)", shps, index=shps.index(secc_guess) if secc_guess in shps else 0)
with c2:
    mza_shp = st.selectbox("Capa de MANZANAS (INEGI)", shps, index=shps.index(mza_guess) if mza_guess in shps else min(1, len(shps)-1))

with st.expander("🧪 Debug: lista de SHP encontrados", expanded=False):
    st.write(shps)

with st.spinner("Leyendo capas..."):
    secc = read_layer(ws, secc_shp, "Secciones")
    mza = read_layer(ws, mza_shp, "Manzanas")

# -------------------------
# Columnas esperadas
# -------------------------
col_ent = pick_col(list(secc.columns), ["ENTIDAD", "CVE_ENT", "ENT"])
col_mun = pick_col(list(secc.columns), ["MUNICIPIO", "CVE_MUN", "MUN"])
col_dl  = pick_col(list(secc.columns), ["DISTRITO_L", "DISTRITO", "DTO_L", "DIST_L"])
col_df  = pick_col(list(secc.columns), ["DISTRITO_F", "DTO_F", "DIST_F"])
col_sec = pick_col(list(secc.columns), ["SECCION", "SECC", "CVE_SECC", "ID_SECC"])
col_manz = pick_col(list(secc.columns), ["MANZANAS"])
col_vot = pick_col(list(secc.columns), ["VOTANTES", "VOT_EST"])
col_p18 = pick_col(list(secc.columns), ["POB18MAS", "POB_18_MAS", "P18MAS"])

mza_sec = pick_col(list(mza.columns), ["SECCION"])
mza_p18 = pick_col(list(mza.columns), ["POB18MAS", "POB_18_MAS", "P18MAS"])

# -------------------------
# Filtros
# -------------------------
st.divider()
st.subheader("🎛️ Filtros (Distrito / Sección)")

secc_f = secc.copy()

f1, f2, f3, f4 = st.columns([1, 1, 1, 1])

with f1:
    if col_dl:
        vals = safe_int_values(secc_f[col_dl])
        dl_sel = st.selectbox("Distrito Local", ["(todos)"] + vals, index=0)
        if dl_sel != "(todos)":
            secc_f = secc_f[pd.to_numeric(secc_f[col_dl], errors="coerce").fillna(-999999).astype(int) == int(dl_sel)].copy()
    else:
        st.write("Distrito Local: (no detectado)")

with f2:
    if col_df:
        vals = safe_int_values(secc_f[col_df])
        df_sel = st.selectbox("Distrito Federal", ["(todos)"] + vals, index=0)
        if df_sel != "(todos)":
            secc_f = secc_f[pd.to_numeric(secc_f[col_df], errors="coerce").fillna(-999999).astype(int) == int(df_sel)].copy()
    else:
        st.write("Distrito Federal: (no detectado)")

with f3:
    if col_mun:
        vals = safe_int_values(secc_f[col_mun])
        mun_sel = st.selectbox("Municipio", ["(todos)"] + vals, index=0)
        if mun_sel != "(todos)":
            secc_f = secc_f[pd.to_numeric(secc_f[col_mun], errors="coerce").fillna(-999999).astype(int) == int(mun_sel)].copy()
    else:
        st.write("Municipio: (no detectado)")

with f4:
    if col_sec:
        secs = safe_int_values(secc_f[col_sec])
        if "sec_multi" not in st.session_state:
            st.session_state["sec_multi"] = []

        b1, b2 = st.columns(2)
        if b1.button("Seleccionar todas", use_container_width=True):
            st.session_state["sec_multi"] = secs
        if b2.button("Limpiar", use_container_width=True):
            st.session_state["sec_multi"] = []

        sec_selected = st.multiselect(
            "Secciones (multi)",
            options=secs,
            default=st.session_state.get("sec_multi", []),
            key="sec_multi",
        )

        if sec_selected:
            secc_f = secc_f[pd.to_numeric(secc_f[col_sec], errors="coerce").fillna(-999999).astype(int).isin(set(int(x) for x in sec_selected))].copy()
    else:
        st.write("Sección: (no detectada)")

if secc_f.empty:
    st.error("Con esos filtros no quedó ninguna sección.")
    st.stop()

# recorte manzanas por bbox de secciones filtradas
minx, miny, maxx, maxy = secc_f.total_bounds
mza_bbox = mza.cx[minx:maxx, miny:maxy].copy()

# si manzanas tiene SECCION, filtrar por las seleccionadas (más exacto que bbox)
if col_sec and mza_sec and col_sec in secc_f.columns:
    try:
        selected_secs = sorted(pd.to_numeric(secc_f[col_sec], errors="coerce").dropna().astype(int).unique().tolist())
        if selected_secs:
            mza_bbox = mza_bbox[pd.to_numeric(mza_bbox[mza_sec], errors="coerce").fillna(-999999).astype(int).isin(set(selected_secs))].copy()
    except Exception:
        pass

# -------------------------
# KPIs / info general
# -------------------------
st.divider()
st.subheader("📌 Información general del recorte")

k1, k2, k3, k4 = st.columns(4)
k1.metric("Secciones", f"{len(secc_f):,}")
k2.metric("Manzanas (recorte)", f"{len(mza_bbox):,}")

if col_manz:
    k3.metric("Manzanas (sum en secciones)", f"{int(pd.to_numeric(secc_f[col_manz], errors='coerce').fillna(0).sum()):,}")
else:
    k3.metric("Manzanas (sum secciones)", "N/D")

if col_vot:
    try:
        k4.metric("Votantes (col)", f"{int(pd.to_numeric(secc_f[col_vot], errors='coerce').fillna(0).sum()):,}")
    except Exception:
        k4.metric("Votantes (col)", "N/D")
elif col_p18:
    k4.metric("POB 18+ (col)", f"{int(pd.to_numeric(secc_f[col_p18], errors='coerce').fillna(0).sum()):,}")
else:
    k4.metric("Votantes/POB", "N/D")

# -------------------------
# Tabs
# -------------------------
tab_map, tab_tables, tab_export = st.tabs(["🗺️ Mapa", "📋 Tablas", "⬇️ Exportar (Excel/CSV/KMZ/HTML)"])

# -------------------------
# MAP
# -------------------------
with tab_map:
    st.subheader("Mapa")

    cA, cB, cC = st.columns([1, 1, 2])
    with cA:
        show_manz = st.checkbox("Mostrar manzanas", value=False)
    with cB:
        show_labels = st.checkbox("Mostrar número de sección (etiqueta)", value=True)
    with cC:
        label_size = st.slider("Tamaño etiqueta", min_value=10, max_value=26, value=16, step=1)

    lat, lon = safe_center(secc_f)
    m = folium.Map(location=[lat, lon], zoom_start=12, tiles=None, control_scale=True)

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

    fields = []
    aliases = []
    for c, a in [
        (col_sec, "Sección:"),
        (col_dl, "DL:"),
        (col_df, "DF:"),
        (col_mun, "Mpio:"),
        (col_manz, "Manzanas:"),
        (col_vot, "Votantes:"),
        (col_p18, "POB18+:"),
    ]:
        if c and c in secc_f.columns:
            fields.append(c)
            aliases.append(a)

    sec_col = col_sec if col_sec and col_sec in secc_f.columns else None

    def style_fn(feat):
        props = feat.get("properties", {})
        sec_val = props.get(sec_col) if sec_col else None
        fc = color_for_section(sec_val if sec_val is not None else props.get("id", 0))
        return {"color": "#000000", "weight": 2, "fillColor": fc, "fillOpacity": 0.35}

    folium.GeoJson(
        secc_f.to_json(),
        name="Secciones",
        style_function=style_fn,
        tooltip=folium.GeoJsonTooltip(fields=fields, aliases=aliases, sticky=False) if fields else None,
    ).add_to(m)

    if show_labels and sec_col:
        for _, row in secc_f.iterrows():
            sec_val = row.get(sec_col)
            pt = row.geometry.representative_point()
            folium.Marker(
                location=[pt.y, pt.x],
                icon=folium.DivIcon(
                    html=f"""
                    <div style="
                        font-size:{label_size}px;
                        font-weight:700;
                        color:#111;
                        background:rgba(255,255,255,0.70);
                        border:1px solid rgba(0,0,0,0.35);
                        border-radius:6px;
                        padding:1px 6px;
                        line-height:1.1;
                        white-space:nowrap;">
                        {sec_val}
                    </div>
                    """
                )
            ).add_to(m)

    if show_manz:
        max_feat = 6000
        mz_show = mza_bbox.sample(max_feat, random_state=7).copy() if len(mza_bbox) > max_feat else mza_bbox
        folium.GeoJson(
            mz_show.to_json(),
            name="Manzanas",
            style_function=lambda feat: {"weight": 1, "fillOpacity": 0.04},
        ).add_to(m)

    folium.LayerControl(collapsed=False).add_to(m)
    bds = secc_f.total_bounds
    m.fit_bounds([[bds[1], bds[0]], [bds[3], bds[2]]])

    st.session_state["LAST_MAP_HTML"] = m.get_root().render()
    st_folium(m, use_container_width=True, height=650)

# -------------------------
# TABLES
# -------------------------
with tab_tables:
    st.subheader("Tabla de secciones (filtradas)")
    show_cols = [c for c in [col_ent, col_mun, col_dl, col_df, col_sec, col_manz, col_p18, col_vot] if c and c in secc_f.columns]
    df_secc = secc_f[show_cols].copy() if show_cols else secc_f.drop(columns=["geometry"], errors="ignore").copy()
    st.dataframe(df_secc.drop(columns=["geometry"], errors="ignore"), use_container_width=True, height=420)

    st.subheader("Tabla de manzanas (recorte)")
    df_mza = mza_bbox.drop(columns=["geometry"], errors="ignore").copy()

    front = [c for c in ["CVE_ENT", "CVE_MUN", "CVE_LOC", "CVE_AGEB", "CVE_MZA", "TIPOMZA"] if c in df_mza.columns]
    extra = [c for c in [mza_sec, mza_p18] if c and c in df_mza.columns and c not in front]
    rest = [c for c in df_mza.columns if c not in front + extra]
    df_mza = df_mza[front + extra + rest]
    st.dataframe(df_mza, use_container_width=True, height=420)

# -------------------------
# EXPORT
# -------------------------
with tab_export:
    st.subheader("Exportación")

    resumen = {"SECCIONES": len(secc_f), "MANZANAS_RECORTE": len(mza_bbox)}
    if col_manz:
        resumen["MANZANAS_SUM_SECCIONES"] = int(pd.to_numeric(secc_f[col_manz], errors="coerce").fillna(0).sum())
    if col_p18:
        resumen["POB18MAS_TOTAL"] = int(pd.to_numeric(secc_f[col_p18], errors="coerce").fillna(0).sum())
    if col_vot:
        resumen["VOTANTES_TOTAL"] = int(pd.to_numeric(secc_f[col_vot], errors="coerce").fillna(0).sum())
    df_res = pd.DataFrame([resumen])

    cA, cB = st.columns(2)
    with cA:
        st.download_button(
            "⬇️ Excel (resumen + secciones + manzanas)",
            data=to_excel_bytes({"RESUMEN": df_res, "SECCIONES": df_secc.drop(columns=["geometry"], errors="ignore"), "MANZANAS": df_mza}),
            file_name="export_distrito_secciones_manzanas.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
    with cB:
        st.download_button(
            "⬇️ CSV (secciones)",
            data=df_secc.drop(columns=["geometry"], errors="ignore").to_csv(index=False).encode("utf-8"),
            file_name="secciones_filtradas.csv",
            mime="text/csv",
            use_container_width=True
        )

    st.download_button(
        "⬇️ CSV (manzanas recorte)",
        data=df_mza.to_csv(index=False).encode("utf-8"),
        file_name="manzanas_recorte.csv",
        mime="text/csv",
        use_container_width=True
    )

    st.divider()
    st.subheader("✏️ Edición manual del globo KMZ (Secciones)")
    st.caption("Aquí sí puedes cambiar manualmente los valores que luego verás en Google Earth al dar clic sobre la sección.")

    secc_for_kmz = secc_f.reset_index(drop=True).copy()
    secc_for_kmz["__KMZ_ID__"] = range(1, len(secc_for_kmz) + 1)

    secc_edit_source = secc_for_kmz.drop(columns=["geometry"], errors="ignore").copy()
    ordered_cols = ["__KMZ_ID__"] + [c for c in secc_edit_source.columns if c != "__KMZ_ID__"]
    secc_edit_source = secc_edit_source[ordered_cols]

    editor_df = ensure_editor_df("KMZ_SECC_EDIT_DF", "KMZ_SECC_EDIT_FP", secc_edit_source)
    edited_df = st.data_editor(
        editor_df,
        key="KMZ_SECC_EDITOR",
        disabled=["__KMZ_ID__"],
        use_container_width=True,
        height=320,
        num_rows="fixed",
    )
    update_editor_df("KMZ_SECC_EDIT_DF", edited_df)

    all_popup_cols = [c for c in edited_df.columns if c != "__KMZ_ID__"]
    default_popup_cols = [c for c in [col_sec, col_dl, col_df, col_mun, col_manz, col_vot, col_p18] if c and c in all_popup_cols]
    if not default_popup_cols:
        default_popup_cols = all_popup_cols[: min(8, len(all_popup_cols))]

    selected_popup_cols = st.multiselect(
        "Campos que se verán en el globo KMZ",
        options=all_popup_cols,
        default=default_popup_cols,
        key="KMZ_POPUP_COLS",
        help="Estos son los campos que saldrán en el popup del KMZ. Los valores los puedes editar en la tabla de arriba.",
    )

    alias_state_key = "KMZ_ALIAS_DF"
    alias_fp_key = "KMZ_ALIAS_FP"
    alias_seed = normalize_alias_df(selected_popup_cols, st.session_state.get(alias_state_key))
    alias_df = ensure_editor_df(alias_state_key, alias_fp_key, alias_seed)
    alias_df = normalize_alias_df(selected_popup_cols, alias_df)

    st.caption("Renombra aquí cómo quieres que aparezca cada campo en el popup de Google Earth.")
    alias_df_edited = st.data_editor(
        alias_df,
        key="KMZ_ALIAS_EDITOR",
        disabled=["CAMPO"],
        use_container_width=True,
        height=min(80 + max(len(alias_df), 1) * 35, 360),
        num_rows="fixed",
    )
    update_editor_df(alias_state_key, normalize_alias_df(selected_popup_cols, alias_df_edited))
    popup_alias_map = alias_df_to_map(st.session_state.get(alias_state_key, pd.DataFrame()))

    st.divider()
    st.subheader("🗺️ Exportar KMZ (Google Earth) — agrupado por Distrito Local")

    include_manz = st.checkbox("Incluir manzanas en el KMZ (puede pesar)", value=False)
    max_mz = st.slider("Límite máximo de manzanas en KMZ", min_value=500, max_value=20000, value=6000, step=500)
    alpha = st.slider("Transparencia relleno (KMZ)", min_value=40, max_value=220, value=140, step=10,
                      help="Más alto = más opaco. Se aplica al relleno de las secciones en el KMZ.")
    include_kmz_labels = st.checkbox("Incluir etiquetas (número de sección) en el KMZ", value=True)
    kmz_label_scale = st.slider("Tamaño etiqueta (KMZ)", min_value=0.6, max_value=3.0, value=1.3, step=0.1)

    if col_dl and col_dl in edited_df.columns:
        dl_export_vals = sorted(edited_df[col_dl].dropna().astype(str).unique().tolist())
        st.info(f"📦 El KMZ se agrupará dentro de carpetas tipo: {', '.join(folder_name_for_dl(v) for v in dl_export_vals[:3])}{' ...' if len(dl_export_vals) > 3 else ''}")
    else:
        st.warning("⚠️ No detecté columna de Distrito Local. El KMZ saldrá sin la carpeta agrupadora por DL.")

    if st.button("Preparar KMZ", use_container_width=True):
        with st.spinner("Generando KMZ..."):
            secc_for_kmz_export = secc_for_kmz.copy()
            merge_df = edited_df.copy()
            secc_for_kmz_export = secc_for_kmz_export.drop(columns=[c for c in secc_for_kmz_export.columns if c != "geometry" and c in merge_df.columns and c != "__KMZ_ID__"], errors="ignore")
            secc_for_kmz_export = secc_for_kmz_export.merge(merge_df, on="__KMZ_ID__", how="left")

            mza_for_kmz = mza_bbox if include_manz else None
            kml = build_kml_document(
                secc_gdf=secc_for_kmz_export,
                mza_gdf=mza_for_kmz,
                section_col_for_style=col_sec if col_sec else None,
                district_local_col=col_dl if col_dl else None,
                mza_section_col=mza_sec if mza_sec else None,
                name_col_secc=col_sec if col_sec else None,
                name_col_mza=mza_sec if mza_sec else None,
                max_manzanas=max_mz,
                alpha_fill=int(alpha),
                label_scale=float(kmz_label_scale),
                include_labels=bool(include_kmz_labels),
                popup_cols_secc=selected_popup_cols,
                popup_alias_map_secc=popup_alias_map,
            )
            kmz_bytes = kml_to_kmz_bytes(kml, "doc.kml")
            st.session_state["LAST_KMZ"] = kmz_bytes
        st.success("KMZ listo ✅ (agrupado por Distrito Local + popup editable)")

    kmz_bytes = st.session_state.get("LAST_KMZ")
    if kmz_bytes:
        st.download_button(
            "⬇️ Descargar KMZ",
            data=kmz_bytes,
            file_name="export_secciones_manzanas_agrupado_dl_popup_editable.kmz",
            mime="application/vnd.google-earth.kmz",
            use_container_width=True
        )

    st.divider()
    st.subheader("🖨️ Imprimir (pantalla del mapa)")

    map_html = st.session_state.get("LAST_MAP_HTML")
    if not map_html:
        st.info("Primero entra a la pestaña **Mapa** para que se genere el HTML.")
    else:
        st.download_button(
            "⬇️ Descargar HTML del mapa (para imprimir)",
            data=map_html.encode("utf-8"),
            file_name="mapa_filtrado.html",
            mime="text/html",
            use_container_width=True
        )
        st.caption("Abre el HTML en tu navegador y usa **Ctrl+P / Imprimir** (o “Guardar como PDF”).")

st.success("✅ Listo. El KMZ ahora puede agruparse por Distrito Local y el popup de secciones ya se puede editar manualmente antes de exportar.")
