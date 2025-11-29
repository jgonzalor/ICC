#!/usr/bin/env python
# -*- coding: utf-8 -*-

import io
import zipfile
import random
import xml.etree.ElementTree as ET

import pandas as pd
import pydeck as pdk
import streamlit as st


st.set_page_config(page_title="Visor KMZ de Secciones", layout="wide")

st.title("Visor web de KMZ — Secciones por Distrito")
st.write(
    """
Sube un archivo **KMZ o KML** (por ejemplo, el que generaste con la app de secciones):

- La app leerá el KML interno.
- Detectará **carpetas por Distrito** (Folder) y **Placemarks** con polígonos.
- Intentará respetar el **color de relleno** que venga en el KMZ (PolyStyle o styleUrl).
- Pintará el **número de sección dentro de cada polígono**.
"""
)


def parse_kml_color(color_text: str):
    """
    Convierte un color KML (aabbggrr en hex) a lista [r, g, b, a] (0-255).

    Ejemplo KML: '96ff0000' -> a=0x96, b=0xff, g=0x00, r=0x00 -> [0, 0, 255, 150]
    """
    if not color_text:
        return None
    txt = color_text.strip().lstrip("#")
    if len(txt) != 8:
        return None
    try:
        aa = int(txt[0:2], 16)
        bb = int(txt[2:4], 16)
        gg = int(txt[4:6], 16)
        rr = int(txt[6:8], 16)
        return [rr, gg, bb, aa]
    except Exception:
        return None


def cargar_kmz_o_kml(uploaded_file: io.BytesIO) -> pd.DataFrame:
    """
    Lee un KMZ/KML y devuelve un DataFrame con:
    - district
    - section
    - polygon  (lista de [lon, lat])
    - centroid_lon
    - centroid_lat
    - color  (lista [r,g,b,a] si venía en el KML, sino None)
    """
    # 1) Leer el texto KML
    filename = uploaded_file.name.lower()
    if filename.endswith(".kmz"):
        zf = zipfile.ZipFile(uploaded_file)
        # Tomamos el primer .kml dentro del KMZ
        kml_name = None
        for name in zf.namelist():
            if name.lower().endswith(".kml"):
                kml_name = name
                break
        if kml_name is None:
            raise ValueError("El KMZ no contiene ningún archivo .kml.")
        kml_bytes = zf.read(kml_name)
        kml_text = kml_bytes.decode("utf-8", errors="ignore")
    else:
        # Asumimos KML plano
        kml_text = uploaded_file.read().decode("utf-8", errors="ignore")

    # 2) Parsear XML
    ns = {"kml": "http://www.opengis.net/kml/2.2"}
    root = ET.fromstring(kml_text)

    # --- Mapa de estilos globales (Style id -> color) ---
    style_colors = {}
    for style in root.findall(".//kml:Style", ns):
        style_id = style.attrib.get("id")
        if not style_id:
            continue
        color_elem = style.find(".//kml:PolyStyle/kml:color", ns)
        if color_elem is None or not color_elem.text:
            continue
        rgba = parse_kml_color(color_elem.text)
        if rgba is None:
            continue
        # Guardamos con y sin '#', porque styleUrl suele venir como '#id'
        style_colors[style_id] = rgba
        style_colors["#" + style_id] = rgba

    registros = []

    # Buscamos Folders (cada uno suele ser un Distrito)
    for folder in root.findall(".//kml:Folder", ns):
        name_elem = folder.find("kml:name", ns)
        district_name = name_elem.text.strip() if name_elem is not None else "Sin distrito"

        # Dentro del folder, buscamos Placemarks con Polygon
        for placemark in folder.findall("kml:Placemark", ns):
            sec_elem = placemark.find("kml:name", ns)
            section_name = sec_elem.text.strip() if sec_elem is not None else ""

            # 1) Intentar leer PolyStyle/color embebido en el Placemark
            color_rgba = None
            color_elem = placemark.find(".//kml:PolyStyle/kml:color", ns)
            if color_elem is not None and color_elem.text:
                color_rgba = parse_kml_color(color_elem.text)

            # 2) Si no trae PolyStyle directo, ver si tiene styleUrl y buscar en styles globales
            if color_rgba is None:
                style_url_elem = placemark.find("kml:styleUrl", ns)
                if style_url_elem is not None and style_url_elem.text:
                    style_ref = style_url_elem.text.strip()
                    if style_ref in style_colors:
                        color_rgba = style_colors[style_ref]

            poly = placemark.find(".//kml:Polygon", ns)
            if poly is None:
                continue
            coords_elem = poly.find(".//kml:coordinates", ns)
            if coords_elem is None or not coords_elem.text:
                continue

            coord_pairs = []
            for token in coords_elem.text.replace("\n", " ").split():
                parts = token.split(",")
                if len(parts) < 2:
                    continue
                try:
                    lon = float(parts[0])
                    lat = float(parts[1])
                    coord_pairs.append([lon, lat])
                except ValueError:
                    continue

            if not coord_pairs:
                continue

            lons = [p[0] for p in coord_pairs]
            lats = [p[1] for p in coord_pairs]
            centroid_lon = sum(lons) / len(lons)
            centroid_lat = sum(lats) / len(lats)

            registros.append(
                {
                    "district": district_name,
                    "section_raw": section_name,
                    "polygon": coord_pairs,
                    "centroid_lon": centroid_lon,
                    "centroid_lat": centroid_lat,
                    "color": color_rgba,
                }
            )

    df = pd.DataFrame(registros)
    if df.empty:
        return df

    # Intentar sacar solo el número de sección
    def limpia_sec(s):
        if s is None:
            return ""
        txt = str(s).strip()
        txt = txt.replace("SEC", "").replace("Sec", "").strip()
        return txt

    df["section"] = df["section_raw"].apply(limpia_sec)

    # Extra: campo numérico cuando se pueda
    def to_int_or_none(s):
        try:
            return int(s)
        except Exception:
            return None

    df["section_int"] = df["section"].apply(to_int_or_none)

    return df


uploaded_kmz = st.file_uploader(
    "Sube tu archivo KMZ/KML con las secciones",
    type=["kmz", "kml"]
)

if uploaded_kmz is not None:
    try:
        df = cargar_kmz_o_kml(uploaded_kmz)
    except Exception as e:
        st.error(f"No se pudo leer el KMZ/KML: {e}")
        st.stop()

    if df.empty:
        st.warning("No se encontraron polígonos en el archivo.")
        st.stop()

    st.success(f"Se cargaron {len(df)} secciones de {df['district'].nunique()} distritos.")

    # --- Panel lateral de filtros ---
    st.sidebar.header("Filtros")

    distritos = sorted(df["district"].unique())
    dist_sel = st.sidebar.multiselect(
        "Distrito",
        options=distritos,
        default=distritos
    )

    df_filtrado = df[df["district"].isin(dist_sel)]

    # Filtrado por sección
    def sort_key(x):
        try:
            return (0, int(x))
        except Exception:
            return (1, x)

    secciones_vals = sorted(df_filtrado["section"].unique().tolist(), key=sort_key)

    sec_sel = st.sidebar.multiselect(
        "Sección (opcional)",
        options=secciones_vals,
        default=[]
    )

    if sec_sel:
        df_filtrado = df_filtrado[df_filtrado["section"].isin(sec_sel)]

    if df_filtrado.empty:
        st.warning("No hay secciones con esos filtros.")
        st.stop()

    # --- Colores: usar los del KMZ si existen, si no, generarlos por distrito ---
    df_filtrado = df_filtrado.copy()

    if df_filtrado["color"].isna().all():
        # El KMZ no traía colores: generamos por distrito
        colores_por_distrito = {}
        for d in df_filtrado["district"].unique():
            random.seed(hash(d) & 0xFFFF)
            r = random.randint(80, 255)
            g = random.randint(80, 255)
            b = random.randint(80, 255)
            colores_por_distrito[d] = [r, g, b, 140]  # RGBA
        df_filtrado["color_vis"] = df_filtrado["district"].map(colores_por_distrito)
    else:
        # Usamos el color que venga del KMZ; si alguno no tiene, le ponemos uno por distrito
        colores_por_distrito = {}
        for d in df_filtrado["district"].unique():
            random.seed(hash(d) & 0xFFFF)
            r = random.randint(80, 255)
            g = random.randint(80, 255)
            b = random.randint(80, 255)
            colores_por_distrito[d] = [r, g, b, 140]

        def pick_color(row):
            if isinstance(row["color"], list):
                return row["color"]
            return colores_por_distrito[row["district"]]

        df_filtrado["color_vis"] = df_filtrado.apply(pick_color, axis=1)

    # --- Vista inicial del mapa ---
    view_state = pdk.ViewState(
        longitude=float(df_filtrado["centroid_lon"].mean()),
        latitude=float(df_filtrado["centroid_lat"].mean()),
        zoom=11,
        pitch=0,
    )

    # --- Capa de polígonos ---
    polygon_layer = pdk.Layer(
        "PolygonLayer",
        df_filtrado,
        get_polygon="polygon",
        get_fill_color="color_vis",
        get_line_color=[80, 80, 80],
        get_line_width=30,
        pickable=True,
        auto_highlight=True,
    )

    # --- Capa de texto con el número de sección ---
    text_layer = pdk.Layer(
        "TextLayer",
        df_filtrado,
        get_position="[centroid_lon, centroid_lat]",
        get_text="section",
        get_size=12,
        get_color=[0, 0, 0, 255],
        get_alignment_baseline="'center'",
    )

    tooltip = {
        "html": "<b>Distrito:</b> {district}<br/>"
                "<b>Sección:</b> {section}",
        "style": {"backgroundColor": "white", "color": "black"}
    }

    deck = pdk.Deck(
        layers=[polygon_layer, text_layer],
        initial_view_state=view_state,
        tooltip=tooltip,
        map_style="light"
    )

    # --- Mapa a todo el ancho ---
    st.subheader("Mapa interactivo")
    st.pydeck_chart(deck, use_container_width=True)

else:
    st.info("Sube un KMZ/KML para comenzar.")
