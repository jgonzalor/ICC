#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import tempfile
import zipfile
from collections import defaultdict
import random  # colores

import shapefile        # pyshp
import simplekml        # para crear KML/KMZ
import streamlit as st  # la app web
from pyproj import CRS, Transformer  # para transformar UTM -> WGS84


st.set_page_config(page_title="KMZ por Distrito y Sección (INE)", layout="centered")

st.title("Generador de KMZ por Distrito y Sección (INE)")
st.write(
    """
Sube un **ZIP** que contenga el shapefile de SECCIONES del INE:

- Debe incluir al menos: `.shp`, `.dbf`, `.shx`, `.prj`
- Ejemplo: `SECCION.shp`, `SECCION.dbf`, `SECCION.shx`, `SECCION.prj`

La app creará un **KMZ** con carpetas por distrito y, dentro de cada carpeta,
las secciones ordenadas por número, posicionadas en Google Earth y rotuladas.
"""
)

def polygon_centroid(coords):
    """
    Centroide de polígono (lon/lat).
    Si el área es muy pequeña, usa promedio simple.
    """
    n = len(coords)
    if n == 0:
        return 0.0, 0.0

    x_list = [p[0] for p in coords]
    y_list = [p[1] for p in coords]

    area_factor = 0.0
    cx = 0.0
    cy = 0.0
    for i in range(n):
        x0, y0 = coords[i]
        x1, y1 = coords[(i + 1) % n]
        cross = x0 * y1 - x1 * y0
        area_factor += cross
        cx += (x0 + x1) * cross
        cy += (y0 + y1) * cross

    area = area_factor / 2.0
    if abs(area) < 1e-9:
        return sum(x_list) / n, sum(y_list) / n

    cx /= (6.0 * area)
    cy /= (6.0 * area)
    return cx, cy


uploaded_zip = st.file_uploader(
    "Sube el archivo ZIP con el shapefile de SECCIONES",
    type=["zip"]
)

# Selector de estilo de color (tipo INE vs aleatorio por sección)
estilo_color = st.radio(
    "Estilo de colores",
    ["Tipo INE (un color por distrito)", "Aleatorio por sección"],
    index=0,
    help="Tipo INE: todas las secciones de un distrito comparten color. "
         "Aleatorio: cada sección recibe un color distinto."
)

if uploaded_zip is not None:
    # Carpeta temporal
    tmpdir = tempfile.mkdtemp()
    zip_path = os.path.join(tmpdir, "input.zip")

    # Guardar ZIP
    with open(zip_path, "wb") as f:
        f.write(uploaded_zip.read())

    # Extraer ZIP
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(tmpdir)

    # Buscar .shp
    shp_files = [f for f in os.listdir(tmpdir) if f.lower().endswith(".shp")]

    if not shp_files:
        st.error("No encontré ningún archivo .shp dentro del ZIP. Revisa el contenido.")
    else:
        if len(shp_files) > 1:
            st.warning(
                "Se encontraron varios .shp en el ZIP. "
                "Se usará el primero: **{}**".format(shp_files[0])
            )

        shp_name = shp_files[0]
        shp_base = os.path.join(tmpdir, shp_name[:-4])  # sin .shp

        # ----- 1. Intentar leer la proyección desde el .prj -----
        transformer = None
        prj_path = shp_base + ".prj"
        if os.path.exists(prj_path):
            try:
                with open(prj_path, "r", encoding="utf-8", errors="ignore") as f:
                    wkt = f.read()
                crs_src = CRS.from_wkt(wkt)      # CRS original (UTM INE)
                crs_dst = CRS.from_epsg(4326)    # WGS84 (lon/lat) para Google Earth
                transformer = Transformer.from_crs(
                    crs_src, crs_dst, always_xy=True
                )
                st.success("Se detectó el sistema de coordenadas del shapefile y se transformará a WGS84.")
            except Exception as e:
                st.warning(
                    "No se pudo interpretar el archivo .prj.\n"
                    "Se usarán las coordenadas tal cual (pueden quedar mal ubicadas).\n"
                    f"Detalle técnico: {e}"
                )
        else:
            st.warning(
                "No se encontró el archivo .prj en el ZIP.\n"
                "Se usarán las coordenadas tal cual (pueden quedar mal ubicadas)."
            )

        # ----- 2. Leer shapefile -----
        try:
            reader = shapefile.Reader(shp_base)
        except Exception as e:
            st.error(f"No se pudo leer el shapefile: {e}")
            st.stop()

        original_fields = reader.fields[1:]  # saltar DeletionFlag
        field_names = [f[0] for f in original_fields]

        st.subheader("Campos detectados en el shapefile")
        st.code(", ".join(field_names))

        # Selección de campos
        distrito_field = st.selectbox(
            "Campo que representa el DISTRITO LOCAL",
            options=field_names,
            index=field_names.index("DISTRITO_L") if "DISTRITO_L" in field_names else 0
        )

        seccion_field = st.selectbox(
            "Campo que representa la SECCIÓN",
            options=field_names,
            index=field_names.index("SECCION") if "SECCION" in field_names else 0
        )

        st.info(
            "Si tus campos se llaman distinto (por ejemplo `DL` o `SEC`), "
            "selecciónalos en las listas de arriba."
        )

        if st.button("Generar KMZ por distrito y sección"):
            registros_por_distrito = defaultdict(list)

            for shape_rec in reader.iterShapeRecords():
                rec_vals = list(shape_rec.record)
                attrs = dict(zip(field_names, rec_vals))

                # Distrito
                try:
                    dist = int(str(attrs[distrito_field]).strip())
                except Exception:
                    dist = attrs[distrito_field]

                # Sección
                try:
                    sec = int(str(attrs[seccion_field]).strip())
                except Exception:
                    sec = attrs[seccion_field]

                registros_por_distrito[dist].append(
                    (sec, shape_rec.shape, attrs)
                )

            # ----- 3. Crear KML -----
            kml = simplekml.Kml()

            # Paleta fija tipo INE (tonos verdes/marrones suaves)
            paleta_distritos = [
                (166, 219, 160),
                (199, 233, 192),
                (161, 217, 155),
                (116, 196, 118),
                (65, 171, 93),
                (35, 139, 69),
                (0, 109, 44),
                (0, 90, 50),
            ]
            color_por_distrito = {}

            for idx_dist, (dist, lista) in enumerate(
                sorted(registros_por_distrito.items(), key=lambda x: x[0])
            ):
                folder = kml.newfolder(name=f"Distrito {dist:02d}")

                lista_ordenada = sorted(lista, key=lambda x: x[0])

                # Color para este distrito si estamos en modo "Tipo INE"
                if estilo_color == "Tipo INE (un color por distrito)":
                    if dist not in color_por_distrito:
                        r, g, b = paleta_distritos[idx_dist % len(paleta_distritos)]
                        color_por_distrito[dist] = (r, g, b)

                for sec, shape, attrs in lista_ordenada:
                    pts = shape.points
                    if not pts:
                        continue

                    coords = []
                    for x, y in pts:
                        if transformer is not None:
                            lon, lat = transformer.transform(x, y)
                        else:
                            lon, lat = x, y
                        coords.append((lon, lat))

                    # Texto de la sección (solo número)
                    if isinstance(sec, int):
                        etiqueta = f"{sec}"
                    else:
                        etiqueta = str(sec)

                    # Polígono
                    pol = folder.newpolygon(
                        name=etiqueta,
                        outerboundaryis=coords
                    )

                    # ----- COLORES -----
                    if estilo_color == "Tipo INE (un color por distrito)":
                        # Mismo color para todo el distrito
                        r, g, b = color_por_distrito[dist]
                    else:
                        # Aleatorio por sección
                        r = random.randint(80, 255)
                        g = random.randint(80, 255)
                        b = random.randint(80, 255)

                    pol.style.polystyle.color = simplekml.Color.rgb(r, g, b, 150)
                    pol.style.polystyle.fill = 1
                    pol.style.linestyle.color = simplekml.Color.rgb(90, 60, 40, 255)
                    pol.style.linestyle.width = 1.2
                    # -------------------

                    # ----- LABEL: número de sección en el centro -----
                    cx, cy = polygon_centroid(coords)

                    punto_label = folder.newpoint(
                        name=etiqueta,
                        coords=[(cx, cy)]
                    )
                    punto_label.style.iconstyle.scale = 0.1   # icono casi invisible
                    punto_label.style.labelstyle.scale = 1.4  # tamaño de texto
                    # ------------------------------------------------

            kmz_path = os.path.join(tmpdir, "secciones_por_distrito.kmz")
            kml.savekmz(kmz_path)

            st.success("KMZ generado correctamente con coordenadas en WGS84, colores y rótulos por sección.")

            with open(kmz_path, "rb") as f:
                st.download_button(
                    label="⬇️ Descargar KMZ por distrito y sección",
                    data=f.read(),
                    file_name="secciones_por_distrito.kmz",
                    mime="application/vnd.google-earth.kmz"
                )

            st.markdown(
                """
                **Uso en Google Earth Pro:**

                1. Archivo → Abrir → `secciones_por_distrito.kmz`
                2. Verás una carpeta por **Distrito**.
                3. Dentro de cada distrito, las secciones ordenadas por número.
                4. En modo *Tipo INE* cada distrito tendrá un color uniforme; en modo *Aleatorio* cada sección tiene su propio color.
                5. El número de la sección aparece rotulado sobre cada polígono.
                """
            )
app_kmz_distritos.py
