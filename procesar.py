#!/usr/bin/env python3
"""
Descarga el Excel semanal del Ministerio de Transición Ecológica
y genera datos.json con todos los embalses peninsulares > 5 hm³.

Fuente oficial:
https://www.miteco.gob.es/es/agua/temas/evaluacion-de-los-recursos-hidricos/boletin-hidrologico.html
"""

import io
import json
import zipfile
import datetime
import urllib.request
import sys

import openpyxl

# ── Metadatos estáticos: cuenca y provincia por embalse ──────────────────────
# El Excel del Ministerio no incluye provincia, así que la añadimos aquí.
# Puedes ampliar este diccionario si aparecen embalses nuevos.
META = {
    # nombre_normalizado: (cuenca_oficial, provincia, ccaa)
    "la serena":               ("Guadiana",                  "Badajoz",       "Extremadura"),
    "cijara":                  ("Guadiana",                  "Badajoz",       "Extremadura"),
    "garcia de sola":          ("Guadiana",                  "Badajoz",       "Extremadura"),
    "orellana":                ("Guadiana",                  "Badajoz",       "Extremadura"),
    "zujar":                   ("Guadiana",                  "Badajoz",       "Extremadura"),
    "proserpina":              ("Guadiana",                  "Badajoz",       "Extremadura"),
    "cornalvo":                ("Guadiana",                  "Badajoz",       "Extremadura"),
    "el vicario":              ("Guadiana",                  "Ciudad Real",   "Castilla-La Mancha"),
    "peñarroya":               ("Guadiana",                  "Ciudad Real",   "Castilla-La Mancha"),
    "alcantara":               ("Tajo",                      "Cáceres",       "Extremadura"),
    "valdecañas":              ("Tajo",                      "Cáceres",       "Extremadura"),
    "gabriel y galan":         ("Tajo",                      "Cáceres",       "Extremadura"),
    "rosarito":                ("Tajo",                      "Cáceres",       "Extremadura"),
    "borbollon":               ("Tajo",                      "Cáceres",       "Extremadura"),
    "buendia":                 ("Tajo",                      "Cuenca",        "Castilla-La Mancha"),
    "entrepeñas":              ("Tajo",                      "Guadalajara",   "Castilla-La Mancha"),
    "el atazar":               ("Tajo",                      "Madrid",        "Madrid"),
    "santillana":              ("Tajo",                      "Madrid",        "Madrid"),
    "el vado":                 ("Tajo",                      "Guadalajara",   "Castilla-La Mancha"),
    "beleña":                  ("Tajo",                      "Guadalajara",   "Castilla-La Mancha"),
    "la tajera":               ("Tajo",                      "Guadalajara",   "Castilla-La Mancha"),
    "valmayor":                ("Tajo",                      "Madrid",        "Madrid"),
    "picadas":                 ("Tajo",                      "Madrid",        "Madrid"),
    "almendra":                ("Duero",                     "Salamanca",     "Castilla y León"),
    "ricobayo":                ("Duero",                     "Zamora",        "Castilla y León"),
    "porma":                   ("Duero",                     "León",          "Castilla y León"),
    "riaño":                   ("Duero",                     "León",          "Castilla y León"),
    "barrios de luna":         ("Duero",                     "León",          "Castilla y León"),
    "villalcampo":             ("Duero",                     "Zamora",        "Castilla y León"),
    "castro":                  ("Duero",                     "Zamora",        "Castilla y León"),
    "linares del arroyo":      ("Duero",                     "Segovia",       "Castilla y León"),
    "aguilar de campoo":       ("Duero",                     "Palencia",      "Castilla y León"),
    "cervera":                 ("Duero",                     "Palencia",      "Castilla y León"),
    "requejada":               ("Duero",                     "Palencia",      "Castilla y León"),
    "compuerto":               ("Duero",                     "Palencia",      "Castilla y León"),
    "cuerda del pozo":         ("Duero",                     "Soria",         "Castilla y León"),
    "santa teresa":            ("Duero",                     "Salamanca",     "Castilla y León"),
    "burgomillodo":            ("Duero",                     "Segovia",       "Castilla y León"),
    "mequinenza":              ("Ebro",                      "Zaragoza",      "Aragón"),
    "yesa":                    ("Ebro",                      "Zaragoza",      "Aragón"),
    "mediano":                 ("Ebro",                      "Huesca",        "Aragón"),
    "ribarroja":               ("Ebro",                      "Tarragona",     "Cataluña"),
    "itoiz":                   ("Ebro",                      "Navarra",       "Navarra"),
    "el grado":                ("Ebro",                      "Huesca",        "Aragón"),
    "vadiello":                ("Ebro",                      "Huesca",        "Aragón"),
    "canelles":                ("Ebro",                      "Huesca",        "Aragón"),
    "santa ana":               ("Ebro",                      "Huesca",        "Aragón"),
    "bubal":                   ("Ebro",                      "Huesca",        "Aragón"),
    "lanuza":                  ("Ebro",                      "Huesca",        "Aragón"),
    "sotonera":                ("Ebro",                      "Huesca",        "Aragón"),
    "alloz":                   ("Ebro",                      "Navarra",       "Navarra"),
    "eugui":                   ("Ebro",                      "Navarra",       "Navarra"),
    "irabia":                  ("Ebro",                      "Navarra",       "Navarra"),
    "ullibarri-gamboa":        ("Ebro",                      "Álava",         "País Vasco"),
    "urrunaga":                ("Ebro",                      "Álava",         "País Vasco"),
    "mansilla":                ("Ebro",                      "La Rioja",      "La Rioja"),
    "gonzalez lacasa":         ("Ebro",                      "La Rioja",      "La Rioja"),
    "pajares":                 ("Ebro",                      "La Rioja",      "La Rioja"),
    "ebro":                    ("Ebro",                      "Cantabria",     "Cantabria"),
    "iznajar":                 ("Guadalquivir",              "Córdoba",       "Andalucía"),
    "tranco de beas":          ("Guadalquivir",              "Jaén",          "Andalucía"),
    "guadalmena":              ("Guadalquivir",              "Jaén",          "Andalucía"),
    "bembezar":                ("Guadalquivir",              "Córdoba",       "Andalucía"),
    "malpasillo":              ("Guadalquivir",              "Córdoba",       "Andalucía"),
    "la breña ii":             ("Guadalquivir",              "Córdoba",       "Andalucía"),
    "la breña":                ("Guadalquivir",              "Córdoba",       "Andalucía"),
    "puente nuevo":            ("Guadalquivir",              "Córdoba",       "Andalucía"),
    "el carpio":               ("Guadalquivir",              "Córdoba",       "Andalucía"),
    "encinarejo":              ("Guadalquivir",              "Córdoba",       "Andalucía"),
    "jandula":                 ("Guadalquivir",              "Jaén",          "Andalucía"),
    "el rumblar":              ("Guadalquivir",              "Jaén",          "Andalucía"),
    "doña aldonza":            ("Guadalquivir",              "Jaén",          "Andalucía"),
    "giribaile":               ("Guadalquivir",              "Jaén",          "Andalucía"),
    "valdeinfierno":           ("Guadalquivir",              "Jaén",          "Andalucía"),
    "colomera":                ("Guadalquivir",              "Granada",       "Andalucía"),
    "cubillas":                ("Guadalquivir",              "Granada",       "Andalucía"),
    "francisco abellan":       ("Guadalquivir",              "Granada",       "Andalucía"),
    "quantar":                 ("Guadalquivir",              "Granada",       "Andalucía"),
    "rules":                   ("Guadalquivir",              "Granada",       "Andalucía"),
    "el pintado":              ("Guadalquivir",              "Sevilla",       "Andalucía"),
    "aracena":                 ("Guadalquivir",              "Huelva",        "Andalucía"),
    "zufre":                   ("Guadalquivir",              "Huelva",        "Andalucía"),
    "agrio":                   ("Guadalquivir",              "Sevilla",       "Andalucía"),
    "hueznar":                 ("Guadalquivir",              "Sevilla",       "Andalucía"),
    "retortillo":              ("Guadalquivir",              "Sevilla",       "Andalucía"),
    "guadalcacin":             ("Guadalete-Barbate",         "Cádiz",         "Andalucía"),
    "bornos":                  ("Guadalete-Barbate",         "Cádiz",         "Andalucía"),
    "arcos":                   ("Guadalete-Barbate",         "Cádiz",         "Andalucía"),
    "zahara-el gastor":        ("Guadalete-Barbate",         "Cádiz",         "Andalucía"),
    "barbate":                 ("Guadalete-Barbate",         "Cádiz",         "Andalucía"),
    "celemin":                 ("Guadalete-Barbate",         "Cádiz",         "Andalucía"),
    "majaceite":               ("Guadalete-Barbate",         "Cádiz",         "Andalucía"),
    "canales":                 ("C. Mediterráneas Andaluzas","Granada",       "Andalucía"),
    "beninar":                 ("C. Mediterráneas Andaluzas","Almería",       "Andalucía"),
    "la viñuela":              ("C. Mediterráneas Andaluzas","Málaga",        "Andalucía"),
    "el limonero":             ("C. Mediterráneas Andaluzas","Málaga",        "Andalucía"),
    "conde del guadalhorce":   ("C. Mediterráneas Andaluzas","Málaga",        "Andalucía"),
    "guadalteba":              ("C. Mediterráneas Andaluzas","Málaga",        "Andalucía"),
    "casasola":                ("C. Mediterráneas Andaluzas","Málaga",        "Andalucía"),
    "guadalhorce":             ("C. Mediterráneas Andaluzas","Málaga",        "Andalucía"),
    "andevalo":                ("Tinto-Odiel-Piedras",       "Huelva",        "Andalucía"),
    "sancho el mayor":         ("Tinto-Odiel-Piedras",       "Huelva",        "Andalucía"),
    "contreras":               ("Júcar",                     "Cuenca",        "Castilla-La Mancha"),
    "tous":                    ("Júcar",                     "Valencia",      "Comunitat Valenciana"),
    "alarcon":                 ("Júcar",                     "Cuenca",        "Castilla-La Mancha"),
    "benageber":               ("Júcar",                     "Valencia",      "Comunitat Valenciana"),
    "loriguilla":              ("Júcar",                     "Valencia",      "Comunitat Valenciana"),
    "forata":                  ("Júcar",                     "Valencia",      "Comunitat Valenciana"),
    "amadorio":                ("Júcar",                     "Alicante",      "Comunitat Valenciana"),
    "guadalest":               ("Júcar",                     "Alicante",      "Comunitat Valenciana"),
    "cenajo":                  ("Segura",                    "Murcia",        "Murcia"),
    "camarillas":              ("Segura",                    "Murcia",        "Murcia"),
    "fuensanta":               ("Segura",                    "Murcia",        "Murcia"),
    "la pedrera":              ("Segura",                    "Murcia",        "Murcia"),
    "alfonso xiii":            ("Segura",                    "Murcia",        "Murcia"),
    "valdeinfierno (segura)":  ("Segura",                    "Murcia",        "Murcia"),
    "puentes":                 ("Segura",                    "Murcia",        "Murcia"),
    "frieira":                 ("Miño-Sil",                  "Ourense",       "Galicia"),
    "peares":                  ("Miño-Sil",                  "Ourense",       "Galicia"),
    "belesar":                 ("Miño-Sil",                  "Lugo",          "Galicia"),
    "das conchas":             ("Miño-Sil",                  "Ourense",       "Galicia"),
    "chandrexa":               ("Miño-Sil",                  "Ourense",       "Galicia"),
    "velle":                   ("Miño-Sil",                  "Ourense",       "Galicia"),
    "castrelo":                ("Miño-Sil",                  "Ourense",       "Galicia"),
    "cachamuiña":              ("Miño-Sil",                  "Ourense",       "Galicia"),
    "cecebre":                 ("Galicia Costa",             "A Coruña",      "Galicia"),
    "baña":                    ("Galicia Costa",             "A Coruña",      "Galicia"),
    "portodemouros":           ("Galicia Costa",             "A Coruña",      "Galicia"),
    "eiras":                   ("Galicia Costa",             "Pontevedra",    "Galicia"),
    "lindoso":                 ("Miño-Sil",                  "Ourense",       "Galicia"),
    "urdalur":                 ("Cantábrico",                "Álava",         "País Vasco"),
    "añarbe":                  ("Cantábrico",                "Guipúzcoa",     "País Vasco"),
    "saelices":                ("Cantábrico",                "Cantabria",     "Cantabria"),
    "la cohilla":              ("Cantábrico",                "Cantabria",     "Cantabria"),
    "la baells":               ("Cuencas Internas Cataluña", "Barcelona",     "Cataluña"),
    "sau":                     ("Cuencas Internas Cataluña", "Barcelona",     "Cataluña"),
    "susqueda":                ("Cuencas Internas Cataluña", "Girona",        "Cataluña"),
    "darnius-boadella":        ("Cuencas Internas Cataluña", "Girona",        "Cataluña"),
    "sant ponç":               ("Cuencas Internas Cataluña", "Lleida",        "Cataluña"),
    "oliana":                  ("Cuencas Internas Cataluña", "Lleida",        "Cataluña"),
    "rialb":                   ("Cuencas Internas Cataluña", "Lleida",        "Cataluña"),
    "terradets":               ("Cuencas Internas Cataluña", "Lleida",        "Cataluña"),
    "camarasa":                ("Cuencas Internas Cataluña", "Lleida",        "Cataluña"),
}

URL_ZIP = "https://www.miteco.gob.es/content/dam/miteco/es/agua/temas/evaluacion-de-los-recursos-hidricos/boletin-hidrologico/BD-Embalses_1988-2022.zip"


def normalizar(s):
    """Minúsculas y sin acentos para buscar en META."""
    import unicodedata
    s = s.lower().strip()
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    # quitar sufijos comunes
    for suf in [" (jose maria oriol)", " (jose m. oriol)", " (juan benet)",
                " i", " ii", " iii"]:
        if s.endswith(suf):
            s = s[: -len(suf)].strip()
    return s


def buscar_meta(nombre):
    clave = normalizar(nombre)
    if clave in META:
        return META[clave]
    # búsqueda parcial como fallback
    for k, v in META.items():
        if k in clave or clave in k:
            return v
    return ("Desconocida", "Desconocida", "Desconocida")


def descargar_excel():
    print("Descargando Excel del Ministerio…")
    req = urllib.request.Request(URL_ZIP, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        data = resp.read()
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        # Buscar el .xlsx dentro del zip
        nombres = [n for n in z.namelist() if n.endswith(".xlsx")]
        if not nombres:
            raise RuntimeError("No se encontró .xlsx dentro del ZIP")
        with z.open(nombres[0]) as f:
            return io.BytesIO(f.read())


def procesar(excel_bytes):
    wb = openpyxl.load_workbook(excel_bytes, read_only=True, data_only=True)
    ws = wb.active

    rows = list(ws.iter_rows(values_only=True))

    # Buscar fila de cabecera
    header_row = None
    for i, row in enumerate(rows):
        cells = [str(c).lower() if c else "" for c in row]
        if any("embalse" in c or "nombre" in c for c in cells):
            header_row = i
            break

    if header_row is None:
        raise RuntimeError("No se encontró la cabecera en el Excel")

    headers = [str(c).lower().strip() if c else "" for c in rows[header_row]]

    # Localizar columnas clave
    def col(keywords):
        for kw in keywords:
            for i, h in enumerate(headers):
                if kw in h:
                    return i
        return None

    c_nombre    = col(["embalse", "nombre"])
    c_fecha     = col(["fecha"])
    c_capacidad = col(["capacidad", "capac"])
    c_actual    = col(["agua embalsada", "embalsada", "reserva", "actual"])

    if None in (c_nombre, c_capacidad, c_actual):
        raise RuntimeError(f"Columnas no encontradas. Cabeceras: {headers}")

    # Leer todos los datos, quedarnos con la semana más reciente por embalse
    # y con la misma semana hace 1 año y hace ~10 años (media)
    from collections import defaultdict
    registros = defaultdict(list)  # nombre -> [(fecha, hm3)]

    hoy = datetime.date.today()

    for row in rows[header_row + 1:]:
        try:
            nombre   = str(row[c_nombre]).strip() if row[c_nombre] else None
            capacidad = float(row[c_capacidad]) if row[c_capacidad] else None
            actual   = float(row[c_actual])   if row[c_actual]   else None

            if not nombre or capacidad is None or actual is None:
                continue
            if nombre.lower() in ("none", "nan", ""):
                continue

            # fecha
            fecha_val = row[c_fecha] if c_fecha is not None else None
            if isinstance(fecha_val, datetime.datetime):
                fecha = fecha_val.date()
            elif isinstance(fecha_val, datetime.date):
                fecha = fecha_val
            else:
                continue

            registros[nombre].append((fecha, capacidad, actual))
        except (TypeError, ValueError):
            continue

    # Construir lista de embalses con datos actuales + hace1year + media10
    embalses = []
    for nombre, datos in registros.items():
        datos.sort(key=lambda x: x[0])
        ultima_fecha = datos[-1][0]
        capacidad    = datos[-1][1]
        hm3_actual   = datos[-1][2]

        # Hace 1 año: dato más cercano a (ultima_fecha - 365 días)
        target_1y = ultima_fecha - datetime.timedelta(days=365)
        hace1y = min(datos, key=lambda x: abs((x[0] - target_1y).days))
        hm3_1y = hace1y[2]

        # Media 10 años: promedio de datos entre 9 y 11 años atrás en la misma época
        target_10y_start = ultima_fecha - datetime.timedelta(days=365 * 11)
        target_10y_end   = ultima_fecha - datetime.timedelta(days=365 * 9)
        datos_10y = [d[2] for d in datos if target_10y_start <= d[0] <= target_10y_end]
        hm3_media10 = round(sum(datos_10y) / len(datos_10y), 1) if datos_10y else hm3_1y

        cuenca, provincia, ccaa = buscar_meta(nombre)

        embalses.append({
            "nombre":    nombre,
            "cuenca":    cuenca,
            "ccaa":      ccaa,
            "provincia": provincia,
            "capacidad": round(capacidad, 1),
            "hm3":       round(hm3_actual, 1),
            "hace1year": round(hm3_1y, 1),
            "media10":   round(hm3_media10, 1),
            "fecha":     str(ultima_fecha),
        })

    embalses.sort(key=lambda x: -x["capacidad"])

    # Totales nacionales
    total_cap    = round(sum(e["capacidad"] for e in embalses), 1)
    total_hm3    = round(sum(e["hm3"]       for e in embalses), 1)
    total_1y     = round(sum(e["hace1year"] for e in embalses), 1)
    total_media  = round(sum(e["media10"]   for e in embalses), 1)

    return {
        "actualizado": str(datetime.date.today()),
        "total": {
            "hm3":       total_hm3,
            "capacidad": total_cap,
            "hace1year": total_1y,
            "media10":   total_media,
        },
        "embalses": embalses,
    }


if __name__ == "__main__":
    try:
        excel = descargar_excel()
        datos = procesar(excel)
        with open("datos.json", "w", encoding="utf-8") as f:
            json.dump(datos, f, ensure_ascii=False, indent=2)
        print(f"✓ datos.json generado: {len(datos['embalses'])} embalses")
        print(f"  Total España: {datos['total']['hm3']} hm³ / {datos['total']['capacidad']} hm³")
        print(f"  Actualizado: {datos['actualizado']}")
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
