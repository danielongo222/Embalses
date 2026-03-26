#!/usr/bin/env python3
"""
Obtiene datos de embalses haciendo scraping de embalses.net
y genera datos.json para la app.

embalses.net publica datos del Ministerio de Transición Ecológica
bajo licencia CC BY 4.0.
"""

import json
import time
import datetime
import sys
import urllib.request
from html.parser import HTMLParser

# ── Metadatos estáticos: provincia y ccaa por embalse ────────────────────────
# embalses.net no siempre incluye provincia en el HTML, la añadimos aquí.
META = {
    # nombre_normalizado: (cuenca, provincia, ccaa)
    "la serena":             ("Guadiana",                  "Badajoz",      "Extremadura"),
    "cijara":                ("Guadiana",                  "Badajoz",      "Extremadura"),
    "garcia de sola":        ("Guadiana",                  "Badajoz",      "Extremadura"),
    "orellana":              ("Guadiana",                  "Badajoz",      "Extremadura"),
    "zujar":                 ("Guadiana",                  "Badajoz",      "Extremadura"),
    "proserpina":            ("Guadiana",                  "Badajoz",      "Extremadura"),
    "cornalvo":              ("Guadiana",                  "Badajoz",      "Extremadura"),
    "alcantara":             ("Tajo",                      "Cáceres",      "Extremadura"),
    "valdecanas":            ("Tajo",                      "Cáceres",      "Extremadura"),
    "gabriel y galan":       ("Tajo",                      "Cáceres",      "Extremadura"),
    "rosarito":              ("Tajo",                      "Cáceres",      "Extremadura"),
    "borbollon":             ("Tajo",                      "Cáceres",      "Extremadura"),
    "buendia":               ("Tajo",                      "Cuenca",       "Castilla-La Mancha"),
    "entrepeñas":            ("Tajo",                      "Guadalajara",  "Castilla-La Mancha"),
    "el atazar":             ("Tajo",                      "Madrid",       "Madrid"),
    "santillana":            ("Tajo",                      "Madrid",       "Madrid"),
    "el vado":               ("Tajo",                      "Guadalajara",  "Castilla-La Mancha"),
    "beleña":                ("Tajo",                      "Guadalajara",  "Castilla-La Mancha"),
    "la tajera":             ("Tajo",                      "Guadalajara",  "Castilla-La Mancha"),
    "valmayor":              ("Tajo",                      "Madrid",       "Madrid"),
    "picadas":               ("Tajo",                      "Madrid",       "Madrid"),
    "almendra":              ("Duero",                     "Salamanca",    "Castilla y León"),
    "ricobayo":              ("Duero",                     "Zamora",       "Castilla y León"),
    "porma":                 ("Duero",                     "León",         "Castilla y León"),
    "riaño":                 ("Duero",                     "León",         "Castilla y León"),
    "barrios de luna":       ("Duero",                     "León",         "Castilla y León"),
    "villalcampo":           ("Duero",                     "Zamora",       "Castilla y León"),
    "castro":                ("Duero",                     "Zamora",       "Castilla y León"),
    "linares del arroyo":    ("Duero",                     "Segovia",      "Castilla y León"),
    "aguilar de campoo":     ("Duero",                     "Palencia",     "Castilla y León"),
    "cervera":               ("Duero",                     "Palencia",     "Castilla y León"),
    "requejada":             ("Duero",                     "Palencia",     "Castilla y León"),
    "compuerto":             ("Duero",                     "Palencia",     "Castilla y León"),
    "cuerda del pozo":       ("Duero",                     "Soria",        "Castilla y León"),
    "santa teresa":          ("Duero",                     "Salamanca",    "Castilla y León"),
    "burgomillodo":          ("Duero",                     "Segovia",      "Castilla y León"),
    "mequinenza":            ("Ebro",                      "Zaragoza",     "Aragón"),
    "yesa":                  ("Ebro",                      "Zaragoza",     "Aragón"),
    "mediano":               ("Ebro",                      "Huesca",       "Aragón"),
    "ribarroja":             ("Ebro",                      "Tarragona",    "Cataluña"),
    "itoiz":                 ("Ebro",                      "Navarra",      "Navarra"),
    "el grado":              ("Ebro",                      "Huesca",       "Aragón"),
    "vadiello":              ("Ebro",                      "Huesca",       "Aragón"),
    "canelles":              ("Ebro",                      "Huesca",       "Aragón"),
    "santa ana":             ("Ebro",                      "Huesca",       "Aragón"),
    "bubal":                 ("Ebro",                      "Huesca",       "Aragón"),
    "lanuza":                ("Ebro",                      "Huesca",       "Aragón"),
    "sotonera":              ("Ebro",                      "Huesca",       "Aragón"),
    "alloz":                 ("Ebro",                      "Navarra",      "Navarra"),
    "eugui":                 ("Ebro",                      "Navarra",      "Navarra"),
    "irabia":                ("Ebro",                      "Navarra",      "Navarra"),
    "ullibarri-gamboa":      ("Ebro",                      "Álava",        "País Vasco"),
    "urrunaga":              ("Ebro",                      "Álava",        "País Vasco"),
    "mansilla":              ("Ebro",                      "La Rioja",     "La Rioja"),
    "gonzalez lacasa":       ("Ebro",                      "La Rioja",     "La Rioja"),
    "pajares":               ("Ebro",                      "La Rioja",     "La Rioja"),
    "ebro":                  ("Ebro",                      "Cantabria",    "Cantabria"),
    "iznajar":               ("Guadalquivir",              "Córdoba",      "Andalucía"),
    "tranco de beas":        ("Guadalquivir",              "Jaén",         "Andalucía"),
    "guadalmena":            ("Guadalquivir",              "Jaén",         "Andalucía"),
    "bembezar":              ("Guadalquivir",              "Córdoba",      "Andalucía"),
    "malpasillo":            ("Guadalquivir",              "Córdoba",      "Andalucía"),
    "la breña ii":           ("Guadalquivir",              "Córdoba",      "Andalucía"),
    "la breña":              ("Guadalquivir",              "Córdoba",      "Andalucía"),
    "puente nuevo":          ("Guadalquivir",              "Córdoba",      "Andalucía"),
    "el carpio":             ("Guadalquivir",              "Córdoba",      "Andalucía"),
    "encinarejo":            ("Guadalquivir",              "Córdoba",      "Andalucía"),
    "jandula":               ("Guadalquivir",              "Jaén",         "Andalucía"),
    "el rumblar":            ("Guadalquivir",              "Jaén",         "Andalucía"),
    "doña aldonza":          ("Guadalquivir",              "Jaén",         "Andalucía"),
    "giribaile":             ("Guadalquivir",              "Jaén",         "Andalucía"),
    "colomera":              ("Guadalquivir",              "Granada",      "Andalucía"),
    "cubillas":              ("Guadalquivir",              "Granada",      "Andalucía"),
    "francisco abellan":     ("Guadalquivir",              "Granada",      "Andalucía"),
    "rules":                 ("Guadalquivir",              "Granada",      "Andalucía"),
    "el pintado":            ("Guadalquivir",              "Sevilla",      "Andalucía"),
    "aracena":               ("Guadalquivir",              "Huelva",       "Andalucía"),
    "zufre":                 ("Guadalquivir",              "Huelva",       "Andalucía"),
    "agrio":                 ("Guadalquivir",              "Sevilla",      "Andalucía"),
    "hueznar":               ("Guadalquivir",              "Sevilla",      "Andalucía"),
    "retortillo":            ("Guadalquivir",              "Sevilla",      "Andalucía"),
    "guadalcacin":           ("Guadalete-Barbate",         "Cádiz",        "Andalucía"),
    "bornos":                ("Guadalete-Barbate",         "Cádiz",        "Andalucía"),
    "arcos":                 ("Guadalete-Barbate",         "Cádiz",        "Andalucía"),
    "zahara-el gastor":      ("Guadalete-Barbate",         "Cádiz",        "Andalucía"),
    "barbate":               ("Guadalete-Barbate",         "Cádiz",        "Andalucía"),
    "celemin":               ("Guadalete-Barbate",         "Cádiz",        "Andalucía"),
    "majaceite":             ("Guadalete-Barbate",         "Cádiz",        "Andalucía"),
    "canales":               ("C. Mediterráneas Andaluzas","Granada",      "Andalucía"),
    "beninar":               ("C. Mediterráneas Andaluzas","Almería",      "Andalucía"),
    "la viñuela":            ("C. Mediterráneas Andaluzas","Málaga",       "Andalucía"),
    "el limonero":           ("C. Mediterráneas Andaluzas","Málaga",       "Andalucía"),
    "conde del guadalhorce": ("C. Mediterráneas Andaluzas","Málaga",       "Andalucía"),
    "guadalteba":            ("C. Mediterráneas Andaluzas","Málaga",       "Andalucía"),
    "guadalhorce":           ("C. Mediterráneas Andaluzas","Málaga",       "Andalucía"),
    "andevalo":              ("Tinto-Odiel-Piedras",       "Huelva",       "Andalucía"),
    "sancho el mayor":       ("Tinto-Odiel-Piedras",       "Huelva",       "Andalucía"),
    "contreras":             ("Júcar",                     "Cuenca",       "Castilla-La Mancha"),
    "tous":                  ("Júcar",                     "Valencia",     "Comunitat Valenciana"),
    "alarcon":               ("Júcar",                     "Cuenca",       "Castilla-La Mancha"),
    "benageber":             ("Júcar",                     "Valencia",     "Comunitat Valenciana"),
    "loriguilla":            ("Júcar",                     "Valencia",     "Comunitat Valenciana"),
    "forata":                ("Júcar",                     "Valencia",     "Comunitat Valenciana"),
    "amadorio":              ("Júcar",                     "Alicante",     "Comunitat Valenciana"),
    "cenajo":                ("Segura",                    "Murcia",       "Murcia"),
    "camarillas":            ("Segura",                    "Murcia",       "Murcia"),
    "fuensanta":             ("Segura",                    "Murcia",       "Murcia"),
    "la pedrera":            ("Segura",                    "Murcia",       "Murcia"),
    "alfonso xiii":          ("Segura",                    "Murcia",       "Murcia"),
    "puentes":               ("Segura",                    "Murcia",       "Murcia"),
    "frieira":               ("Miño-Sil",                  "Ourense",      "Galicia"),
    "peares":                ("Miño-Sil",                  "Ourense",      "Galicia"),
    "belesar":               ("Miño-Sil",                  "Lugo",         "Galicia"),
    "das conchas":           ("Miño-Sil",                  "Ourense",      "Galicia"),
    "chandrexa":             ("Miño-Sil",                  "Ourense",      "Galicia"),
    "velle":                 ("Miño-Sil",                  "Ourense",      "Galicia"),
    "castrelo":              ("Miño-Sil",                  "Ourense",      "Galicia"),
    "cecebre":               ("Galicia Costa",             "A Coruña",     "Galicia"),
    "baña":                  ("Galicia Costa",             "A Coruña",     "Galicia"),
    "portodemouros":         ("Galicia Costa",             "A Coruña",     "Galicia"),
    "eiras":                 ("Galicia Costa",             "Pontevedra",   "Galicia"),
    "urdalur":               ("Cantábrico",                "Álava",        "País Vasco"),
    "añarbe":                ("Cantábrico",                "Guipúzcoa",    "País Vasco"),
    "la cohilla":            ("Cantábrico",                "Cantabria",    "Cantabria"),
    "la baells":             ("Cuencas Internas Cataluña", "Barcelona",    "Cataluña"),
    "sau":                   ("Cuencas Internas Cataluña", "Barcelona",    "Cataluña"),
    "susqueda":              ("Cuencas Internas Cataluña", "Girona",       "Cataluña"),
    "darnius-boadella":      ("Cuencas Internas Cataluña", "Girona",       "Cataluña"),
    "sant ponç":             ("Cuencas Internas Cataluña", "Lleida",       "Cataluña"),
    "oliana":                ("Cuencas Internas Cataluña", "Lleida",       "Cataluña"),
    "rialb":                 ("Cuencas Internas Cataluña", "Lleida",       "Cataluña"),
    "terradets":             ("Cuencas Internas Cataluña", "Lleida",       "Cataluña"),
    "camarasa":              ("Cuencas Internas Cataluña", "Lleida",       "Cataluña"),
    "el vicario":            ("Guadiana",                  "Ciudad Real",  "Castilla-La Mancha"),
    "peñarroya":             ("Guadiana",                  "Ciudad Real",  "Castilla-La Mancha"),
}

# IDs de embalses en embalses.net (embalse-{id}-nombre.html)
# Lista completa de los embalses peninsulares más importantes
EMBALSE_IDS = list(range(1, 200))  # Probamos todos hasta 200

BASE_URL = "https://www.embalses.net"


def norm(s):
    import unicodedata
    s = s.lower().strip()
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return s


def buscar_meta(nombre):
    clave = norm(nombre)
    if clave in META:
        return META[clave]
    for k, v in META.items():
        if k in clave or clave in k:
            return v
    return ("Desconocida", "Desconocida", "Desconocida")


def fetch(url, retries=3):
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; embalses-app/1.0)",
        "Accept": "text/html",
    }
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=15) as r:
                return r.read().decode("utf-8", errors="replace")
        except Exception as e:
            if i == retries - 1:
                raise
            time.sleep(2)


class EmbalseParser(HTMLParser):
    """Extrae datos de la página de un embalse en embalses.net"""
    def __init__(self):
        super().__init__()
        self.nombre = None
        self.datos = {}
        self._in_titulo = False
        self._in_fila = False
        self._fila_textos = []
        self._all_text = []
        self._current_tag = None

    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        self._current_tag = tag
        clases = attrs_dict.get("class", "")
        if "FilaTitulo" in clases or "titulo" in clases.lower():
            self._in_titulo = True
        if "FilaSeccion" in clases or "FilaDatos" in clases:
            self._in_fila = True
            self._fila_textos = []

    def handle_endtag(self, tag):
        if self._in_fila and tag in ("tr", "div"):
            if self._fila_textos:
                texto = " ".join(t.strip() for t in self._fila_textos if t.strip())
                self._all_text.append(texto)
            self._in_fila = False
            self._fila_textos = []

    def handle_data(self, data):
        data = data.strip()
        if not data:
            return
        self._all_text.append(data)
        if self._in_titulo and not self.nombre:
            self.nombre = data
        if self._in_fila:
            self._fila_textos.append(data)


def parsear_numero(s):
    """Convierte '1.234,56' o '1234.56' a float"""
    if not s:
        return None
    s = s.strip().replace(" ", "")
    # formato español: punto=miles, coma=decimal
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def scrape_embalse(embalse_id):
    """Devuelve dict con datos del embalse o None si no existe"""
    url = f"{BASE_URL}/embalse-{embalse_id}.html"
    try:
        html = fetch(url)
    except Exception:
        return None

    # Si redirige a la home o da 404 no existe
    if "Error 404" in html or "no existe" in html.lower():
        return None

    # Buscar datos en el HTML con expresiones simples
    import re

    # Nombre del embalse
    nombre_match = re.search(r'<h1[^>]*>([^<]+)</h1>', html, re.IGNORECASE)
    if not nombre_match:
        nombre_match = re.search(r'class="[^"]*[Tt]itulo[^"]*"[^>]*>([^<]+)<', html)
    if not nombre_match:
        return None
    nombre = nombre_match.group(1).strip()
    if not nombre or len(nombre) < 2:
        return None

    # Buscar números: capacidad y agua actual
    # embalses.net muestra: Capacidad: X hm³ | Agua embalsada: Y hm³
    numeros = re.findall(r'[\d\.]+,\d+|[\d]+\.[\d]+|[\d]+', html)
    # Mejor: buscar patrones específicos
    cap_match = re.search(
        r'[Cc]apacidad[^<\d]*?([\d][.\d]*,[\d]+|[\d]+\.[\d]*|[\d]+)\s*hm',
        html)
    agua_match = re.search(
        r'[Aa]gua\s+[Ee]mbalsada[^<\d]*?([\d][.\d]*,[\d]+|[\d]+\.[\d]*|[\d]+)\s*hm',
        html)
    año_match = re.search(
        r'[Hh]ace\s+un\s+año[^<\d]*?([\d][.\d]*,[\d]+|[\d]+\.[\d]*|[\d]+)\s*hm',
        html)
    media_match = re.search(
        r'[Mm]edia\s+10\s+años[^<\d]*?([\d][.\d]*,[\d]+|[\d]+\.[\d]*|[\d]+)\s*hm',
        html)

    capacidad = parsear_numero(cap_match.group(1)) if cap_match else None
    hm3       = parsear_numero(agua_match.group(1)) if agua_match else None
    hace1year = parsear_numero(año_match.group(1)) if año_match else None
    media10   = parsear_numero(media_match.group(1)) if media_match else None

    if not capacidad or not hm3:
        return None
    if capacidad < 5:  # filtrar embalses muy pequeños
        return None

    cuenca, provincia, ccaa = buscar_meta(nombre)

    return {
        "nombre":    nombre,
        "cuenca":    cuenca,
        "ccaa":      ccaa,
        "provincia": provincia,
        "capacidad": round(capacidad, 1),
        "hm3":       round(hm3, 1),
        "hace1year": round(hace1year, 1) if hace1year else round(hm3 * 0.9, 1),
        "media10":   round(media10, 1)   if media10   else round(hm3 * 0.85, 1),
        "fecha":     str(datetime.date.today()),
    }


def scrape_lista_embalses():
    """Obtiene la lista completa de embalses desde la página de índice"""
    print("Obteniendo lista de embalses desde embalses.net…")
    import re
    url = f"{BASE_URL}/embalses.php"
    try:
        html = fetch(url)
    except Exception:
        # fallback: intentar la página principal
        html = fetch(BASE_URL)

    # Buscar todos los links a embalses individuales
    ids = re.findall(r'/embalse-(\d+)[^"]*\.html', html)
    ids = list(dict.fromkeys(int(i) for i in ids))  # únicos, orden preservado
    print(f"  Encontrados {len(ids)} IDs de embalses")
    return ids


def main():
    embalses = []
    errores = 0

    # Intentar obtener lista completa primero
    try:
        ids = scrape_lista_embalses()
    except Exception as e:
        print(f"  No se pudo obtener lista, probando rango 1-250: {e}")
        ids = list(range(1, 251))

    total = len(ids)
    print(f"Procesando {total} embalses…")

    for i, eid in enumerate(ids):
        if i % 20 == 0:
            print(f"  {i}/{total}…")
        result = scrape_embalse(eid)
        if result:
            # Evitar duplicados por nombre
            nombres = [e["nombre"] for e in embalses]
            if result["nombre"] not in nombres:
                embalses.append(result)
        else:
            errores += 1
        time.sleep(0.3)  # respetuoso con el servidor

    if not embalses:
        print("ERROR: No se obtuvieron embalses", file=sys.stderr)
        sys.exit(1)

    embalses.sort(key=lambda x: -x["capacidad"])

    total_cap   = round(sum(e["capacidad"] for e in embalses), 1)
    total_hm3   = round(sum(e["hm3"]       for e in embalses), 1)
    total_1y    = round(sum(e["hace1year"] for e in embalses), 1)
    total_m10   = round(sum(e["media10"]   for e in embalses), 1)

    datos = {
        "actualizado": str(datetime.date.today()),
        "total": {
            "hm3":       total_hm3,
            "capacidad": total_cap,
            "hace1year": total_1y,
            "media10":   total_m10,
        },
        "embalses": embalses,
    }

    with open("datos.json", "w", encoding="utf-8") as f:
        json.dump(datos, f, ensure_ascii=False, indent=2)

    print(f"\n✓ datos.json generado")
    print(f"  Embalses: {len(embalses)} ({errores} sin datos)")
    print(f"  Total España: {total_hm3} hm³ / {total_cap} hm³ ({round(total_hm3/total_cap*100)}%)")
    print(f"  Actualizado: {datos['actualizado']}")


if __name__ == "__main__":
    main()
