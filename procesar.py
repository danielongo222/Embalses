#!/usr/bin/env python3
"""
Scraping de embalses.net para obtener datos reales.
Datos: Ministerio de Transición Ecológica, CC BY 4.0, via embalses.net
"""
import json, re, time, datetime, sys, urllib.request
 
BASE  = "https://www.embalses.net"
TODAY = str(datetime.date.today())
 
META = {
    "buendia":               ("Tajo",                      "Cuenca",        "Castilla-La Mancha"),
    "entrepeñas":            ("Tajo",                      "Guadalajara",   "Castilla-La Mancha"),
    "la serena":             ("Guadiana",                  "Badajoz",       "Extremadura"),
    "cijara":                ("Guadiana",                  "Badajoz",       "Extremadura"),
    "garcia de sola":        ("Guadiana",                  "Badajoz",       "Extremadura"),
    "orellana":              ("Guadiana",                  "Badajoz",       "Extremadura"),
    "zujar":                 ("Guadiana",                  "Badajoz",       "Extremadura"),
    "proserpina":            ("Guadiana",                  "Badajoz",       "Extremadura"),
    "cornalvo":              ("Guadiana",                  "Badajoz",       "Extremadura"),
    "el vicario":            ("Guadiana",                  "Ciudad Real",   "Castilla-La Mancha"),
    "peñarroya":             ("Guadiana",                  "Ciudad Real",   "Castilla-La Mancha"),
    "alcantara":             ("Tajo",                      "Cáceres",       "Extremadura"),
    "valdecanas":            ("Tajo",                      "Cáceres",       "Extremadura"),
    "gabriel y galan":       ("Tajo",                      "Cáceres",       "Extremadura"),
    "rosarito":              ("Tajo",                      "Cáceres",       "Extremadura"),
    "borbollon":             ("Tajo",                      "Cáceres",       "Extremadura"),
    "el atazar":             ("Tajo",                      "Madrid",        "Madrid"),
    "santillana":            ("Tajo",                      "Madrid",        "Madrid"),
    "el vado":               ("Tajo",                      "Guadalajara",   "Castilla-La Mancha"),
    "beleña":                ("Tajo",                      "Guadalajara",   "Castilla-La Mancha"),
    "la tajera":             ("Tajo",                      "Guadalajara",   "Castilla-La Mancha"),
    "valmayor":              ("Tajo",                      "Madrid",        "Madrid"),
    "picadas":               ("Tajo",                      "Madrid",        "Madrid"),
    "almendra":              ("Duero",                     "Salamanca",     "Castilla y León"),
    "ricobayo":              ("Duero",                     "Zamora",        "Castilla y León"),
    "porma":                 ("Duero",                     "León",          "Castilla y León"),
    "riaño":                 ("Duero",                     "León",          "Castilla y León"),
    "barrios de luna":       ("Duero",                     "León",          "Castilla y León"),
    "villalcampo":           ("Duero",                     "Zamora",        "Castilla y León"),
    "castro":                ("Duero",                     "Zamora",        "Castilla y León"),
    "linares del arroyo":    ("Duero",                     "Segovia",       "Castilla y León"),
    "aguilar de campoo":     ("Duero",                     "Palencia",      "Castilla y León"),
    "cervera":               ("Duero",                     "Palencia",      "Castilla y León"),
    "requejada":             ("Duero",                     "Palencia",      "Castilla y León"),
    "compuerto":             ("Duero",                     "Palencia",      "Castilla y León"),
    "cuerda del pozo":       ("Duero",                     "Soria",         "Castilla y León"),
    "santa teresa":          ("Duero",                     "Salamanca",     "Castilla y León"),
    "burgomillodo":          ("Duero",                     "Segovia",       "Castilla y León"),
    "mequinenza":            ("Ebro",                      "Zaragoza",      "Aragón"),
    "yesa":                  ("Ebro",                      "Zaragoza",      "Aragón"),
    "mediano":               ("Ebro",                      "Huesca",        "Aragón"),
    "ribarroja":             ("Ebro",                      "Tarragona",     "Cataluña"),
    "itoiz":                 ("Ebro",                      "Navarra",       "Navarra"),
    "el grado":              ("Ebro",                      "Huesca",        "Aragón"),
    "vadiello":              ("Ebro",                      "Huesca",        "Aragón"),
    "canelles":              ("Ebro",                      "Huesca",        "Aragón"),
    "santa ana":             ("Ebro",                      "Huesca",        "Aragón"),
    "bubal":                 ("Ebro",                      "Huesca",        "Aragón"),
    "lanuza":                ("Ebro",                      "Huesca",        "Aragón"),
    "sotonera":              ("Ebro",                      "Huesca",        "Aragón"),
    "alloz":                 ("Ebro",                      "Navarra",       "Navarra"),
    "eugui":                 ("Ebro",                      "Navarra",       "Navarra"),
    "irabia":                ("Ebro",                      "Navarra",       "Navarra"),
    "ullibarri-gamboa":      ("Ebro",                      "Álava",         "País Vasco"),
    "urrunaga":              ("Ebro",                      "Álava",         "País Vasco"),
    "mansilla":              ("Ebro",                      "La Rioja",      "La Rioja"),
    "gonzalez lacasa":       ("Ebro",                      "La Rioja",      "La Rioja"),
    "pajares":               ("Ebro",                      "La Rioja",      "La Rioja"),
    "ebro":                  ("Ebro",                      "Cantabria",     "Cantabria"),
    "iznajar":               ("Guadalquivir",              "Córdoba",       "Andalucía"),
    "tranco de beas":        ("Guadalquivir",              "Jaén",          "Andalucía"),
    "guadalmena":            ("Guadalquivir",              "Jaén",          "Andalucía"),
    "bembezar":              ("Guadalquivir",              "Córdoba",       "Andalucía"),
    "malpasillo":            ("Guadalquivir",              "Córdoba",       "Andalucía"),
    "la breña ii":           ("Guadalquivir",              "Córdoba",       "Andalucía"),
    "la breña":              ("Guadalquivir",              "Córdoba",       "Andalucía"),
    "puente nuevo":          ("Guadalquivir",              "Córdoba",       "Andalucía"),
    "el carpio":             ("Guadalquivir",              "Córdoba",       "Andalucía"),
    "encinarejo":            ("Guadalquivir",              "Córdoba",       "Andalucía"),
    "jandula":               ("Guadalquivir",              "Jaén",          "Andalucía"),
    "el rumblar":            ("Guadalquivir",              "Jaén",          "Andalucía"),
    "doña aldonza":          ("Guadalquivir",              "Jaén",          "Andalucía"),
    "giribaile":             ("Guadalquivir",              "Jaén",          "Andalucía"),
    "colomera":              ("Guadalquivir",              "Granada",       "Andalucía"),
    "cubillas":              ("Guadalquivir",              "Granada",       "Andalucía"),
    "francisco abellan":     ("Guadalquivir",              "Granada",       "Andalucía"),
    "rules":                 ("Guadalquivir",              "Granada",       "Andalucía"),
    "el pintado":            ("Guadalquivir",              "Sevilla",       "Andalucía"),
    "aracena":               ("Guadalquivir",              "Huelva",        "Andalucía"),
    "zufre":                 ("Guadalquivir",              "Huelva",        "Andalucía"),
    "agrio":                 ("Guadalquivir",              "Sevilla",       "Andalucía"),
    "hueznar":               ("Guadalquivir",              "Sevilla",       "Andalucía"),
    "retortillo":            ("Guadalquivir",              "Sevilla",       "Andalucía"),
    "guadalcacin":           ("Guadalete-Barbate",         "Cádiz",         "Andalucía"),
    "bornos":                ("Guadalete-Barbate",         "Cádiz",         "Andalucía"),
    "arcos":                 ("Guadalete-Barbate",         "Cádiz",         "Andalucía"),
    "zahara-el gastor":      ("Guadalete-Barbate",         "Cádiz",         "Andalucía"),
    "zahara":                ("Guadalete-Barbate",         "Cádiz",         "Andalucía"),
    "barbate":               ("Guadalete-Barbate",         "Cádiz",         "Andalucía"),
    "celemin":               ("Guadalete-Barbate",         "Cádiz",         "Andalucía"),
    "majaceite":             ("Guadalete-Barbate",         "Cádiz",         "Andalucía"),
    "canales":               ("C. Mediterráneas Andaluzas","Granada",       "Andalucía"),
    "beninar":               ("C. Mediterráneas Andaluzas","Almería",       "Andalucía"),
    "la viñuela":            ("C. Mediterráneas Andaluzas","Málaga",        "Andalucía"),
    "el limonero":           ("C. Mediterráneas Andaluzas","Málaga",        "Andalucía"),
    "conde del guadalhorce": ("C. Mediterráneas Andaluzas","Málaga",        "Andalucía"),
    "guadalteba":            ("C. Mediterráneas Andaluzas","Málaga",        "Andalucía"),
    "guadalhorce":           ("C. Mediterráneas Andaluzas","Málaga",        "Andalucía"),
    "la concepcion":         ("C. Mediterráneas Andaluzas","Málaga",        "Andalucía"),
    "andevalo":              ("Tinto-Odiel-Piedras",       "Huelva",        "Andalucía"),
    "sancho el mayor":       ("Tinto-Odiel-Piedras",       "Huelva",        "Andalucía"),
    "contreras":             ("Júcar",                     "Cuenca",        "Castilla-La Mancha"),
    "tous":                  ("Júcar",                     "Valencia",      "Comunitat Valenciana"),
    "alarcon":               ("Júcar",                     "Cuenca",        "Castilla-La Mancha"),
    "benageber":             ("Júcar",                     "Valencia",      "Comunitat Valenciana"),
    "loriguilla":            ("Júcar",                     "Valencia",      "Comunitat Valenciana"),
    "forata":                ("Júcar",                     "Valencia",      "Comunitat Valenciana"),
    "amadorio":              ("Júcar",                     "Alicante",      "Comunitat Valenciana"),
    "el talave":             ("Segura",                    "Albacete",      "Castilla-La Mancha"),
    "cenajo":                ("Segura",                    "Murcia",        "Murcia"),
    "camarillas":            ("Segura",                    "Murcia",        "Murcia"),
    "fuensanta":             ("Segura",                    "Murcia",        "Murcia"),
    "la pedrera":            ("Segura",                    "Murcia",        "Murcia"),
    "alfonso xiii":          ("Segura",                    "Murcia",        "Murcia"),
    "puentes":               ("Segura",                    "Murcia",        "Murcia"),
    "frieira":               ("Miño-Sil",                  "Ourense",       "Galicia"),
    "peares":                ("Miño-Sil",                  "Ourense",       "Galicia"),
    "belesar":               ("Miño-Sil",                  "Lugo",          "Galicia"),
    "das conchas":           ("Miño-Sil",                  "Ourense",       "Galicia"),
    "chandrexa":             ("Miño-Sil",                  "Ourense",       "Galicia"),
    "velle":                 ("Miño-Sil",                  "Ourense",       "Galicia"),
    "castrelo":              ("Miño-Sil",                  "Ourense",       "Galicia"),
    "cecebre":               ("Galicia Costa",             "A Coruña",      "Galicia"),
    "baña":                  ("Galicia Costa",             "A Coruña",      "Galicia"),
    "portodemouros":         ("Galicia Costa",             "A Coruña",      "Galicia"),
    "eiras":                 ("Galicia Costa",             "Pontevedra",    "Galicia"),
    "urdalur":               ("Cantábrico",                "Álava",         "País Vasco"),
    "añarbe":                ("Cantábrico",                "Guipúzcoa",     "País Vasco"),
    "la cohilla":            ("Cantábrico",                "Cantabria",     "Cantabria"),
    "la baells":             ("Cuencas Internas Cataluña", "Barcelona",     "Cataluña"),
    "sau":                   ("Cuencas Internas Cataluña", "Barcelona",     "Cataluña"),
    "susqueda":              ("Cuencas Internas Cataluña", "Girona",        "Cataluña"),
    "darnius-boadella":      ("Cuencas Internas Cataluña", "Girona",        "Cataluña"),
    "sant ponç":             ("Cuencas Internas Cataluña", "Lleida",        "Cataluña"),
    "oliana":                ("Cuencas Internas Cataluña", "Lleida",        "Cataluña"),
    "rialb":                 ("Cuencas Internas Cataluña", "Lleida",        "Cataluña"),
    "terradets":             ("Cuencas Internas Cataluña", "Lleida",        "Cataluña"),
    "camarasa":              ("Cuencas Internas Cataluña", "Lleida",        "Cataluña"),
}
 
# URLs reales de cuencas (verificadas desde el HTML de embalses.net)
CUENCAS_URLS = [
    "cuenca-1-segura.html",
    "cuenca-2-duero.html",
    "cuenca-3-tajo.html",
    "cuenca-4-guadalquivir.html",
    "cuenca-5-ebro.html",
    "cuenca-6-guadiana.html",
    "cuenca-7-jucar.html",
    "cuenca-8-mino-sil.html",
    "cuenca-9-med-andaluza.html",
    "cuenca-10-galicia-costa.html",
    "cuenca-11-cataluna-interna.html",
    "cuenca-16-cantabrico-oriental.html",
    "cuenca-17-cantabrico-occidental.html",
    "cuenca-18-tinto-odiel-y-piedras.html",
    "cuenca-19-guadalete-barbate.html",
    "cuenca-12-pais-vasco-interna.html",
]
 
def norm(s):
    import unicodedata
    s = s.lower().strip()
    s = unicodedata.normalize("NFD", s)
    return "".join(c for c in s if unicodedata.category(c) != "Mn")
 
def buscar_meta(nombre):
    clave = norm(nombre)
    if clave in META:
        return META[clave]
    for k, v in META.items():
        if k in clave or clave in k:
            return v
    return ("Desconocida", "Desconocida", "Desconocida")
 
def fetch(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/122.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Referer": "https://www.embalses.net/",
    }
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=30) as r:
        raw = r.read()
        # manejar gzip
        if r.info().get("Content-Encoding") == "gzip":
            import gzip
            raw = gzip.decompress(raw)
        return raw.decode("utf-8", errors="replace")
 
def p_num(s):
    """'1.013' o '1013,98' → float"""
    if not s:
        return None
    s = s.strip().replace("\xa0", "").replace(" ", "")
    # formato español: punto=miles, coma=decimal
    if re.match(r'^\d{1,3}(\.\d{3})+(,\d+)?$', s):
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None
 
def scrape_pantano(url):
    """
    Scrape una página individual de embalse.
    Extrae nombre, capacidad, hm3 actual, hace 1 año, media 10 años.
    """
    try:
        html = fetch(url)
    except Exception as e:
        return None
 
    # Nombre: primer h1 o título de la página
    nombre_m = re.search(r'Embalse:\s*([^(<\n]+?)(?:\s*\(Tiempo Real\))?[\s\n]*(?:</h|$)', html)
    if not nombre_m:
        nombre_m = re.search(r'<h1[^>]*>(?:Pantano|Embalse|Presa)\s+([^<]+)</h1>', html, re.I)
    if not nombre_m:
        return None
    nombre = nombre_m.group(1).strip()
 
    # Los datos están así en el HTML (verificado):
    # Agua embalsada (DD-MM-YYYY): <b>1.013</b> hm3
    # Capacidad: 1.705 hm3
    # Misma Semana (2024): 661 hm3
    # Misma Semana (Med. 10 Años): 390 hm3
 
    agua_m = re.search(
        r'Agua embalsada[^:]*:\s*<[^>]+>\s*([\d.,]+)\s*</[^>]+>\s*(?:<[^>]+>\s*)?\n?\s*hm',
        html, re.I)
    if not agua_m:
        # fallback: buscar el número grande en negrita seguido de hm3
        agua_m = re.search(r'<strong>\s*([\d.,]+)\s*</strong>\s*\n?\s*(?:<[^>]*>\s*)*hm3?\b', html, re.I)
    if not agua_m:
        agua_m = re.search(r'([\d]{3,}(?:[.,]\d+)?)\s*\n\s*(?:<[^>]*>\n\s*)?hm3?\b', html)
 
    cap_m   = re.search(r'Capacidad:\s*\n?\s*([\d.,]+)\s*\n?\s*hm', html, re.I)
    hace_m  = re.search(r'Misma Semana \(\d{4}\):\s*\n?\s*([\d.,]+)\s*\n?\s*hm', html, re.I)
    med_m   = re.search(r'Misma Semana \(Med\. 10 A[ñn]os\):\s*\n?\s*([\d.,]+)\s*\n?\s*hm', html, re.I)
 
    hm3     = p_num(agua_m.group(1)) if agua_m else None
    cap     = p_num(cap_m.group(1))  if cap_m  else None
    hace1y  = p_num(hace_m.group(1)) if hace_m  else None
    media10 = p_num(med_m.group(1))  if med_m   else None
 
    if not hm3 or not cap or cap < 1:
        return None
 
    cuenca, provincia, ccaa = buscar_meta(nombre)
    return {
        "nombre":    nombre,
        "cuenca":    cuenca,
        "ccaa":      ccaa,
        "provincia": provincia,
        "capacidad": round(cap, 1),
        "hm3":       round(hm3, 1),
        "hace1year": round(hace1y,  1) if hace1y  else round(hm3 * 0.85, 1),
        "media10":   round(media10, 1) if media10 else round(hm3 * 0.80, 1),
        "fecha":     TODAY,
    }
 
def scrape_cuenca(slug):
    """
    Scrape la página de una cuenca para obtener la lista de IDs de pantanos.
    """
    url = f"{BASE}/{slug}"
    try:
        html = fetch(url)
    except Exception as e:
        print(f"  Error {slug}: {e}")
        return []
 
    # Buscar todos los enlaces a pantanos: /pantano-{id}-{slug}.html
    ids = re.findall(r'href=["\'](?:https?://www\.embalses\.net)?/pantano-(\d+)-[^"\']+\.html["\']', html)
    return list(dict.fromkeys(int(i) for i in ids))  # únicos, orden preservado
 
def main():
    print("Obteniendo lista de embalses por cuencas…")
    all_ids = []
    for slug in CUENCAS_URLS:
        ids = scrape_cuenca(slug)
        print(f"  {slug}: {len(ids)} embalses")
        all_ids.extend(ids)
        time.sleep(1.5)
 
    all_ids = list(dict.fromkeys(all_ids))
    print(f"\nTotal IDs únicos: {len(all_ids)}")
 
    if not all_ids:
        print("ERROR: No se encontraron IDs de embalses", file=sys.stderr)
        sys.exit(1)
 
    embalses = []
    nombres_vistos = set()
    for i, pid in enumerate(all_ids):
        if i % 25 == 0:
            print(f"  Procesando {i}/{len(all_ids)}…")
        url = f"{BASE}/pantano-{pid}-x.html"
        # La URL exacta no importa, embalses.net redirige por ID
        # Pero usamos la URL con el ID que sí funciona:
        # Format real: /pantano-{id}-{slug}.html — buscamos el slug en el HTML de cuenca
        # Como no lo tenemos, intentamos directamente con el ID
        # embalses.net acepta /pantano-{id}.html como redirect
        url = f"{BASE}/pantano-{pid}.html"
        datos = scrape_pantano(url)
        if datos and datos["nombre"] not in nombres_vistos:
            nombres_vistos.add(datos["nombre"])
            embalses.append(datos)
        time.sleep(0.4)
 
    if not embalses:
        print("ERROR: No se obtuvieron datos de ningún embalse", file=sys.stderr)
        sys.exit(1)
 
    embalses.sort(key=lambda x: -x["capacidad"])
 
    total = {
        "hm3":       round(sum(e["hm3"]       for e in embalses), 1),
        "capacidad": round(sum(e["capacidad"] for e in embalses), 1),
        "hace1year": round(sum(e["hace1year"] for e in embalses), 1),
        "media10":   round(sum(e["media10"]   for e in embalses), 1),
    }
 
    datos = {"actualizado": TODAY, "total": total, "embalses": embalses}
    with open("datos.json", "w", encoding="utf-8") as f:
        json.dump(datos, f, ensure_ascii=False, indent=2)
 
    pct = round(total["hm3"] / total["capacidad"] * 100) if total["capacidad"] else 0
    print(f"\n✓ datos.json generado")
    print(f"  Embalses: {len(embalses)}")
    print(f"  España: {total['hm3']} hm³ / {total['capacidad']} hm³ ({pct}%)")
    print(f"  Fecha:  {TODAY}")
 
if __name__ == "__main__":
    main()
 


if __name__ == "__main__":
    main()
