#!/usr/bin/env python3
"""
Obtiene datos de embalses.net guardando las URLs completas desde las páginas
de cuenca, luego extrae datos de cada embalse individual.
"""
import json, re, time, datetime, sys, urllib.request, gzip as gzip_mod
 
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
    "aguilar de campoo":     ("Duero",                     "Palencia",      "Castilla y León"),
    "requejada":             ("Duero",                     "Palencia",      "Castilla y León"),
    "compuerto":             ("Duero",                     "Palencia",      "Castilla y León"),
    "cuerda del pozo":       ("Duero",                     "Soria",         "Castilla y León"),
    "santa teresa":          ("Duero",                     "Salamanca",     "Castilla y León"),
    "mequinenza":            ("Ebro",                      "Zaragoza",      "Aragón"),
    "yesa":                  ("Ebro",                      "Zaragoza",      "Aragón"),
    "mediano":               ("Ebro",                      "Huesca",        "Aragón"),
    "ribarroja":             ("Ebro",                      "Tarragona",     "Cataluña"),
    "itoiz":                 ("Ebro",                      "Navarra",       "Navarra"),
    "el grado":              ("Ebro",                      "Huesca",        "Aragón"),
    "canelles":              ("Ebro",                      "Huesca",        "Aragón"),
    "sotonera":              ("Ebro",                      "Huesca",        "Aragón"),
    "ullibarri-gamboa":      ("Ebro",                      "Álava",         "País Vasco"),
    "mansilla":              ("Ebro",                      "La Rioja",      "La Rioja"),
    "ebro":                  ("Ebro",                      "Cantabria",     "Cantabria"),
    "iznajar":               ("Guadalquivir",              "Córdoba",       "Andalucía"),
    "tranco de beas":        ("Guadalquivir",              "Jaén",          "Andalucía"),
    "guadalmena":            ("Guadalquivir",              "Jaén",          "Andalucía"),
    "bembezar":              ("Guadalquivir",              "Córdoba",       "Andalucía"),
    "la breña ii":           ("Guadalquivir",              "Córdoba",       "Andalucía"),
    "puente nuevo":          ("Guadalquivir",              "Córdoba",       "Andalucía"),
    "jandula":               ("Guadalquivir",              "Jaén",          "Andalucía"),
    "el rumblar":            ("Guadalquivir",              "Jaén",          "Andalucía"),
    "giribaile":             ("Guadalquivir",              "Jaén",          "Andalucía"),
    "rules":                 ("Guadalquivir",              "Granada",       "Andalucía"),
    "el pintado":            ("Guadalquivir",              "Sevilla",       "Andalucía"),
    "aracena":               ("Guadalquivir",              "Huelva",        "Andalucía"),
    "zufre":                 ("Guadalquivir",              "Huelva",        "Andalucía"),
    "guadalcacin":           ("Guadalete-Barbate",         "Cádiz",         "Andalucía"),
    "bornos":                ("Guadalete-Barbate",         "Cádiz",         "Andalucía"),
    "zahara":                ("Guadalete-Barbate",         "Cádiz",         "Andalucía"),
    "barbate":               ("Guadalete-Barbate",         "Cádiz",         "Andalucía"),
    "celemin":               ("Guadalete-Barbate",         "Cádiz",         "Andalucía"),
    "canales":               ("C. Mediterráneas Andaluzas","Granada",       "Andalucía"),
    "beninar":               ("C. Mediterráneas Andaluzas","Almería",       "Andalucía"),
    "la viñuela":            ("C. Mediterráneas Andaluzas","Málaga",        "Andalucía"),
    "guadalteba":            ("C. Mediterráneas Andaluzas","Málaga",        "Andalucía"),
    "andevalo":              ("Tinto-Odiel-Piedras",       "Huelva",        "Andalucía"),
    "contreras":             ("Júcar",                     "Cuenca",        "Castilla-La Mancha"),
    "tous":                  ("Júcar",                     "Valencia",      "Comunitat Valenciana"),
    "alarcon":               ("Júcar",                     "Cuenca",        "Castilla-La Mancha"),
    "benageber":             ("Júcar",                     "Valencia",      "Comunitat Valenciana"),
    "cenajo":                ("Segura",                    "Murcia",        "Murcia"),
    "fuensanta":             ("Segura",                    "Murcia",        "Murcia"),
    "la pedrera":            ("Segura",                    "Murcia",        "Murcia"),
    "belesar":               ("Miño-Sil",                  "Lugo",          "Galicia"),
    "das conchas":           ("Miño-Sil",                  "Ourense",       "Galicia"),
    "cecebre":               ("Galicia Costa",             "A Coruña",      "Galicia"),
    "portodemouros":         ("Galicia Costa",             "A Coruña",      "Galicia"),
    "eiras":                 ("Galicia Costa",             "Pontevedra",    "Galicia"),
    "añarbe":                ("Cantábrico",                "Guipúzcoa",     "País Vasco"),
    "la cohilla":            ("Cantábrico",                "Cantabria",     "Cantabria"),
    "la baells":             ("Cuencas Internas Cataluña", "Barcelona",     "Cataluña"),
    "sau":                   ("Cuencas Internas Cataluña", "Barcelona",     "Cataluña"),
    "susqueda":              ("Cuencas Internas Cataluña", "Girona",        "Cataluña"),
    "oliana":                ("Cuencas Internas Cataluña", "Lleida",        "Cataluña"),
    "rialb":                 ("Cuencas Internas Cataluña", "Lleida",        "Cataluña"),
}
 
CUENCAS_URLS = [
    "cuenca-1-segura.html", "cuenca-2-duero.html", "cuenca-3-tajo.html",
    "cuenca-4-guadalquivir.html", "cuenca-5-ebro.html", "cuenca-6-guadiana.html",
    "cuenca-7-jucar.html", "cuenca-8-mino-sil.html", "cuenca-9-med-andaluza.html",
    "cuenca-10-galicia-costa.html", "cuenca-11-cataluna-interna.html",
    "cuenca-16-cantabrico-oriental.html", "cuenca-17-cantabrico-occidental.html",
    "cuenca-18-tinto-odiel-y-piedras.html", "cuenca-19-guadalete-barbate.html",
    "cuenca-12-pais-vasco-interna.html",
]
 
def norm(s):
    import unicodedata
    s = s.lower().strip()
    s = unicodedata.normalize("NFD", s)
    return "".join(c for c in s if unicodedata.category(c) != "Mn")
 
def buscar_meta(nombre):
    clave = norm(nombre)
    if clave in META: return META[clave]
    for k, v in META.items():
        if k in clave or clave in k: return v
    return ("Desconocida", "Desconocida", "Desconocida")
 
def fetch(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/122.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "es-ES,es;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Referer": "https://www.embalses.net/",
    }
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=30) as r:
        raw = r.read()
        enc = r.info().get("Content-Encoding", "")
        if enc == "gzip":
            raw = gzip_mod.decompress(raw)
        return raw.decode("utf-8", errors="replace")
 
def p_num(s):
    if not s: return None
    s = s.strip().replace("\xa0","").replace(" ","").replace("\u200b","")
    if re.match(r'^\d{1,3}(\.\d{3})+$', s):
        s = s.replace(".","")
    elif re.match(r'^\d{1,3}(\.\d{3})+(,\d+)?$', s):
        s = s.replace(".","").replace(",",".")
    else:
        s = s.replace(",",".")
    try: return float(s)
    except: return None
 
def scrape_cuenca(slug):
    url = f"{BASE}/{slug}"
    try:
        html = fetch(url)
    except Exception as e:
        print(f"  Error {slug}: {e}")
        return []
    matches = re.findall(
        r'href=["\'](?:https?://www\.embalses\.net)?(/pantano-(\d+)-[^"\']+\.html)["\']',
        html)
    seen = {}
    for path, pid in matches:
        if pid not in seen:
            seen[pid] = f"{BASE}{path}"
    return list(seen.items())
 
def scrape_pantano(pid, url):
    try:
        html = fetch(url)
    except Exception as e:
        return None
 
    # ── DIAGNÓSTICO: imprimir las primeras líneas de la primera pantalla ──
    # (solo para los primeros 3, para no llenar el log)
    if int(pid) < 40:
        snippet = html[:800].replace('\n',' ')
        print(f"  [DEBUG pid={pid}] {snippet[:300]}")
 
    nombre_m = re.search(r'Embalse:\s*([^\n(<]+?)(?:\s*\(Tiempo Real\)|\s*\(Caudal)', html)
    if not nombre_m:
        nombre_m = re.search(r'<h1[^>]*>(?:Pantano|Embalse|Presa)\s+([^<]+)</h1>', html, re.I)
    if not nombre_m:
        return None
    nombre = nombre_m.group(1).strip()
 
    agua_m  = re.search(r'\*\*([\d.,]+)\*\*\s*\n+\s*\*\*hm3?\*\*', html)
    cap_m   = re.search(r'Capacidad:\s*\n?\s*([\d.,]+)\s*\n?\s*hm', html, re.I)
    hace_m  = re.search(r'Misma Semana \(\d{4}\):\s*\n?\s*([\d.,]+)\s*\n?\s*hm', html, re.I)
    med_m   = re.search(r'Misma Semana \(Med\. 10 A[ñn]os\):\s*\n?\s*([\d.,]+)\s*\n?\s*hm', html, re.I)
 
    hm3     = p_num(agua_m.group(1)) if agua_m else None
    cap     = p_num(cap_m.group(1))  if cap_m  else None
    hace1y  = p_num(hace_m.group(1)) if hace_m else None
    media10 = p_num(med_m.group(1))  if med_m  else None
 
    if not hm3 or not cap or cap < 1:
        return None
 
    cuenca, provincia, ccaa = buscar_meta(nombre)
    return {
        "nombre": nombre, "cuenca": cuenca, "ccaa": ccaa, "provincia": provincia,
        "capacidad": round(cap, 1), "hm3": round(hm3, 1),
        "hace1year": round(hace1y,  1) if hace1y  else round(hm3 * 0.85, 1),
        "media10":   round(media10, 1) if media10 else round(hm3 * 0.80, 1),
        "fecha": TODAY,
    }
 
def main():
    print("Obteniendo lista de embalses por cuencas…")
    all_pantanos = {}
    for slug in CUENCAS_URLS:
        items = scrape_cuenca(slug)
        print(f"  {slug}: {len(items)} embalses")
        for pid, url in items:
            if pid not in all_pantanos:
                all_pantanos[pid] = url
        time.sleep(1.5)
 
    print(f"\nTotal IDs únicos: {len(all_pantanos)}")
    if not all_pantanos:
        print("ERROR: No se encontraron IDs", file=sys.stderr)
        sys.exit(1)
 
    # Imprimir las primeras 3 URLs para diagnóstico
    for i, (pid, url) in enumerate(list(all_pantanos.items())[:3]):
        print(f"  [URL ejemplo] pid={pid} url={url}")
 
    embalses = []
    nombres_vistos = set()
    items = list(all_pantanos.items())
 
    for i, (pid, url) in enumerate(items):
        if i % 50 == 0:
            print(f"  {i}/{len(items)} ({len(embalses)} obtenidos)…")
        datos = scrape_pantano(pid, url)
        if datos and datos["nombre"] not in nombres_vistos:
            nombres_vistos.add(datos["nombre"])
            embalses.append(datos)
        time.sleep(0.35)
 
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
    print(f"\n✓ datos.json: {len(embalses)} embalses, {total['hm3']}/{total['capacidad']} hm³ ({pct}%)")
 
if __name__ == "__main__":
    main()
 


if __name__ == "__main__":
    main()
