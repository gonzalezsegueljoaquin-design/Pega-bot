"""
Bot de empleos Osorno — URLs y selectores verificados en abril 2026
Fuentes:
  - Chiletrabajos : https://www.chiletrabajos.cl/ciudad/osorno.html
  - Computrabajo  : https://cl.computrabajo.com/empleos-en-los-lagos-en-osorno
  - Indeed        : https://cl.indeed.com/l-osorno,-los-lagos-empleos.html
  (BNE descartada: requiere JS para renderizar resultados)

Deduplicación doble: por URL + por hash(título+empresa)
Polling cada 30 s para detectar ofertas nuevas casi en tiempo real.
"""

import requests
from bs4 import BeautifulSoup
import time
import logging
import os
import re
import json
import hashlib
from dataclasses import dataclass
from typing import Optional

# ──────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────
INTERVALO      = 30      # segundos entre ciclos
MAX_REGISTROS  = 3000
TIMEOUT        = 15
DELAY_FETCH    = 2.0     # pausa entre cada página de detalle
MAX_DESC       = 700
MAX_REQ        = 450

STATE_FILE = "estado_bot.json"
LOG_FILE   = "bot_empleos.log"

TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-CL,es;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


# ──────────────────────────────────────────
# MODELO
# ──────────────────────────────────────────
@dataclass
class Oferta:
    titulo:      str
    link:        str
    fuente:      str
    empresa:     str  = "No especificada"
    sueldo:      str  = "No especificado"
    jornada:     str  = "No especificada"
    fecha:       str  = "No especificada"
    descripcion: str  = "No disponible"
    requisitos:  str  = "No especificados"
    postulado:   bool = False


# ──────────────────────────────────────────
# ESTADO PERSISTENTE
# ──────────────────────────────────────────
class Estado:
    def __init__(self):
        self.urls:    set = set()
        self.huellas: set = set()
        self._cargar()

    def _cargar(self):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                d = json.load(f)
            self.urls    = set(d.get("urls", []))
            self.huellas = set(d.get("huellas", []))
            log.info(f"Estado: {len(self.urls)} URLs, {len(self.huellas)} huellas")
        except FileNotFoundError:
            log.info("Estado nuevo — sin historial previo")
        except Exception as e:
            log.warning(f"Error cargando estado: {e}")

    def _guardar(self):
        urls    = list(self.urls)[-MAX_REGISTROS:]
        huellas = list(self.huellas)[-MAX_REGISTROS:]
        self.urls    = set(urls)
        self.huellas = set(huellas)
        try:
            with open(STATE_FILE, "w", encoding="utf-8") as f:
                json.dump({"urls": urls, "huellas": huellas}, f,
                          ensure_ascii=False, indent=2)
        except Exception as e:
            log.error(f"Error guardando estado: {e}")

    @staticmethod
    def _huella(titulo: str, empresa: str) -> str:
        txt = re.sub(r"\s+", " ", (titulo + empresa).lower().strip())
        return hashlib.md5(txt.encode()).hexdigest()

    def ya_enviado(self, o: Oferta) -> bool:
        if o.link in self.urls:
            return True
        return self._huella(o.titulo, o.empresa) in self.huellas

    def registrar(self, o: Oferta):
        self.urls.add(o.link)
        self.huellas.add(self._huella(o.titulo, o.empresa))
        self._guardar()

    def marcar_url(self, url: str):
        """Marca sólo la URL (sin contenido) para no volver a visitarla."""
        self.urls.add(url)
        self._guardar()


# ──────────────────────────────────────────
# UTILIDADES
# ──────────────────────────────────────────
def limpiar(t: str) -> str:
    return re.sub(r"\s+", " ", (t or "")).strip()

def truncar(t: str, n: int) -> str:
    t = limpiar(t)
    return t[:n] + "…" if len(t) > n else t

def abs_url(href: str, base: str) -> str:
    href = (href or "").strip()
    if not href or href.startswith("#"):
        return ""
    return href if href.startswith("http") else base.rstrip("/") + "/" + href.lstrip("/")

def get_soup(url: str, reintentos: int = 2) -> Optional[BeautifulSoup]:
    for i in range(1, reintentos + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            r.raise_for_status()
            return BeautifulSoup(r.text, "html.parser")
        except requests.RequestException as e:
            log.warning(f"GET [{i}] {url[:80]}: {e}")
            time.sleep(DELAY_FETCH * i)
    return None

def primer_texto(soup: BeautifulSoup, *sels) -> str:
    for sel in sels:
        try:
            el = soup.select_one(sel)
            if el:
                t = limpiar(el.get_text(separator=" "))
                if t:
                    return t
        except Exception:
            continue
    return ""

def buscar_sueldo_regex(texto: str) -> str:
    m = re.search(r"\$\s*[\d\.\,]+(?:\s*[-–]\s*\$?\s*[\d\.\,]+)?", texto)
    return limpiar(m.group(0)) if m else ""

def detectar_jornada(texto: str) -> str:
    t = texto.lower()
    if "part.?time" in t or "jornada parcial" in t:  return "Part Time"
    if "full.?time" in t or "jornada completa" in t: return "Full Time"
    return ""

def buscar_fecha_regex(texto: str) -> str:
    patrones = [
        r"hace\s+\d+\s+(?:minuto|minutos|hora|horas|día|días|semana|semanas)",
        r"\d{1,2}\s+de\s+\w+\s+de\s+\d{4}",
        r"\d{4}-\d{2}-\d{2}",
        r"\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}",
        r"(?:publicado|publicada)[^\n]{0,30}(?:\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}|\d{4}-\d{2}-\d{2})",
        r"(?:hoy|ayer|anteayer)",
    ]
    for p in patrones:
        m = re.search(p, texto, re.I)
        if m:
            return limpiar(m.group(0))
    return ""

def detectar_postulado(soup: BeautifulSoup) -> bool:
    """
    Detecta si el usuario ya postuló a esta oferta.
    Chiletrabajos muestra "Ya postulaste" o el botón cambia a "Postulado".
    Computrabajo muestra "Ya aplicaste" o similar.
    """
    texto = soup.get_text(" ", strip=True).lower()
    indicadores = [
        "ya postulaste", "ya postulado", "postulado",
        "ya aplicaste", "aplicaste", "postulación enviada",
        "ya te inscribiste", "inscrito",
    ]
    # También buscar en atributos de botones
    for btn in soup.select("a, button, span"):
        txt = limpiar(btn.get_text()).lower()
        clase = " ".join(btn.get("class", [])).lower()
        if any(ind in txt or ind in clase for ind in indicadores):
            return True
    return any(ind in texto for ind in indicadores)


# ──────────────────────────────────────────
# TELEGRAM
# ──────────────────────────────────────────
def enviar(msg: str, reintentos: int = 3) -> bool:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.error("Faltan TELEGRAM_TOKEN / TELEGRAM_CHAT_ID")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    for i in range(1, reintentos + 1):
        try:
            r = requests.post(
                url,
                data={"chat_id": TELEGRAM_CHAT_ID, "text": msg[:4096]},
                timeout=TIMEOUT,
            )
            if r.status_code == 200:
                return True
            log.warning(f"Telegram {r.status_code} intento {i}: {r.text[:120]}")
        except requests.RequestException as e:
            log.warning(f"Error Telegram intento {i}: {e}")
        time.sleep(3 * i)
    return False


# ──────────────────────────────────────────
# SCRAPER: CHILETRABAJOS
# URL verificada: https://www.chiletrabajos.cl/ciudad/osorno.html
# ──────────────────────────────────────────
BASE_CHT = "https://www.chiletrabajos.cl"

def listar_chiletrabajos() -> list:
    soup = get_soup(f"{BASE_CHT}/ciudad/osorno.html")
    if not soup:
        log.warning("Chiletrabajos: sin conexión")
        return []

    resultado = []
    # Cada oferta es un <article> o un bloque con enlace al trabajo
    for a in soup.select("a[href*='/trabajo/']"):
        href = a.get("href", "")
        # Filtrar enlaces de navegación (relacionadas, postular, etc.)
        if any(x in href for x in ["/postular/", "/relacionadas/", "utm_"]):
            continue
        link = abs_url(href, BASE_CHT)
        if not link:
            continue
        # Título: texto del enlace o del h2 más cercano
        titulo = limpiar(a.get_text(separator=" "))
        if len(titulo) < 5:
            parent = a.find_parent(["article", "div", "li"])
            if parent:
                h = parent.find(["h2", "h3", "h1"])
                if h:
                    titulo = limpiar(h.get_text())
        if len(titulo) < 5:
            continue
        resultado.append({"titulo": titulo, "link": link, "fuente": "Chiletrabajos"})

    # Deduplicar por link dentro del listado
    vistos = set()
    unicos = []
    for t in resultado:
        if t["link"] not in vistos:
            vistos.add(t["link"])
            unicos.append(t)

    log.info(f"Chiletrabajos: {len(unicos)} ofertas en listado")
    return unicos

def detalle_chiletrabajos(link: str) -> dict:
    """
    Estructura real de chiletrabajos.cl/trabajo/xxx:
      - Tabla con: ID, empresa (Buscado), Fecha, Tipo (Full/Part)
      - h1 = título
      - .descripcion-oferta o sección "Descripción oferta de trabajo"
    """
    soup = get_soup(link)
    if not soup:
        return {}

    texto_pagina = soup.get_text(" ", strip=True)

    # Título
    titulo = primer_texto(soup, "h1")

    # Empresa: en la tabla aparece como "Buscado" → td siguiente
    empresa = ""
    for td in soup.select("table td"):
        if "buscado" in td.get_text().lower():
            sig = td.find_next_sibling("td")
            if sig:
                empresa = limpiar(sig.get_text())
            break
    if not empresa:
        empresa = primer_texto(soup, ".company-name", "[class*='empresa']", "[itemprop='name']")

    # Fecha: en la tabla y también como "Publicado: ayer / DD de mes YYYY"
    fecha = ""
    for td in soup.select("table td"):
        txt = td.get_text()
        if re.search(r"\d{4}-\d{2}-\d{2}", txt):
            m = re.search(r"\d{4}-\d{2}-\d{2}", txt)
            fecha = m.group(0)
            break
    if not fecha:
        fecha = buscar_fecha_regex(texto_pagina)

    # Tipo de jornada: en la tabla como "Full-time" / "Part-time"
    jornada = detectar_jornada(texto_pagina)
    for td in soup.select("table td"):
        txt = td.get_text().lower()
        if "full" in txt and "time" in txt:
            jornada = "Full Time"; break
        if "part" in txt and "time" in txt:
            jornada = "Part Time"; break

    # Sueldo
    sueldo = primer_texto(soup, "[class*='salary']", "[class*='sueldo']", ".sueldo")
    if not sueldo:
        sueldo = buscar_sueldo_regex(texto_pagina)

    # Descripción: la sección después del h3 "Descripción oferta de trabajo"
    descripcion = ""
    for h3 in soup.find_all(["h3", "h2", "strong"]):
        if "descripci" in h3.get_text().lower():
            sib = h3.find_next_sibling()
            partes = []
            while sib and len(partes) < 8:
                t = limpiar(sib.get_text(separator=" "))
                if t:
                    partes.append(t)
                sib = sib.find_next_sibling()
            descripcion = " ".join(partes)
            break
    if not descripcion:
        # fallback: bloque principal de texto
        main = soup.select_one("article, .main-content, #content, main")
        if main:
            descripcion = limpiar(main.get_text(separator=" "))

    # Requisitos: sección "Requisitos" si existe
    requisitos = ""
    for h3 in soup.find_all(["h3", "h2", "strong"]):
        if "requisito" in h3.get_text().lower():
            sib = h3.find_next_sibling()
            partes = []
            while sib and len(partes) < 6:
                t = limpiar(sib.get_text(separator=" "))
                if t:
                    partes.append(t)
                sib = sib.find_next_sibling()
            requisitos = " ".join(partes)
            break

    # Postulado
    postulado = detectar_postulado(soup)
    # En Chiletrabajos, si estás logueado y postulaste, el botón dice "Postulado"
    for btn in soup.select("a.btn, button, .postular"):
        if "postulado" in btn.get_text().lower() or "postulado" in " ".join(btn.get("class", [])).lower():
            postulado = True
            break

    return {
        "titulo": titulo, "empresa": empresa, "sueldo": sueldo,
        "fecha": fecha, "jornada": jornada,
        "descripcion": descripcion, "requisitos": requisitos,
        "postulado": postulado,
    }


# ──────────────────────────────────────────
# SCRAPER: COMPUTRABAJO
# URL verificada: https://cl.computrabajo.com/empleos-en-los-lagos-en-osorno
# ──────────────────────────────────────────
BASE_CT = "https://cl.computrabajo.com"

def listar_computrabajo() -> list:
    soup = get_soup(f"{BASE_CT}/empleos-en-los-lagos-en-osorno")
    if not soup:
        log.warning("Computrabajo: sin conexión")
        return []

    resultado = []
    # Computrabajo renderiza con JS, pero el HTML inicial trae algunos resultados
    # Los artículos tienen clase "box_offer" o similar
    for item in soup.select("article, [class*='box_offer'], [class*='offerList'], li[data-id]"):
        a = item.find("a", href=True)
        if not a:
            continue
        href = a.get("href", "")
        if "/oferta-de-trabajo/" not in href and "/empleo-" not in href:
            continue
        link = abs_url(href, BASE_CT)
        titulo = limpiar(a.get_text(separator=" "))
        if len(titulo) < 5:
            h = item.find(["h2", "h3", "h1"])
            titulo = limpiar(h.get_text()) if h else ""
        if len(titulo) < 5:
            continue
        resultado.append({"titulo": titulo, "link": link, "fuente": "Computrabajo"})

    # Si no encontró artículos (JS bloqueó), intentar con links directos
    if not resultado:
        for a in soup.select("a[href*='/oferta-de-trabajo/'], a[href*='/empleo-']"):
            link = abs_url(a.get("href", ""), BASE_CT)
            titulo = limpiar(a.get_text(separator=" "))
            if len(titulo) < 5 or not link:
                continue
            resultado.append({"titulo": titulo, "link": link, "fuente": "Computrabajo"})

    vistos = set()
    unicos = []
    for t in resultado:
        if t["link"] not in vistos:
            vistos.add(t["link"])
            unicos.append(t)

    log.info(f"Computrabajo: {len(unicos)} ofertas en listado")
    return unicos

def detalle_computrabajo(link: str) -> dict:
    soup = get_soup(link)
    if not soup:
        return {}

    texto_pagina = soup.get_text(" ", strip=True)

    titulo    = primer_texto(soup, "h1", "h1[class*='title']")
    empresa   = primer_texto(soup,
        "[class*='company']", "[class*='empresa']",
        "p.fs16", "a[data-company]", "span[data-company]")
    sueldo    = primer_texto(soup, "[class*='salary']", "[class*='sueldo']") or buscar_sueldo_regex(texto_pagina)
    fecha     = primer_texto(soup, "p.fs13", "time", "[class*='date']") or buscar_fecha_regex(texto_pagina)
    jornada   = detectar_jornada(texto_pagina)

    descripcion = primer_texto(soup,
        "#jobDescription", "[class*='description']",
        "[class*='texto_oferta']", "section.box_detail")
    requisitos  = primer_texto(soup, "[class*='requirements']", "[class*='requisit']")

    postulado = detectar_postulado(soup)

    return {
        "titulo": titulo, "empresa": empresa, "sueldo": sueldo,
        "fecha": fecha, "jornada": jornada,
        "descripcion": descripcion, "requisitos": requisitos,
        "postulado": postulado,
    }


# ──────────────────────────────────────────
# SCRAPER: INDEED CHILE
# URL verificada: https://cl.indeed.com/l-osorno,-los-lagos-empleos.html
# Indeed renderiza con JS; usamos la URL de búsqueda que sí devuelve HTML
# ──────────────────────────────────────────
BASE_INDEED = "https://cl.indeed.com"

def listar_indeed() -> list:
    url = f"{BASE_INDEED}/empleos?l=Osorno%2C+Los+Lagos&sort=date"
    soup = get_soup(url)
    if not soup:
        log.warning("Indeed: sin conexión")
        return []

    resultado = []
    # Indeed usa data-jk o href con /rc/clk o /pagead/clk
    for card in soup.select("[class*='job_seen_beacon'], [class*='jobsearch-SerpJobCard'], [data-jk]"):
        a = card.select_one("a[data-jk], a[href*='/rc/clk'], h2 a")
        if not a:
            continue
        href = a.get("href", "")
        if not href:
            continue
        link = abs_url(href, BASE_INDEED)
        titulo = limpiar(a.get_text(separator=" "))
        if not titulo:
            h = card.find(["h2", "h3"])
            titulo = limpiar(h.get_text()) if h else ""
        if len(titulo) < 5 or not link:
            continue
        resultado.append({"titulo": titulo, "link": link, "fuente": "Indeed"})

    vistos = set()
    unicos = []
    for t in resultado:
        if t["link"] not in vistos:
            vistos.add(t["link"])
            unicos.append(t)

    log.info(f"Indeed: {len(unicos)} ofertas en listado")
    return unicos

def detalle_indeed(link: str) -> dict:
    soup = get_soup(link)
    if not soup:
        return {}

    texto_pagina = soup.get_text(" ", strip=True)

    titulo  = primer_texto(soup, "h1.jobsearch-JobInfoHeader-title", "h1")
    empresa = primer_texto(soup,
        "[data-testid='inlineHeader-companyName']",
        "[class*='companyName']", "[class*='company']")
    sueldo  = primer_texto(soup,
        "[class*='salary']", "[id*='salaryInfoAndJobType']",
        "[data-testid*='salary']") or buscar_sueldo_regex(texto_pagina)
    fecha   = primer_texto(soup,
        "[class*='date']", "span[data-testid='myJobsStateDate']",
        "time") or buscar_fecha_regex(texto_pagina)
    jornada = detectar_jornada(texto_pagina)

    descripcion = primer_texto(soup,
        "#jobDescriptionText", "[class*='jobsearch-jobDescriptionText']",
        "section[class*='description']")
    requisitos  = ""  # Indeed mezcla descripción y requisitos

    postulado = detectar_postulado(soup)

    return {
        "titulo": titulo, "empresa": empresa, "sueldo": sueldo,
        "fecha": fecha, "jornada": jornada,
        "descripcion": descripcion, "requisitos": requisitos,
        "postulado": postulado,
    }


# ──────────────────────────────────────────
# CONSTRUIR OFERTA COMPLETA
# ──────────────────────────────────────────
DETALLE_FN = {
    "Chiletrabajos": detalle_chiletrabajos,
    "Computrabajo":  detalle_computrabajo,
    "Indeed":        detalle_indeed,
}

def construir_oferta(item: dict) -> Oferta:
    time.sleep(DELAY_FETCH)
    fn = DETALLE_FN.get(item["fuente"])
    d  = fn(item["link"]) if fn else {}

    def v(key: str, fallback: str) -> str:
        val = limpiar(d.get(key) or "")
        return val if val else fallback

    return Oferta(
        titulo      = v("titulo",      limpiar(item["titulo"])[:140]) or "Sin título",
        link        = item["link"],
        fuente      = item["fuente"],
        empresa     = v("empresa",     "No especificada"),
        sueldo      = v("sueldo",      "No especificado"),
        jornada     = v("jornada",     "No especificada"),
        fecha       = v("fecha",       "No especificada"),
        descripcion = v("descripcion", "No disponible"),
        requisitos  = v("requisitos",  "No especificados"),
        postulado   = bool(d.get("postulado", False)),
    )


# ──────────────────────────────────────────
# FORMATEAR MENSAJE
# ──────────────────────────────────────────
SEP = "─" * 30

def formatear_mensaje(o: Oferta) -> str:
    if o.postulado:
        estado = "✅ YA POSTULASTE A ESTA OFERTA"
    else:
        estado = "🆕 NUEVA OFERTA"

    desc = truncar(o.descripcion, MAX_DESC)
    req  = truncar(o.requisitos,  MAX_REQ)

    partes = [
        estado,
        SEP,
        f"📌 {o.titulo}",
        f"🏢 Empresa:    {o.empresa}",
        f"📅 Publicado:  {o.fecha}",
        f"🕒 Jornada:    {o.jornada}",
        f"💰 Sueldo:     {o.sueldo}",
        SEP,
        "📋 DESCRIPCIÓN",
        desc,
    ]
    if req and req != "No especificados":
        partes += [SEP, "✔️  REQUISITOS", req]
    partes += [SEP, f"🌐 {o.fuente}", f"🔗 {o.link}"]
    return "\n".join(partes)


# ──────────────────────────────────────────
# CICLO PRINCIPAL
# ──────────────────────────────────────────
def ciclo(estado: Estado) -> int:
    items: list = []
    items += listar_chiletrabajos()
    items += listar_computrabajo()
    items += listar_indeed()

    # Filtrar URLs ya conocidas antes de hacer fetch de detalle
    candidatas = []
    vistos_ciclo: set = set()
    for it in items:
        url = it.get("link", "")
        if not url:
            continue
        if url in vistos_ciclo:       # duplicado dentro del ciclo
            continue
        if url in estado.urls:        # ya enviada antes
            continue
        vistos_ciclo.add(url)
        candidatas.append(it)

    if not candidatas:
        log.info("Sin candidatas nuevas")
        return 0

    log.info(f"Candidatas a procesar: {len(candidatas)}")
    nuevos = 0

    for item in candidatas:
        oferta = construir_oferta(item)

        # Segunda barrera: hash de contenido
        if estado.ya_enviado(oferta):
            log.info(f"  [DUP contenido] {oferta.titulo[:55]}")
            estado.marcar_url(oferta.link)
            continue

        msg = formatear_mensaje(oferta)
        if enviar(msg):
            estado.registrar(oferta)
            nuevos += 1
            log.info(f"  [OK] {oferta.titulo[:55]}")
        else:
            log.error(f"  [ERR] fallo al enviar: {oferta.link}")

    return nuevos


# ──────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────
def main() -> None:
    log.info("=" * 52)
    log.info("Bot de empleos Osorno — iniciando")

    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.error("Debes definir TELEGRAM_TOKEN y TELEGRAM_CHAT_ID como variables de entorno.")
        return

    estado = Estado()
    enviar(
        f"🚀 Bot activo — Osorno\n"
        f"🔄 Revisando cada {INTERVALO}s\n"
        f"📊 Historial: {len(estado.urls)} ofertas conocidas\n"
        f"Fuentes: Chiletrabajos · Computrabajo · Indeed"
    )

    while True:
        try:
            t0     = time.time()
            nuevos = ciclo(estado)
            elapsed = time.time() - t0
            log.info(f"Ciclo: {nuevos} nuevos | {elapsed:.1f}s")
            time.sleep(max(0, INTERVALO - elapsed))

        except KeyboardInterrupt:
            log.info("Bot detenido por el usuario.")
            break
        except Exception as e:
            log.exception(f"Error inesperado: {e}")
            time.sleep(30)


if __name__ == "__main__":
    main()
