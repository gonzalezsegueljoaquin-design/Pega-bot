"""
Bot de empleos Osorno
Selectores basados en HTML real inspeccionado de cada sitio.
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

# ──────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────
INTERVALO     = 30       # segundos entre ciclos de búsqueda
MAX_REGISTROS = 3000
TIMEOUT       = 15
DELAY_FETCH   = 2.0      # pausa entre fetches de detalle
MAX_DESC      = 700
MAX_REQ       = 500

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


# ──────────────────────────────────────────────────────
# MODELO
# ──────────────────────────────────────────────────────
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


# ──────────────────────────────────────────────────────
# ESTADO PERSISTENTE (doble índice: URL + hash contenido)
# ──────────────────────────────────────────────────────
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
            log.info(f"Estado cargado: {len(self.urls)} URLs, {len(self.huellas)} huellas")
        except FileNotFoundError:
            log.info("Sin historial previo — empezando desde cero")
        except Exception as e:
            log.warning(f"Error cargando estado: {e}")

    def _guardar(self):
        urls    = list(self.urls)[-MAX_REGISTROS:]
        huellas = list(self.huellas)[-MAX_REGISTROS:]
        self.urls    = set(urls)
        self.huellas = set(huellas)
        try:
            with open(STATE_FILE, "w", encoding="utf-8") as f:
                json.dump({"urls": urls, "huellas": huellas}, f, ensure_ascii=False, indent=2)
        except Exception as e:
            log.error(f"Error guardando estado: {e}")

    @staticmethod
    def _huella(titulo: str, empresa: str) -> str:
        txt = re.sub(r"\s+", " ", (titulo + "|" + empresa).lower().strip())
        return hashlib.md5(txt.encode()).hexdigest()

    def ya_enviado(self, o: Oferta) -> bool:
        return o.link in self.urls or self._huella(o.titulo, o.empresa) in self.huellas

    def registrar(self, o: Oferta):
        self.urls.add(o.link)
        self.huellas.add(self._huella(o.titulo, o.empresa))
        self._guardar()

    def marcar_url(self, url: str):
        self.urls.add(url)
        self._guardar()


# ──────────────────────────────────────────────────────
# UTILIDADES
# ──────────────────────────────────────────────────────
def L(t) -> str:
    """Limpia espacios."""
    return re.sub(r"\s+", " ", (t or "")).strip()

def truncar(t: str, n: int) -> str:
    t = L(t)
    return t[:n] + "…" if len(t) > n else t

def abs_url(href: str, base: str) -> str:
    href = (href or "").strip()
    if not href or href.startswith("#") or href.startswith("javascript"):
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

def regex_sueldo(texto: str) -> str:
    m = re.search(r"\$\s*[\d\.\,]+(?:\s*[-–]\s*\$?\s*[\d\.\,]+)?", texto)
    return L(m.group(0)) if m else ""

def regex_fecha(texto: str) -> str:
    for p in [
        r"hace\s+\d+\s+(?:minuto|minutos|hora|horas|día|días|semana|semanas)",
        r"\d{1,2}\s+de\s+\w+\s+de\s+\d{4}",
        r"\d{4}-\d{2}-\d{2}",
        r"\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}",
        r"(?:hoy|ayer)",
    ]:
        m = re.search(p, texto, re.I)
        if m:
            return L(m.group(0))
    return ""

def jornada_texto(texto: str) -> str:
    t = texto.lower()
    if re.search(r"part.?time|jornada parcial", t): return "Part Time"
    if re.search(r"full.?time|jornada completa",  t): return "Full Time"
    return ""

def tabla_valor(soup: BeautifulSoup, clave: str) -> str:
    """
    En Chiletrabajos, los datos están en una tabla de dos columnas:
    <td>Clave</td><td>Valor</td>
    """
    for td in soup.select("table td"):
        if clave.lower() in td.get_text().lower():
            sig = td.find_next_sibling("td")
            if sig:
                return L(sig.get_text(separator=" "))
    return ""


# ──────────────────────────────────────────────────────
# TELEGRAM
# ──────────────────────────────────────────────────────
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


# ──────────────────────────────────────────────────────
# CHILETRABAJOS
# Listado: https://www.chiletrabajos.cl/ciudad/osorno.html
#
# HTML real del detalle:
#   <table>
#     <tr><td>ID</td>     <td>3812588</td></tr>
#     <tr><td>Buscado</td><td>Valor & Talento</td></tr>
#     <tr><td>Fecha</td>  <td>2026-03-31 16:13:45</td></tr>
#     <tr><td>Salario</td><td>1.300.000</td></tr>
#     <tr><td>Tipo</td>   <td>Full-time</td></tr>
#   </table>
#   <h1>Jefe de Laboratorio</h1>
#   <h3>Descripción oferta de trabajo</h3>
#   <p>...descripción... Requisitos: - ...</p>
#
#   Botón postular: <a href="/trabajo/postular/3812588">Postular</a>
#   Si ya postulaste: el texto del botón cambia a "Postulado" o aparece
#   una clase CSS "postulado" en el enlace.
# ──────────────────────────────────────────────────────
BASE_CHT = "https://www.chiletrabajos.cl"

def listar_chiletrabajos() -> list:
    soup = get_soup(f"{BASE_CHT}/ciudad/osorno.html")
    if not soup:
        log.warning("Chiletrabajos: sin conexión")
        return []

    resultado = []
    # Los enlaces de oferta siguen el patrón /trabajo/nombre-oferta-NNNNNNN
    # Excluir: /postular/, /relacionadas/, ?utm_, /ciudad/, /trabajos/
    patron = re.compile(r"^/trabajo/[^/]+-\d+$")
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if not patron.match(href):
            continue
        link = BASE_CHT + href
        # Título: texto del enlace o del heading padre
        titulo = L(a.get_text(separator=" "))
        if len(titulo) < 4:
            for anc in a.parents:
                h = anc.find(["h2", "h3", "h1"])
                if h:
                    titulo = L(h.get_text())
                    break
        if len(titulo) < 4:
            continue
        resultado.append({"titulo": titulo, "link": link, "fuente": "Chiletrabajos"})

    # Deduplicar
    seen, unicos = set(), []
    for t in resultado:
        if t["link"] not in seen:
            seen.add(t["link"])
            unicos.append(t)
    log.info(f"Chiletrabajos: {len(unicos)} en listado")
    return unicos


def detalle_chiletrabajos(link: str) -> dict:
    soup = get_soup(link)
    if not soup:
        return {}

    # ── Título ──
    titulo = L(soup.find("h1").get_text()) if soup.find("h1") else ""

    # ── Datos de la tabla ──
    empresa = tabla_valor(soup, "buscado")
    fecha   = tabla_valor(soup, "fecha")
    if fecha:
        # Quedarnos sólo con la parte de fecha (sin hora)
        m = re.match(r"(\d{4}-\d{2}-\d{2})", fecha)
        fecha = m.group(1) if m else fecha

    sueldo  = tabla_valor(soup, "salario")
    if sueldo:
        # Formatear: "1.300.000" → "$1.300.000"
        sueldo = sueldo.strip()
        if sueldo and not sueldo.startswith("$"):
            sueldo = "$" + sueldo
    else:
        sueldo = regex_sueldo(soup.get_text())

    jornada = tabla_valor(soup, "tipo")
    if not jornada:
        jornada = jornada_texto(soup.get_text())

    # ── Descripción y Requisitos ──
    # El bloque de texto principal viene después de la tabla.
    # Patrón real: todo el texto está en párrafos/divs después del h3 "Descripción oferta de trabajo"
    descripcion = ""
    requisitos  = ""

    # Buscar el bloque de texto que contiene la descripción
    bloque_texto = ""
    for tag in soup.find_all(["h2", "h3", "h4", "strong", "b"]):
        txt_tag = tag.get_text(strip=True).lower()
        if "descripci" in txt_tag and "oferta" in txt_tag:
            # Recoger todos los siblings de texto hasta el próximo h2/h3 de nivel similar
            partes = []
            for sib in tag.next_siblings:
                if hasattr(sib, "name") and sib.name in ["h2", "h3"] and sib.name == tag.name:
                    break
                t = L(sib.get_text(separator=" ")) if hasattr(sib, "get_text") else L(str(sib))
                if t:
                    partes.append(t)
            bloque_texto = " ".join(partes)
            break

    if not bloque_texto:
        # Fallback: buscar el <article> o div principal
        for sel in ["article", ".main-content", ".oferta-content", "#oferta", "main"]:
            el = soup.select_one(sel)
            if el:
                bloque_texto = L(el.get_text(separator=" "))
                break

    if bloque_texto:
        # Separar descripción de requisitos usando el patrón "Requisitos:"
        split_patterns = [
            r"(?i)requisitos\s*:",
            r"(?i)requisitos\s*\n",
            r"(?i)-\s*requisito",
        ]
        separado = False
        for sp in split_patterns:
            partes = re.split(sp, bloque_texto, maxsplit=1)
            if len(partes) == 2:
                descripcion = L(partes[0])
                requisitos  = L(partes[1])
                separado = True
                break
        if not separado:
            descripcion = bloque_texto

    # Si aún no hay descripción, usar todo el texto de la página menos nav/header
    if not descripcion:
        for el in soup.select("nav, header, footer, script, style, .publicidad, [class*='ad']"):
            el.decompose()
        descripcion = L(soup.get_text(separator=" "))

    # ── ¿Ya postulaste? ──
    # Chiletrabajos: botón dice "Postular" (no postulado) o "Postulado" (ya postulado)
    # El link de postular es /trabajo/postular/NNNNN
    postulado = False
    for a in soup.select("a[href]"):
        href_a = a.get("href", "")
        txt_a  = L(a.get_text()).lower()
        clase  = " ".join(a.get("class", [])).lower()
        # Si el link de postular existe y dice exactamente "Postular" → NO postulado
        # Si dice "Postulado" o tiene clase "postulado" → SÍ postulado
        if "/postular/" in href_a:
            if "postulado" in txt_a or "postulado" in clase:
                postulado = True
            # Si sólo dice "postular" (sin 'do') → no postulado, dejar False
            break
    # También revisar el texto completo de la página
    texto_pagina = soup.get_text().lower()
    if any(x in texto_pagina for x in [
        "ya postulaste", "ya postulado", "postulación enviada",
        "aplicación enviada", "ya aplicaste", "ya te has postulado",
    ]):
        postulado = True

    return {
        "titulo": titulo, "empresa": empresa, "sueldo": sueldo,
        "fecha": fecha,   "jornada": jornada,
        "descripcion": descripcion, "requisitos": requisitos,
        "postulado": postulado,
    }


# ──────────────────────────────────────────────────────
# COMPUTRABAJO
# Listado: https://cl.computrabajo.com/empleos-en-los-lagos-en-osorno
# ──────────────────────────────────────────────────────
BASE_CT = "https://cl.computrabajo.com"

def listar_computrabajo() -> list:
    soup = get_soup(f"{BASE_CT}/empleos-en-los-lagos-en-osorno")
    if not soup:
        log.warning("Computrabajo: sin conexión")
        return []

    resultado = []
    # Computrabajo: artículos con clase box_offer o links a /oferta-de-trabajo/
    patron = re.compile(r"/oferta-de-trabajo/|/empleo-")
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if not patron.search(href):
            continue
        link   = abs_url(href, BASE_CT)
        titulo = L(a.get_text(separator=" "))
        # Buscar título en heading padre si el link no tiene texto
        if len(titulo) < 4:
            for anc in a.parents:
                h = anc.find(["h2", "h3"])
                if h:
                    titulo = L(h.get_text())
                    break
        if len(titulo) < 4 or not link:
            continue
        resultado.append({"titulo": titulo, "link": link, "fuente": "Computrabajo"})

    seen, unicos = set(), []
    for t in resultado:
        if t["link"] not in seen:
            seen.add(t["link"])
            unicos.append(t)
    log.info(f"Computrabajo: {len(unicos)} en listado")
    return unicos


def detalle_computrabajo(link: str) -> dict:
    soup = get_soup(link)
    if not soup:
        return {}

    texto_pagina = soup.get_text(" ", strip=True)

    titulo  = L(soup.find("h1").get_text()) if soup.find("h1") else ""
    empresa = ""
    for sel in ["[class*='company']", "[class*='empresa']", "p.fs16",
                "a[data-company]", "span[itemprop='name']", "[class*='companyName']"]:
        el = soup.select_one(sel)
        if el and L(el.get_text()):
            empresa = L(el.get_text())
            break

    sueldo  = ""
    for sel in ["[class*='salary']", "[class*='sueldo']", "[id*='salary']"]:
        el = soup.select_one(sel)
        if el and L(el.get_text()):
            sueldo = L(el.get_text())
            break
    if not sueldo:
        sueldo = regex_sueldo(texto_pagina)

    fecha   = ""
    for sel in ["time[datetime]", "p.fs13", "[class*='date']", "[class*='fecha']"]:
        el = soup.select_one(sel)
        if el:
            fecha = L(el.get("datetime") or el.get_text())
            if fecha:
                break
    if not fecha:
        fecha = regex_fecha(texto_pagina)

    jornada = jornada_texto(texto_pagina)

    descripcion = ""
    for sel in ["#jobDescription", "[class*='description']",
                "[class*='texto_oferta']", ".box_detail", "article"]:
        el = soup.select_one(sel)
        if el and len(L(el.get_text())) > 50:
            descripcion = L(el.get_text(separator=" "))
            break

    requisitos = ""
    for sel in ["[class*='requirements']", "[class*='requisit']"]:
        el = soup.select_one(sel)
        if el:
            requisitos = L(el.get_text(separator=" "))
            break
    # Si no hay sección separada, intentar split en el texto de descripción
    if not requisitos and descripcion:
        partes = re.split(r"(?i)requisitos\s*:", descripcion, maxsplit=1)
        if len(partes) == 2:
            descripcion = L(partes[0])
            requisitos  = L(partes[1])

    # Postulado
    postulado = False
    texto_lower = texto_pagina.lower()
    if any(x in texto_lower for x in [
        "ya postulaste", "ya aplicaste", "postulación enviada",
        "aplicaste a esta", "ya te postulaste",
    ]):
        postulado = True
    for btn in soup.select("button, a.btn, [class*='apply']"):
        txt = L(btn.get_text()).lower()
        cls = " ".join(btn.get("class", [])).lower()
        if "postulado" in txt or "aplicado" in txt or "applied" in txt:
            postulado = True
            break

    return {
        "titulo": titulo, "empresa": empresa, "sueldo": sueldo,
        "fecha": fecha,   "jornada": jornada,
        "descripcion": descripcion, "requisitos": requisitos,
        "postulado": postulado,
    }


# ──────────────────────────────────────────────────────
# INDEED CHILE
# Listado: https://cl.indeed.com/empleos?l=Osorno&sort=date
# ──────────────────────────────────────────────────────
BASE_INDEED = "https://cl.indeed.com"

def listar_indeed() -> list:
    url  = f"{BASE_INDEED}/empleos?l=Osorno%2C+Los+Lagos&sort=date"
    soup = get_soup(url)
    if not soup:
        log.warning("Indeed: sin conexión")
        return []

    resultado = []
    for card in soup.select("[data-jk], [class*='job_seen_beacon'], [class*='SerpJobCard']"):
        a = card.select_one("h2 a[href], a[data-jk]")
        if not a:
            continue
        href  = a.get("href", "")
        link  = abs_url(href, BASE_INDEED)
        titulo = L(a.get_text(separator=" "))
        if not titulo:
            h = card.find(["h2", "h3"])
            titulo = L(h.get_text()) if h else ""
        if len(titulo) < 4 or not link:
            continue
        resultado.append({"titulo": titulo, "link": link, "fuente": "Indeed"})

    seen, unicos = set(), []
    for t in resultado:
        if t["link"] not in seen:
            seen.add(t["link"])
            unicos.append(t)
    log.info(f"Indeed: {len(unicos)} en listado")
    return unicos


def detalle_indeed(link: str) -> dict:
    soup = get_soup(link)
    if not soup:
        return {}

    texto_pagina = soup.get_text(" ", strip=True)

    titulo  = ""
    for sel in ["h1.jobsearch-JobInfoHeader-title", "h1[class*='title']", "h1"]:
        el = soup.select_one(sel)
        if el:
            titulo = L(el.get_text())
            break

    empresa = ""
    for sel in [
        "[data-testid='inlineHeader-companyName']",
        "[class*='companyName']", "[class*='company']",
        "span[itemprop='name']",
    ]:
        el = soup.select_one(sel)
        if el and L(el.get_text()):
            empresa = L(el.get_text())
            break

    sueldo = ""
    for sel in [
        "[class*='salary']", "[id*='salaryInfoAndJobType']",
        "[data-testid*='salary']", "[class*='compensation']",
    ]:
        el = soup.select_one(sel)
        if el and L(el.get_text()):
            sueldo = L(el.get_text())
            break
    if not sueldo:
        sueldo = regex_sueldo(texto_pagina)

    fecha = ""
    for sel in ["[data-testid='myJobsStateDate']", "span[class*='date']", "time"]:
        el = soup.select_one(sel)
        if el:
            fecha = L(el.get("datetime") or el.get_text())
            if fecha:
                break
    if not fecha:
        fecha = regex_fecha(texto_pagina)

    jornada = jornada_texto(texto_pagina)

    descripcion = ""
    for sel in [
        "#jobDescriptionText",
        "[class*='jobsearch-jobDescriptionText']",
        "[class*='description']", "section",
    ]:
        el = soup.select_one(sel)
        if el and len(L(el.get_text())) > 50:
            descripcion = L(el.get_text(separator=" "))
            break

    requisitos = ""
    if descripcion:
        partes = re.split(r"(?i)requisitos\s*:", descripcion, maxsplit=1)
        if len(partes) == 2:
            descripcion = L(partes[0])
            requisitos  = L(partes[1])

    postulado = False
    texto_lower = texto_pagina.lower()
    if any(x in texto_lower for x in ["applied", "ya postulaste", "ya aplicaste"]):
        postulado = True
    for btn in soup.select("button, [class*='apply']"):
        txt = L(btn.get_text()).lower()
        if "applied" in txt or "postulado" in txt:
            postulado = True
            break

    return {
        "titulo": titulo, "empresa": empresa, "sueldo": sueldo,
        "fecha": fecha,   "jornada": jornada,
        "descripcion": descripcion, "requisitos": requisitos,
        "postulado": postulado,
    }


# ──────────────────────────────────────────────────────
# CONSTRUIR OFERTA
# ──────────────────────────────────────────────────────
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
        val = L(d.get(key) or "")
        return val if val else fallback

    return Oferta(
        titulo      = v("titulo",      L(item["titulo"])[:140]) or "Sin título",
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


# ──────────────────────────────────────────────────────
# MENSAJE TELEGRAM
# ──────────────────────────────────────────────────────
SEP = "─" * 30

def formatear_mensaje(o: Oferta) -> str:
    estado = "✅ YA POSTULASTE" if o.postulado else "🆕 NUEVA OFERTA"

    # Descripción limpia: eliminar frases de navegación que filtran del HTML
    desc = o.descripcion
    for ruido in [
        "PUBLICIDAD", "Volver", "Buscar Ofertas", "Detalle oferta",
        "Ofertas relacionadas", "Más ofertas", "Guardar", "Compartir",
        "El anuncio ha sido visto", "Interesados:", "Comparte por redes",
        "Estadísticas del anuncio", "Denunciar oferta", "Compartir enlace",
    ]:
        desc = desc.replace(ruido, "")
    desc = truncar(L(desc), MAX_DESC)

    req = truncar(L(o.requisitos), MAX_REQ) if o.requisitos != "No especificados" else ""

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
    if req:
        partes += [SEP, "✔️  REQUISITOS", req]
    partes += [SEP, f"🌐 {o.fuente}", f"🔗 {o.link}"]

    return "\n".join(partes)


# ──────────────────────────────────────────────────────
# CICLO
# ──────────────────────────────────────────────────────
def ciclo(estado: Estado) -> int:
    items: list = []
    items += listar_chiletrabajos()
    items += listar_computrabajo()
    items += listar_indeed()

    # Filtrar conocidas antes de hacer fetch de detalle
    candidatas: list = []
    vistos_ciclo: set = set()
    for it in items:
        url = it.get("link", "")
        if not url or url in vistos_ciclo or url in estado.urls:
            continue
        vistos_ciclo.add(url)
        candidatas.append(it)

    if not candidatas:
        log.info("Sin candidatas nuevas en este ciclo")
        return 0

    log.info(f"Candidatas: {len(candidatas)}")
    nuevos = 0

    for item in candidatas:
        oferta = construir_oferta(item)

        if estado.ya_enviado(oferta):
            log.info(f"  [DUP] {oferta.titulo[:55]}")
            estado.marcar_url(oferta.link)
            continue

        msg = formatear_mensaje(oferta)
        if enviar(msg):
            estado.registrar(oferta)
            nuevos += 1
            log.info(f"  [OK]  {oferta.titulo[:55]}")
        else:
            log.error(f"  [ERR] {oferta.link}")

    return nuevos


# ──────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────
def main() -> None:
    log.info("=" * 52)
    log.info("Bot de empleos Osorno — iniciando")

    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.error("Define TELEGRAM_TOKEN y TELEGRAM_CHAT_ID como variables de entorno.")
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
            t0      = time.time()
            nuevos  = ciclo(estado)
            elapsed = time.time() - t0
            log.info(f"Ciclo: {nuevos} nuevos | {elapsed:.1f}s")
            time.sleep(max(0, INTERVALO - elapsed))
        except KeyboardInterrupt:
            log.info("Bot detenido.")
            break
        except Exception as e:
            log.exception(f"Error inesperado: {e}")
            time.sleep(30)


if __name__ == "__main__":
    main()
