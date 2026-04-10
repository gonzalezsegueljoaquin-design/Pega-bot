"""
Bot de empleos Osorno — versión con HTML real verificado abril 2026
Fuentes que funcionan con scraping estático:
  - Chiletrabajos: /ciudad/osorno.html  (páginas 1-4, 30 ofertas c/u)
  - Indeed Chile : /empleos?l=Osorno&sort=date

Computrabajo usa React/JS para renderizar → no funciona sin navegador headless.
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
INTERVALO     = 30
MAX_REGISTROS = 3000
TIMEOUT       = 15
DELAY_FETCH   = 1.5
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
# ESTADO — doble índice: URL + hash(título+empresa)
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
            log.info(f"Estado: {len(self.urls)} URLs, {len(self.huellas)} huellas")
        except FileNotFoundError:
            log.info("Sin historial — empezando desde cero")
        except Exception as e:
            log.warning(f"Error cargando estado: {e}")

    def _guardar(self):
        urls    = list(self.urls)[-MAX_REGISTROS:]
        huellas = list(self.huellas)[-MAX_REGISTROS:]
        self.urls    = set(urls)
        self.huellas = set(huellas)
        try:
            with open(STATE_FILE, "w", encoding="utf-8") as f:
                json.dump({"urls": urls, "huellas": huellas},
                          f, ensure_ascii=False, indent=2)
        except Exception as e:
            log.error(f"Error guardando estado: {e}")

    @staticmethod
    def _huella(titulo: str, empresa: str) -> str:
        txt = re.sub(r"\s+", " ", (titulo + "|" + empresa).lower().strip())
        return hashlib.md5(txt.encode()).hexdigest()

    def ya_enviado(self, o: Oferta) -> bool:
        return (o.link in self.urls or
                self._huella(o.titulo, o.empresa) in self.huellas)

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
def limpiar(t) -> str:
    return re.sub(r"\s+", " ", str(t or "")).strip()

def truncar(t: str, n: int) -> str:
    t = limpiar(t)
    return t[:n] + "…" if len(t) > n else t

def abs_url(href: str, base: str) -> str:
    href = (href or "").strip()
    if not href or href.startswith(("#", "javascript", "mailto")):
        return ""
    return href if href.startswith("http") else base.rstrip("/") + "/" + href.lstrip("/")

def get_soup(url: str, reintentos: int = 3) -> Optional[BeautifulSoup]:
    for i in range(1, reintentos + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            r.raise_for_status()
            return BeautifulSoup(r.text, "html.parser")
        except requests.RequestException as e:
            log.warning(f"GET [{i}/{reintentos}] {url[:80]}: {e}")
            time.sleep(DELAY_FETCH * i)
    return None

def tabla_valor(soup: BeautifulSoup, clave: str) -> str:
    """Lee tabla de dos columnas: <td>Clave</td><td>Valor</td>"""
    for td in soup.select("table td"):
        if clave.lower() in td.get_text().lower():
            sib = td.find_next_sibling("td")
            if sib:
                return limpiar(sib.get_text(separator=" "))
    return ""

def regex_sueldo(texto: str) -> str:
    m = re.search(r"\$\s*[\d\.\,]+(?:\s*[-–]\s*\$?\s*[\d\.\,]+)?", texto)
    return limpiar(m.group(0)) if m else ""

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
            return limpiar(m.group(0))
    return ""

def jornada_texto(texto: str) -> str:
    t = texto.lower()
    if re.search(r"part.?time|jornada parcial", t): return "Part Time"
    if re.search(r"full.?time|jornada completa", t): return "Full Time"
    return ""

# Ruido HTML que aparece en el get_text() de las páginas
RUIDO = [
    "PUBLICIDAD", "Volver", "Buscar Ofertas", "Detalle oferta",
    "Ofertas relacionadas", "Más ofertas", "Guardar", "Compartir",
    "El anuncio ha sido visto", "Estadísticas del anuncio",
    "Denunciar oferta", "Compartir enlace", "Interesados:",
    "Comparte por redes sociales", "Ver más",
]

def quitar_ruido(texto: str) -> str:
    for r in RUIDO:
        texto = texto.replace(r, "")
    return limpiar(texto)


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
# CHILETRABAJOS — LISTADO
#
# HTML real verificado:
#   <h2><a href="/trabajo/nombre-NNNNNNN">Título</a></h2>
#   <h3>Empresa, <a href="/ciudad/osorno.html">Osorno</a></h3>
#   <h3><a href="/trabajo/nombre-NNNNNNN">08 de Abril de 2026</a></h3>
#
# Paginación: /ciudad/osorno.html, /30, /60, /90 (4 páginas × 30 = ~120 ofertas)
# ──────────────────────────────────────────────────────
BASE_CHT = "https://www.chiletrabajos.cl"
PAGINAS_CHT = [
    f"{BASE_CHT}/ciudad/osorno.html",
    f"{BASE_CHT}/ciudad/osorno.html/30",
    f"{BASE_CHT}/ciudad/osorno.html/60",
    f"{BASE_CHT}/ciudad/osorno.html/90",
]
# Patrón real: /trabajo/slug-con-guiones-NNNNNNN
RE_LINK_CHT = re.compile(r"^/trabajo/[a-z0-9\-]+-\d+$")

def listar_chiletrabajos() -> list:
    resultado = []
    seen: set = set()

    for url_pag in PAGINAS_CHT:
        soup = get_soup(url_pag)
        if not soup:
            log.warning(f"Chiletrabajos: sin respuesta en {url_pag}")
            continue

        # Cada oferta tiene un <h2> con el <a> al trabajo
        for h2 in soup.find_all("h2"):
            a = h2.find("a", href=True)
            if not a:
                continue
            href = a.get("href", "")
            if not RE_LINK_CHT.match(href):
                continue
            link   = BASE_CHT + href
            titulo = limpiar(a.get_text())

            # Empresa: en el primer <h3> después del <h2>
            empresa = ""
            h3 = h2.find_next_sibling("h3")
            if h3:
                # El h3 tiene "Empresa, Ciudad" — quedarse solo con la empresa
                txt_h3 = h3.get_text(separator=" ")
                # Eliminar la parte de ciudad (después de la última coma)
                partes = txt_h3.rsplit(",", 1)
                empresa = limpiar(partes[0])

            # Fecha: en el segundo <h3> (el que tiene link al trabajo)
            fecha_listado = ""
            h3_fecha = h3.find_next_sibling("h3") if h3 else None
            if h3_fecha:
                fecha_listado = limpiar(h3_fecha.get_text())

            if not titulo or link in seen:
                continue
            seen.add(link)
            resultado.append({
                "titulo":         titulo,
                "link":           link,
                "fuente":         "Chiletrabajos",
                "empresa_rapida": empresa,
                "fecha_rapida":   fecha_listado,
            })

        time.sleep(0.5)  # pausa entre páginas

    log.info(f"Chiletrabajos: {len(resultado)} ofertas en listado")
    return resultado


# ──────────────────────────────────────────────────────
# CHILETRABAJOS — DETALLE
# ──────────────────────────────────────────────────────
def detalle_chiletrabajos(link: str) -> dict:
    soup = get_soup(link)
    if not soup:
        return {}

    # Título
    h1 = soup.find("h1")
    titulo = limpiar(h1.get_text()) if h1 else ""

    # Empresa, fecha, salario, tipo — todos en la tabla
    empresa = tabla_valor(soup, "buscado")
    tipo    = tabla_valor(soup, "tipo")     # "Full-time" / "Part-time"
    salario = tabla_valor(soup, "salario")
    fecha   = tabla_valor(soup, "fecha")

    # Limpiar fecha (quitar hora)
    m = re.match(r"(\d{4}-\d{2}-\d{2})", fecha)
    fecha = m.group(1) if m else fecha

    # Formatear sueldo
    if salario and not salario.startswith("$"):
        salario = "$" + salario

    # Jornada desde tabla o texto
    jornada = tipo if tipo else jornada_texto(soup.get_text())

    # ── Descripción y Requisitos ──
    # El bloque empieza después del <h3> "Descripción oferta de trabajo"
    # y termina en el siguiente <h3> (Beneficios, Comparte, etc.)
    descripcion = ""
    requisitos  = ""

    h3_desc = None
    for tag in soup.find_all(["h3", "h2"]):
        if "descripci" in tag.get_text().lower():
            h3_desc = tag
            break

    if h3_desc:
        partes = []
        for sib in h3_desc.next_siblings:
            # Parar en el siguiente h3 (Beneficios, etc.)
            if getattr(sib, "name", None) in ("h3", "h2"):
                break
            t = limpiar(sib.get_text(separator=" ")) if hasattr(sib, "get_text") else ""
            if t:
                partes.append(t)
        bloque = quitar_ruido(" ".join(partes))

        # Separar descripción de requisitos en el bloque
        m_req = re.search(r"(?i)requisitos\s*:", bloque)
        if m_req:
            descripcion = limpiar(bloque[:m_req.start()])
            requisitos  = limpiar(bloque[m_req.end():])
        else:
            descripcion = bloque

    # Fallback si el bloque quedó vacío
    if not descripcion:
        for sel in ["article", ".oferta-content", "#oferta", "main"]:
            el = soup.select_one(sel)
            if el:
                descripcion = quitar_ruido(el.get_text(separator=" "))
                break

    # ── Postulado ──
    # Botón dice "Postular" (no postulado) o "Postulado" (ya postulado)
    postulado = False
    for a in soup.find_all("a", href=True):
        if "/postular/" in a["href"]:
            txt = limpiar(a.get_text()).lower()
            cls = " ".join(a.get("class", [])).lower()
            if "postulado" in txt or "postulado" in cls:
                postulado = True
            break  # el primero que encuentre es el botón principal

    # También buscar en el texto completo
    texto_pg = soup.get_text().lower()
    if not postulado and any(x in texto_pg for x in [
        "ya postulaste", "ya te has postulado",
        "postulación enviada", "ya aplicaste",
    ]):
        postulado = True

    return {
        "titulo":      titulo,
        "empresa":     empresa,
        "sueldo":      salario,
        "fecha":       fecha,
        "jornada":     jornada,
        "descripcion": descripcion,
        "requisitos":  requisitos,
        "postulado":   postulado,
    }


# ──────────────────────────────────────────────────────
# INDEED CHILE — LISTADO
# URL: https://cl.indeed.com/empleos?l=Osorno&sort=date
# Indeed renderiza algo de HTML inicial (no todo JS)
# ──────────────────────────────────────────────────────
BASE_INDEED = "https://cl.indeed.com"

def listar_indeed() -> list:
    url  = f"{BASE_INDEED}/empleos?l=Osorno%2C+Los+Lagos&sort=date&limit=50"
    soup = get_soup(url)
    if not soup:
        log.warning("Indeed: sin conexión")
        return []

    resultado = []
    seen: set = set()

    # Indeed: tarjetas con atributo data-jk o class que contiene 'job'
    for card in soup.select("[data-jk], [class*='job_seen_beacon'], [class*='resultContent']"):
        # Título en h2 > a
        a = card.select_one("h2 a, a[data-jk]")
        if not a:
            continue
        href = a.get("href", "")
        if not href:
            continue
        link = abs_url(href, BASE_INDEED)
        if not link or link in seen:
            continue

        titulo = limpiar(a.get_text(separator=" "))
        if not titulo:
            span = a.select_one("span[title], span")
            titulo = limpiar(span.get("title") or span.get_text()) if span else ""
        if len(titulo) < 3:
            continue

        # Empresa desde la tarjeta
        empresa = ""
        for sel in ["[data-testid='company-name']", "[class*='companyName']",
                    "span[class*='company']"]:
            el = card.select_one(sel)
            if el:
                empresa = limpiar(el.get_text())
                break

        seen.add(link)
        resultado.append({
            "titulo":         titulo,
            "link":           link,
            "fuente":         "Indeed",
            "empresa_rapida": empresa,
            "fecha_rapida":   "",
        })

    log.info(f"Indeed: {len(resultado)} ofertas en listado")
    return resultado


# ──────────────────────────────────────────────────────
# INDEED — DETALLE
# ──────────────────────────────────────────────────────
def detalle_indeed(link: str) -> dict:
    soup = get_soup(link)
    if not soup:
        return {}

    texto_pg = soup.get_text(" ", strip=True)

    titulo = ""
    for sel in ["h1.jobsearch-JobInfoHeader-title", "h1[class*='title']", "h1"]:
        el = soup.select_one(sel)
        if el:
            titulo = limpiar(el.get_text())
            break

    empresa = ""
    for sel in ["[data-testid='inlineHeader-companyName']",
                "[class*='companyName']", "[class*='company']"]:
        el = soup.select_one(sel)
        if el and limpiar(el.get_text()):
            empresa = limpiar(el.get_text())
            break

    sueldo = ""
    for sel in ["[class*='salary']", "[id*='salaryInfoAndJobType']",
                "[data-testid*='salary']"]:
        el = soup.select_one(sel)
        if el and limpiar(el.get_text()):
            sueldo = limpiar(el.get_text())
            break
    if not sueldo:
        sueldo = regex_sueldo(texto_pg)

    fecha = ""
    for sel in ["[data-testid='myJobsStateDate']", "span[class*='date']", "time"]:
        el = soup.select_one(sel)
        if el:
            fecha = limpiar(el.get("datetime") or el.get_text())
            if fecha:
                break
    if not fecha:
        fecha = regex_fecha(texto_pg)

    jornada = jornada_texto(texto_pg)

    descripcion = ""
    for sel in ["#jobDescriptionText",
                "[class*='jobsearch-jobDescriptionText']",
                "[class*='jobDescription']"]:
        el = soup.select_one(sel)
        if el and len(limpiar(el.get_text())) > 50:
            descripcion = quitar_ruido(el.get_text(separator=" "))
            break

    requisitos = ""
    if descripcion:
        m = re.search(r"(?i)requisitos\s*:", descripcion)
        if m:
            requisitos  = limpiar(descripcion[m.end():])
            descripcion = limpiar(descripcion[:m.start()])

    postulado = False
    if any(x in texto_pg.lower() for x in
           ["applied", "ya postulaste", "ya aplicaste", "postulado"]):
        postulado = True

    return {
        "titulo": titulo, "empresa": empresa, "sueldo": sueldo,
        "fecha": fecha, "jornada": jornada,
        "descripcion": descripcion, "requisitos": requisitos,
        "postulado": postulado,
    }


# ──────────────────────────────────────────────────────
# CONSTRUIR OFERTA COMPLETA
# ──────────────────────────────────────────────────────
DETALLE_FN = {
    "Chiletrabajos": detalle_chiletrabajos,
    "Indeed":        detalle_indeed,
}

def construir_oferta(item: dict) -> Oferta:
    time.sleep(DELAY_FETCH)
    fn = DETALLE_FN.get(item["fuente"])
    d  = fn(item["link"]) if fn else {}

    def v(key: str, rapida: str, fallback: str) -> str:
        # Prioridad: detalle > dato rápido del listado > fallback
        val = limpiar(d.get(key) or "")
        if val:
            return val
        val = limpiar(rapida)
        return val if val else fallback

    return Oferta(
        titulo      = v("titulo",      item["titulo"],              "Sin título"),
        link        = item["link"],
        fuente      = item["fuente"],
        empresa     = v("empresa",     item.get("empresa_rapida",""), "No especificada"),
        sueldo      = v("sueldo",      "",                           "No especificado"),
        jornada     = v("jornada",     "",                           "No especificada"),
        fecha       = v("fecha",       item.get("fecha_rapida",""),  "No especificada"),
        descripcion = v("descripcion", "",                           "No disponible"),
        requisitos  = v("requisitos",  "",                           "No especificados"),
        postulado   = bool(d.get("postulado", False)),
    )


# ──────────────────────────────────────────────────────
# FORMATEAR MENSAJE TELEGRAM
# ──────────────────────────────────────────────────────
SEP = "─" * 30

def formatear_mensaje(o: Oferta) -> str:
    estado = "✅ YA POSTULASTE" if o.postulado else "🆕 NUEVA OFERTA"

    desc = truncar(o.descripcion, MAX_DESC)
    req  = truncar(o.requisitos,  MAX_REQ)

    partes = [
        estado, SEP,
        f"📌 {o.titulo}",
        f"🏢 Empresa:    {o.empresa}",
        f"📅 Publicado:  {o.fecha}",
        f"🕒 Jornada:    {o.jornada}",
        f"💰 Sueldo:     {o.sueldo}",
        SEP,
        "📋 DESCRIPCIÓN",
        desc,
    ]
    if o.requisitos and o.requisitos != "No especificados":
        partes += [SEP, "✔️  REQUISITOS", req]
    partes += [SEP, f"🌐 {o.fuente}", f"🔗 {o.link}"]

    return "\n".join(partes)


# ──────────────────────────────────────────────────────
# CICLO
# ──────────────────────────────────────────────────────
def ciclo(estado: Estado) -> int:
    items: list = []
    items += listar_chiletrabajos()
    items += listar_indeed()

    # Filtrar URLs ya conocidas antes de hacer fetch de detalle
    candidatas: list = []
    vistos_ciclo: set = set()
    for it in items:
        url = it.get("link", "")
        if not url or url in vistos_ciclo or url in estado.urls:
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

        # Segunda barrera: hash de contenido (captura misma oferta en URL distinta)
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
        log.error("Debes exportar TELEGRAM_TOKEN y TELEGRAM_CHAT_ID antes de correr el bot.")
        log.error("  export TELEGRAM_TOKEN=tu_token")
        log.error("  export TELEGRAM_CHAT_ID=tu_chat_id")
        return

    estado = Estado()
    enviado = enviar(
        f"🚀 Bot activo — Osorno\n"
        f"🔄 Revisando cada {INTERVALO}s\n"
        f"📊 Historial: {len(estado.urls)} ofertas conocidas\n"
        f"Fuentes: Chiletrabajos · Indeed"
    )
    if not enviado:
        log.error("No se pudo enviar mensaje a Telegram. Verifica el token y el chat_id.")
        return

    while True:
        try:
            t0      = time.time()
            nuevos  = ciclo(estado)
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
