"""
Bot de empleos Osorno
- Polling agresivo (30s) para detectar ofertas nuevas rápido
- Deduplicación robusta: por URL exacta + por huella de contenido (título+empresa)
- Nunca reenvía lo que ya fue enviado, ni dentro del mismo ciclo
"""

import requests
from bs4 import BeautifulSoup
import time
import logging
import os
import re
import json
import hashlib
from dataclasses import dataclass, asdict
from typing import Optional

# ==============================
# CONFIG
# ==============================
INTERVALO      = 30      # segundos entre ciclos (polling agresivo)
MAX_REGISTROS  = 3000    # máximo de entradas en el archivo de estado
TIMEOUT        = 12
DELAY_REQUESTS = 1.5     # pausa entre fetches de detalle
MAX_DESC       = 600
MAX_REQUISITOS = 400

STATE_FILE = "estado_bot.json"   # reemplaza enviados.txt — guarda más info
LOG_FILE   = "bot_empleos.log"

TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-CL,es;q=0.9",
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


# ==============================
# MODELO DE DATOS
# ==============================
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


# ==============================
# ESTADO PERSISTENTE
# Guarda dos índices:
#   "urls"     : set de URLs ya enviadas
#   "huellas"  : set de hashes título+empresa (captura duplicados con URL distinta)
# ==============================
class Estado:
    def __init__(self):
        self.urls:    set = set()
        self.huellas: set = set()
        self._cargar()

    def _cargar(self):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            self.urls    = set(data.get("urls", []))
            self.huellas = set(data.get("huellas", []))
            log.info(f"Estado cargado: {len(self.urls)} URLs, {len(self.huellas)} huellas")
        except FileNotFoundError:
            log.info("Archivo de estado no encontrado — iniciando desde cero")
        except Exception as e:
            log.warning(f"Error cargando estado: {e} — iniciando desde cero")

    def _guardar(self):
        # Rotar si supera el límite
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
        """Hash reproducible de título+empresa, normalizado."""
        clave = re.sub(r"\s+", " ", (titulo + empresa).lower().strip())
        return hashlib.md5(clave.encode()).hexdigest()

    def ya_enviado(self, oferta: Oferta) -> bool:
        """True si la oferta ya fue enviada (por URL o por contenido)."""
        if oferta.link in self.urls:
            return True
        h = self._huella(oferta.titulo, oferta.empresa)
        if h in self.huellas:
            return True
        return False

    def registrar(self, oferta: Oferta):
        """Marca la oferta como enviada y persiste el estado."""
        self.urls.add(oferta.link)
        self.huellas.add(self._huella(oferta.titulo, oferta.empresa))
        self._guardar()


# ==============================
# UTILIDADES
# ==============================
def limpiar(texto: str) -> str:
    return re.sub(r"\s+", " ", texto or "").strip()

def truncar(texto: str, n: int) -> str:
    texto = limpiar(texto)
    return texto[:n] + "…" if len(texto) > n else texto

def abs_url(href: str, base: str) -> str:
    if not href:
        return ""
    href = href.strip()
    return href if href.startswith("http") else base.rstrip("/") + "/" + href.lstrip("/")

def get_soup(url: str, reintentos: int = 2) -> Optional[BeautifulSoup]:
    for i in range(1, reintentos + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            r.raise_for_status()
            return BeautifulSoup(r.text, "html.parser")
        except requests.RequestException as e:
            log.warning(f"GET {url[:80]} — intento {i}: {e}")
            time.sleep(DELAY_REQUESTS * i)
    return None

def texto_de(soup: BeautifulSoup, *selectores) -> str:
    for sel in selectores:
        try:
            el = soup.select_one(sel)
            if el and el.get_text(strip=True):
                return limpiar(el.get_text(separator=" "))
        except Exception:
            continue
    return ""

def contiene_postulado(soup: BeautifulSoup) -> bool:
    texto = soup.get_text().lower()
    return any(p in texto for p in [
        "ya postulaste", "ya postulado", "postulación enviada",
        "aplicación enviada", "ya aplicaste",
    ])

def buscar_fecha(texto: str) -> str:
    for p in [
        r"hace\s+\d+\s+(?:minuto|minutos|hora|horas|día|días|semana|semanas)",
        r"(?:publicado|publicada)\s+(?:el\s+)?[\d/\-]+",
        r"\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}",
    ]:
        m = re.search(p, texto, re.I)
        if m:
            return m.group(0)
    return ""

def buscar_sueldo(texto: str) -> str:
    m = re.search(r"\$[\d\.\,]+(?:\s*[-–]\s*\$[\d\.\,]+)?", texto)
    return m.group(0) if m else ""

def detectar_jornada(texto: str) -> str:
    t = texto.lower()
    if "part time" in t:        return "Part Time"
    if "full time" in t:        return "Full Time"
    if "jornada completa" in t: return "Jornada completa"
    if "jornada parcial" in t:  return "Jornada parcial"
    return ""


# ==============================
# TELEGRAM
# ==============================
def enviar(msg: str, reintentos: int = 3) -> bool:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.error("Faltan TELEGRAM_TOKEN o TELEGRAM_CHAT_ID")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    for i in range(1, reintentos + 1):
        try:
            r = requests.post(
                url,
                data={"chat_id": TELEGRAM_CHAT_ID, "text": msg},
                timeout=TIMEOUT,
            )
            if r.status_code == 200:
                return True
            log.warning(f"Telegram {r.status_code} intento {i}: {r.text[:150]}")
        except requests.RequestException as e:
            log.warning(f"Error Telegram intento {i}: {e}")
        time.sleep(2 * i)
    return False


# ==============================
# EXTRACCIÓN DE DETALLE POR FUENTE
# ==============================
def detalle_chiletrabajos(link: str) -> dict:
    soup = get_soup(link)
    if not soup:
        return {}
    texto = soup.get_text()
    return {
        "titulo":      texto_de(soup, "h1.job-title", "h1.titulo", "h1"),
        "empresa":     texto_de(soup, ".company-name", ".empresa", ".job-company", "span[itemprop='name']"),
        "sueldo":      texto_de(soup, ".salary", ".sueldo", "[class*='salary']") or buscar_sueldo(texto),
        "fecha":       texto_de(soup, ".published-date", ".fecha", "time", "[class*='date']") or buscar_fecha(texto),
        "jornada":     detectar_jornada(texto),
        "descripcion": texto_de(soup, ".job-description", ".description", "#jobDescription", "article .content", ".oferta-descripcion", "article"),
        "requisitos":  texto_de(soup, ".requirements", ".requisitos", "#requirements", "[class*='requisit']"),
        "postulado":   contiene_postulado(soup),
    }

def detalle_computrabajo(link: str) -> dict:
    soup = get_soup(link)
    if not soup:
        return {}
    texto = soup.get_text()
    return {
        "titulo":      texto_de(soup, "h1.title-offer", "h1[class*='title']", "h1"),
        "empresa":     texto_de(soup, ".company-name", "p.fs16", "a[class*='company']", "[class*='company']"),
        "sueldo":      texto_de(soup, "[class*='salary']", "[class*='sueldo']", "p.salary") or buscar_sueldo(texto),
        "fecha":       texto_de(soup, "p.fs13.fc_base", "[class*='date']", "time") or buscar_fecha(texto),
        "jornada":     detectar_jornada(texto),
        "descripcion": texto_de(soup, "#jobDescription", "div[class*='description']", ".text-offer", "article"),
        "requisitos":  texto_de(soup, "[class*='requirements']", "[class*='requisit']"),
        "postulado":   contiene_postulado(soup),
    }

def detalle_bne(link: str) -> dict:
    soup = get_soup(link)
    if not soup:
        return {}
    texto = soup.get_text()
    return {
        "titulo":      texto_de(soup, "h1.oferta-titulo", "h1", ".titulo-oferta"),
        "empresa":     texto_de(soup, ".empresa-nombre", ".nombre-empresa", "[class*='empresa']"),
        "sueldo":      texto_de(soup, "[class*='sueldo']", "[class*='salary']", "[class*='remuneracion']") or buscar_sueldo(texto),
        "fecha":       texto_de(soup, ".fecha-publicacion", "[class*='fecha']", "time") or buscar_fecha(texto),
        "jornada":     detectar_jornada(texto),
        "descripcion": texto_de(soup, ".descripcion-oferta", "[class*='descripcion']", "[class*='description']", "article", ".contenido"),
        "requisitos":  texto_de(soup, "[class*='requisit']", "[class*='requirement']"),
        "postulado":   contiene_postulado(soup),
    }

DETALLE_FN = {
    "Chiletrabajos": detalle_chiletrabajos,
    "Computrabajo":  detalle_computrabajo,
    "BNE":           detalle_bne,
}

def construir_oferta(item: dict) -> Oferta:
    time.sleep(DELAY_REQUESTS)
    fn = DETALLE_FN.get(item["fuente"])
    d  = fn(item["link"]) if fn else {}

    def v(key: str, default: str) -> str:
        val = (d.get(key) or "").strip()
        return val if val else default

    return Oferta(
        titulo      = v("titulo",      limpiar(item["titulo"])[:120]) or "Sin título",
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


# ==============================
# FORMATEAR MENSAJE
# ==============================
def formatear_mensaje(o: Oferta) -> str:
    estado = "✅ YA POSTULASTE" if o.postulado else "🆕 NUEVA OFERTA"
    sep    = "─" * 28
    return (
        f"{estado}\n"
        f"{sep}\n"
        f"📌 {o.titulo}\n"
        f"🏢 Empresa:   {o.empresa}\n"
        f"📅 Publicado: {o.fecha}\n"
        f"🕒 Jornada:   {o.jornada}\n"
        f"💰 Sueldo:    {o.sueldo}\n"
        f"{sep}\n"
        f"📋 DESCRIPCIÓN\n{truncar(o.descripcion, MAX_DESC)}\n"
        f"{sep}\n"
        f"✔️  REQUISITOS\n{truncar(o.requisitos, MAX_REQUISITOS)}\n"
        f"{sep}\n"
        f"🌐 Fuente: {o.fuente}\n"
        f"🔗 {o.link}"
    )


# ==============================
# SCRAPERS DE LISTADO
# ==============================
def buscar_chiletrabajos() -> list:
    trabajos = []
    soup = get_soup("https://www.chiletrabajos.cl/trabajo/?q=osorno")
    if not soup:
        log.warning("Chiletrabajos: sin conexión")
        return trabajos
    for item in soup.select("article"):
        a = item.find("a", href=True)
        if not a:
            continue
        titulo = limpiar(item.get_text(separator=" "))
        if len(titulo) < 15:
            continue
        trabajos.append({
            "titulo": titulo,
            "link":   abs_url(a["href"], "https://www.chiletrabajos.cl"),
            "fuente": "Chiletrabajos",
        })
    log.info(f"Chiletrabajos: {len(trabajos)} en listado")
    return trabajos

def buscar_computrabajo() -> list:
    trabajos = []
    soup = get_soup("https://www.computrabajo.cl/trabajo-de-osorno")
    if not soup:
        log.warning("Computrabajo: sin conexión")
        return trabajos
    for item in soup.select("article"):
        a = item.find("a", href=True)
        if not a:
            continue
        titulo = limpiar(item.get_text(separator=" "))
        if len(titulo) < 15:
            continue
        trabajos.append({
            "titulo": titulo,
            "link":   abs_url(a["href"], "https://www.computrabajo.cl"),
            "fuente": "Computrabajo",
        })
    log.info(f"Computrabajo: {len(trabajos)} en listado")
    return trabajos

def buscar_bne() -> list:
    trabajos = []
    soup = get_soup("https://www.bne.cl/ofertas?textoBusqueda=osorno")
    if not soup:
        log.warning("BNE: sin conexión")
        return trabajos
    for a in soup.select("a[href]"):
        if "/oferta/" not in a["href"]:
            continue
        titulo = limpiar(a.get_text(separator=" "))
        if len(titulo) < 15:
            continue
        trabajos.append({
            "titulo": titulo,
            "link":   abs_url(a["href"], "https://www.bne.cl"),
            "fuente": "BNE",
        })
    log.info(f"BNE: {len(trabajos)} en listado")
    return trabajos


# ==============================
# CICLO PRINCIPAL
# ==============================
def ciclo(estado: Estado) -> int:
    # 1. Recolectar listados
    items = []
    items += buscar_chiletrabajos()
    items += buscar_computrabajo()
    items += buscar_bne()

    # 2. Deduplicar URLs dentro del ciclo actual
    vistos:   set  = set()
    unicos:   list = []
    for it in items:
        url = it.get("link", "")
        if not url or url in vistos:
            continue
        # Saltar si la URL ya fue enviada (chequeo rápido antes de hacer fetch)
        if url in estado.urls:
            continue
        vistos.add(url)
        unicos.append(it)

    if not unicos:
        log.info("Sin ofertas nuevas en este ciclo")
        return 0

    log.info(f"Candidatas a procesar: {len(unicos)}")

    # 3. Para cada candidata: obtener detalle y verificar duplicado por contenido
    nuevos = 0
    for item in unicos:
        oferta = construir_oferta(item)

        if estado.ya_enviado(oferta):
            log.info(f"  [DUP] {oferta.titulo[:60]} — omitido")
            # Registrar la URL igual para no volver a intentarla
            estado.urls.add(oferta.link)
            estado._guardar()
            continue

        mensaje = formatear_mensaje(oferta)
        if enviar(mensaje):
            estado.registrar(oferta)
            nuevos += 1
            log.info(f"  [OK]  {oferta.titulo[:60]}")
        else:
            log.error(f"  [ERR] No se pudo enviar: {oferta.link}")

    return nuevos


# ==============================
# MAIN
# ==============================
def main() -> None:
    log.info("=" * 50)
    log.info("Bot de empleos Osorno — iniciando")

    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.error("TELEGRAM_TOKEN y TELEGRAM_CHAT_ID son requeridos.")
        return

    estado = Estado()
    enviar(f"🚀 Bot activo — polling cada {INTERVALO}s\n"
           f"📊 Historial: {len(estado.urls)} URLs conocidas")

    while True:
        try:
            t0      = time.time()
            nuevos  = ciclo(estado)
            elapsed = time.time() - t0
            log.info(f"Ciclo: {nuevos} nuevos enviados ({elapsed:.1f}s)")

            espera = max(0, INTERVALO - elapsed)
            time.sleep(espera)

        except KeyboardInterrupt:
            log.info("Bot detenido por el usuario.")
            break
        except Exception as e:
            log.exception(f"Error inesperado: {e}")
            time.sleep(30)


if __name__ == "__main__":
    main()
    
