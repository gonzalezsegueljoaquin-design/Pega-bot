import json
import logging
import os
import re
import time
import hashlib
import html
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, Set
from urllib.parse import quote_plus, urljoin

import feedparser
import psycopg2
import psycopg2.extras
import requests
from bs4 import BeautifulSoup, Comment


INTERVALO = int(os.getenv("INTERVALO", "45"))
TIMEOUT = int(os.getenv("TIMEOUT", "20"))
CONNECT_TIMEOUT = int(os.getenv("CONNECT_TIMEOUT", "8"))
READ_TIMEOUT = int(os.getenv("READ_TIMEOUT", "20"))
CHILETRABAJOS_CONNECT_TIMEOUT = int(os.getenv("CHILETRABAJOS_CONNECT_TIMEOUT", "5"))
CHILETRABAJOS_READ_TIMEOUT = int(os.getenv("CHILETRABAJOS_READ_TIMEOUT", "12"))
MAX_DESC = 700
MAX_MSG = 4096
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
COLOR_LOGS = os.getenv("COLOR_LOGS", "1") == "1"
ENABLE_DETAIL_ENRICHMENT = os.getenv("ENABLE_DETAIL_ENRICHMENT", "1") == "1"
DETAIL_RETRIES = int(os.getenv("DETAIL_RETRIES", "1"))
DETAIL_DELAY_MS = int(os.getenv("DETAIL_DELAY_MS", "350"))
KEYWORDS_INCLUDE = [k.strip().lower() for k in os.getenv("KEYWORDS_INCLUDE", "").split(",") if k.strip()]
KEYWORDS_EXCLUDE = [k.strip().lower() for k in os.getenv("KEYWORDS_EXCLUDE", "").split(",") if k.strip()]
MIN_SCORE_IMMEDIATE = int(os.getenv("MIN_SCORE_IMMEDIATE", "1"))
DIGEST_EVERY_CYCLES = int(os.getenv("DIGEST_EVERY_CYCLES", "3"))
DAILY_REPORT_HOUR_UTC = int(os.getenv("DAILY_REPORT_HOUR_UTC", "23"))
HEARTBEAT_EVERY_CYCLES = int(os.getenv("HEARTBEAT_EVERY_CYCLES", "4"))

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DATABASE_URL = os.getenv("DATABASE_URL")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-CL,es;q=0.9",
}

OSORNO_SIGNALS = [
    "osorno",
    "los lagos",
    "x región",
    "xregión",
    "región de los lagos",
    "region de los lagos",
    "5290",
    "comuna de osorno",
    "rahue",
    "buena vista",
    "cancha rayada",
]

# Palabras/frases de basura comunes en descripciones
NOISE_PATTERNS = [
    r'\bPUBLICIDAD\b',
    r'\b(Home|Volver|Buscar Ofertas|Ofertas relacionadas|Más ofertas|Guardar|Postular)\b',
    r'\bDetalle oferta\b',
    r'\bID\s+\d+\b',
    r'\bBuscado\s+[\w\s]+\b(?=Fecha)',
    r'\bFecha\s+\d{4}-\d{2}-\d{2}',
    r'\bExpira\s+\d{4}-\d{2}-\d{2}',
    r'\bUbicación\s+[\w\s]+\b(?=Categoría)',
    r'\bCategoría\s+[\w\s]+\b(?=Duración)',
    r'\bDuración\s+[\w\s]+\b(?=Tipo)',
    r'\bTipo\s+[\w\s-]+\b(?=Postular)',
    r'\ben\s+\d+\s+días?\)',
    r'Compartir en.*?(?:\n|$)',
]


class ColorFormatter(logging.Formatter):
    COLORS = {
        logging.DEBUG: "\033[36m",
        logging.INFO: "\033[32m",
        logging.WARNING: "\033[33m",
        logging.ERROR: "\033[31m",
        logging.CRITICAL: "\033[35m",
    }
    RESET = "\033[0m"
    LEVEL_LABEL = {
        logging.INFO: "OK",
        logging.WARNING: "WARN",
        logging.ERROR: "ERR",
        logging.CRITICAL: "CRIT",
        logging.DEBUG: "DBG",
    }

    def format(self, record: logging.LogRecord) -> str:
        base = super().format(record)
        tag = self.LEVEL_LABEL.get(record.levelno, record.levelname)
        if COLOR_LOGS:
            color = self.COLORS.get(record.levelno, "")
            return f"{color}[{tag}] {base}{self.RESET}"
        return f"[{tag}] {base}"


_handler = logging.StreamHandler()
_handler.setFormatter(ColorFormatter("%(asctime)s [%(levelname)s] %(message)s"))
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO), handlers=[_handler], force=True)
log = logging.getLogger("pega-bot")

SESSION = requests.Session()
SESSION.headers.update(HEADERS)


@dataclass
class Oferta:
    source: str
    title: str
    link: str
    company: str = "No especificada"
    date_text: str = "No especificada"
    salary: str = "No especificado"
    jornada: str = "No especificada"
    description: str = "No disponible"
    postulado_detectado: Optional[bool] = None
    location_verified: bool = False


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def limpiar(txt: str) -> str:
    return re.sub(r"\s+", " ", str(txt or "")).strip()


def limpiar_descripcion(txt: str) -> str:
    """Limpia basura de descripciones (navegación, ads, metadata)"""
    limpio = limpiar(txt)
    
    # Remover patrones de basura
    for pattern in NOISE_PATTERNS:
        limpio = re.sub(pattern, '', limpio, flags=re.IGNORECASE)
    
    # Remover líneas muy cortas (navegación)
    lines = [l.strip() for l in limpio.split('\n') if len(l.strip()) > 20]
    limpio = ' '.join(lines)
    
    # Limpieza final
    limpio = re.sub(r'\s+', ' ', limpio).strip()
    
    # Limitar longitud
    if len(limpio) > MAX_DESC:
        limpio = limpio[:MAX_DESC].rsplit(' ', 1)[0] + '...'
    
    return limpio if limpio else "No disponible"


def short_hash(*parts: str) -> str:
    base = limpiar("|".join(parts)).lower()
    return hashlib.sha1(base.encode("utf-8")).hexdigest()[:10].upper()


def resumen_texto(texto: str) -> str:
    limpio = limpiar_descripcion(texto)
    if not limpio or limpio == "No disponible":
        return "No disponible"
    
    # Tomar primeras 2-3 oraciones significativas
    oraciones = re.split(r'(?<=[.!?])\s+', limpio)
    utiles: List[str] = []
    
    for oracion in oraciones:
        o = limpiar(oracion)
        if len(o) < 30:  # Muy corta, probablemente basura
            continue
        utiles.append(o)
        if len(utiles) >= 3:
            break
    
    if not utiles:
        return limpio[:350]
    
    resultado = ' '.join(utiles)
    if len(resultado) > 450:
        resultado = resultado[:450].rsplit(' ', 1)[0] + '...'
    
    return resultado


def contiene_senal_osorno(txt: str) -> bool:
    low = txt.lower()
    return any(sig in low for sig in OSORNO_SIGNALS)


def score_oferta(o: Oferta) -> int:
    txt = f"{o.title} {o.company} {o.description}".lower()
    score = 0
    if contiene_senal_osorno(txt) or o.location_verified:
        score += 1
    if o.salary and o.salary != "No especificado":
        score += 1
    if o.jornada and o.jornada != "No especificada":
        score += 1
    if KEYWORDS_INCLUDE and any(k in txt for k in KEYWORDS_INCLUDE):
        score += 2
    if KEYWORDS_EXCLUDE and any(k in txt for k in KEYWORDS_EXCLUDE):
        score -= 3
    return score


def pasa_filtros(o: Oferta) -> bool:
    txt = f"{o.title} {o.company} {o.description} {o.link}".lower()
    if not o.location_verified and not contiene_senal_osorno(txt):
        return False
    if KEYWORDS_INCLUDE and not any(k in txt for k in KEYWORDS_INCLUDE):
        return False
    if KEYWORDS_EXCLUDE and any(k in txt for k in KEYWORDS_EXCLUDE):
        return False
    return True


def get_db():
    if not DATABASE_URL:
        raise RuntimeError("Falta DATABASE_URL")
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def init_db() -> None:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    id BIGSERIAL PRIMARY KEY,
                    job_code TEXT UNIQUE NOT NULL,
                    source TEXT NOT NULL,
                    title TEXT NOT NULL,
                    company TEXT,
                    link TEXT UNIQUE NOT NULL,
                    date_text TEXT,
                    salary TEXT,
                    jornada TEXT,
                    description TEXT,
                    fingerprint TEXT NOT NULL,
                    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    applied_status TEXT NOT NULL DEFAULT 'unknown',
                    applied_updated_at TIMESTAMPTZ
                );
                CREATE UNIQUE INDEX IF NOT EXISTS jobs_fingerprint_idx ON jobs(fingerprint);
                CREATE TABLE IF NOT EXISTS notifications (
                    id BIGSERIAL PRIMARY KEY,
                    job_id BIGINT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
                    sent_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    telegram_message_id BIGINT
                );
                CREATE TABLE IF NOT EXISTS source_health (
                    source TEXT PRIMARY KEY,
                    consecutive_errors INT NOT NULL DEFAULT 0,
                    last_error TEXT,
                    last_success_at TIMESTAMPTZ
                );
                CREATE TABLE IF NOT EXISTS bot_state (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                """
            )
        conn.commit()


_tg_retry_after_until: float = 0.0


def telegram_api(method: str, payload: Dict, _retries: int = 5) -> Dict:
    global _tg_retry_after_until
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}"

    for attempt in range(1, _retries + 1):
        remaining = _tg_retry_after_until - time.time()
        if remaining > 0:
            log.warning("Telegram rate-limit activo — esperando %.0fs antes de %s", remaining, method)
            time.sleep(remaining)

        r = SESSION.post(url, data=payload, timeout=TIMEOUT)
        try:
            data = r.json()
        except Exception:
            data = {"ok": False, "description": r.text[:300]}

        if r.status_code == 429 or data.get("error_code") == 429:
            retry_after = int(data.get("parameters", {}).get("retry_after", 30))
            retry_after = min(retry_after, 600)
            _tg_retry_after_until = time.time() + retry_after + 2
            log.warning(
                "Telegram 429 en %s (intento %s/%s) — retry_after=%ss",
                method, attempt, _retries, retry_after,
            )
            if attempt < _retries:
                time.sleep(retry_after + 2)
                continue
            raise RuntimeError(
                f"Telegram {method} fallo tras {_retries} intentos por rate-limit (429)"
            )

        if not r.ok or not data.get("ok"):
            raise RuntimeError(f"Telegram {method} fallo: {data}")
        return data

    raise RuntimeError(f"Telegram {method} sin reintentos disponibles")


def enviar(msg: str) -> Optional[int]:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.error("Faltan TELEGRAM_TOKEN/TELEGRAM_CHAT_ID")
        return None
    try:
        data = telegram_api("sendMessage", {"chat_id": TELEGRAM_CHAT_ID, "text": msg[:MAX_MSG]})
        return data.get("result", {}).get("message_id")
    except RuntimeError as exc:
        log.error("enviar() fallo permanentemente — mensaje descartado: %s", exc)
        return None


def format_estado(status: str) -> str:
    mapping = {"applied": "✅ Ya postule", "not_applied": "❌ No postule", "unknown": "❓ Desconocido"}
    return mapping.get(status, "❓ Desconocido")


def formatear_oferta(o: Oferta, job_code: str, applied_status: str) -> str:
    footer = f"Comandos: /postule {job_code} | /nopostule {job_code} | /estado {job_code}"
    header_parts = [
        f"🆕 NUEVA OFERTA [{o.source}]",
        "────────────────────────────",
        f"🆔 Codigo: {job_code}",
        f"📌 {o.title}",
        f"🏢 Empresa: {o.company}",
        f"📅 Publicado: {o.date_text}",
        f"🕒 Jornada: {o.jornada}",
        f"💰 Sueldo: {o.salary}",
        f"📮 Estado postulacion: {format_estado(applied_status)}",
    ]
    core = "\n".join(header_parts) + "\n────────────────────────────\n"
    tail = f"\n🔗 {o.link}\n────────────────────────────\n{footer}"
    max_desc_len = max(80, MAX_MSG - len(core) - len(tail) - 10)
    desc = resumen_texto(o.description)
    if len(desc) > max_desc_len:
        desc = desc[: max_desc_len - 3].rstrip() + "..."
    return core + desc + tail


def formatear_digest(ofertas: List[Tuple[Oferta, str]]) -> str:
    lines = ["📦 DIGEST OFERTAS OSORNO", "────────────────────────────"]
    for o, code in ofertas[:15]:
        lines.append(f"- [{o.source}] {o.title} ({code})")
    if len(ofertas) > 15:
        lines.append(f"... y {len(ofertas) - 15} mas")
    lines.append("Usa /estado CODIGO para revisar una oferta.")
    return "\n".join(lines)


def formatear_heartbeat(
    cycle_num: int,
    total_found: int,
    new_count: int,
    existing_count: int,
    per_source: Dict[str, int],
) -> str:
    fuentes_line = " | ".join([f"{k}:{v}" for k, v in per_source.items()]) if per_source else "sin datos"
    return (
        "🫀 BOT EN LINEA - OSORNO\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"🔁 Ciclo: {cycle_num}\n"
        f"📥 Encontradas: {total_found}\n"
        f"🆕 Nuevas: {new_count}\n"
        f"📚 Ya registradas: {existing_count}\n"
        f"🌐 Fuentes: {fuentes_line}\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "Comando rapido: /stats"
    )


def set_source_ok(source: str) -> None:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO source_health(source, consecutive_errors, last_success_at)
            VALUES (%s, 0, NOW())
            ON CONFLICT (source)
            DO UPDATE SET consecutive_errors=0, last_error=NULL, last_success_at=NOW();
            """,
            (source,),
        )
        conn.commit()


def set_source_error(source: str, err: str) -> int:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO source_health(source, consecutive_errors, last_error)
            VALUES (%s, 1, %s)
            ON CONFLICT (source)
            DO UPDATE SET consecutive_errors=source_health.consecutive_errors + 1,
                          last_error=EXCLUDED.last_error
            RETURNING consecutive_errors;
            """,
            (source, err[:250]),
        )
        count = cur.fetchone()[0]
        conn.commit()
    return count


def should_cooldown(source: str) -> bool:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("SELECT consecutive_errors FROM source_health WHERE source=%s", (source,))
        row = cur.fetchone()
        if not row:
            return False
        return row[0] >= 5


def get_soup(url: str, retries: int = 2) -> Optional[BeautifulSoup]:
    is_chiletrabajos = "chiletrabajos.cl" in url.lower()
    timeout_cfg = (
        (CHILETRABAJOS_CONNECT_TIMEOUT, CHILETRABAJOS_READ_TIMEOUT)
        if is_chiletrabajos
        else (CONNECT_TIMEOUT, READ_TIMEOUT)
    )
    for i in range(1, retries + 1):
        try:
            r = SESSION.get(url, timeout=timeout_cfg)
            r.raise_for_status()
            return BeautifulSoup(r.text, "html.parser")
        except requests.RequestException as e:
            log.warning("GET [%s/%s] %s: %s", i, retries, url, e)
            if isinstance(e, (requests.ConnectTimeout, requests.ReadTimeout)):
                break
            time.sleep(0.8 * i)
    return None


def extraer_texto_limpio(element) -> str:
    """Extrae texto de un elemento removiendo scripts, styles y comentarios"""
    if not element:
        return ""
    
    # Clonar para no modificar el original
    elem_copy = element
    
    # Remover scripts, styles, y comentarios
    for script in elem_copy.find_all(['script', 'style', 'nav', 'header', 'footer']):
        script.decompose()
    
    for comment in elem_copy.find_all(string=lambda text: isinstance(text, Comment)):
        comment.extract()
    
    texto = elem_copy.get_text(separator=' ', strip=True)
    return limpiar(texto)


def extraer_detalle_chiletrabajos(link: str) -> Dict[str, str]:
    """Extrae detalles de Chiletrabajos con limpieza mejorada"""
    soup = get_soup(link, retries=2)
    if not soup:
        return {}

    # Extraer valores de tabla primero
    def get_table_value(key: str) -> str:
        for td in soup.select("table td"):
            txt = limpiar(td.get_text(" ")).lower()
            if key in txt:
                sib = td.find_next_sibling("td")
                if sib:
                    return limpiar(sib.get_text(" "))
        return ""

    empresa = get_table_value("buscado") or get_table_value("empresa") or ""
    fecha = get_table_value("fecha") or ""
    jornada = get_table_value("tipo") or ""
    sueldo = get_table_value("salario") or get_table_value("sueldo") or ""
    
    if sueldo and not sueldo.startswith("$") and sueldo.lower() not in ("no especificado", "a convenir"):
        sueldo = f"${sueldo}"

    # Descripción - estrategia mejorada
    descripcion = ""
    
    # 1. Buscar sección de descripción específica
    for heading in soup.find_all(['h2', 'h3', 'h4']):
        heading_text = limpiar(heading.get_text()).lower()
        if 'descripci' in heading_text or 'detalle' in heading_text or 'funciones' in heading_text:
            # Extraer contenido después del heading
            content_parts = []
            for sibling in heading.next_siblings:
                if sibling.name in ['h2', 'h3', 'h4']:
                    break
                if sibling.name in ['p', 'div', 'ul', 'ol']:
                    txt = extraer_texto_limpio(sibling)
                    if len(txt) > 20:
                        content_parts.append(txt)
            
            if content_parts:
                descripcion = ' '.join(content_parts)
                break
    
    # 2. Si no encontró, buscar por selectores específicos
    if not descripcion:
        for selector in ['[class*="description"]', '[class*="detalle"]', 'article', 'main']:
            elem = soup.select_one(selector)
            if elem:
                descripcion = extraer_texto_limpio(elem)
                if len(descripcion) > 50:
                    break
    
    # 3. Último recurso: texto general pero limpiado
    if not descripcion:
        # Remover navegación y basura antes de extraer
        for basura in soup.select('nav, header, footer, [class*="menu"], [class*="navigation"]'):
            basura.decompose()
        descripcion = soup.get_text(separator=' ', strip=True)
    
    # Limpiar descripción
    descripcion = limpiar_descripcion(descripcion)
    
    # Detectar postulación
    texto_completo = soup.get_text().lower()
    postulado = bool(re.search(
        r'\b(postulado|ya postulaste|postulaci[oó]n enviada|ya aplicaste)\b',
        texto_completo,
        re.I
    ))

    return {
        "empresa": empresa or "No especificada",
        "fecha": fecha or "No especificada",
        "jornada": jornada or "No especificada",
        "sueldo": sueldo or "No especificado",
        "descripcion": descripcion or "No disponible",
        "postulado": "1" if postulado else "0",
    }


def enriquecer_oferta(o: Oferta) -> Oferta:
    """Enriquece una oferta con detalles adicionales"""
    if not ENABLE_DETAIL_ENRICHMENT:
        return o
    
    if o.source == "Chiletrabajos":
        return o
    
    try:
        soup = get_soup(o.link, retries=1)
        if not soup:
            return o
        
        if DETAIL_DELAY_MS > 0:
            time.sleep(DETAIL_DELAY_MS / 1000)
        
        # Título mejorado
        titulo = o.title
        for sel in ["h1", "[class*='job-title']", "[class*='vacancy-title']", "meta[property='og:title']"]:
            el = soup.select_one(sel)
            if el:
                t = limpiar(el.get_text()) if el.name != "meta" else limpiar(el.get("content", ""))
                if len(t) > 5:
                    titulo = t
                    break
        
        # Empresa
        empresa = o.company
        for sel in ["[class*='company-name']", "[class*='employer']", "[data-testid='company-name']"]:
            el = soup.select_one(sel)
            if el:
                e = limpiar(el.get_text())
                if e and e != "No especificada":
                    empresa = e
                    break
        
        # Descripción
        descripcion = ""
        for sel in ["#jobDescriptionText", "[class*='job-description']", "[class*='description']", "article"]:
            el = soup.select_one(sel)
            if el:
                d = extraer_texto_limpio(el)
                if len(d) > 50:
                    descripcion = d
                    break
        
        if not descripcion:
            descripcion = extraer_texto_limpio(soup.find("body"))
        
        descripcion = limpiar_descripcion(descripcion)
        
        # Sueldo
        sueldo = o.salary
        texto_soup = soup.get_text()
        m_sueldo = re.search(r'\$\s*[\d\.\,]+(?:\s*[-–a]\s*\$?\s*[\d\.\,]+)?', texto_soup)
        if m_sueldo:
            sueldo = limpiar(m_sueldo.group(0))
        
        # Fecha
        fecha = o.date_text
        m_fecha = re.search(
            r'(hace\s+\d+\s+(?:minuto|hora|día|semana)s?'
            r'|\d{1,2}\s+de\s+\w+(?:\s+de\s+)?\d{4}'
            r'|\d{4}-\d{2}-\d{2})',
            texto_soup,
            re.I
        )
        if m_fecha:
            fecha = limpiar(m_fecha.group(0))
        
        # Jornada
        jornada = o.jornada
        t = texto_soup.lower()
        if re.search(r'part.?time|jornada parcial|medio tiempo', t):
            jornada = "Part Time"
        elif re.search(r'full.?time|jornada completa|tiempo completo', t):
            jornada = "Full Time"
        
        return Oferta(
            source=o.source,
            title=titulo,
            link=o.link,
            company=empresa,
            date_text=fecha,
            salary=sueldo,
            jornada=jornada,
            description=descripcion,
            postulado_detectado=o.postulado_detectado,
            location_verified=o.location_verified,
        )
    except Exception as e:
        log.warning("Error enriqueciendo %s: %s", o.source, e)
        return o


# =============================================================================
# PARSERS POR FUENTE
# =============================================================================

def parse_chiletrabajos() -> List[Oferta]:
    """Chiletrabajos - Optimizado"""
    source = "Chiletrabajos"
    if should_cooldown(source):
        log.info("%s en cooldown", source)
        return []
    
    out: List[Oferta] = []
    base = "https://www.chiletrabajos.cl"
    
    # Aumentar páginas
    pages = [
        f"{base}/ciudad/osorno.html",
        f"{base}/ciudad/osorno.html/30",
        f"{base}/ciudad/osorno.html/60",
        f"{base}/ciudad/osorno.html/90",
    ]
    
    try:
        for p in pages:
            log.debug("Chiletrabajos: scraping %s", p)
            soup = get_soup(p, retries=2)
            if not soup:
                continue
            
            # Buscar H2 con enlaces a trabajos
            for h2 in soup.find_all("h2"):
                a = h2.find("a", href=True)
                if not a:
                    continue
                
                href = a.get("href", "")
                if "/trabajo/" not in href:
                    continue
                
                link = href if href.startswith("http") else f"{base}{href}"
                title = limpiar(a.get_text())
                
                if not title or len(title) < 5:
                    continue
                
                # Info del listado
                h3 = h2.find_next_sibling("h3")
                empresa_listado = ""
                if h3:
                    empresa_listado = limpiar(h3.get_text().split(",")[0])
                
                # Extraer detalles
                d = extraer_detalle_chiletrabajos(link)
                
                empresa = d.get("empresa", empresa_listado) or empresa_listado or "No especificada"
                
                # Evitar RUTs como empresa
                if re.fullmatch(r'\d{1,2}\.\d{3}\.\d{3}-[\dkK]', empresa):
                    empresa = empresa_listado or "No especificada"
                
                out.append(
                    Oferta(
                        source=source,
                        title=title,
                        link=link,
                        company=empresa,
                        date_text=d.get("fecha", "No especificada"),
                        salary=d.get("sueldo", "No especificado"),
                        jornada=d.get("jornada", "No especificada"),
                        description=d.get("descripcion", "No disponible"),
                        postulado_detectado=d.get("postulado") == "1",
                        location_verified=True,
                    )
                )
                
                # Delay entre requests
                time.sleep(0.3)
        
        log.info("Chiletrabajos: %s ofertas", len(out))
        set_source_ok(source)
        return dedup(out)
    
    except Exception as e:
        log.exception("Chiletrabajos error: %s", e)
        c = set_source_error(source, str(e))
        if c in (1, 3, 5):
            enviar(f"⚠️ {source} error: {str(e)[:180]}")
        return []


def parse_bne() -> List[Oferta]:
    """BNE - Optimizado con variación de URLs"""
    source = "BNE"
    if should_cooldown(source):
        return []
    
    out: List[Oferta] = []
    base = "https://www.bne.cl"
    
    # Probar múltiples variantes
    urls = [
        # Variante 1: ubicacion
        f"{base}/ofertas?ubicacion=Osorno",
        f"{base}/ofertas?ubicacion=Osorno&pagina=2",
        # Variante 2: comuna  
        f"{base}/ofertas?comuna=Osorno",
        # Variante 3: región
        f"{base}/ofertas?region=Los Lagos",
    ]
    
    try:
        seen: Set[str] = set()
        
        for u in urls:
            log.debug("BNE: %s", u)
            soup = get_soup(u, retries=2)
            if not soup:
                continue
            
            # Estrategia 1: Links con keywords
            for a in soup.select("a[href]"):
                href = a.get("href", "")
                text = limpiar(a.get_text())
                
                if len(text) < 8:
                    continue
                
                href_low = href.lower()
                
                # Debe ser link de oferta
                if not any(tok in href_low for tok in ["/oferta", "/empleo", "/trabajo", "/vacante"]):
                    continue
                
                # No navegación
                if any(tok in href_low for tok in ["filtro", "buscar", "categoria", "page="]):
                    continue
                
                link = href if href.startswith("http") else urljoin(base, href)
                
                if link in seen:
                    continue
                seen.add(link)
                
                out.append(Oferta(source=source, title=text, link=link, location_verified=True))
            
            time.sleep(0.2)
        
        log.info("BNE: %s ofertas", len(out))
        set_source_ok(source)
        return dedup(out)
    
    except Exception as e:
        log.exception("BNE error: %s", e)
        c = set_source_error(source, str(e))
        if c in (1, 3, 5):
            enviar(f"⚠️ {source} error: {str(e)[:180]}")
        return []


def parse_indeed_rss() -> List[Oferta]:
    """Indeed RSS Feed - Osorno específico"""
    source = "Indeed"
    if should_cooldown(source):
        return []
    
    out: List[Oferta] = []
    try:
        # Feed RSS con parámetros optimizados
        q = quote_plus("osorno")
        l_param = quote_plus("Osorno, Los Lagos")
        feed_url = f"https://cl.indeed.com/rss?q={q}&l={l_param}&sort=date&limit=50"
        
        log.debug("Indeed RSS: %s", feed_url)
        feed = feedparser.parse(feed_url)
        
        for e in feed.entries:
            title = limpiar(html.unescape(getattr(e, "title", "")))
            link = getattr(e, "link", "")
            summary = limpiar(BeautifulSoup(getattr(e, "summary", ""), "html.parser").get_text())
            
            if not title or not link:
                continue
            
            # Parsear título compuesto: "Titulo - Empresa"
            company = "No especificada"
            if " - " in title:
                parts = title.split(" - ", 1)
                if len(parts) == 2:
                    title, company = limpiar(parts[0]), limpiar(parts[1])
            
            out.append(
                Oferta(
                    source=source,
                    title=title,
                    company=company,
                    link=link,
                    description=summary,
                    location_verified=True,
                )
            )
        
        log.info("Indeed: %s ofertas", len(out))
        set_source_ok(source)
        return dedup(out)
    
    except Exception as e:
        log.exception("Indeed error: %s", e)
        c = set_source_error(source, str(e))
        if c in (1, 3, 5):
            enviar(f"⚠️ {source} error: {str(e)[:180]}")
        return []


def parse_computrabajo() -> List[Oferta]:
    """Computrabajo - Filtro estricto por patrón de URL"""
    source = "Computrabajo"
    if should_cooldown(source):
        return []
    
    BASE = "https://cl.computrabajo.com"
    
    # Patrón de listing: -HASH.html al final
    RE_LISTING = re.compile(r'-[0-9a-f]{6,}\.html?$', re.I)
    
    urls = [
        f"{BASE}/empleos-en-los-lagos-en-osorno",
        f"{BASE}/empleos-en-los-lagos-en-osorno?p=2",
        f"{BASE}/empleos-en-los-lagos-en-osorno?p=3",
        f"{BASE}/ofertas-de-trabajo/en-osorno",
    ]
    
    out: List[Oferta] = []
    try:
        seen: Set[str] = set()
        
        for u in urls:
            log.debug("Computrabajo: %s", u)
            soup = get_soup(u, retries=2)
            if not soup:
                continue
            
            for a in soup.select("a[href]"):
                href = a.get("href", "")
                if not href:
                    continue
                
                # DEBE tener el patrón de hash
                if not RE_LISTING.search(href.lower()):
                    continue
                
                # DEBE contener "oferta"
                if "oferta" not in href.lower():
                    continue
                
                text = limpiar(a.get_text())
                if len(text) < 8:
                    continue
                
                link = href if href.startswith("http") else f"{BASE}{href}"
                
                if link in seen:
                    continue
                seen.add(link)
                
                out.append(Oferta(source=source, title=text, link=link, location_verified=True))
            
            time.sleep(0.2)
        
        log.info("Computrabajo: %s ofertas", len(out))
        set_source_ok(source)
        return dedup(out)
    
    except Exception as e:
        log.exception("Computrabajo error: %s", e)
        c = set_source_error(source, str(e))
        if c in (1, 3, 5):
            enviar(f"⚠️ {source} error: {str(e)[:180]}")
        return []


def parse_yapo() -> List[Oferta]:
    """Yapo - Filtros estrictos para empleo"""
    source = "Yapo"
    if should_cooldown(source):
        return []
    
    BASE = "https://www.yapo.cl"
    
    # Categorías de empleo
    JOB_SLUGS = ("/empleos/", "/trabajo/", "/empleo/", "/oferta-laboral/")
    
    # Categorías a rechazar
    REJECT_SLUGS = (
        "/arriendo", "/venta", "/compra", "/autos", "/motos",
        "/servicios", "/inmueble", "/electronico", "/moda",
        "/deporte", "/mascota", "/hogar", "/mueble"
    )
    
    # Pattern de ID numérico
    RE_ID = re.compile(r'\d{5,}\.html?$')
    
    urls = [
        f"{BASE}/empleos?ca=12_s&l=0&q=osorno",
        f"{BASE}/empleos?ca=12_s&l=0&q=osorno&o=25",
        f"{BASE}/empleos?ca=12_s&l=0&q=osorno&o=50",
    ]
    
    out: List[Oferta] = []
    try:
        seen: Set[str] = set()
        
        for u in urls:
            log.debug("Yapo: %s", u)
            soup = get_soup(u, retries=2)
            if not soup:
                continue
            
            for a in soup.select("a[href]"):
                href = a.get("href", "")
                if not href:
                    continue
                
                href_low = href.lower()
                
                # Debe terminar con ID.html
                if not RE_ID.search(href_low):
                    continue
                
                # Debe ser categoría de empleo
                if not any(slug in href_low for slug in JOB_SLUGS):
                    continue
                
                # NO debe ser otra categoría
                if any(slug in href_low for slug in REJECT_SLUGS):
                    continue
                
                text = limpiar(a.get_text())
                if len(text) < 8:
                    continue
                
                # Filtrar navegación
                if re.match(r'^(ver|filtrar|buscar|ordenar|página)', text, re.I):
                    continue
                
                link = href if href.startswith("http") else urljoin(BASE, href)
                
                if link in seen:
                    continue
                seen.add(link)
                
                out.append(Oferta(source=source, title=text, link=link, location_verified=True))
            
            time.sleep(0.2)
        
        log.info("Yapo: %s ofertas", len(out))
        set_source_ok(source)
        return dedup(out)
    
    except Exception as e:
        log.exception("Yapo error: %s", e)
        c = set_source_error(source, str(e))
        if c in (1, 3, 5):
            enviar(f"⚠️ {source} error: {str(e)[:180]}")
        return []


def parse_trabajando() -> List[Oferta]:
    """Trabajando.com - Osorno"""
    source = "Trabajando"
    if should_cooldown(source):
        return []
    
    BASE = "https://www.trabajando.cl"
    urls = [
        f"{BASE}/trabajo-osorno",
        f"{BASE}/trabajo-osorno/pagina-2",
        f"{BASE}/trabajo-osorno/pagina-3",
    ]
    
    out: List[Oferta] = []
    try:
        seen: Set[str] = set()
        
        for u in urls:
            log.debug("Trabajando: %s", u)
            soup = get_soup(u, retries=2)
            if not soup:
                continue
            
            for a in soup.select("a[href]"):
                href = a.get("href", "")
                text = limpiar(a.get_text())
                
                if len(text) < 8:
                    continue
                
                href_low = href.lower()
                
                # Debe contener empleo/oferta/trabajo
                if not any(tok in href_low for tok in ["/empleo/", "/oferta/", "/trabajo/", "/vacante/"]):
                    continue
                
                # No navegación
                if any(tok in href_low for tok in ["filtro", "categoria", "buscar", "pagina"]):
                    continue
                
                link = href if href.startswith("http") else urljoin(BASE, href)
                
                if link in seen:
                    continue
                seen.add(link)
                
                out.append(Oferta(source=source, title=text, link=link, location_verified=True))
            
            time.sleep(0.2)
        
        log.info("Trabajando: %s ofertas", len(out))
        set_source_ok(source)
        return dedup(out)
    
    except Exception as e:
        log.exception("Trabajando error: %s", e)
        c = set_source_error(source, str(e))
        if c in (1, 3, 5):
            enviar(f"⚠️ {source} error: {str(e)[:180]}")
        return []


def parse_acciontrabajo() -> List[Oferta]:
    """Acciontrabajo.com - Osorno"""
    source = "Acciontrabajo"
    if should_cooldown(source):
        return []
    
    BASE = "https://cl.acciontrabajo.com"
    urls = [
        f"{BASE}/trabajo/osorno",
        f"{BASE}/trabajo/osorno/pagina-2",
        f"{BASE}/trabajo/osorno/pagina-3",
    ]
    
    out: List[Oferta] = []
    try:
        seen: Set[str] = set()
        
        for u in urls:
            log.debug("Acciontrabajo: %s", u)
            soup = get_soup(u, retries=2)
            if not soup:
                continue
            
            for a in soup.select("a[href]"):
                href = a.get("href", "")
                text = limpiar(a.get_text())
                
                if len(text) < 8:
                    continue
                
                href_low = href.lower()
                
                # Debe contener trabajo/empleo/oferta
                if not any(tok in href_low for tok in ["/trabajo/", "/empleo/", "/oferta/"]):
                    continue
                
                # No navegación
                if any(tok in href_low for tok in ["/buscar", "/filtro", "/region", "page"]):
                    continue
                
                link = href if href.startswith("http") else urljoin(BASE, href)
                
                if link in seen:
                    continue
                seen.add(link)
                
                out.append(Oferta(source=source, title=text, link=link, location_verified=True))
            
            time.sleep(0.2)
        
        log.info("Acciontrabajo: %s ofertas", len(out))
        set_source_ok(source)
        return dedup(out)
    
    except Exception as e:
        log.exception("Acciontrabajo error: %s", e)
        c = set_source_error(source, str(e))
        if c in (1, 3, 5):
            enviar(f"⚠️ {source} error: {str(e)[:180]}")
        return []


# =============================================================================
# FUNCIONES AUXILIARES
# =============================================================================

def dedup(items: List[Oferta]) -> List[Oferta]:
    """Elimina duplicados por link"""
    seen: Set[str] = set()
    out: List[Oferta] = []
    for o in items:
        k = o.link.strip().lower()
        if not k or k in seen:
            continue
        seen.add(k)
        out.append(o)
    return out


def upsert_job(o: Oferta) -> Tuple[int, str, str]:
    fingerprint = short_hash(o.link)
    job_code = f"{o.source[:2].upper()}-{short_hash(o.link)}"
    
    with get_db() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        try:
            cur.execute(
                """
                INSERT INTO jobs(job_code, source, title, company, link, date_text, salary, jornada,
                                 description, fingerprint, last_seen_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                ON CONFLICT (link) DO UPDATE SET last_seen_at=NOW()
                RETURNING id, applied_status, (xmax = 0) AS inserted;
                """,
                (
                    job_code, o.source, o.title, o.company, o.link,
                    o.date_text, o.salary, o.jornada, o.description, fingerprint,
                ),
            )
            row = cur.fetchone()
            if not row:
                conn.rollback()
                cur.execute("SELECT id, applied_status FROM jobs WHERE link=%s", (o.link,))
                row = cur.fetchone()
                return int(row["id"]), str(row["applied_status"]), "exists"
            
            conn.commit()
            return int(row["id"]), str(row["applied_status"]), "inserted" if row["inserted"] else "updated"
        
        except psycopg2.errors.UniqueViolation:
            conn.rollback()
            cur.execute("SELECT id, applied_status FROM jobs WHERE fingerprint=%s", (fingerprint,))
            row = cur.fetchone()
            if not row:
                raise
            return int(row["id"]), str(row["applied_status"]), "exists"


def register_notification(job_id: int, message_id: Optional[int]) -> None:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO notifications(job_id, telegram_message_id) VALUES (%s,%s)",
            (job_id, message_id),
        )
        conn.commit()


def set_offset(v: int) -> None:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO bot_state(key, value) VALUES ('telegram_offset', %s)
            ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value
            """,
            (str(v),),
        )
        conn.commit()


def get_offset() -> int:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("SELECT value FROM bot_state WHERE key='telegram_offset'")
        row = cur.fetchone()
        return int(row[0]) if row else 0


def get_state_int(key: str, default: int = 0) -> int:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("SELECT value FROM bot_state WHERE key=%s", (key,))
        row = cur.fetchone()
        return int(row[0]) if row else default


def set_state_str(key: str, value: str) -> None:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO bot_state(key, value) VALUES (%s, %s)
            ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value
            """,
            (key, value),
        )
        conn.commit()


def update_applied(job_code: str, status: str) -> bool:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE jobs SET applied_status=%s, applied_updated_at=NOW() WHERE job_code=%s",
            (status, job_code.upper()),
        )
        ok = cur.rowcount > 0
        conn.commit()
    return ok


def get_job_status(job_code: str) -> Optional[Tuple[str, str]]:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT title, applied_status FROM jobs WHERE job_code=%s",
            (job_code.upper(),),
        )
        row = cur.fetchone()
        if not row:
            return None
        return row[0], row[1]


def job_counts() -> Tuple[int, int]:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM jobs")
        total = int(cur.fetchone()[0])
        cur.execute("SELECT COUNT(*) FROM jobs WHERE first_seen_at >= NOW() - INTERVAL '24 hours'")
        day = int(cur.fetchone()[0])
    return total, day


def parse_command(text: str) -> Tuple[str, str]:
    t = limpiar(text)
    if re.match(r"^/stats$", t, re.I):
        return "stats", ""
    if re.match(r"^/ultimas$", t, re.I):
        return "ultimas", ""
    m = re.match(r"^/(postule|nopostule|estado)\s+([A-Za-z]{2}-[A-F0-9]{10})$", t, re.I)
    if not m:
        return "", ""
    return m.group(1).lower(), m.group(2).upper()


def ultimas_ofertas(limit: int = 8) -> List[Tuple[str, str, str, str]]:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT source, title, job_code, link
            FROM jobs
            ORDER BY last_seen_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
    return [(str(r[0]), str(r[1]), str(r[2]), str(r[3])) for r in rows]


def formatear_ultimas(rows: List[Tuple[str, str, str, str]]) -> str:
    if not rows:
        return "📭 Aun no hay ofertas guardadas."
    lines = ["📌 ULTIMAS OFERTAS DETECTADAS", "━━━━━━━━━━━━━━━━━━━━"]
    for source, title, code, link in rows:
        lines.append(f"• [{source}] {title}")
        lines.append(f"  🆔 {code}")
        lines.append(f"  🔗 {link}")
    lines.append("━━━━━━━━━━━━━━━━━━━━")
    lines.append("Tip: usa /estado CODIGO o /postule CODIGO")
    return "\n".join(lines)


def process_telegram_commands() -> None:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    
    offset = get_offset()
    try:
        data = telegram_api("getUpdates", {"timeout": 1, "offset": offset + 1})
    except Exception as e:
        log.warning("Error procesando comandos: %s", e)
        return
    
    for upd in data.get("result", []):
        uid = int(upd.get("update_id", 0))
        msg = upd.get("message", {})
        chat_id = str(msg.get("chat", {}).get("id", ""))
        text = msg.get("text", "")
        set_offset(uid)
        
        if chat_id != str(TELEGRAM_CHAT_ID):
            continue
        
        cmd, code = parse_command(text)
        if not cmd:
            continue
        
        if cmd == "postule":
            ok = update_applied(code, "applied")
            enviar(f"{'✅' if ok else '❌'} {code} -> Ya postule")
        elif cmd == "nopostule":
            ok = update_applied(code, "not_applied")
            enviar(f"{'✅' if ok else '❌'} {code} -> No postule")
        elif cmd == "estado":
            st = get_job_status(code)
            if not st:
                enviar(f"❌ No encuentro el codigo {code}")
            else:
                enviar(f"📌 {st[0]}\n🆔 {code}\n📮 {format_estado(st[1])}")
        elif cmd == "stats":
            total, day = job_counts()
            enviar(f"📊 Estadisticas\nTotal ofertas en DB: {total}\nNuevas ultimas 24h: {day}")
        elif cmd == "ultimas":
            enviar(formatear_ultimas(ultimas_ofertas(8)))


def run_cycle() -> Dict[str, object]:
    """Ejecuta un ciclo de scraping"""
    sources = [
        parse_chiletrabajos,
        parse_bne,
        parse_indeed_rss,
        parse_computrabajo,
        parse_yapo,
        parse_trabajando,
        parse_acciontrabajo,
    ]
    
    found: List[Oferta] = []
    per_source: Dict[str, int] = {}
    
    for fn in sources:
        try:
            items = fn()
            found.extend(items)
            source_name = fn.__name__.replace("parse_", "").replace("_rss", "").capitalize()
            per_source[source_name] = len(items)
        except Exception as e:
            log.exception("Fuente %s fallo: %s", fn.__name__, e)
            source_name = fn.__name__.replace("parse_", "").replace("_rss", "").capitalize()
            per_source[source_name] = 0
    
    new_count = 0
    existing_count = 0
    digest_bucket: List[Tuple[Oferta, str]] = []
    
    for o in dedup(found):
        o = enriquecer_oferta(o)
        
        if not pasa_filtros(o):
            log.debug("Filtrada: %s - %s", o.source, o.title)
            continue
        
        job_id, applied_status, op = upsert_job(o)
        
        if op != "inserted":
            existing_count += 1
            continue
        
        code = f"{o.source[:2].upper()}-{short_hash(o.link)}"
        
        if score_oferta(o) >= MIN_SCORE_IMMEDIATE:
            msg = formatear_oferta(o, code, applied_status)
            message_id = enviar(msg)
            register_notification(job_id, message_id)
        else:
            digest_bucket.append((o, code))
        
        new_count += 1
    
    cycle_num = get_state_int("cycle_counter", 0) + 1
    set_state_str("cycle_counter", str(cycle_num))
    
    if digest_bucket and cycle_num % max(1, DIGEST_EVERY_CYCLES) == 0:
        enviar(formatear_digest(digest_bucket))
    
    log.info(
        "Ciclo OK | fuentes=%s | encontradas=%s | nuevas=%s | ya_registradas=%s",
        per_source,
        len(found),
        new_count,
        existing_count,
    )
    
    return {
        "new_count": new_count,
        "found_count": len(found),
        "existing_count": existing_count,
        "per_source": per_source,
        "cycle_num": cycle_num,
    }


def enviar_reporte_diario_si_corresponde() -> None:
    now = now_utc()
    if now.hour != DAILY_REPORT_HOUR_UTC:
        return
    
    today_key = now.strftime("%Y-%m-%d")
    last_key = "daily_report_last_date"
    
    if get_state_int(last_key, 0) == int(today_key.replace("-", "")):
        return
    
    total, day = job_counts()
    enviar(
        "📈 REPORTE DIARIO\n"
        f"Fecha UTC: {today_key}\n"
        f"Total ofertas en DB: {total}\n"
        f"Nuevas ultimas 24h: {day}"
    )
    set_state_str(last_key, str(int(today_key.replace("-", ""))))


def main() -> None:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID or not DATABASE_URL:
        raise RuntimeError("Config incompleta")
    
    init_db()
    enviar(
        "🚀 BOT EMPLEOS OSORNO - OPTIMIZADO\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "🌐 Fuentes activas: 7\n"
        f"⏱️ Intervalo: {INTERVALO}s\n"
        f"🧠 Enriquecimiento: {'ON' if ENABLE_DETAIL_ENRICHMENT else 'OFF'}\n"
        "✨ Descripciones limpias\n"
        "🔧 Parsers optimizados\n"
        "━━━━━━━━━━━━━━━━━━━━"
    )
    
    while True:
        start = time.time()
        try:
            process_telegram_commands()
            cycle_stats = run_cycle()
            enviar_reporte_diario_si_corresponde()
            
            if cycle_stats["cycle_num"] % max(1, HEARTBEAT_EVERY_CYCLES) == 0:
                enviar(
                    formatear_heartbeat(
                        cycle_num=int(cycle_stats["cycle_num"]),
                        total_found=int(cycle_stats["found_count"]),
                        new_count=int(cycle_stats["new_count"]),
                        existing_count=int(cycle_stats["existing_count"]),
                        per_source=dict(cycle_stats["per_source"]),
                    )
                )
            
            elapsed = time.time() - start
            log.info("Ciclo completo en %.1fs", elapsed)
        
        except Exception as e:
            log.exception("Error en ciclo: %s", e)
            enviar(f"❌ Error: {str(e)[:200]}")
            elapsed = time.time() - start
        
        sleep_time = max(10, INTERVALO - int(elapsed))
        time.sleep(sleep_time)


if __name__ == "__main__":
    main()
