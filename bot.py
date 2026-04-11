import json
import logging
import os
import re
import time
import hashlib
import html
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote_plus

import feedparser
import psycopg2
import psycopg2.extras
import requests
from bs4 import BeautifulSoup


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
# FIX: Default lowered to 1 so offers actually reach Telegram instead of
#      piling up silently in the digest bucket.
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

# ---------------------------------------------------------------------------
# Osorno geographic signals: region names, commune codes, ZIP codes, and
# common address fragments that appear even when "Osorno" is absent.
# Used by is_osorno_context() to detect implicit Osorno listings.
# ---------------------------------------------------------------------------
OSORNO_SIGNALS = [
    "osorno",
    "los lagos",              # Región de Los Lagos
    "x región",
    "xregión",
    "región de los lagos",
    "region de los lagos",
    "5290",                   # ZIP code prefix for Osorno
    "comuna de osorno",
    # Osorno neighbourhoods / landmarks that appear in job ads
    "rahue",
    "buena vista",            # common barrio name in Osorno
    "cancha rayada",
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
    # FIX: new flag — True when the parser already applied a geographic filter
    # (e.g. the search URL contained ?ubicacion=Osorno).  When True, pasa_filtros
    # will accept the offer even if none of the OSORNO_SIGNALS appear in the text.
    location_verified: bool = False


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def limpiar(txt: str) -> str:
    return re.sub(r"\s+", " ", str(txt or "")).strip()


def short_hash(*parts: str) -> str:
    base = limpiar("|".join(parts)).lower()
    return hashlib.sha1(base.encode("utf-8")).hexdigest()[:10].upper()


def resumen_texto(texto: str) -> str:
    limpio = limpiar(texto)
    if not limpio:
        return "No disponible"
    partes = re.split(r"(?<=[\.\!\?])\s+", limpio)
    utiles: List[str] = []
    for p in partes:
        t = limpiar(p)
        if len(t) < 35:
            continue
        utiles.append(t)
        if len(utiles) >= 3:
            break
    if not utiles:
        return limpio[:280]
    return " ".join(utiles)[:420]


def contiene_senal_osorno(txt: str) -> bool:
    """Return True if *txt* contains any geographic signal associated with Osorno."""
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
    """
    Accept an offer when EITHER:
      a) location_verified=True  (parser already filtered by Osorno city)
      b) At least one Osorno geographic signal appears in the combined text
    Plus optional keyword filters.
    """
    txt = f"{o.title} {o.company} {o.description} {o.link}".lower()

    # Geographic gate
    if not o.location_verified and not contiene_senal_osorno(txt):
        return False

    # Keyword filters (only applied when configured)
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


# Global rate-limit state: bot will not call Telegram before this epoch second.
# Shared across all telegram_api() calls so a 429 on one send automatically
# throttles every subsequent send in the same process.
_tg_retry_after_until: float = 0.0


def telegram_api(method: str, payload: Dict, _retries: int = 5) -> Dict:
    """
    Call a Telegram Bot API method with automatic 429 back-off.

    Strategy:
    - Before every attempt, check the global cooldown and sleep if needed.
    - On 429, read retry_after from the response, update the global cooldown,
      and retry (up to _retries times).
    - On any other non-OK response, raise immediately.
    - If all retries are exhausted due to 429, raise so the caller can decide
      whether to drop the message or log the failure.
    """
    global _tg_retry_after_until
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}"

    for attempt in range(1, _retries + 1):
        # Honour global cooldown before sending
        remaining = _tg_retry_after_until - time.time()
        if remaining > 0:
            log.warning("Telegram rate-limit activo — esperando %.0fs antes de %s", remaining, method)
            time.sleep(remaining)

        r = SESSION.post(url, data=payload, timeout=TIMEOUT)
        try:
            data = r.json()
        except Exception:
            data = {"ok": False, "description": r.text[:300]}

        # 429 — Too Many Requests
        if r.status_code == 429 or data.get("error_code") == 429:
            retry_after = int(data.get("parameters", {}).get("retry_after", 30))
            retry_after = min(retry_after, 600)  # cap at 10 min — avoid hanging forever
            _tg_retry_after_until = time.time() + retry_after + 2  # +2 s buffer
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
    """Send a Telegram message. Errors are logged but never propagate to the
    caller — a failed notification must never crash a scraping cycle."""
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


def extraer_tabla_valor(soup: BeautifulSoup, clave: str) -> str:
    for td in soup.select("table td"):
        txt = limpiar(td.get_text(" ")).lower()
        if clave in txt:
            sib = td.find_next_sibling("td")
            if sib:
                return limpiar(sib.get_text(" "))
    return ""


def detalle_chiletrabajos(link: str) -> Dict[str, str]:
    soup = get_soup(link, retries=2)
    if not soup:
        return {}

    texto = limpiar(soup.get_text(" "))
    empresa = (
        extraer_tabla_valor(soup, "empresa")
        or extraer_tabla_valor(soup, "buscado")
        or ""
    )
    fecha = extraer_tabla_valor(soup, "fecha")
    jornada = extraer_tabla_valor(soup, "tipo")
    sueldo = extraer_tabla_valor(soup, "salario")

    if sueldo and not sueldo.startswith("$"):
        sueldo = f"${sueldo}"

    descripcion = ""
    h_desc = None
    for tag in soup.find_all(["h2", "h3"]):
        if "descripci" in limpiar(tag.get_text(" ")).lower():
            h_desc = tag
            break
    if h_desc:
        bloques: List[str] = []
        for sib in h_desc.next_siblings:
            if getattr(sib, "name", None) in ("h2", "h3"):
                break
            if hasattr(sib, "get_text"):
                t = limpiar(sib.get_text(" "))
                if t:
                    bloques.append(t)
        descripcion = limpiar(" ".join(bloques))

    if not descripcion:
        bloque = soup.select_one("article, main, .oferta-content, #oferta")
        if bloque:
            descripcion = limpiar(bloque.get_text(" "))

    if not descripcion:
        descripcion = texto[:900]

    postulado = bool(
        re.search(r"\b(postulado|ya postulaste|postulaci[oó]n enviada|ya aplicaste)\b", texto, re.I)
    )

    return {
        "empresa": empresa,
        "fecha": fecha,
        "jornada": jornada,
        "sueldo": sueldo,
        "descripcion": descripcion,
        "postulado": "1" if postulado else "0",
    }


def detalle_generico(link: str) -> Dict[str, str]:
    soup = get_soup(link, retries=max(1, DETAIL_RETRIES))
    if not soup:
        return {}
    texto = limpiar(soup.get_text(" "))

    titulo = ""
    for sel in ["h1", "meta[property='og:title']"]:
        el = soup.select_one(sel)
        if not el:
            continue
        titulo = limpiar(el.get_text(" ")) if el.name != "meta" else limpiar(el.get("content", ""))
        if titulo:
            break

    descripcion = ""
    for sel in [
        "#jobDescriptionText",
        "article",
        "main",
        "[class*='description']",
        "meta[property='og:description']",
    ]:
        el = soup.select_one(sel)
        if not el:
            continue
        if el.name == "meta":
            descripcion = limpiar(el.get("content", ""))
        else:
            descripcion = limpiar(el.get_text(" "))
        if len(descripcion) >= 60:
            break
    if not descripcion:
        descripcion = texto[:900]

    empresa = ""
    for sel in [
        "[data-testid='company-name']",
        "[data-testid='inlineHeader-companyName']",
        "[class*='company']",
        "meta[property='og:site_name']",
    ]:
        el = soup.select_one(sel)
        if not el:
            continue
        empresa = limpiar(el.get_text(" ")) if el.name != "meta" else limpiar(el.get("content", ""))
        if empresa:
            break

    sueldo = ""
    m_sueldo = re.search(r"\$\s*[\d\.\,]+(?:\s*[-–]\s*\$?\s*[\d\.\,]+)?", texto)
    if m_sueldo:
        sueldo = limpiar(m_sueldo.group(0))

    fecha = ""
    m_fecha = re.search(
        r"(hace\s+\d+\s+(?:minuto|minutos|hora|horas|d[ií]a|d[ií]as|semana|semanas)"
        r"|\d{1,2}\s+de\s+\w+\s+de\s+\d{4}|\d{4}-\d{2}-\d{2})",
        texto,
        re.I,
    )
    if m_fecha:
        fecha = limpiar(m_fecha.group(0))

    jornada = ""
    t = texto.lower()
    if re.search(r"part.?time|jornada parcial|medio tiempo", t):
        jornada = "Part Time"
    elif re.search(r"full.?time|jornada completa|tiempo completo", t):
        jornada = "Full Time"

    postulado = bool(
        re.search(r"\b(applied|postulado|ya postulaste|postulaci[oó]n enviada|ya aplicaste)\b", texto, re.I)
    )
    return {
        "titulo": titulo,
        "empresa": empresa,
        "fecha": fecha,
        "jornada": jornada,
        "sueldo": sueldo,
        "descripcion": descripcion,
        "postulado": "1" if postulado else "0",
    }


def enriquecer_oferta(o: Oferta) -> Oferta:
    if not ENABLE_DETAIL_ENRICHMENT or o.source == "Chiletrabajos":
        return o
    d = detalle_generico(o.link)
    if DETAIL_DELAY_MS > 0:
        time.sleep(DETAIL_DELAY_MS / 1000)
    if not d:
        return o
    return Oferta(
        source=o.source,
        title=limpiar(d.get("titulo", "")) or o.title,
        link=o.link,
        company=limpiar(d.get("empresa", "")) or o.company,
        date_text=limpiar(d.get("fecha", "")) or o.date_text,
        salary=limpiar(d.get("sueldo", "")) or o.salary,
        jornada=limpiar(d.get("jornada", "")) or o.jornada,
        description=limpiar(d.get("descripcion", "")) or o.description,
        postulado_detectado=True if d.get("postulado") == "1" else o.postulado_detectado,
        # Preserve location trust from original offer
        location_verified=o.location_verified,
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


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------

def parse_chiletrabajos() -> List[Oferta]:
    """
    Chiletrabajos — scrapes the Osorno city listing pages.
    All results come from a city-filtered URL → location_verified=True.
    """
    source = "Chiletrabajos"
    if should_cooldown(source):
        return []
    out: List[Oferta] = []
    base = "https://www.chiletrabajos.cl"
    pages = [
        f"{base}/ciudad/osorno.html",
        f"{base}/ciudad/osorno.html/30",
        f"{base}/ciudad/osorno.html/60",
    ]
    try:
        fallas_consecutivas = 0
        for p in pages:
            soup = get_soup(p, retries=1)
            if not soup:
                fallas_consecutivas += 1
                if fallas_consecutivas >= 2:
                    log.warning("Chiletrabajos: corte temprano por timeouts consecutivos")
                    break
                continue
            fallas_consecutivas = 0
            for h2 in soup.find_all("h2"):
                a = h2.find("a", href=True)
                if not a:
                    continue
                href = a["href"]
                if "/trabajo/" not in href:
                    continue
                link = href if href.startswith("http") else f"{base}{href}"
                title = limpiar(a.get_text())
                if not title:
                    continue
                h3 = h2.find_next_sibling("h3")
                empresa_listado = limpiar((h3.get_text(" ") if h3 else "").split(",")[0])
                fecha_listado = ""
                h3_fecha = h3.find_next_sibling("h3") if h3 else None
                if h3_fecha:
                    fecha_listado = limpiar(h3_fecha.get_text(" "))

                d = detalle_chiletrabajos(link)
                empresa = limpiar(d.get("empresa", "")) or empresa_listado or "No especificada"
                if re.fullmatch(r"\d{1,2}\.\d{3}\.\d{3}-[\dkK]", empresa):
                    empresa = empresa_listado or "No especificada"

                out.append(
                    Oferta(
                        source=source,
                        title=title,
                        link=link,
                        company=empresa,
                        date_text=limpiar(d.get("fecha", "")) or fecha_listado or "No especificada",
                        salary=limpiar(d.get("sueldo", "")) or "No especificado",
                        jornada=limpiar(d.get("jornada", "")) or "No especificada",
                        description=limpiar(d.get("descripcion", "")) or "No disponible",
                        postulado_detectado=True if d.get("postulado") == "1" else None,
                        location_verified=True,  # city URL filter
                    )
                )
        set_source_ok(source)
        return dedup(out)
    except Exception as e:
        c = set_source_error(source, str(e))
        if c in (1, 3, 5):
            enviar(f"⚠️ {source} con errores consecutivos: {c}\n{str(e)[:180]}")
        return []


def parse_bne() -> List[Oferta]:
    """
    BNE (Bolsa Nacional de Empleo) — searches by Osorno location.
    Results are location-verified; we do NOT require 'osorno' in the link or text.

    FIX: Broadened href filter — BNE uses /ofertas/ID patterns.
         Added location_verified=True.
    """
    source = "BNE"
    if should_cooldown(source):
        return []
    out: List[Oferta] = []
    # Multiple URL variants to maximise coverage
    urls = [
        "https://www.bne.cl/ofertas?textoBusqueda=&ubicacion=Osorno",
        "https://www.bne.cl/ofertas?textoBusqueda=&comuna=Osorno",
        "https://www.bne.cl/ofertas?textoBusqueda=&region=LosLagos",
    ]
    try:
        for u in urls:
            soup = get_soup(u)
            if not soup:
                continue
            for a in soup.select("a[href]"):
                href = a.get("href", "")
                text = limpiar(a.get_text(" "))
                if len(text) < 4:
                    continue
                # FIX: BNE uses /ofertas/<id> — "oferta" (substring) matches "ofertas"
                href_low = href.lower()
                if not any(tok in href_low for tok in ("oferta", "job", "detalle", "vacante", "empleo")):
                    continue
                # FIX: resolve relative URLs
                if href.startswith("http"):
                    link = href
                elif href.startswith("/"):
                    link = f"https://www.bne.cl{href}"
                else:
                    continue
                out.append(
                    Oferta(
                        source=source,
                        title=text,
                        link=link,
                        location_verified=True,  # search URL filters by city
                    )
                )
        set_source_ok(source)
        return dedup(out)
    except Exception as e:
        c = set_source_error(source, str(e))
        if c in (1, 3, 5):
            enviar(f"⚠️ {source} con errores consecutivos: {c}\n{str(e)[:180]}")
        return []


def parse_indeed_rss() -> List[Oferta]:
    """
    Indeed RSS — query locked to 'Osorno, Los Lagos'.
    All feed entries are from that location → location_verified=True.
    """
    source = "Indeed"
    if should_cooldown(source):
        return []
    out: List[Oferta] = []
    try:
        q = quote_plus("osorno")
        l_param = quote_plus("Osorno, Los Lagos")
        feed_url = f"https://cl.indeed.com/rss?q={q}&l={l_param}&sort=date"
        feed = feedparser.parse(feed_url)
        for e in feed.entries:
            title = limpiar(html.unescape(getattr(e, "title", "")))
            link = getattr(e, "link", "")
            summary = limpiar(
                BeautifulSoup(getattr(e, "summary", ""), "html.parser").get_text(" ")
            )
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
                    location_verified=True,  # search locked to Osorno, Los Lagos
                )
            )
        set_source_ok(source)
        return dedup(out)
    except Exception as e:
        c = set_source_error(source, str(e))
        if c in (1, 3, 5):
            enviar(f"⚠️ {source} con errores consecutivos: {c}\n{str(e)[:180]}")
        return []


def parse_generic_source(
    source: str,
    url: str,
    must_have: Tuple[str, ...],
    base_url: str = "",
    location_verified: bool = False,
) -> List[Oferta]:
    """
    Generic link scraper.

    FIX: Added *base_url* parameter so relative hrefs can be resolved.
         Previously relative URLs were silently discarded, causing zero results
         from Computrabajo and Yapo (both use relative hrefs).

    FIX: Added *location_verified* parameter so callers that search by city
         can mark results without needing 'osorno' in the link text.
    """
    if should_cooldown(source):
        return []
    out: List[Oferta] = []
    try:
        soup = get_soup(url, retries=3)
        if not soup:
            raise RuntimeError("sin respuesta")
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            text = limpiar(a.get_text(" "))
            if len(text) < 4:
                continue
            low = (href + " " + text).lower()
            if not all(token in low for token in must_have):
                continue
            # FIX: resolve relative URLs using the provided base_url
            if href.startswith("http"):
                link = href
            elif href.startswith("/") and base_url:
                link = f"{base_url.rstrip('/')}{href}"
            else:
                # Cannot resolve — skip
                log.debug("Descartando href no resolvible: %s", href)
                continue
            out.append(
                Oferta(
                    source=source,
                    title=text,
                    link=link,
                    location_verified=location_verified,
                )
            )
        set_source_ok(source)
        return dedup(out)
    except Exception as e:
        c = set_source_error(source, str(e))
        if c in (1, 3, 5):
            enviar(f"⚠️ {source} con errores consecutivos: {c}\n{str(e)[:180]}")
        return []


def parse_computrabajo() -> List[Oferta]:
    """
    Computrabajo CL — city-specific URL for Osorno.
    FIX: pass base_url so relative hrefs are resolved; location_verified=True.
    """
    return parse_generic_source(
        source="Computrabajo",
        url="https://cl.computrabajo.com/empleos-en-los-lagos-en-osorno",
        must_have=(),          # URL already filters by city; no extra token needed
        base_url="https://cl.computrabajo.com",
        location_verified=True,
    )


def parse_yapo() -> List[Oferta]:
    """
    Yapo empleos — Osorno job listings only.

    Yapo is a general classifieds site (arriendos, autos, servicios, etc.).
    We apply three layers of filtering to return only job postings:

    Layer 1 — URL must look like a real job listing:
        Yapo listing URLs follow the pattern /<category>/<slug>-<numeric-id>.htm
        We require the numeric-id + .htm suffix AND that the path starts with
        a job-related category slug.  Navigation/filter links don't match this.

    Layer 2 — URL must NOT belong to a non-job category path:
        Hard-reject any href whose path contains known non-job slugs.

    Layer 3 — Title sanity check:
        Navigation labels, breadcrumbs, and button texts are usually very short
        or match known UI strings.  Require at least 6 characters and reject
        obvious non-job labels.
    """
    source = "Yapo"
    if should_cooldown(source):
        return []

    BASE = "https://www.yapo.cl"

    # Layer 1 — must match: a known job category path AND end with digits + .htm
    # Yapo job category slugs observed in practice:
    JOB_CATEGORY_SLUGS = (
        "/empleos/",
        "/trabajo/",
        "/oferta-laboral/",
        "/empleo/",
    )
    # Regex: path ends with one or more digits followed by .htm (or .html)
    RE_LISTING_ID = re.compile(r"\d+\.html?$")

    # Layer 2 — hard-reject these category path prefixes
    REJECT_SLUGS = (
        "/arriendo", "/venta", "/compra",
        "/autos", "/motos", "/vehiculo",
        "/servicios", "/servicio",
        "/inmueble", "/propiedad", "/casa-en-",
        "/electronico", "/electrónico", "/computacion",
        "/moda", "/ropa",
        "/deporte", "/recreacion",
        "/mascota", "/animal",
        "/hogar", "/mueble",
        "/regalo", "/gratis",
    )

    # Layer 3 — reject titles that match common UI/navigation strings
    REJECT_TITLE_PATTERNS = re.compile(
        r"^(ver m[aá]s|publicar|iniciar sesi[oó]n|registrar|subir|buscar|"
        r"filtrar|ordenar|anterior|siguiente|p[aá]gina|volver|inicio|"
        r"mi cuenta|ayuda|contacto|\d+\s*(resultados|anuncios))$",
        re.I,
    )

    urls = [
        f"{BASE}/empleos?ca=12_s&l=0&q=osorno",
        f"{BASE}/empleos?ca=12_s&l=0&q=osorno&o=25",
        f"{BASE}/empleos?ca=12_s&l=0&q=osorno&o=50",
    ]

    out: List[Oferta] = []
    try:
        for u in urls:
            soup = get_soup(u, retries=2)
            if not soup:
                continue

            for a in soup.select("a[href]"):
                href = a.get("href", "")
                if not href:
                    continue
                href_low = href.lower()

                # Layer 1 — must end with <digits>.htm[l]
                if not RE_LISTING_ID.search(href_low):
                    continue
                # Layer 1 — must belong to a job category path
                if not any(slug in href_low for slug in JOB_CATEGORY_SLUGS):
                    continue

                # Layer 2 — reject non-job categories
                if any(slug in href_low for slug in REJECT_SLUGS):
                    continue

                # Layer 3 — title sanity
                text = limpiar(a.get_text(" "))
                if len(text) < 6:
                    continue
                if REJECT_TITLE_PATTERNS.match(text):
                    continue

                # Resolve URL
                if href.startswith("http"):
                    link = href
                elif href.startswith("/"):
                    link = f"{BASE}{href}"
                else:
                    continue

                out.append(
                    Oferta(
                        source=source,
                        title=text,
                        link=link,
                        location_verified=True,  # search URL already scoped to Osorno
                    )
                )

        set_source_ok(source)
        log.debug("Yapo: %s ofertas de empleo encontradas", len(out))
        return dedup(out)
    except Exception as e:
        c = set_source_error(source, str(e))
        if c in (1, 3, 5):
            enviar(f"⚠️ {source} con errores consecutivos: {c}\n{str(e)[:180]}")
        return []


def parse_trabajando() -> List[Oferta]:
    """
    Trabajando.com — Chilean job board.
    Correct URL confirmed: trabajando.cl/trabajo-osorno
    """
    source = "Trabajando"
    if should_cooldown(source):
        return []
    out: List[Oferta] = []
    urls = [
        "https://www.trabajando.cl/trabajo-osorno",
        "https://www.trabajando.cl/trabajo-osorno/pagina-2",
    ]
    try:
        for u in urls:
            soup = get_soup(u, retries=2)
            if not soup:
                continue
            for a in soup.select("a[href]"):
                href = a.get("href", "")
                text = limpiar(a.get_text(" "))
                if len(text) < 6:
                    continue
                href_low = href.lower()
                if not any(tok in href_low for tok in ("empleo", "oferta", "job", "vacante", "trabajo")):
                    continue
                if href.startswith("http"):
                    link = href
                elif href.startswith("/"):
                    link = f"https://www.trabajando.cl{href}"
                else:
                    continue
                out.append(Oferta(source=source, title=text, link=link, location_verified=True))
        set_source_ok(source)
        return dedup(out)
    except Exception as e:
        c = set_source_error(source, str(e))
        if c in (1, 3, 5):
            enviar(f"⚠️ {source} con errores consecutivos: {c}\n{str(e)[:180]}")
        return []


def parse_acciontrabajo() -> List[Oferta]:
    """
    Acciontrabajo.com — Chilean classifieds job board with Osorno listings.
    URL confirmed: cl.acciontrabajo.com/trabajo/osorno
    """
    source = "Acciontrabajo"
    if should_cooldown(source):
        return []
    out: List[Oferta] = []
    urls = [
        "https://cl.acciontrabajo.com/trabajo/osorno",
        "https://cl.acciontrabajo.com/trabajo/osorno/pagina-2",
    ]
    try:
        for u in urls:
            soup = get_soup(u, retries=2)
            if not soup:
                continue
            for a in soup.select("a[href]"):
                href = a.get("href", "")
                text = limpiar(a.get_text(" "))
                if len(text) < 6:
                    continue
                href_low = href.lower()
                # Acciontrabajo listing URLs contain /trabajo/ or /empleo/ or a numeric ID
                if not any(tok in href_low for tok in ("/trabajo/", "/empleo/", "/oferta/")):
                    continue
                # Skip search/filter/pagination links
                if any(tok in href_low for tok in ("/buscar", "/filtro", "/categoria", "/region")):
                    continue
                if href.startswith("http"):
                    link = href
                elif href.startswith("/"):
                    link = f"https://cl.acciontrabajo.com{href}"
                else:
                    continue
                out.append(Oferta(source=source, title=text, link=link, location_verified=True))
        set_source_ok(source)
        return dedup(out)
    except Exception as e:
        c = set_source_error(source, str(e))
        if c in (1, 3, 5):
            enviar(f"⚠️ {source} con errores consecutivos: {c}\n{str(e)[:180]}")
        return []


def dedup(items: List[Oferta]) -> List[Oferta]:
    seen: set = set()
    out: List[Oferta] = []
    for o in items:
        k = o.link.strip()
        if not k or k in seen:
            continue
        seen.add(k)
        out.append(o)
    return out


def upsert_job(o: Oferta) -> Tuple[int, str, str]:
    # FIX: fingerprint was previously built from (title, company, source) which
    #      caused false duplicates when two different companies published the same
    #      role title.  Using the link as the canonical identity is safer because
    #      each job posting has a unique URL.
    fingerprint = short_hash(o.link)
    job_code = f"{o.source[:2].upper()}-{short_hash(o.link)}"
    with get_db() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        try:
            cur.execute(
                """
                INSERT INTO jobs(job_code, source, title, company, link, date_text, salary, jornada,
                                 description, fingerprint, last_seen_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                ON CONFLICT (link) DO NOTHING
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
    data = telegram_api("getUpdates", {"timeout": 1, "offset": offset + 1})
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
            log.exception("Fuente fallo %s: %s", fn.__name__, e)

    new_count = 0
    existing_count = 0
    digest_bucket: List[Tuple[Oferta, str]] = []
    for o in dedup(found):
        o = enriquecer_oferta(o)
        if not pasa_filtros(o):
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
        "Detalle ciclo | fuentes=%s | encontradas=%s | nuevas=%s | ya_registradas=%s",
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
        raise RuntimeError("Config incompleta. Requiere TELEGRAM_TOKEN, TELEGRAM_CHAT_ID y DATABASE_URL")
    init_db()
    enviar(
        "🚀 BOT EMPLEOS OSORNO ACTIVO\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "🌐 Fuentes: Chiletrabajos | BNE | Indeed | Computrabajo | Yapo | Trabajando | Acciontrabajo\n"
        f"⏱️ Intervalo: {INTERVALO}s\n"
        f"🧠 Detail enrichment: {'ON' if ENABLE_DETAIL_ENRICHMENT else 'OFF'}\n"
        f"⭐ Min score alerta inmediata: {MIN_SCORE_IMMEDIATE}\n"
        "🛠️ Comandos: /postule CODIGO | /nopostule CODIGO | /estado CODIGO | /stats | /ultimas\n"
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
            log.info("Ciclo OK | nuevos=%s | %.1fs", int(cycle_stats["new_count"]), elapsed)
        except Exception as e:
            log.exception("Error ciclo: %s", e)
            enviar(f"❌ Error ciclo: {str(e)[:200]}")
            elapsed = time.time() - start
        time.sleep(max(10, INTERVALO - int(elapsed)))


if __name__ == "__main__":
    main()
