import json
import logging
import os
import re
import time
import hashlib
import html
from dataclasses import dataclass
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
MIN_SCORE_IMMEDIATE = int(os.getenv("MIN_SCORE_IMMEDIATE", "3"))
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

class ColorFormatter(logging.Formatter):
    COLORS = {
        logging.DEBUG: "\033[36m",   # cyan
        logging.INFO: "\033[32m",    # green
        logging.WARNING: "\033[33m", # yellow
        logging.ERROR: "\033[31m",   # red
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


def score_oferta(o: Oferta) -> int:
    txt = f"{o.title} {o.company} {o.description}".lower()
    score = 0
    if "osorno" in txt:
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
    if "osorno" not in txt:
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


def telegram_api(method: str, payload: Dict) -> Dict:
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}"
    r = SESSION.post(url, data=payload, timeout=TIMEOUT)
    try:
        data = r.json()
    except Exception:
        data = {"ok": False, "description": r.text[:300]}
    if not r.ok or not data.get("ok"):
        raise RuntimeError(f"Telegram {method} fallo: {data}")
    return data


def enviar(msg: str) -> Optional[int]:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.error("Faltan TELEGRAM_TOKEN/TELEGRAM_CHAT_ID")
        return None
    data = telegram_api("sendMessage", {"chat_id": TELEGRAM_CHAT_ID, "text": msg[:MAX_MSG]})
    return data.get("result", {}).get("message_id")


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


def formatear_heartbeat(cycle_num: int, total_found: int, new_count: int, updated_count: int, per_source: Dict[str, int]) -> str:
    fuentes_line = " | ".join([f"{k}:{v}" for k, v in per_source.items()]) if per_source else "sin datos"
    return (
        "🫀 BOT EN LINEA - OSORNO\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"🔁 Ciclo: {cycle_num}\n"
        f"📥 Encontradas: {total_found}\n"
        f"🆕 Nuevas: {new_count}\n"
        f"♻️ Actualizadas: {updated_count}\n"
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
    for sel in ["#jobDescriptionText", "article", "main", "[class*='description']", "meta[property='og:description']"]:
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
        r"(hace\s+\d+\s+(?:minuto|minutos|hora|horas|d[ií]a|d[ií]as|semana|semanas)|\d{1,2}\s+de\s+\w+\s+de\s+\d{4}|\d{4}-\d{2}-\d{2})",
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
    # Chiletrabajos already has dedicated detail parsing in listing phase.
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
            DO UPDATE SET consecutive_errors=source_health.consecutive_errors + 1, last_error=EXCLUDED.last_error
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
        CHILETRABAJOS_CONNECT_TIMEOUT,
        CHILETRABAJOS_READ_TIMEOUT,
    ) if is_chiletrabajos else (CONNECT_TIMEOUT, READ_TIMEOUT)

    for i in range(1, retries + 1):
        try:
            r = SESSION.get(url, timeout=timeout_cfg)
            r.raise_for_status()
            return BeautifulSoup(r.text, "html.parser")
        except requests.RequestException as e:
            log.warning("GET [%s/%s] %s: %s", i, retries, url, e)
            # For portal/network timeout storms, fail fast to keep cycles healthy.
            if isinstance(e, (requests.ConnectTimeout, requests.ReadTimeout)):
                break
            time.sleep(0.8 * i)
    return None


def parse_chiletrabajos() -> List[Oferta]:
    source = "Chiletrabajos"
    if should_cooldown(source):
        return []
    out: List[Oferta] = []
    base = "https://www.chiletrabajos.cl"
    pages = [f"{base}/ciudad/osorno.html", f"{base}/ciudad/osorno.html/30", f"{base}/ciudad/osorno.html/60"]
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
                # Avoid sending RUT as company name
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
    source = "BNE"
    if should_cooldown(source):
        return []
    out: List[Oferta] = []
    urls = [
        "https://www.bne.cl/ofertas?textoBusqueda=&ubicacion=Osorno",
        "https://www.bne.cl/ofertas?textoBusqueda=&comuna=Osorno",
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
                if "oferta" not in href and "job" not in href and "detalle" not in href:
                    continue
                link = href if href.startswith("http") else f"https://www.bne.cl{href}"
                if "osorno" not in (text + " " + link).lower():
                    continue
                out.append(Oferta(source=source, title=text, link=link))
        set_source_ok(source)
        return dedup(out)
    except Exception as e:
        c = set_source_error(source, str(e))
        if c in (1, 3, 5):
            enviar(f"⚠️ {source} con errores consecutivos: {c}\n{str(e)[:180]}")
        return []


def parse_indeed_rss() -> List[Oferta]:
    source = "Indeed"
    if should_cooldown(source):
        return []
    out: List[Oferta] = []
    try:
        q = quote_plus("osorno")
        l = quote_plus("Osorno, Los Lagos")
        feed_url = f"https://cl.indeed.com/rss?q={q}&l={l}&sort=date"
        feed = feedparser.parse(feed_url)
        for e in feed.entries:
            title = limpiar(html.unescape(getattr(e, "title", "")))
            link = getattr(e, "link", "")
            summary = limpiar(BeautifulSoup(getattr(e, "summary", ""), "html.parser").get_text(" "))
            company = "No especificada"
            if " - " in title:
                parts = title.split(" - ", 1)
                if len(parts) == 2:
                    title, company = limpiar(parts[0]), limpiar(parts[1])
            out.append(Oferta(source=source, title=title, company=company, link=link, description=summary))
        set_source_ok(source)
        return dedup(out)
    except Exception as e:
        c = set_source_error(source, str(e))
        if c in (1, 3, 5):
            enviar(f"⚠️ {source} con errores consecutivos: {c}\n{str(e)[:180]}")
        return []


def parse_generic_source(source: str, url: str, must_have: Tuple[str, ...]) -> List[Oferta]:
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
            link = href if href.startswith("http") else ""
            if not link:
                continue
            out.append(Oferta(source=source, title=text, link=link))
        set_source_ok(source)
        return dedup(out)
    except Exception as e:
        c = set_source_error(source, str(e))
        if c in (1, 3, 5):
            enviar(f"⚠️ {source} con errores consecutivos: {c}\n{str(e)[:180]}")
        return []


def parse_computrabajo() -> List[Oferta]:
    return parse_generic_source(
        source="Computrabajo",
        url="https://cl.computrabajo.com/trabajo-de-en-osorno",
        must_have=("osorno",),
    )


def parse_yapo() -> List[Oferta]:
    return parse_generic_source(
        source="Yapo",
        url="https://www.yapo.cl/empleos?ca=12_s&l=0&q=osorno",
        must_have=("osorno",),
    )


def dedup(items: List[Oferta]) -> List[Oferta]:
    seen = set()
    out: List[Oferta] = []
    for o in items:
        k = o.link.strip()
        if not k or k in seen:
            continue
        seen.add(k)
        out.append(o)
    return out


def upsert_job(o: Oferta) -> Tuple[int, str, str]:
    fingerprint = short_hash(o.title, o.company, o.source)
    job_code = f"{o.source[:2].upper()}-{short_hash(o.link)}"
    with get_db() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        try:
            cur.execute(
                """
                INSERT INTO jobs(job_code, source, title, company, link, date_text, salary, jornada, description, fingerprint, last_seen_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                ON CONFLICT (link) DO UPDATE SET
                    title=EXCLUDED.title,
                    company=EXCLUDED.company,
                    date_text=EXCLUDED.date_text,
                    salary=EXCLUDED.salary,
                    jornada=EXCLUDED.jornada,
                    description=EXCLUDED.description,
                    fingerprint=EXCLUDED.fingerprint,
                    last_seen_at=NOW()
                RETURNING id, applied_status, (xmax = 0) AS inserted;
                """,
                (job_code, o.source, o.title, o.company, o.link, o.date_text, o.salary, o.jornada, o.description, fingerprint),
            )
            row = cur.fetchone()
            conn.commit()
            return int(row["id"]), str(row["applied_status"]), "inserted" if row["inserted"] else "updated"
        except psycopg2.errors.UniqueViolation:
            # Same posting can appear with a different URL. If fingerprint already exists,
            # update the existing row instead of failing the whole cycle.
            conn.rollback()
            cur.execute(
                """
                UPDATE jobs
                SET
                    title=%s,
                    company=%s,
                    date_text=%s,
                    salary=%s,
                    jornada=%s,
                    description=%s,
                    last_seen_at=NOW()
                WHERE fingerprint=%s
                RETURNING id, applied_status;
                """,
                (o.title, o.company, o.date_text, o.salary, o.jornada, o.description, fingerprint),
            )
            row = cur.fetchone()
            conn.commit()
            if not row:
                raise
            return int(row["id"]), str(row["applied_status"]), "updated"


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
        cur.execute("SELECT title, applied_status FROM jobs WHERE job_code=%s", (job_code.upper(),))
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
    m = re.match(r"^/(postule|nopostule|estado)\s+([A-Za-z]{2}-[A-F0-9]{10})$", t, re.I)
    if not m:
        return "", ""
    return m.group(1).lower(), m.group(2).upper()


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


def run_cycle() -> Dict[str, object]:
    sources = [
        parse_chiletrabajos,
        parse_bne,
        parse_indeed_rss,
        parse_computrabajo,
        parse_yapo,
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
    updated_count = 0
    digest_bucket: List[Tuple[Oferta, str]] = []
    for o in dedup(found):
        o = enriquecer_oferta(o)
        if not pasa_filtros(o):
            continue
        job_id, applied_status, op = upsert_job(o)
        if op != "inserted":
            updated_count += 1
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
        "Detalle ciclo | fuentes=%s | encontradas=%s | nuevas=%s | actualizadas=%s",
        per_source,
        len(found),
        new_count,
        updated_count,
    )
    return {
        "new_count": new_count,
        "found_count": len(found),
        "updated_count": updated_count,
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
        "🌐 Fuentes: Chiletrabajos | BNE | Indeed | Computrabajo | Yapo\n"
        f"⏱️ Intervalo: {INTERVALO}s\n"
        f"🧠 Detail enrichment: {'ON' if ENABLE_DETAIL_ENRICHMENT else 'OFF'}\n"
        f"⭐ Min score alerta inmediata: {MIN_SCORE_IMMEDIATE}\n"
        "🛠️ Comandos: /postule CODIGO | /nopostule CODIGO | /estado CODIGO | /stats\n"
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
                        updated_count=int(cycle_stats["updated_count"]),
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
