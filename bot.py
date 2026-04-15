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

INTERVALO = int(os.getenv("INTERVALO", "60"))
TIMEOUT = int(os.getenv("TIMEOUT", "20"))
CONNECT_TIMEOUT = int(os.getenv("CONNECT_TIMEOUT", "10"))
READ_TIMEOUT = int(os.getenv("READ_TIMEOUT", "25"))
MAX_DESC = 500
MAX_MSG = 4096
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
COLOR_LOGS = os.getenv("COLOR_LOGS", "1") == "1"
ENABLE_DETAIL_ENRICHMENT = os.getenv("ENABLE_DETAIL_ENRICHMENT", "0") == "1"
MIN_SCORE_IMMEDIATE = int(os.getenv("MIN_SCORE_IMMEDIATE", "1"))
DIGEST_EVERY_CYCLES = int(os.getenv("DIGEST_EVERY_CYCLES", "3"))
DAILY_REPORT_HOUR_UTC = int(os.getenv("DAILY_REPORT_HOUR_UTC", "23"))
HEARTBEAT_EVERY_CYCLES = int(os.getenv("HEARTBEAT_EVERY_CYCLES", "4"))

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DATABASE_URL = os.getenv("DATABASE_URL")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "es-CL,es;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1"
}

OSORNO_SIGNALS = ["osorno", "los lagos", "región de los lagos", "region de los lagos", "5290", "rahue"]
KEYWORDS_INCLUDE = [k.strip().lower() for k in os.getenv("KEYWORDS_INCLUDE", "").split(",") if k.strip()]
KEYWORDS_EXCLUDE = [k.strip().lower() for k in os.getenv("KEYWORDS_EXCLUDE", "").split(",") if k.strip()]

class ColorFormatter(logging.Formatter):
    COLORS = {logging.DEBUG: "\033[36m", logging.INFO: "\033[32m", logging.WARNING: "\033[33m", logging.ERROR: "\033[31m", logging.CRITICAL: "\033[35m"}
    RESET = "\033[0m"
    LEVEL_LABEL = {logging.INFO: "OK", logging.WARNING: "WARN", logging.ERROR: "ERR", logging.CRITICAL: "CRIT", logging.DEBUG: "DBG"}
    
    def format(self, record: logging.LogRecord) -> str:
        base = super().format(record)
        tag = self.LEVEL_LABEL.get(record.levelno, record.levelname)
        if COLOR_LOGS:
            color = self.COLORS.get(record.levelno, "")
            return f"{color}[{tag}] {base}{self.RESET}"
        return f"[{tag}] {base}"

_handler = logging.StreamHandler()
_handler.setFormatter(ColorFormatter("%(asctime)s %(message)s"))
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

def limpiar(txt: str) -> str:
    return re.sub(r"\s+", " ", str(txt or "")).strip()

def limpiar_desc(txt: str) -> str:
    limpio = limpiar(txt)
    noise = ["PUBLICIDAD", "Home", "Volver", "Buscar Ofertas", "Detalle oferta", "Postular", "Guardar", "Compartir", "Ofertas relacionadas"]
    for n in noise:
        limpio = limpio.replace(n, "")
    limpio = re.sub(r'\bID\s+\d+\b', '', limpio)
    limpio = re.sub(r'\bFecha\s+\d{4}-\d{2}-\d{2}', '', limpio)
    limpio = re.sub(r'\bExpira.*?\d+ días?\)', '', limpio)
    limpio = re.sub(r'\bUbicación\s+\w+\s+CL\b', '', limpio)
    limpio = re.sub(r'\bCategoría.*?(?=Duración|Tipo|$)', '', limpio)
    limpio = re.sub(r'\bDuración.*?(?=Tipo|$)', '', limpio)
    limpio = re.sub(r'\bTipo\s+[\w\s-]+?(?=Postular|$)', '', limpio)
    limpio = re.sub(r'\bBuscado\s+.*?(?=Fecha|$)', '', limpio, flags=re.DOTALL)
    lines = [l for l in limpio.split('\n') if len(l.strip()) > 25]
    limpio = ' '.join(lines)
    limpio = re.sub(r'\s+', ' ', limpio).strip()
    if len(limpio) > MAX_DESC:
        limpio = limpio[:MAX_DESC].rsplit(' ', 1)[0] + '...'
    return limpio if limpio else "No disponible"

def short_hash(*parts: str) -> str:
    base = limpiar("|".join(parts)).lower()
    return hashlib.sha1(base.encode("utf-8")).hexdigest()[:10].upper()

def contiene_osorno(txt: str) -> bool:
    low = txt.lower()
    return any(sig in low for sig in OSORNO_SIGNALS)

def score_oferta(o: Oferta) -> int:
    txt = f"{o.title} {o.company} {o.description}".lower()
    score = 0
    if contiene_osorno(txt) or o.location_verified:
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
    if not o.location_verified and not contiene_osorno(txt):
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
            cur.execute("""
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
            """)
        conn.commit()

_tg_retry_after_until: float = 0.0

def telegram_api(method: str, payload: Dict, _retries: int = 5) -> Dict:
    global _tg_retry_after_until
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}"
    for attempt in range(1, _retries + 1):
        remaining = _tg_retry_after_until - time.time()
        if remaining > 0:
            time.sleep(remaining)
        r = SESSION.post(url, data=payload, timeout=TIMEOUT)
        try:
            data = r.json()
        except:
            data = {"ok": False, "description": r.text[:300]}
        if r.status_code == 429 or data.get("error_code") == 429:
            retry_after = min(int(data.get("parameters", {}).get("retry_after", 30)), 600)
            _tg_retry_after_until = time.time() + retry_after + 2
            if attempt < _retries:
                time.sleep(retry_after + 2)
                continue
            raise RuntimeError(f"Telegram 429 tras {_retries} intentos")
        if not r.ok or not data.get("ok"):
            raise RuntimeError(f"Telegram {method} fallo: {data}")
        return data
    raise RuntimeError(f"Telegram {method} sin reintentos")

def enviar(msg: str) -> Optional[int]:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return None
    try:
        data = telegram_api("sendMessage", {"chat_id": TELEGRAM_CHAT_ID, "text": msg[:MAX_MSG]})
        return data.get("result", {}).get("message_id")
    except Exception as e:
        log.error("enviar() fallo: %s", e)
        return None

def format_estado(status: str) -> str:
    return {"applied": "✅ Ya postule", "not_applied": "❌ No postule", "unknown": "❓ Desconocido"}.get(status, "❓ Desconocido")

def formatear_oferta(o: Oferta, job_code: str, applied_status: str) -> str:
    header = [
        f"🆕 [{o.source}]",
        "─────────────────",
        f"🆔 {job_code}",
        f"📌 {o.title}",
        f"🏢 {o.company}",
        f"📅 {o.date_text}",
        f"🕒 {o.jornada}",
        f"💰 {o.salary}",
        f"📮 {format_estado(applied_status)}",
    ]
    core = "\n".join(header) + "\n─────────────────\n"
    tail = f"\n🔗 {o.link}\n─────────────────\n/postule {job_code} | /nopostule {job_code} | /estado {job_code}"
    max_len = max(80, MAX_MSG - len(core) - len(tail) - 10)
    desc = limpiar_desc(o.description)
    if len(desc) > max_len:
        desc = desc[:max_len-3].rstrip() + "..."
    return core + desc + tail

def formatear_heartbeat(cycle: int, total: int, new: int, existing: int, per_source: Dict[str, int]) -> str:
    fuentes = " | ".join([f"{k}:{v}" for k, v in per_source.items()]) if per_source else "sin datos"
    return f"🫀 CICLO {cycle}\n━━━━━━━━━━\n📥 Total: {total}\n🆕 Nuevas: {new}\n📚 Existentes: {existing}\n🌐 {fuentes}\n━━━━━━━━━━\n/stats"

def set_source_ok(source: str) -> None:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("INSERT INTO source_health(source, consecutive_errors, last_success_at) VALUES (%s, 0, NOW()) ON CONFLICT (source) DO UPDATE SET consecutive_errors=0, last_error=NULL, last_success_at=NOW();", (source,))
        conn.commit()

def set_source_error(source: str, err: str) -> int:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("INSERT INTO source_health(source, consecutive_errors, last_error) VALUES (%s, 1, %s) ON CONFLICT (source) DO UPDATE SET consecutive_errors=source_health.consecutive_errors + 1, last_error=EXCLUDED.last_error RETURNING consecutive_errors;", (source, err[:250]))
        count = cur.fetchone()[0]
        conn.commit()
    return count

def should_cooldown(source: str) -> bool:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("SELECT consecutive_errors FROM source_health WHERE source=%s", (source,))
        row = cur.fetchone()
        return row[0] >= 5 if row else False

def get_soup(url: str, retries: int = 2) -> Optional[BeautifulSoup]:
    for i in range(1, retries + 1):
        try:
            r = SESSION.get(url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
            r.raise_for_status()
            return BeautifulSoup(r.text, "html.parser")
        except Exception as e:
            log.warning("GET [%s/%s] %s: %s", i, retries, url, e)
            if i < retries:
                time.sleep(1)
    return None

# PARSERS ULTRA-ESPECÍFICOS

def parse_chiletrabajos() -> List[Oferta]:
    source = "Chiletrabajos"
    if should_cooldown(source):
        return []
    out: List[Oferta] = []
    base = "https://www.chiletrabajos.cl"
    urls = [f"{base}/ciudad/osorno.html", f"{base}/ciudad/osorno.html/30", f"{base}/ciudad/osorno.html/60"]
    
    try:
        for url in urls:
            soup = get_soup(url, 3)
            if not soup:
                continue
            
            # Estrategia 1: H2 > A con /trabajo/
            for h2 in soup.find_all("h2"):
                a = h2.find("a", href=True)
                if not a:
                    continue
                href = a.get("href", "")
                if "/trabajo/" not in href:
                    continue
                
                link = href if href.startswith("http") else f"{base}{href}"
                title = limpiar(a.get_text())
                if len(title) < 5:
                    continue
                
                # Extraer info básica del listado
                empresa = "No especificada"
                h3 = h2.find_next_sibling("h3")
                if h3:
                    empresa = limpiar(h3.get_text().split(",")[0])
                
                # Extraer detalles de la oferta
                det_soup = get_soup(link, 2)
                fecha = "No especificada"
                sueldo = "No especificado"
                jornada = "No especificada"
                desc = "No disponible"
                
                if det_soup:
                    # Tabla de detalles
                    for td in det_soup.select("table td"):
                        txt = limpiar(td.get_text()).lower()
                        sib = td.find_next_sibling("td")
                        if not sib:
                            continue
                        val = limpiar(sib.get_text())
                        if "fecha" in txt:
                            fecha = val
                        elif "salario" in txt or "sueldo" in txt:
                            sueldo = val if val.startswith("$") else (f"${val}" if val and val != "No especificado" else "No especificado")
                        elif "tipo" in txt or "jornada" in txt:
                            jornada = val
                        elif "buscado" in txt or "empresa" in txt:
                            if not re.match(r'\d{1,2}\.\d{3}\.\d{3}', val):
                                empresa = val
                    
                    # Descripción - buscar solo contenido útil
                    desc_parts = []
                    for tag in det_soup.find_all(["p", "div", "li"]):
                        txt = limpiar(tag.get_text())
                        if 30 < len(txt) < 400 and not any(noise in txt for noise in ["PUBLICIDAD", "Volver", "Buscar"]):
                            desc_parts.append(txt)
                        if len(desc_parts) >= 3:
                            break
                    if desc_parts:
                        desc = " ".join(desc_parts)
                    
                    time.sleep(0.2)
                
                out.append(Oferta(
                    source=source,
                    title=title,
                    link=link,
                    company=empresa,
                    date_text=fecha,
                    salary=sueldo,
                    jornada=jornada,
                    description=limpiar_desc(desc),
                    location_verified=True
                ))
            
            time.sleep(0.3)
        
        set_source_ok(source)
        log.info("%s: %s ofertas", source, len(out))
    except Exception as e:
        log.exception("%s error: %s", source, e)
        set_source_error(source, str(e))
    
    return dedup(out)

def parse_bne() -> List[Oferta]:
    source = "BNE"
    if should_cooldown(source):
        return []
    out: List[Oferta] = []
    base = "https://www.bne.cl"
    urls = [
        f"{base}/ofertas?ubicacion=Osorno",
        f"{base}/ofertas?comuna=Osorno",
        f"{base}/ofertas?region=Los+Lagos&ubicacion=Osorno",
    ]
    
    try:
        seen: Set[str] = set()
        for url in urls:
            soup = get_soup(url, 3)
            if not soup:
                continue
            
            # Estrategia: Cualquier <a> con "/oferta" en href
            for a in soup.select("a[href]"):
                href = a.get("href", "")
                if not href or "/oferta" not in href.lower():
                    continue
                if any(x in href.lower() for x in ["filtro", "buscar", "page="]):
                    continue
                
                text = limpiar(a.get_text())
                if len(text) < 8:
                    continue
                
                link = href if href.startswith("http") else urljoin(base, href)
                if link in seen:
                    continue
                seen.add(link)
                
                out.append(Oferta(source=source, title=text, link=link, location_verified=True))
            
            time.sleep(0.2)
        
        set_source_ok(source)
        log.info("%s: %s ofertas", source, len(out))
    except Exception as e:
        log.exception("%s error: %s", source, e)
        set_source_error(source, str(e))
    
    return dedup(out)

def parse_indeed() -> List[Oferta]:
    source = "Indeed"
    if should_cooldown(source):
        return []
    out: List[Oferta] = []
    
    try:
        feed_url = f"https://cl.indeed.com/rss?q={quote_plus('osorno')}&l={quote_plus('Osorno, Los Lagos')}&sort=date&limit=50"
        feed = feedparser.parse(feed_url)
        
        for e in feed.entries:
            title = limpiar(html.unescape(getattr(e, "title", "")))
            link = getattr(e, "link", "")
            summary = limpiar(BeautifulSoup(getattr(e, "summary", ""), "html.parser").get_text())
            
            if not title or not link:
                continue
            
            company = "No especificada"
            if " - " in title:
                parts = title.split(" - ", 1)
                if len(parts) == 2:
                    title, company = limpiar(parts[0]), limpiar(parts[1])
            
            out.append(Oferta(
                source=source,
                title=title,
                company=company,
                link=link,
                description=summary,
                location_verified=True
            ))
        
        set_source_ok(source)
        log.info("%s: %s ofertas", source, len(out))
    except Exception as e:
        log.exception("%s error: %s", source, e)
        set_source_error(source, str(e))
    
    return dedup(out)

def parse_computrabajo() -> List[Oferta]:
    source = "Computrabajo"
    if should_cooldown(source):
        return []
    out: List[Oferta] = []
    base = "https://cl.computrabajo.com"
    urls = [
        f"{base}/empleos-en-los-lagos-en-osorno",
        f"{base}/empleos-en-los-lagos-en-osorno?p=2",
        f"{base}/ofertas-de-trabajo/en-osorno"
    ]
    
    try:
        seen: Set[str] = set()
        for url in urls:
            soup = get_soup(url, 3)
            if not soup:
                continue
            
            # Buscar enlaces con patrón: /ofertas-de-trabajo/oferta-...-HASH.html
            for a in soup.select("a[href]"):
                href = a.get("href", "")
                if not href:
                    continue
                
                # Debe contener "oferta" y terminar en hash.html
                if "oferta" not in href.lower():
                    continue
                if not re.search(r'-[0-9a-f]{6,}\.html?$', href, re.I):
                    continue
                
                text = limpiar(a.get_text())
                if len(text) < 8:
                    continue
                
                link = href if href.startswith("http") else f"{base}{href}"
                if link in seen:
                    continue
                seen.add(link)
                
                out.append(Oferta(source=source, title=text, link=link, location_verified=True))
            
            time.sleep(0.2)
        
        set_source_ok(source)
        log.info("%s: %s ofertas", source, len(out))
    except Exception as e:
        log.exception("%s error: %s", source, e)
        set_source_error(source, str(e))
    
    return dedup(out)

def parse_yapo() -> List[Oferta]:
    source = "Yapo"
    if should_cooldown(source):
        return []
    out: List[Oferta] = []
    base = "https://www.yapo.cl"
    urls = [
        f"{base}/empleos?ca=12_s&l=0&q=osorno",
        f"{base}/empleos?ca=12_s&l=0&q=osorno&o=25"
    ]
    
    try:
        seen: Set[str] = set()
        for url in urls:
            soup = get_soup(url, 3)
            if not soup:
                continue
            
            for a in soup.select("a[href]"):
                href = a.get("href", "")
                if not href:
                    continue
                
                # Debe terminar en NÚMEROS.html y contener /empleo o /trabajo
                if not re.search(r'\d{5,}\.html?$', href):
                    continue
                if not any(x in href.lower() for x in ["/empleo", "/trabajo", "/oferta"]):
                    continue
                # Rechazar otras categorías
                if any(x in href.lower() for x in ["/arriendo", "/venta", "/auto", "/servicio", "/inmueble"]):
                    continue
                
                text = limpiar(a.get_text())
                if len(text) < 8:
                    continue
                if re.match(r'^(ver|filtrar|buscar)', text, re.I):
                    continue
                
                link = href if href.startswith("http") else urljoin(base, href)
                if link in seen:
                    continue
                seen.add(link)
                
                out.append(Oferta(source=source, title=text, link=link, location_verified=True))
            
            time.sleep(0.2)
        
        set_source_ok(source)
        log.info("%s: %s ofertas", source, len(out))
    except Exception as e:
        log.exception("%s error: %s", source, e)
        set_source_error(source, str(e))
    
    return dedup(out)

def parse_trabajando() -> List[Oferta]:
    source = "Trabajando"
    if should_cooldown(source):
        return []
    out: List[Oferta] = []
    base = "https://www.trabajando.cl"
    urls = [f"{base}/trabajo-osorno", f"{base}/trabajo-osorno/pagina-2"]
    
    try:
        seen: Set[str] = set()
        for url in urls:
            soup = get_soup(url, 3)
            if not soup:
                continue
            
            for a in soup.select("a[href]"):
                href = a.get("href", "")
                text = limpiar(a.get_text())
                if len(text) < 8:
                    continue
                if not any(x in href.lower() for x in ["/empleo/", "/oferta/", "/trabajo/"]):
                    continue
                if any(x in href.lower() for x in ["filtro", "categoria", "buscar"]):
                    continue
                
                link = href if href.startswith("http") else urljoin(base, href)
                if link in seen:
                    continue
                seen.add(link)
                
                out.append(Oferta(source=source, title=text, link=link, location_verified=True))
            
            time.sleep(0.2)
        
        set_source_ok(source)
        log.info("%s: %s ofertas", source, len(out))
    except Exception as e:
        log.exception("%s error: %s", source, e)
        set_source_error(source, str(e))
    
    return dedup(out)

def parse_acciontrabajo() -> List[Oferta]:
    source = "Acciontrabajo"
    if should_cooldown(source):
        return []
    out: List[Oferta] = []
    base = "https://cl.acciontrabajo.com"
    urls = [f"{base}/trabajo/osorno", f"{base}/trabajo/osorno/pagina-2"]
    
    try:
        seen: Set[str] = set()
        for url in urls:
            soup = get_soup(url, 3)
            if not soup:
                continue
            
            for a in soup.select("a[href]"):
                href = a.get("href", "")
                text = limpiar(a.get_text())
                if len(text) < 8:
                    continue
                if not any(x in href.lower() for x in ["/trabajo/", "/empleo/", "/oferta/"]):
                    continue
                if any(x in href.lower() for x in ["/buscar", "/filtro", "/region"]):
                    continue
                
                link = href if href.startswith("http") else urljoin(base, href)
                if link in seen:
                    continue
                seen.add(link)
                
                out.append(Oferta(source=source, title=text, link=link, location_verified=True))
            
            time.sleep(0.2)
        
        set_source_ok(source)
        log.info("%s: %s ofertas", source, len(out))
    except Exception as e:
        log.exception("%s error: %s", source, e)
        set_source_error(source, str(e))
    
    return dedup(out)

def dedup(items: List[Oferta]) -> List[Oferta]:
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
            cur.execute("""
                INSERT INTO jobs(job_code, source, title, company, link, date_text, salary, jornada, description, fingerprint, last_seen_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                ON CONFLICT (link) DO UPDATE SET last_seen_at=NOW()
                RETURNING id, applied_status, (xmax = 0) AS inserted;
            """, (job_code, o.source, o.title, o.company, o.link, o.date_text, o.salary, o.jornada, o.description, fingerprint))
            
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
        cur.execute("INSERT INTO notifications(job_id, telegram_message_id) VALUES (%s,%s)", (job_id, message_id))
        conn.commit()

def set_offset(v: int) -> None:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("INSERT INTO bot_state(key, value) VALUES ('telegram_offset', %s) ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value", (str(v),))
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
        cur.execute("INSERT INTO bot_state(key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value", (key, value))
        conn.commit()

def update_applied(job_code: str, status: str) -> bool:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("UPDATE jobs SET applied_status=%s, applied_updated_at=NOW() WHERE UPPER(job_code)=UPPER(%s)", (status, job_code))
        ok = cur.rowcount > 0
        conn.commit()
        log.info("update_applied(%s, %s) = %s", job_code, status, ok)
        return ok

def get_job_status(job_code: str) -> Optional[Tuple[str, str]]:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("SELECT title, applied_status FROM jobs WHERE UPPER(job_code)=UPPER(%s)", (job_code,))
        row = cur.fetchone()
        return (row[0], row[1]) if row else None

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
    if m:
        return m.group(1).lower(), m.group(2).upper()
    return "", ""

def ultimas_ofertas(limit: int = 8) -> List[Tuple[str, str, str, str]]:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("SELECT source, title, job_code, link FROM jobs ORDER BY last_seen_at DESC LIMIT %s", (limit,))
        return [(str(r[0]), str(r[1]), str(r[2]), str(r[3])) for r in cur.fetchall()]

def formatear_ultimas(rows: List[Tuple[str, str, str, str]]) -> str:
    if not rows:
        return "📭 Sin ofertas"
    lines = ["📌 ULTIMAS OFERTAS", "━━━━━━━━━━"]
    for source, title, code, link in rows:
        lines.append(f"• [{source}] {title}\n  🆔 {code}\n  🔗 {link}")
    lines.append("━━━━━━━━━━\n/estado CODIGO | /postule CODIGO")
    return "\n".join(lines)

def process_telegram_commands() -> None:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    offset = get_offset()
    try:
        data = telegram_api("getUpdates", {"timeout": 1, "offset": offset + 1})
    except:
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
            if st:
                enviar(f"📌 {st[0]}\n🆔 {code}\n📮 {format_estado(st[1])}")
            else:
                enviar(f"❌ Codigo {code} no encontrado")
        elif cmd == "stats":
            total, day = job_counts()
            enviar(f"📊 Total: {total} | Nuevas 24h: {day}")
        elif cmd == "ultimas":
            enviar(formatear_ultimas(ultimas_ofertas(8)))

def run_cycle() -> Dict[str, object]:
    sources = [parse_chiletrabajos, parse_bne, parse_indeed, parse_computrabajo, parse_yapo, parse_trabajando, parse_acciontrabajo]
    found: List[Oferta] = []
    per_source: Dict[str, int] = {}
    
    for fn in sources:
        try:
            items = fn()
            found.extend(items)
            name = fn.__name__.replace("parse_", "").capitalize()
            per_source[name] = len(items)
        except Exception as e:
            log.exception("Fuente %s: %s", fn.__name__, e)
            per_source[fn.__name__.replace("parse_", "").capitalize()] = 0
    
    new_count = 0
    existing_count = 0
    digest_bucket: List[Tuple[Oferta, str]] = []
    
    for o in dedup(found):
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
        lines = ["📦 DIGEST", "─────────"]
        for o, code in digest_bucket[:10]:
            lines.append(f"[{o.source}] {o.title} ({code})")
        enviar("\n".join(lines))
    
    log.info("Ciclo %s | total=%s nuevas=%s existentes=%s | %s", cycle_num, len(found), new_count, existing_count, per_source)
    
    return {"new_count": new_count, "found_count": len(found), "existing_count": existing_count, "per_source": per_source, "cycle_num": cycle_num}

def main() -> None:
    if not all([TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, DATABASE_URL]):
        raise RuntimeError("Faltan variables: TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, DATABASE_URL")
    
    init_db()
    enviar(f"🚀 BOT OSORNO v2\n━━━━━━━━━━\n7 fuentes activas\nIntervalo: {INTERVALO}s\n━━━━━━━━━━")
    
    while True:
        start = time.time()
        try:
            process_telegram_commands()
            stats = run_cycle()
            
            if stats["cycle_num"] % HEARTBEAT_EVERY_CYCLES == 0:
                enviar(formatear_heartbeat(
                    stats["cycle_num"],
                    stats["found_count"],
                    stats["new_count"],
                    stats["existing_count"],
                    stats["per_source"]
                ))
        except Exception as e:
            log.exception("Ciclo error: %s", e)
            enviar(f"❌ {str(e)[:150]}")
        
        elapsed = time.time() - start
        sleep_time = max(10, INTERVALO - int(elapsed))
        time.sleep(sleep_time)

if __name__ == "__main__":
    main()
