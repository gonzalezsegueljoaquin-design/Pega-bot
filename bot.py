import json
import logging
import os
import re
import time
import hashlib
import html
import random
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, Set
from urllib.parse import quote_plus, urljoin, urlparse

import feedparser
import psycopg2
import psycopg2.extras
import requests
from bs4 import BeautifulSoup

INTERVALO = int(os.getenv("INTERVALO", "90"))
MAX_DESC = 400
MAX_MSG = 4096
LOG_LEVEL = os.getenv("LOG_LEVEL", "DEBUG").upper()
MIN_SCORE_IMMEDIATE = int(os.getenv("MIN_SCORE_IMMEDIATE", "1"))
DIGEST_EVERY_CYCLES = int(os.getenv("DIGEST_EVERY_CYCLES", "5"))
HEARTBEAT_EVERY_CYCLES = int(os.getenv("HEARTBEAT_EVERY_CYCLES", "10"))

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DATABASE_URL = os.getenv("DATABASE_URL")

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
]

OSORNO_SIGNALS = ["osorno", "los lagos", "región de los lagos", "5290"]
KEYWORDS_INCLUDE = [k.strip().lower() for k in os.getenv("KEYWORDS_INCLUDE", "").split(",") if k.strip()]
KEYWORDS_EXCLUDE = [k.strip().lower() for k in os.getenv("KEYWORDS_EXCLUDE", "").split(",") if k.strip()]

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.DEBUG),
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger("bot")

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
    location_verified: bool = False

def limpiar(txt: str) -> str:
    return re.sub(r"\s+", " ", str(txt or "")).strip()

def limpiar_desc(txt: str) -> str:
    limpio = limpiar(txt)
    for noise in ["PUBLICIDAD", "Home", "Volver", "Buscar", "Detalle", "Postular", "Guardar"]:
        limpio = limpio.replace(noise, "")
    limpio = re.sub(r'\b(ID|Fecha|Expira|Ubicación|Categoría|Duración|Tipo|Buscado)\s+[^\n]+', '', limpio)
    lines = [l for l in limpio.split('\n') if len(l.strip()) > 20]
    limpio = ' '.join(lines)
    limpio = re.sub(r'\s+', ' ', limpio).strip()
    return limpio[:MAX_DESC] + ('...' if len(limpio) > MAX_DESC else '') if limpio else "No disponible"

def short_hash(*parts: str) -> str:
    return hashlib.sha1(limpiar("|".join(parts)).lower().encode()).hexdigest()[:10].upper()

def contiene_osorno(txt: str) -> bool:
    return any(sig in txt.lower() for sig in OSORNO_SIGNALS)

def score_oferta(o: Oferta) -> int:
    txt = f"{o.title} {o.company} {o.description}".lower()
    score = 0
    if contiene_osorno(txt) or o.location_verified:
        score += 1
    if o.salary != "No especificado":
        score += 1
    if o.jornada != "No especificada":
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
    return psycopg2.connect(DATABASE_URL, sslmode="require")

def init_db() -> None:
    with get_db() as conn, conn.cursor() as cur:
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
                first_seen_at TIMESTAMPTZ DEFAULT NOW(),
                last_seen_at TIMESTAMPTZ DEFAULT NOW(),
                applied_status TEXT DEFAULT 'unknown',
                applied_updated_at TIMESTAMPTZ
            );
            CREATE UNIQUE INDEX IF NOT EXISTS jobs_link_idx ON jobs(link);
            CREATE TABLE IF NOT EXISTS source_health (
                source TEXT PRIMARY KEY,
                consecutive_errors INT DEFAULT 0,
                last_error TEXT,
                last_success_at TIMESTAMPTZ
            );
            CREATE TABLE IF NOT EXISTS bot_state (key TEXT PRIMARY KEY, value TEXT);
        """)
        conn.commit()

def get_soup(url: str, retries: int = 3) -> Optional[BeautifulSoup]:
    for i in range(retries):
        try:
            headers = {
                "User-Agent": random.choice(USER_AGENTS),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "es-CL,es;q=0.9,en;q=0.8",
                "Accept-Encoding": "gzip, deflate, br",
                "DNT": "1",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Cache-Control": "max-age=0"
            }
            
            session = requests.Session()
            session.headers.update(headers)
            
            time.sleep(random.uniform(0.5, 1.5))
            
            r = session.get(url, timeout=(15, 30), allow_redirects=True)
            r.raise_for_status()
            
            log.debug(f"GET {url} → {r.status_code} ({len(r.text)} bytes)")
            
            if len(r.text) < 500:
                log.warning(f"Respuesta muy corta para {url}: {len(r.text)} bytes")
                continue
            
            return BeautifulSoup(r.text, "html.parser")
            
        except Exception as e:
            log.warning(f"GET {url} intento {i+1}/{retries}: {e}")
            if i < retries - 1:
                time.sleep(random.uniform(2, 4))
    
    return None

def telegram_send(msg: str) -> Optional[int]:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return None
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data={"chat_id": TELEGRAM_CHAT_ID, "text": msg[:MAX_MSG]},
            timeout=10
        )
        if r.ok:
            return r.json().get("result", {}).get("message_id")
    except Exception as e:
        log.error(f"Telegram error: {e}")
    return None

def formatear_oferta(o: Oferta, code: str, status: str) -> str:
    estado_emoji = {"applied": "✅", "not_applied": "❌", "unknown": "❓"}[status]
    return f"""🆕 [{o.source}] {o.title}
━━━━━━━━━━
🆔 {code}
🏢 {o.company}
📅 {o.date_text}
💰 {o.salary}
🕒 {o.jornada}
📮 {estado_emoji} {status}
━━━━━━━━━━
{limpiar_desc(o.description)[:300]}
━━━━━━━━━━
🔗 {o.link}
/postule {code} | /nopostule {code}"""

def set_source_ok(source: str) -> None:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO source_health(source, consecutive_errors, last_success_at) VALUES (%s, 0, NOW()) "
            "ON CONFLICT (source) DO UPDATE SET consecutive_errors=0, last_error=NULL, last_success_at=NOW()",
            (source,)
        )
        conn.commit()

def set_source_error(source: str, err: str) -> int:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO source_health(source, consecutive_errors, last_error) VALUES (%s, 1, %s) "
            "ON CONFLICT (source) DO UPDATE SET consecutive_errors=source_health.consecutive_errors+1, last_error=EXCLUDED.last_error "
            "RETURNING consecutive_errors",
            (source, err[:250])
        )
        count = cur.fetchone()[0]
        conn.commit()
    return count

def should_cooldown(source: str) -> bool:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("SELECT consecutive_errors FROM source_health WHERE source=%s", (source,))
        row = cur.fetchone()
        return (row[0] >= 3) if row else False

# ==================== PARSERS ESPECÍFICOS ====================

def parse_chiletrabajos() -> List[Oferta]:
    """Chiletrabajos - Parser especializado con múltiples estrategias"""
    source = "Chiletrabajos"
    if should_cooldown(source):
        log.info(f"{source} en cooldown")
        return []
    
    out: List[Oferta] = []
    base = "https://www.chiletrabajos.cl"
    
    try:
        # ESTRATEGIA 1: Listado por ciudad
        for offset in [0, 30, 60]:
            url = f"{base}/ciudad/osorno.html" + (f"/{offset}" if offset > 0 else "")
            log.debug(f"Scrapeando {url}")
            
            soup = get_soup(url, retries=3)
            if not soup:
                continue
            
            # Buscar todos los H2 con enlaces a trabajos
            job_links = []
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
                
                # Info básica del listado
                empresa = "No especificada"
                h3 = h2.find_next_sibling("h3")
                if h3:
                    empresa_text = limpiar(h3.get_text())
                    if empresa_text and not re.match(r'^\d{1,2}\.\d{3}\.\d{3}', empresa_text):
                        empresa = empresa_text.split(",")[0]
                
                job_links.append((link, title, empresa))
            
            log.info(f"{source} encontró {len(job_links)} enlaces en {url}")
            
            # Procesar cada trabajo
            for link, title, empresa_base in job_links:
                try:
                    det_soup = get_soup(link, retries=2)
                    if not det_soup:
                        out.append(Oferta(
                            source=source,
                            title=title,
                            link=link,
                            company=empresa_base,
                            location_verified=True
                        ))
                        continue
                    
                    # Extraer de tabla
                    empresa = empresa_base
                    fecha = "No especificada"
                    sueldo = "No especificado"
                    jornada = "No especificada"
                    
                    for td in det_soup.select("table td"):
                        txt = limpiar(td.get_text()).lower()
                        sib = td.find_next_sibling("td")
                        if not sib:
                            continue
                        
                        val = limpiar(sib.get_text())
                        
                        if "fecha" in txt and "expira" not in txt:
                            fecha = val
                        elif "salario" in txt or "sueldo" in txt or "remuneración" in txt:
                            if val and val.lower() not in ["no especificado", "a convenir"]:
                                sueldo = val if val.startswith("$") else f"${val}"
                        elif "tipo" in txt or "jornada" in txt:
                            jornada = val
                        elif ("buscado" in txt or "empresa" in txt) and not re.match(r'\d{1,2}\.\d{3}', val):
                            empresa = val
                    
                    # Descripción limpia
                    desc_parts = []
                    for tag in det_soup.find_all(["p", "div", "li"]):
                        txt = limpiar(tag.get_text())
                        if 40 < len(txt) < 500:
                            if not any(noise in txt for noise in ["PUBLICIDAD", "Volver", "Buscar", "Guardar"]):
                                desc_parts.append(txt)
                        if len(desc_parts) >= 2:
                            break
                    
                    desc = " ".join(desc_parts) if desc_parts else "No disponible"
                    
                    out.append(Oferta(
                        source=source,
                        title=title,
                        link=link,
                        company=empresa,
                        date_text=fecha,
                        salary=sueldo,
                        jornada=jornada,
                        description=desc,
                        location_verified=True
                    ))
                    
                    time.sleep(random.uniform(0.3, 0.7))
                    
                except Exception as e:
                    log.warning(f"Error procesando {link}: {e}")
                    continue
            
            time.sleep(random.uniform(1, 2))
        
        set_source_ok(source)
        log.info(f"✓ {source}: {len(out)} ofertas extraídas")
        
    except Exception as e:
        log.exception(f"✗ {source} error: {e}")
        set_source_error(source, str(e))
    
    return dedup(out)

def parse_bne() -> List[Oferta]:
    """BNE - Estrategias múltiples"""
    source = "BNE"
    if should_cooldown(source):
        return []
    
    out: List[Oferta] = []
    seen: Set[str] = set()
    
    try:
        # Múltiples variantes de búsqueda
        urls = [
            "https://www.bne.cl/ofertas?ubicacion=Osorno",
            "https://www.bne.cl/ofertas?comuna=Osorno",
            "https://www.bne.cl/ofertas?textoBusqueda=&ubicacion=Osorno",
        ]
        
        for url in urls:
            soup = get_soup(url, retries=3)
            if not soup:
                continue
            
            log.debug(f"Scrapeando {url}")
            
            # ESTRATEGIA 1: Buscar todos los enlaces con clase/id de oferta
            for selector in ["a.job-link", "a[href*='oferta']", "a[href*='empleo']", "div.job-card a", "article a"]:
                for a in soup.select(selector):
                    href = a.get("href", "")
                    if not href:
                        continue
                    
                    # Filtros
                    if any(x in href.lower() for x in ["filtro", "buscar", "categoria", "page="]):
                        continue
                    
                    text = limpiar(a.get_text())
                    if len(text) < 8:
                        continue
                    
                    link = href if href.startswith("http") else urljoin("https://www.bne.cl", href)
                    
                    if link in seen:
                        continue
                    seen.add(link)
                    
                    out.append(Oferta(
                        source=source,
                        title=text,
                        link=link,
                        location_verified=True
                    ))
            
            # ESTRATEGIA 2: Buscar por texto que parezca título de trabajo
            for a in soup.find_all("a", href=True):
                href = a.get("href", "")
                text = limpiar(a.get_text())
                
                if len(text) < 10 or len(text) > 100:
                    continue
                
                # Debe parecer un título de trabajo
                if not any(keyword in text.lower() for keyword in ["vendedor", "asistente", "técnico", "ejecutivo", "analista", "ingeniero", "administr", "operador", "supervisor", "coordinador"]):
                    continue
                
                if "/oferta" not in href.lower():
                    continue
                
                link = href if href.startswith("http") else urljoin("https://www.bne.cl", href)
                
                if link in seen:
                    continue
                seen.add(link)
                
                out.append(Oferta(
                    source=source,
                    title=text,
                    link=link,
                    location_verified=True
                ))
            
            time.sleep(random.uniform(1, 2))
        
        set_source_ok(source)
        log.info(f"✓ {source}: {len(out)} ofertas extraídas")
        
    except Exception as e:
        log.exception(f"✗ {source} error: {e}")
        set_source_error(source, str(e))
    
    return dedup(out)

def parse_indeed() -> List[Oferta]:
    """Indeed RSS"""
    source = "Indeed"
    if should_cooldown(source):
        return []
    
    out: List[Oferta] = []
    
    try:
        feed_url = f"https://cl.indeed.com/rss?q={quote_plus('osorno')}&l={quote_plus('Osorno, Los Lagos')}&sort=date&limit=50"
        log.debug(f"Feed: {feed_url}")
        
        feed = feedparser.parse(feed_url)
        
        log.info(f"Indeed RSS retornó {len(feed.entries)} entradas")
        
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
        log.info(f"✓ {source}: {len(out)} ofertas extraídas")
        
    except Exception as e:
        log.exception(f"✗ {source} error: {e}")
        set_source_error(source, str(e))
    
    return dedup(out)

def parse_computrabajo() -> List[Oferta]:
    """Computrabajo - Estrategia ultra-específica"""
    source = "Computrabajo"
    if should_cooldown(source):
        return []
    
    out: List[Oferta] = []
    seen: Set[str] = set()
    base = "https://cl.computrabajo.com"
    
    try:
        urls = [
            f"{base}/empleos-en-los-lagos-en-osorno",
            f"{base}/empleos-en-los-lagos-en-osorno?p=2",
        ]
        
        for url in urls:
            soup = get_soup(url, retries=3)
            if not soup:
                continue
            
            log.debug(f"Scrapeando {url}")
            
            # ESTRATEGIA 1: Artículos de oferta
            for article in soup.find_all("article"):
                a = article.find("a", href=True)
                if not a:
                    continue
                
                href = a.get("href", "")
                if "oferta" not in href.lower():
                    continue
                
                # Debe terminar en hash.html
                if not re.search(r'-[0-9a-f]{6,}\.html?$', href, re.I):
                    continue
                
                text = limpiar(a.get_text())
                if len(text) < 8:
                    # Buscar en H2/H3 del article
                    for h in article.find_all(["h2", "h3"]):
                        text = limpiar(h.get_text())
                        if len(text) >= 8:
                            break
                
                if len(text) < 8:
                    continue
                
                link = href if href.startswith("http") else f"{base}{href}"
                
                if link in seen:
                    continue
                seen.add(link)
                
                out.append(Oferta(
                    source=source,
                    title=text,
                    link=link,
                    location_verified=True
                ))
            
            # ESTRATEGIA 2: Todos los enlaces con pattern de oferta
            for a in soup.find_all("a", href=True):
                href = a.get("href", "")
                
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
                
                out.append(Oferta(
                    source=source,
                    title=text,
                    link=link,
                    location_verified=True
                ))
            
            time.sleep(random.uniform(1, 2))
        
        set_source_ok(source)
        log.info(f"✓ {source}: {len(out)} ofertas extraídas")
        
    except Exception as e:
        log.exception(f"✗ {source} error: {e}")
        set_source_error(source, str(e))
    
    return dedup(out)

def parse_yapo() -> List[Oferta]:
    """Yapo - Filtrado estricto"""
    source = "Yapo"
    if should_cooldown(source):
        return []
    
    out: List[Oferta] = []
    seen: Set[str] = set()
    
    try:
        urls = [
            "https://www.yapo.cl/empleos?ca=12_s&l=0&q=osorno",
            "https://www.yapo.cl/empleos?ca=12_s&l=0&q=osorno&o=25",
        ]
        
        for url in urls:
            soup = get_soup(url, retries=3)
            if not soup:
                continue
            
            log.debug(f"Scrapeando {url}")
            
            for a in soup.find_all("a", href=True):
                href = a.get("href", "")
                
                # Debe terminar en números.html
                if not re.search(r'\d{5,}\.html?$', href):
                    continue
                
                # Debe contener empleo/trabajo
                if not any(x in href.lower() for x in ["/empleo", "/trabajo"]):
                    continue
                
                # Rechazar otras categorías
                if any(x in href.lower() for x in ["/arriendo", "/venta", "/auto", "/servicio", "/inmueble"]):
                    continue
                
                text = limpiar(a.get_text())
                if len(text) < 8:
                    continue
                
                # Rechazar navegación
                if re.match(r'^(ver|filtrar|buscar|ordenar)', text, re.I):
                    continue
                
                link = href if href.startswith("http") else urljoin("https://www.yapo.cl", href)
                
                if link in seen:
                    continue
                seen.add(link)
                
                out.append(Oferta(
                    source=source,
                    title=text,
                    link=link,
                    location_verified=True
                ))
            
            time.sleep(random.uniform(1, 2))
        
        set_source_ok(source)
        log.info(f"✓ {source}: {len(out)} ofertas extraídas")
        
    except Exception as e:
        log.exception(f"✗ {source} error: {e}")
        set_source_error(source, str(e))
    
    return dedup(out)

def dedup(items: List[Oferta]) -> List[Oferta]:
    seen: Set[str] = set()
    out: List[Oferta] = []
    for o in items:
        k = o.link.strip().lower()
        if k and k not in seen:
            seen.add(k)
            out.append(o)
    return out

def upsert_job(o: Oferta) -> Tuple[int, str, str]:
    job_code = f"{o.source[:2].upper()}-{short_hash(o.link)}"
    fingerprint = short_hash(o.link)
    
    with get_db() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        try:
            cur.execute("""
                INSERT INTO jobs(job_code, source, title, company, link, date_text, salary, jornada, description, fingerprint, last_seen_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                ON CONFLICT (link) DO UPDATE SET last_seen_at=NOW()
                RETURNING id, applied_status, (xmax = 0) AS inserted
            """, (job_code, o.source, o.title, o.company, o.link, o.date_text, o.salary, o.jornada, o.description, fingerprint))
            
            row = cur.fetchone()
            if not row:
                cur.execute("SELECT id, applied_status FROM jobs WHERE link=%s", (o.link,))
                row = cur.fetchone()
                return int(row["id"]), str(row["applied_status"]), "exists"
            
            conn.commit()
            return int(row["id"]), str(row["applied_status"]), "inserted" if row["inserted"] else "updated"
            
        except psycopg2.errors.UniqueViolation:
            conn.rollback()
            cur.execute("SELECT id, applied_status FROM jobs WHERE fingerprint=%s", (fingerprint,))
            row = cur.fetchone()
            return int(row["id"]), str(row["applied_status"]), "exists" if row else (0, "unknown", "error")

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
        return ok

def process_commands() -> None:
    # Implementación simplificada de comandos Telegram
    pass

def run_cycle() -> Dict:
    parsers = [
        parse_chiletrabajos,
        parse_bne,
        parse_indeed,
        parse_computrabajo,
        parse_yapo,
    ]
    
    all_ofertas: List[Oferta] = []
    per_source: Dict[str, int] = {}
    
    for parser in parsers:
        try:
            ofertas = parser()
            all_ofertas.extend(ofertas)
            name = parser.__name__.replace("parse_", "").capitalize()
            per_source[name] = len(ofertas)
        except Exception as e:
            log.exception(f"Parser {parser.__name__} falló: {e}")
            per_source[parser.__name__.replace("parse_", "").capitalize()] = 0
    
    new_count = 0
    existing_count = 0
    
    for o in dedup(all_ofertas):
        if not pasa_filtros(o):
            continue
        
        job_id, status, op = upsert_job(o)
        
        if op == "inserted":
            new_count += 1
            code = f"{o.source[:2].upper()}-{short_hash(o.link)}"
            if score_oferta(o) >= MIN_SCORE_IMMEDIATE:
                msg = formatear_oferta(o, code, status)
                telegram_send(msg)
        else:
            existing_count += 1
    
    cycle_num = get_state_int("cycle_counter", 0) + 1
    set_state_str("cycle_counter", str(cycle_num))
    
    log.info(f"═══ CICLO {cycle_num} ═══")
    log.info(f"Total encontradas: {len(all_ofertas)}")
    log.info(f"Nuevas: {new_count}")
    log.info(f"Ya existentes: {existing_count}")
    log.info(f"Por fuente: {per_source}")
    log.info("═══════════════")
    
    if cycle_num % HEARTBEAT_EVERY_CYCLES == 0:
        msg = f"🫀 CICLO {cycle_num}\n━━━━━━━━━━\n📥 Total: {len(all_ofertas)}\n🆕 Nuevas: {new_count}\n📚 Existentes: {existing_count}\n\n"
        msg += "\n".join([f"{k}: {v}" for k, v in per_source.items()])
        telegram_send(msg)
    
    return {"cycle": cycle_num, "new": new_count, "total": len(all_ofertas), "per_source": per_source}

def main():
    if not all([DATABASE_URL, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID]):
        raise RuntimeError("Faltan variables de entorno")
    
    init_db()
    telegram_send("🚀 BOT OSORNO v3\n━━━━━━━━━━\nParsers anti-detección activos\n5 fuentes verificadas\n━━━━━━━━━━")
    
    while True:
        start = time.time()
        try:
            process_commands()
            run_cycle()
        except Exception as e:
            log.exception(f"Ciclo error: {e}")
            telegram_send(f"❌ Error: {str(e)[:200]}")
        
        elapsed = time.time() - start
        sleep_time = max(15, INTERVALO - int(elapsed))
        log.info(f"Siguiente ciclo en {sleep_time}s\n")
        time.sleep(sleep_time)

if __name__ == "__main__":
    main()
