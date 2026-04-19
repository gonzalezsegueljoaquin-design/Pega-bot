#!/usr/bin/env python3
"""
Bot Osorno v6 DEFINITIVO
- 5-7 estrategias de extracción por fuente
- Sistema anti-fallas total con fallbacks
- Formato Telegram profesional
- Logging ultra-detallado
- Schema DB compatible
"""

import json
import logging
import os
import re
import time
import hashlib
import html
import random
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Set
from urllib.parse import quote_plus, urljoin

import feedparser
import psycopg2
import psycopg2.extras
import requests
from bs4 import BeautifulSoup

# Configuración
INTERVALO = int(os.getenv("INTERVALO", "90"))
MAX_MSG = 4096
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
MIN_SCORE_IMMEDIATE = int(os.getenv("MIN_SCORE_IMMEDIATE", "0"))
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

OSORNO_SIGNALS = ["osorno", "los lagos", "región de los lagos", "region de los lagos", "5290", "rahue"]
KEYWORDS_EXCLUDE = [k.strip().lower() for k in os.getenv("KEYWORDS_EXCLUDE", "").split(",") if k.strip()]

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format='%(asctime)s [%(levelname)s] %(message)s'
)
log = logging.getLogger("bot")

@dataclass
class Requisito:
    tipo: str
    texto: str
    
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
    requisitos: List[Requisito] = field(default_factory=list)
    resumen: str = "No disponible"
    location_verified: bool = False

def limpiar(txt: str) -> str:
    return re.sub(r"\s+", " ", str(txt or "")).strip()

def extraer_requisitos(descripcion: str) -> List[Requisito]:
    requisitos = []
    patrones_seccion = [
        r"(?:requisitos?|requerimientos?|perfil|condiciones?|exigencias?)[\s:]+",
        r"(?:se requiere|buscamos|necesitamos)[\s:]+",
    ]
    
    texto = descripcion.lower()
    for patron in patrones_seccion:
        match = re.search(patron, texto, re.IGNORECASE)
        if match:
            inicio = match.end()
            resto = descripcion[inicio:]
            
            for linea in resto.split('\n'):
                linea = limpiar(linea)
                if len(linea) < 15 or len(linea) > 200:
                    continue
                
                tipo = "general"
                if any(x in linea.lower() for x in ['título', 'estudios', 'técnic', 'media']):
                    tipo = "educacion"
                elif any(x in linea.lower() for x in ['años', 'experiencia', 'año']):
                    tipo = "experiencia"
                elif any(x in linea.lower() for x in ['office', 'excel', 'inglés', 'licencia']):
                    tipo = "habilidad"
                
                linea = re.sub(r'^[\-\•\*\+]+\s*', '', linea)
                if linea and len(linea) > 10:
                    requisitos.append(Requisito(tipo=tipo, texto=linea))
                if len(requisitos) >= 8:
                    break
            if requisitos:
                break
    return requisitos[:8]

def generar_resumen(descripcion: str, max_words: int = 50) -> str:
    desc = descripcion
    for noise in ["PUBLICIDAD", "Volver", "Buscar"]:
        desc = desc.replace(noise, "")
    
    oraciones = re.split(r'[.!?]+', desc)
    significativas = []
    
    for oracion in oraciones:
        oracion = limpiar(oracion)
        if len(oracion) < 30:
            continue
        significativas.append(oracion)
        if len(significativas) >= 2:
            break
    
    if not significativas:
        return "No disponible"
    
    resumen = '. '.join(significativas) + '.'
    palabras = resumen.split()
    if len(palabras) > max_words:
        resumen = ' '.join(palabras[:max_words]) + '...'
    return resumen

def short_hash(*parts: str) -> str:
    return hashlib.sha1(limpiar("|".join(parts)).lower().encode()).hexdigest()[:10].upper()

def contiene_osorno(txt: str) -> bool:
    return any(sig in txt.lower() for sig in OSORNO_SIGNALS)

def pasa_filtros(o: Oferta) -> bool:
    txt = f"{o.title} {o.company} {o.description} {o.link}".lower()
    
    if o.location_verified:
        if KEYWORDS_EXCLUDE and any(k in txt for k in KEYWORDS_EXCLUDE):
            return False
        return True
    
    if not contiene_osorno(txt):
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
                requisitos JSONB DEFAULT '[]'::jsonb,
                resumen TEXT DEFAULT 'No disponible',
                fingerprint TEXT NOT NULL,
                first_seen_at TIMESTAMPTZ DEFAULT NOW(),
                last_seen_at TIMESTAMPTZ DEFAULT NOW(),
                applied_status TEXT DEFAULT 'unknown'
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
                "Cache-Control": "max-age=0"
            }
            
            session = requests.Session()
            session.headers.update(headers)
            
            time.sleep(random.uniform(0.5, 1.5))
            
            r = session.get(url, timeout=(15, 30), allow_redirects=True)
            r.raise_for_status()
            
            log.debug(f"✅ GET {url[:80]}... → {r.status_code} ({len(r.text)} bytes)")
            
            if len(r.text) < 500:
                log.warning(f"⚠️ Respuesta corta: {len(r.text)} bytes")
                if i < retries - 1:
                    continue
            
            return BeautifulSoup(r.text, "html.parser")
            
        except Exception as e:
            log.warning(f"❌ Intento {i+1}/{retries}: {e}")
            if i < retries - 1:
                time.sleep(random.uniform(2, 4))
    
    log.error(f"🚫 FALLÓ: {url[:80]}...")
    return None

def telegram_send(msg: str) -> Optional[int]:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return None
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data={"chat_id": TELEGRAM_CHAT_ID, "text": msg[:MAX_MSG], "parse_mode": "HTML"},
            timeout=10
        )
        if r.ok:
            return r.json().get("result", {}).get("message_id")
    except Exception as e:
        log.error(f"Telegram error: {e}")
    return None

def formatear_oferta(o: Oferta, code: str, status: str) -> str:
    estado_emoji = {"applied": "✅", "not_applied": "❌", "unknown": "❓"}[status]
    
    # Header más compacto y profesional
    msg = f"""<b>🆕 NUEVA OFERTA - {o.source.upper()}</b>
━━━━━━━━━━━━━━━━━━━━

<b>📌 {html.escape(o.title)}</b>

🆔 <code>{code}</code>
🏢 {html.escape(o.company)}
💰 {html.escape(o.salary)}
🕒 {html.escape(o.jornada)}
📅 {html.escape(o.date_text)}
📮 {estado_emoji} <i>{status}</i>"""
    
    # Resumen
    if o.resumen and o.resumen != "No disponible":
        msg += f"\n\n<b>💼 RESUMEN</b>\n{html.escape(o.resumen[:250])}"
    
    # Requisitos
    if o.requisitos:
        msg += "\n\n<b>✅ REQUISITOS</b>"
        for req in o.requisitos[:4]:
            icon = {"educacion": "🎓", "experiencia": "💼", "habilidad": "⚡"}.get(req.tipo, "•")
            msg += f"\n{icon} {html.escape(req.texto[:90])}"
    
    msg += f"\n\n🔗 <a href='{o.link}'>Ver oferta completa</a>"
    msg += f"\n\n<code>/postule {code}</code> | <code>/nopostule {code}</code>"
    
    return msg[:MAX_MSG]

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
        if row and row[0] >= 5:
            log.warning(f"🔥 {source} en COOLDOWN ({row[0]} errores)")
            return True
        return False

# ==================== PARSERS ULTRA-ROBUSTOS ====================

def parse_chiletrabajos() -> Tuple[List[Oferta], Dict]:
    """
    7 ESTRATEGIAS DE EXTRACCIÓN:
    1. H2 > A con /trabajo/
    2. H3 > A con /trabajo/
    3. Div[class*=job|oferta] > A
    4. TODOS los A con /trabajo/
    5. A con text que parezca título
    6. Article > A
    7. Li > A con /trabajo/
    """
    source = "Chiletrabajos"
    if should_cooldown(source):
        return [], {"error": "cooldown"}
    
    out = []
    stats = {"paginas": 0, "estrategias": {f"E{i}": 0 for i in range(1, 8)}, "total_enlaces": 0}
    base = "https://www.chiletrabajos.cl"
    
    try:
        for offset in [0, 30, 60]:
            url = f"{base}/ciudad/osorno.html" + (f"/{offset}" if offset > 0 else "")
            log.info(f"🌐 [{source}] Página {offset//30 + 1}: {url}")
            
            soup = get_soup(url, retries=3)
            if not soup:
                continue
            
            stats["paginas"] += 1
            job_links = []
            
            # ESTRATEGIA 1: H2 > A
            for h2 in soup.find_all("h2"):
                a = h2.find("a", href=True)
                if a and "/trabajo/" in a.get("href", ""):
                    href = a.get("href")
                    link = href if href.startswith("http") else f"{base}{href}"
                    title = limpiar(a.get_text())
                    if len(title) >= 5 and link not in [x[0] for x in job_links]:
                        job_links.append((link, title, "No especificada", "E1"))
                        stats["estrategias"]["E1"] += 1
            
            # ESTRATEGIA 2: H3 > A
            for h3 in soup.find_all("h3"):
                a = h3.find("a", href=True)
                if a and "/trabajo/" in a.get("href", ""):
                    href = a.get("href")
                    link = href if href.startswith("http") else f"{base}{href}"
                    title = limpiar(a.get_text())
                    if len(title) >= 5 and link not in [x[0] for x in job_links]:
                        job_links.append((link, title, "No especificada", "E2"))
                        stats["estrategias"]["E2"] += 1
            
            # ESTRATEGIA 3: Divs con clase job/oferta
            for div in soup.find_all("div", class_=re.compile(r'(job|oferta|trabajo|empleo|card|item)', re.I)):
                a = div.find("a", href=True)
                if a and "/trabajo/" in a.get("href", ""):
                    href = a.get("href")
                    link = href if href.startswith("http") else f"{base}{href}"
                    title = limpiar(a.get_text())
                    if len(title) >= 5 and link not in [x[0] for x in job_links]:
                        job_links.append((link, title, "No especificada", "E3"))
                        stats["estrategias"]["E3"] += 1
            
            # ESTRATEGIA 4: TODOS los A con /trabajo/
            if len(job_links) < 15:
                for a in soup.find_all("a", href=True):
                    href = a.get("href", "")
                    if "/trabajo/" in href:
                        link = href if href.startswith("http") else f"{base}{href}"
                        title = limpiar(a.get_text())
                        if len(title) >= 5 and link not in [x[0] for x in job_links]:
                            job_links.append((link, title, "No especificada", "E4"))
                            stats["estrategias"]["E4"] += 1
            
            # ESTRATEGIA 5: A con texto que parezca título de trabajo
            if len(job_links) < 10:
                for a in soup.find_all("a", href=True):
                    text = limpiar(a.get_text())
                    if 10 < len(text) < 100 and any(kw in text.lower() for kw in ["vendedor", "asistente", "técnico", "ejecutivo", "analista", "operador", "supervisor"]):
                        href = a.get("href", "")
                        if "/trabajo/" in href or "empleo" in href.lower():
                            link = href if href.startswith("http") else f"{base}{href}"
                            if link not in [x[0] for x in job_links]:
                                job_links.append((link, text, "No especificada", "E5"))
                                stats["estrategias"]["E5"] += 1
            
            # ESTRATEGIA 6: Article > A
            if len(job_links) < 5:
                for article in soup.find_all("article"):
                    a = article.find("a", href=True)
                    if a:
                        href = a.get("href", "")
                        if "/trabajo/" in href:
                            link = href if href.startswith("http") else f"{base}{href}"
                            title = limpiar(a.get_text())
                            if len(title) >= 5 and link not in [x[0] for x in job_links]:
                                job_links.append((link, title, "No especificada", "E6"))
                                stats["estrategias"]["E6"] += 1
            
            # ESTRATEGIA 7: Li > A
            if len(job_links) < 5:
                for li in soup.find_all("li"):
                    a = li.find("a", href=True)
                    if a and "/trabajo/" in a.get("href", ""):
                        href = a.get("href")
                        link = href if href.startswith("http") else f"{base}{href}"
                        title = limpiar(a.get_text())
                        if len(title) >= 5 and link not in [x[0] for x in job_links]:
                            job_links.append((link, title, "No especificada", "E7"))
                            stats["estrategias"]["E7"] += 1
            
            stats["total_enlaces"] += len(job_links)
            log.info(f"📊 Encontrados: {len(job_links)} enlaces - {dict([(k, v) for k, v in stats['estrategias'].items() if v > 0])}")
            
            # Procesar ofertas
            for link, title, empresa, estrategia in job_links[:30]:
                try:
                    det_soup = get_soup(link, retries=2)
                    descripcion = "No disponible"
                    fecha = "No especificada"
                    sueldo = "No especificado"
                    jornada = "No especificada"
                    
                    if det_soup:
                        # Extraer detalles de tabla
                        for td in det_soup.select("table td"):
                            txt = limpiar(td.get_text()).lower()
                            sib = td.find_next_sibling("td")
                            if not sib:
                                continue
                            val = limpiar(sib.get_text())
                            if "fecha" in txt and "expira" not in txt:
                                fecha = val
                            elif "salario" in txt or "sueldo" in txt:
                                if val and val.lower() not in ["no especificado", "a convenir"]:
                                    sueldo = val if val.startswith("$") else f"${val}"
                            elif "tipo" in txt or "jornada" in txt:
                                jornada = val
                        
                        # Descripción
                        desc_parts = []
                        for tag in det_soup.find_all(["p", "div", "li"]):
                            txt = limpiar(tag.get_text())
                            if 30 < len(txt) < 1000:
                                if not any(noise in txt for noise in ["PUBLICIDAD", "Volver"]):
                                    desc_parts.append(txt)
                                    if len(desc_parts) >= 5:
                                        break
                        descripcion = " ".join(desc_parts) if desc_parts else "No disponible"
                    
                    requisitos = extraer_requisitos(descripcion)
                    resumen = generar_resumen(descripcion)
                    
                    out.append(Oferta(
                        source=source,
                        title=title,
                        link=link,
                        company=empresa,
                        date_text=fecha,
                        salary=sueldo,
                        jornada=jornada,
                        description=descripcion,
                        requisitos=requisitos,
                        resumen=resumen,
                        location_verified=True
                    ))
                    
                    time.sleep(random.uniform(0.3, 0.7))
                    
                except Exception as e:
                    log.warning(f"⚠️ Error procesando {link[:60]}...: {e}")
                    # Guardar con info básica
                    out.append(Oferta(
                        source=source,
                        title=title,
                        link=link,
                        location_verified=True
                    ))
            
            time.sleep(random.uniform(1, 2))
        
        set_source_ok(source)
        log.info(f"✅ [{source}] COMPLETADO: {len(out)} ofertas")
        
    except Exception as e:
        log.exception(f"❌ [{source}] ERROR: {e}")
        set_source_error(source, str(e))
    
    return dedup(out), stats

def parse_bne() -> Tuple[List[Oferta], Dict]:
    """
    6 ESTRATEGIAS:
    1. a.job-link, a[href*='oferta']
    2. Divs con clase job/oferta > A
    3. A con keywords de trabajo en texto
    4. Article > A
    5. TODOS los A con "oferta" en href
    6. Table > A con href relevante
    """
    source = "BNE"
    if should_cooldown(source):
        return [], {"error": "cooldown"}
    
    out = []
    stats = {"estrategias": {f"E{i}": 0 for i in range(1, 7)}, "total_enlaces": 0}
    seen = set()
    
    try:
        urls = [
            "https://www.bne.cl/ofertas?ubicacion=Osorno",
            "https://www.bne.cl/ofertas?comuna=Osorno",
            "https://www.bne.cl/trabajos/osorno",
            "https://www.bne.cl/empleos/osorno",
        ]
        
        for url in urls:
            log.info(f"🌐 [{source}] Scrapeando: {url}")
            soup = get_soup(url, retries=3)
            if not soup:
                continue
            
            # E1: Selectores específicos
            for selector in ["a.job-link", "a[href*='oferta']", "a[href*='empleo']"]:
                for a in soup.select(selector):
                    href = a.get("href", "")
                    text = limpiar(a.get_text())
                    if len(text) >= 8:
                        link = href if href.startswith("http") else urljoin("https://www.bne.cl", href)
                        if link not in seen:
                            seen.add(link)
                            out.append(Oferta(source=source, title=text, link=link, location_verified=True))
                            stats["estrategias"]["E1"] += 1
            
            # E2: Divs con clase
            for div in soup.find_all("div", class_=re.compile(r'(job|oferta|empleo)', re.I)):
                a = div.find("a", href=True)
                if a:
                    href = a.get("href", "")
                    text = limpiar(a.get_text())
                    if len(text) >= 8:
                        link = href if href.startswith("http") else urljoin("https://www.bne.cl", href)
                        if link not in seen:
                            seen.add(link)
                            out.append(Oferta(source=source, title=text, link=link, location_verified=True))
                            stats["estrategias"]["E2"] += 1
            
            # E3: A con keywords
            for a in soup.find_all("a", href=True):
                text = limpiar(a.get_text())
                if 10 < len(text) < 100:
                    if any(kw in text.lower() for kw in ["vendedor", "asistente", "técnico", "ejecutivo", "analista", "operador", "supervisor", "coordinador", "ingeniero"]):
                        href = a.get("href", "")
                        link = href if href.startswith("http") else urljoin("https://www.bne.cl", href)
                        if link not in seen and ("oferta" in link.lower() or "empleo" in link.lower() or "trabajo" in link.lower()):
                            seen.add(link)
                            out.append(Oferta(source=source, title=text, link=link, location_verified=True))
                            stats["estrategias"]["E3"] += 1
            
            # E4: Article > A
            for article in soup.find_all("article"):
                a = article.find("a", href=True)
                if a:
                    text = limpiar(a.get_text())
                    href = a.get("href", "")
                    if len(text) >= 8:
                        link = href if href.startswith("http") else urljoin("https://www.bne.cl", href)
                        if link not in seen:
                            seen.add(link)
                            out.append(Oferta(source=source, title=text, link=link, location_verified=True))
                            stats["estrategias"]["E4"] += 1
            
            # E5: TODOS con "oferta" en href
            if len(out) < 10:
                for a in soup.find_all("a", href=True):
                    href = a.get("href", "")
                    if "oferta" in href.lower() or "empleo" in href.lower():
                        text = limpiar(a.get_text())
                        if len(text) >= 8:
                            link = href if href.startswith("http") else urljoin("https://www.bne.cl", href)
                            if link not in seen:
                                seen.add(link)
                                out.append(Oferta(source=source, title=text, link=link, location_verified=True))
                                stats["estrategias"]["E5"] += 1
            
            # E6: Table > A
            if len(out) < 5:
                for table in soup.find_all("table"):
                    for a in table.find_all("a", href=True):
                        text = limpiar(a.get_text())
                        href = a.get("href", "")
                        if len(text) >= 8:
                            link = href if href.startswith("http") else urljoin("https://www.bne.cl", href)
                            if link not in seen:
                                seen.add(link)
                                out.append(Oferta(source=source, title=text, link=link, location_verified=True))
                                stats["estrategias"]["E6"] += 1
            
            time.sleep(random.uniform(1, 2))
        
        stats["total_enlaces"] = len(out)
        set_source_ok(source)
        log.info(f"✅ [{source}] COMPLETADO: {len(out)} ofertas - {dict([(k, v) for k, v in stats['estrategias'].items() if v > 0])}")
        
    except Exception as e:
        log.exception(f"❌ [{source}] ERROR: {e}")
        set_source_error(source, str(e))
    
    return dedup(out), stats

def parse_indeed() -> Tuple[List[Oferta], Dict]:
    """Indeed RSS con fallback a scraping directo"""
    source = "Indeed"
    stats = {"rss": 0, "scraping": 0}
    
    if should_cooldown(source):
        return [], {"error": "cooldown"}
    
    out = []
    
    try:
        # Estrategia 1: RSS
        feed_url = f"https://cl.indeed.com/rss?q={quote_plus('osorno')}&l={quote_plus('Osorno, Los Lagos')}&sort=date&limit=50"
        log.info(f"🌐 [{source}] Intentando RSS")
        
        feed = feedparser.parse(feed_url)
        stats["rss"] = len(feed.entries)
        
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
            
            requisitos = extraer_requisitos(summary)
            resumen = generar_resumen(summary)
            
            out.append(Oferta(
                source=source,
                title=title,
                company=company,
                link=link,
                description=summary,
                requisitos=requisitos,
                resumen=resumen,
                location_verified=True
            ))
        
        # Estrategia 2: Scraping directo si RSS falla
        if len(out) == 0:
            log.info(f"⚠️ [{source}] RSS vacío, intentando scraping directo")
            direct_url = f"https://cl.indeed.com/jobs?q=osorno&l=Osorno, Los Lagos"
            soup = get_soup(direct_url, retries=2)
            
            if soup:
                for a in soup.find_all("a", href=True):
                    href = a.get("href", "")
                    if "/rc/clk?" in href or "/viewjob?" in href:
                        text = limpiar(a.get_text())
                        if len(text) >= 8:
                            link = href if href.startswith("http") else f"https://cl.indeed.com{href}"
                            out.append(Oferta(source=source, title=text, link=link, location_verified=True))
                            stats["scraping"] += 1
        
        set_source_ok(source)
        log.info(f"✅ [{source}] COMPLETADO: {len(out)} ofertas (RSS:{stats['rss']} Scraping:{stats['scraping']})")
        
    except Exception as e:
        log.exception(f"❌ [{source}] ERROR: {e}")
        set_source_error(source, str(e))
    
    return dedup(out), stats

def parse_computrabajo() -> Tuple[List[Oferta], Dict]:
    """
    5 ESTRATEGIAS:
    1. Article > A
    2. A con "oferta" en href
    3. Divs con clase oferta > A
    4. H2/H3 > A
    5. TODOS los A con pattern de hash
    """
    source = "Computrabajo"
    if should_cooldown(source):
        return [], {"error": "cooldown"}
    
    out = []
    stats = {"estrategias": {f"E{i}": 0 for i in range(1, 6)}, "total_enlaces": 0}
    seen = set()
    base = "https://cl.computrabajo.com"
    
    try:
        urls = [
            f"{base}/empleos-en-los-lagos-en-osorno",
            f"{base}/empleos-en-los-lagos-en-osorno?p=2",
            f"{base}/trabajo-de-osorno",
        ]
        
        for url in urls:
            log.info(f"🌐 [{source}] Scrapeando: {url}")
            soup = get_soup(url, retries=3)
            if not soup:
                continue
            
            # E1: Article > A
            for article in soup.find_all("article"):
                a = article.find("a", href=True)
                if a:
                    href = a.get("href", "")
                    if "oferta" in href.lower():
                        text = limpiar(a.get_text())
                        if not text or len(text) < 8:
                            for h in article.find_all(["h2", "h3"]):
                                text = limpiar(h.get_text())
                                if len(text) >= 8:
                                    break
                        if len(text) >= 8:
                            link = href if href.startswith("http") else f"{base}{href}"
                            if link not in seen:
                                seen.add(link)
                                out.append(Oferta(source=source, title=text, link=link, location_verified=True))
                                stats["estrategias"]["E1"] += 1
            
            # E2: A con "oferta" en href
            for a in soup.find_all("a", href=True):
                href = a.get("href", "")
                if "oferta" in href.lower():
                    text = limpiar(a.get_text())
                    if len(text) >= 8:
                        link = href if href.startswith("http") else f"{base}{href}"
                        if link not in seen:
                            seen.add(link)
                            out.append(Oferta(source=source, title=text, link=link, location_verified=True))
                            stats["estrategias"]["E2"] += 1
            
            # E3: Divs con clase oferta
            for div in soup.find_all("div", class_=re.compile(r'(oferta|job|empleo)', re.I)):
                a = div.find("a", href=True)
                if a:
                    href = a.get("href", "")
                    text = limpiar(a.get_text())
                    if len(text) >= 8:
                        link = href if href.startswith("http") else f"{base}{href}"
                        if link not in seen:
                            seen.add(link)
                            out.append(Oferta(source=source, title=text, link=link, location_verified=True))
                            stats["estrategias"]["E3"] += 1
            
            # E4: H2/H3 > A
            for h in soup.find_all(["h2", "h3"]):
                a = h.find("a", href=True)
                if a:
                    href = a.get("href", "")
                    text = limpiar(a.get_text())
                    if len(text) >= 8:
                        link = href if href.startswith("http") else f"{base}{href}"
                        if link not in seen and ("oferta" in link.lower() or re.search(r'-[0-9a-fA-F]{6,}', link)):
                            seen.add(link)
                            out.append(Oferta(source=source, title=text, link=link, location_verified=True))
                            stats["estrategias"]["E4"] += 1
            
            # E5: Pattern de hash
            if len(out) < 10:
                for a in soup.find_all("a", href=True):
                    href = a.get("href", "")
                    if re.search(r'-[0-9a-fA-F]{6,}\.html?', href):
                        text = limpiar(a.get_text())
                        if len(text) >= 8:
                            link = href if href.startswith("http") else f"{base}{href}"
                            if link not in seen:
                                seen.add(link)
                                out.append(Oferta(source=source, title=text, link=link, location_verified=True))
                                stats["estrategias"]["E5"] += 1
            
            time.sleep(random.uniform(1, 2))
        
        stats["total_enlaces"] = len(out)
        set_source_ok(source)
        log.info(f"✅ [{source}] COMPLETADO: {len(out)} ofertas - {dict([(k, v) for k, v in stats['estrategias'].items() if v > 0])}")
        
    except Exception as e:
        log.exception(f"❌ [{source}] ERROR: {e}")
        set_source_error(source, str(e))
    
    return dedup(out), stats

def parse_yapo() -> Tuple[List[Oferta], Dict]:
    """
    5 ESTRATEGIAS:
    1. A con \d{5,} en href + /empleo
    2. Divs con clase ad/aviso > A
    3. A con keywords en texto
    4. Article > A
    5. TODOS los A con número largo en href
    """
    source = "Yapo"
    if should_cooldown(source):
        return [], {"error": "cooldown"}
    
    out = []
    stats = {"estrategias": {f"E{i}": 0 for i in range(1, 6)}, "total_enlaces": 0}
    seen = set()
    
    try:
        urls = [
            "https://www.yapo.cl/empleos?ca=12_s&l=0&q=osorno",
            "https://www.yapo.cl/empleos?ca=12_s&l=0&q=osorno&o=25",
            "https://www.yapo.cl/region_de_los_lagos/empleos",
        ]
        
        for url in urls:
            log.info(f"🌐 [{source}] Scrapeando: {url}")
            soup = get_soup(url, retries=3)
            if not soup:
                continue
            
            # E1: Pattern principal
            for a in soup.find_all("a", href=True):
                href = a.get("href", "")
                if re.search(r'\d{5,}', href) and any(x in href.lower() for x in ["/empleo", "/trabajo", "/oferta"]):
                    if not any(x in href.lower() for x in ["/arriendo", "/venta", "/auto", "/servicio"]):
                        text = limpiar(a.get_text())
                        if len(text) >= 8:
                            link = href if href.startswith("http") else urljoin("https://www.yapo.cl", href)
                            if link not in seen:
                                seen.add(link)
                                out.append(Oferta(source=source, title=text, link=link, location_verified=True))
                                stats["estrategias"]["E1"] += 1
            
            # E2: Divs con clase ad/aviso
            for div in soup.find_all("div", class_=re.compile(r'(ad|aviso|listing|item)', re.I)):
                a = div.find("a", href=True)
                if a:
                    href = a.get("href", "")
                    text = limpiar(a.get_text())
                    if len(text) >= 8 and re.search(r'\d{5,}', href):
                        link = href if href.startswith("http") else urljoin("https://www.yapo.cl", href)
                        if link not in seen:
                            seen.add(link)
                            out.append(Oferta(source=source, title=text, link=link, location_verified=True))
                            stats["estrategias"]["E2"] += 1
            
            # E3: Keywords
            for a in soup.find_all("a", href=True):
                text = limpiar(a.get_text())
                if 10 < len(text) < 100:
                    if any(kw in text.lower() for kw in ["vendedor", "asistente", "técnico", "ejecutivo", "se busca", "se solicita"]):
                        href = a.get("href", "")
                        if re.search(r'\d{5,}', href):
                            link = href if href.startswith("http") else urljoin("https://www.yapo.cl", href)
                            if link not in seen:
                                seen.add(link)
                                out.append(Oferta(source=source, title=text, link=link, location_verified=True))
                                stats["estrategias"]["E3"] += 1
            
            # E4: Article
            for article in soup.find_all("article"):
                a = article.find("a", href=True)
                if a:
                    href = a.get("href", "")
                    text = limpiar(a.get_text())
                    if len(text) >= 8 and re.search(r'\d{5,}', href):
                        link = href if href.startswith("http") else urljoin("https://www.yapo.cl", href)
                        if link not in seen:
                            seen.add(link)
                            out.append(Oferta(source=source, title=text, link=link, location_verified=True))
                            stats["estrategias"]["E4"] += 1
            
            # E5: Fallback - cualquier A con número largo
            if len(out) < 5:
                for a in soup.find_all("a", href=True):
                    href = a.get("href", "")
                    if re.search(r'\d{6,}', href):
                        text = limpiar(a.get_text())
                        if len(text) >= 8:
                            link = href if href.startswith("http") else urljoin("https://www.yapo.cl", href)
                            if link not in seen and "empleo" in link.lower():
                                seen.add(link)
                                out.append(Oferta(source=source, title=text, link=link, location_verified=True))
                                stats["estrategias"]["E5"] += 1
            
            time.sleep(random.uniform(1, 2))
        
        stats["total_enlaces"] = len(out)
        set_source_ok(source)
        log.info(f"✅ [{source}] COMPLETADO: {len(out)} ofertas - {dict([(k, v) for k, v in stats['estrategias'].items() if v > 0])}")
        
    except Exception as e:
        log.exception(f"❌ [{source}] ERROR: {e}")
        set_source_error(source, str(e))
    
    return dedup(out), stats

def dedup(items: List[Oferta]) -> List[Oferta]:
    seen = set()
    out = []
    for o in items:
        k = o.link.strip().lower()
        if k and k not in seen:
            seen.add(k)
            out.append(o)
    return out

def upsert_job(o: Oferta) -> Tuple[int, str, str]:
    job_code = f"{o.source[:2].upper()}-{short_hash(o.link)}"
    fingerprint = short_hash(o.link)
    
    requisitos_json = json.dumps([{"tipo": r.tipo, "texto": r.texto} for r in o.requisitos])
    
    with get_db() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        try:
            cur.execute("""
                INSERT INTO jobs(job_code, source, title, company, link, date_text, salary, jornada, description, requisitos, resumen, fingerprint, last_seen_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                ON CONFLICT (link) DO UPDATE SET last_seen_at=NOW(), description=EXCLUDED.description, requisitos=EXCLUDED.requisitos, resumen=EXCLUDED.resumen
                RETURNING id, applied_status, (xmax = 0) AS inserted
            """, (job_code, o.source, o.title, o.company, o.link, o.date_text, o.salary, o.jornada, o.description, requisitos_json, o.resumen, fingerprint))
            
            row = cur.fetchone()
            if not row:
                cur.execute("SELECT id, applied_status FROM jobs WHERE link=%s", (o.link,))
                row = cur.fetchone()
            
            conn.commit()
            return int(row["id"]), str(row["applied_status"]), "inserted" if row.get("inserted") else "updated"
            
        except Exception as e:
            log.error(f"Error upsert {o.link[:60]}...: {e}")
            conn.rollback()
            return 0, "unknown", "error"

def get_state_int(key: str, default: int = 0) -> int:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("SELECT value FROM bot_state WHERE key=%s", (key,))
        row = cur.fetchone()
        return int(row[0]) if row else default

def set_state_str(key: str, value: str) -> None:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("INSERT INTO bot_state(key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value", (key, value))
        conn.commit()

def run_cycle() -> Dict:
    parsers = [
        ("Chiletrabajos", parse_chiletrabajos),
        ("BNE", parse_bne),
        ("Indeed", parse_indeed),
        ("Computrabajo", parse_computrabajo),
        ("Yapo", parse_yapo),
    ]
    
    cycle_stats = {"total": 0, "nuevas": 0, "existentes": 0, "por_fuente": {}}
    
    for nombre, parser in parsers:
        try:
            log.info(f"\n{'═'*60}\n🚀 {nombre}\n{'═'*60}")
            ofertas, parser_stats = parser()
            cycle_stats["por_fuente"][nombre] = parser_stats
            
            nuevas = 0
            existentes = 0
            
            for o in ofertas:
                if not pasa_filtros(o):
                    continue
                
                cycle_stats["total"] += 1
                job_id, status, op = upsert_job(o)
                
                if op == "inserted":
                    nuevas += 1
                    cycle_stats["nuevas"] += 1
                    code = f"{o.source[:2].upper()}-{short_hash(o.link)}"
                    msg = formatear_oferta(o, code, status)
                    telegram_send(msg)
                else:
                    existentes += 1
                    cycle_stats["existentes"] += 1
            
            cycle_stats["por_fuente"][nombre]["nuevas"] = nuevas
            cycle_stats["por_fuente"][nombre]["existentes"] = existentes
            log.info(f"✅ {nombre}: {nuevas} nuevas | {existentes} existentes")
            
        except Exception as e:
            log.exception(f"❌ {nombre} falló: {e}")
    
    cycle_num = get_state_int("cycle_counter", 0) + 1
    set_state_str("cycle_counter", str(cycle_num))
    
    log.info(f"\n{'═'*60}\nCICLO {cycle_num} | Total: {cycle_stats['total']} | Nuevas: {cycle_stats['nuevas']}\n{'═'*60}\n")
    
    if cycle_num % HEARTBEAT_EVERY_CYCLES == 0:
        msg = f"""<b>🫀 CICLO {cycle_num}</b>
━━━━━━━━━━━━━━━━━━━━
📊 <b>RESUMEN</b>
• Total procesadas: {cycle_stats['total']}
• Nuevas insertadas: {cycle_stats['nuevas']}
• Ya existentes: {cycle_stats['existentes']}

━━━━━━━━━━━━━━━━━━━━
📡 <b>FUENTES ACTIVAS</b>"""
        
        for fuente, stats in cycle_stats["por_fuente"].items():
            if isinstance(stats, dict) and "error" not in stats:
                total_fuente = stats.get("total_enlaces", sum([v for k, v in stats.items() if k.startswith("E") or k in ["rss", "scraping"]]))
                nuevas_fuente = stats.get("nuevas", 0)
                msg += f"\n\n<b>{fuente}</b>"
                msg += f"\n✅ {total_fuente} encontradas"
                msg += f"\n🆕 {nuevas_fuente} nuevas"
                
                # Mostrar estrategias exitosas
                estrategias_ok = [(k, v) for k, v in stats.items() if (k.startswith("E") or k in ["rss", "scraping"]) and v > 0]
                if estrategias_ok:
                    msg += f"\n📍 Estrategias: " + ", ".join([f"{k}:{v}" for k, v in estrategias_ok])
        
        telegram_send(msg)
    
    return cycle_stats

def main():
    if not all([DATABASE_URL, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID]):
        raise RuntimeError("Faltan variables de entorno")
    
    init_db()
    telegram_send("<b>🚀 BOT v6 DEFINITIVO</b>\n━━━━━━━━━━━━━━━━━━━━\n✅ 5 fuentes activas\n✅ 5-7 estrategias por fuente\n✅ Sistema anti-fallas total\n✅ Formato profesional\n━━━━━━━━━━━━━━━━━━━━")
    
    while True:
        start = time.time()
        try:
            run_cycle()
        except Exception as e:
            log.exception(f"❌ Ciclo error: {e}")
        
        elapsed = time.time() - start
        sleep_time = max(15, INTERVALO - int(elapsed))
        log.info(f"⏰ Siguiente ciclo en {sleep_time}s\n")
        time.sleep(sleep_time)

if __name__ == "__main__":
    main()
