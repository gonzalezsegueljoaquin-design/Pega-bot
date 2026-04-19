#!/usr/bin/env python3
"""
Bot Osorno v5 - ULTRA-ROBUSTO
- Múltiples estrategias de extracción por fuente
- Parsers resilientes que NO fallan
- Logging detallado para debugging
- Testing integrado por fuente
- SIN filtros estrictos
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
LOG_LEVEL = os.getenv("LOG_LEVEL", "DEBUG").upper()  # DEBUG por defecto
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
    level=getattr(logging, LOG_LEVEL, logging.DEBUG),
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
                if any(noise in linea.lower() for noise in ['publicidad', 'postular']):
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
        if any(x in oracion.lower() for x in ['fecha:', 'id:']):
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
    """Filtros MUY PERMISIVOS - solo excluir lo explícito"""
    txt = f"{o.title} {o.company} {o.description} {o.link}".lower()
    
    # Si está verificado, solo excluir keywords explícitas
    if o.location_verified:
        if KEYWORDS_EXCLUDE and any(k in txt for k in KEYWORDS_EXCLUDE):
            log.debug(f"❌ Rechazado por EXCLUDE: {o.title}")
            return False
        return True
    
    # Si no está verificado, debe contener osorno
    if not contiene_osorno(txt):
        log.debug(f"❌ Rechazado por ubicación: {o.title}")
        return False
    
    if KEYWORDS_EXCLUDE and any(k in txt for k in KEYWORDS_EXCLUDE):
        log.debug(f"❌ Rechazado por EXCLUDE: {o.title}")
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
                requisitos JSONB,
                resumen TEXT,
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
    """GET con anti-detección y logging detallado"""
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
            
            log.debug(f"✅ GET {url} → {r.status_code} ({len(r.text)} bytes)")
            
            if len(r.text) < 500:
                log.warning(f"⚠️ Respuesta muy corta: {len(r.text)} bytes para {url}")
                if i < retries - 1:
                    continue
            
            return BeautifulSoup(r.text, "html.parser")
            
        except Exception as e:
            log.warning(f"❌ GET {url} intento {i+1}/{retries}: {e}")
            if i < retries - 1:
                time.sleep(random.uniform(2, 4))
    
    log.error(f"🚫 FALLÓ COMPLETAMENTE: {url}")
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
    
    msg = f"""🆕 <b>NUEVA OFERTA</b>
━━━━━━━━━━━━━━━━━━━━

📌 <b>TÍTULO</b>
{html.escape(o.title)}

🆔 {code}
🏢 <b>EMPRESA:</b> {html.escape(o.company)}
📅 <b>PUBLICADO:</b> {html.escape(o.date_text)}
💰 <b>SUELDO:</b> {html.escape(o.salary)}
🕒 <b>JORNADA:</b> {html.escape(o.jornada)}
📮 <b>ESTADO:</b> {estado_emoji} {status}

━━━━━━━━━━━━━━━━━━━━

💼 <b>RESUMEN</b>
{html.escape(o.resumen[:300])}"""
    
    if o.requisitos:
        msg += "\n\n✅ <b>REQUISITOS</b>"
        for req in o.requisitos[:5]:
            icon = {"educacion": "🎓", "experiencia": "💼", "habilidad": "⚡", "certificacion": "📜"}.get(req.tipo, "•")
            msg += f"\n{icon} {html.escape(req.texto[:100])}"
    
    msg += f"\n\n━━━━━━━━━━━━━━━━━━━━\n🔗 <a href='{o.link}'>Ver oferta completa</a>"
    msg += f"\n\n/postule {code} | /nopostule {code}"
    
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
    """Cooldown después de 5 errores (más permisivo)"""
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("SELECT consecutive_errors FROM source_health WHERE source=%s", (source,))
        row = cur.fetchone()
        if row and row[0] >= 5:
            log.warning(f"🔥 {source} en COOLDOWN ({row[0]} errores)")
            return True
        return False

# ==================== PARSERS ULTRA-ROBUSTOS ====================

def parse_chiletrabajos() -> Tuple[List[Oferta], Dict[str, int]]:
    """
    Parser Chiletrabajos - MÚLTIPLES ESTRATEGIAS
    Estrategia 1: H2 > A con /trabajo/
    Estrategia 2: Cualquier A con /trabajo/
    Estrategia 3: Div con clase de oferta
    """
    source = "Chiletrabajos"
    if should_cooldown(source):
        return [], {"error": "cooldown"}
    
    out: List[Oferta] = []
    stats = {"paginas": 0, "enlaces_encontrados": 0, "estrategia_1": 0, "estrategia_2": 0, "estrategia_3": 0}
    base = "https://www.chiletrabajos.cl"
    
    try:
        for offset in [0, 30, 60]:
            url = f"{base}/ciudad/osorno.html" + (f"/{offset}" if offset > 0 else "")
            log.info(f"🌐 [{source}] Scrapeando: {url}")
            
            soup = get_soup(url, retries=3)
            if not soup:
                log.error(f"❌ [{source}] No se pudo obtener soup de {url}")
                continue
            
            stats["paginas"] += 1
            job_links_found = []
            
            # ESTRATEGIA 1: H2 > A con /trabajo/
            log.debug(f"🔍 [{source}] Probando estrategia 1: H2 > A")
            for h2 in soup.find_all("h2"):
                a = h2.find("a", href=True)
                if not a:
                    continue
                href = a.get("href", "")
                if "/trabajo/" in href:
                    link = href if href.startswith("http") else f"{base}{href}"
                    title = limpiar(a.get_text())
                    if len(title) >= 5 and link not in [x[0] for x in job_links_found]:
                        job_links_found.append((link, title, "No especificada"))
                        stats["estrategia_1"] += 1
            
            # ESTRATEGIA 2: TODOS los <a> con /trabajo/
            if len(job_links_found) < 10:
                log.debug(f"🔍 [{source}] Probando estrategia 2: Todos los <a> con /trabajo/")
                for a in soup.find_all("a", href=True):
                    href = a.get("href", "")
                    if "/trabajo/" in href:
                        link = href if href.startswith("http") else f"{base}{href}"
                        title = limpiar(a.get_text())
                        if len(title) >= 5 and link not in [x[0] for x in job_links_found]:
                            job_links_found.append((link, title, "No especificada"))
                            stats["estrategia_2"] += 1
            
            # ESTRATEGIA 3: Divs con clases comunes de ofertas
            if len(job_links_found) < 5:
                log.debug(f"🔍 [{source}] Probando estrategia 3: Divs con clase oferta")
                for div in soup.find_all("div", class_=re.compile(r'(job|oferta|trabajo|card|item)', re.I)):
                    a = div.find("a", href=True)
                    if a and "/trabajo/" in a.get("href", ""):
                        href = a.get("href", "")
                        link = href if href.startswith("http") else f"{base}{href}"
                        title = limpiar(a.get_text())
                        if len(title) >= 5 and link not in [x[0] for x in job_links_found]:
                            job_links_found.append((link, title, "No especificada"))
                            stats["estrategia_3"] += 1
            
            stats["enlaces_encontrados"] += len(job_links_found)
            log.info(f"📊 [{source}] Página {stats['paginas']}: {len(job_links_found)} enlaces (E1:{stats['estrategia_1']} E2:{stats['estrategia_2']} E3:{stats['estrategia_3']})")
            
            # Procesar ofertas
            for link, title, empresa in job_links_found[:30]:  # Limitar a 30 por página
                try:
                    det_soup = get_soup(link, retries=2)
                    if not det_soup:
                        # Si falla detalles, guardar con info básica
                        out.append(Oferta(
                            source=source,
                            title=title,
                            link=link,
                            company=empresa,
                            location_verified=True
                        ))
                        continue
                    
                    # Extraer detalles
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
                            if not any(noise in txt for noise in ["PUBLICIDAD", "Volver", "Buscar"]):
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
                    log.warning(f"⚠️ [{source}] Error procesando {link}: {e}")
                    # Guardar con info básica
                    out.append(Oferta(
                        source=source,
                        title=title,
                        link=link,
                        location_verified=True
                    ))
            
            time.sleep(random.uniform(1, 2))
        
        set_source_ok(source)
        log.info(f"✅ [{source}] COMPLETADO: {len(out)} ofertas extraídas")
        
    except Exception as e:
        log.exception(f"❌ [{source}] ERROR CRÍTICO: {e}")
        set_source_error(source, str(e))
    
    return dedup(out), stats

def parse_bne() -> Tuple[List[Oferta], Dict[str, int]]:
    """
    Parser BNE - MÚLTIPLES ESTRATEGIAS
    """
    source = "BNE"
    if should_cooldown(source):
        return [], {"error": "cooldown"}
    
    out: List[Oferta] = []
    stats = {"enlaces_encontrados": 0}
    seen: Set[str] = set()
    
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
            
            # ESTRATEGIA 1: Selectores CSS específicos
            for selector in ["a.job-link", "a[href*='oferta']", "a[href*='empleo']", "a[href*='trabajo']"]:
                for a in soup.select(selector):
                    href = a.get("href", "")
                    if not href:
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
                    stats["enlaces_encontrados"] += 1
            
            # ESTRATEGIA 2: Todos los enlaces que parezcan ofertas
            for a in soup.find_all("a", href=True):
                href = a.get("href", "")
                text = limpiar(a.get_text())
                
                # Debe parecer un título de trabajo
                if len(text) < 10 or len(text) > 100:
                    continue
                
                if not any(kw in text.lower() for kw in ["vendedor", "asistente", "técnico", "ejecutivo", "analista", "operador", "supervisor", "administrativo", "coordinador", "ingeniero"]):
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
                stats["enlaces_encontrados"] += 1
            
            time.sleep(random.uniform(1, 2))
        
        set_source_ok(source)
        log.info(f"✅ [{source}] COMPLETADO: {len(out)} ofertas extraídas")
        
    except Exception as e:
        log.exception(f"❌ [{source}] ERROR: {e}")
        set_source_error(source, str(e))
    
    return dedup(out), stats

def parse_indeed() -> Tuple[List[Oferta], Dict[str, int]]:
    """Parser Indeed RSS"""
    source = "Indeed"
    stats = {"entradas_rss": 0}
    
    if should_cooldown(source):
        return [], {"error": "cooldown"}
    
    out: List[Oferta] = []
    
    try:
        feed_url = f"https://cl.indeed.com/rss?q={quote_plus('osorno')}&l={quote_plus('Osorno, Los Lagos')}&sort=date&limit=50"
        log.info(f"🌐 [{source}] Scrapeando RSS: {feed_url}")
        
        feed = feedparser.parse(feed_url)
        stats["entradas_rss"] = len(feed.entries)
        
        log.info(f"📊 [{source}] RSS retornó {stats['entradas_rss']} entradas")
        
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
        
        set_source_ok(source)
        log.info(f"✅ [{source}] COMPLETADO: {len(out)} ofertas extraídas")
        
    except Exception as e:
        log.exception(f"❌ [{source}] ERROR: {e}")
        set_source_error(source, str(e))
    
    return dedup(out), stats

def parse_computrabajo() -> Tuple[List[Oferta], Dict[str, int]]:
    """Parser Computrabajo"""
    source = "Computrabajo"
    if should_cooldown(source):
        return [], {"error": "cooldown"}
    
    out: List[Oferta] = []
    stats = {"enlaces_encontrados": 0}
    seen: Set[str] = set()
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
            
            # ESTRATEGIA 1: Articles
            for article in soup.find_all("article"):
                a = article.find("a", href=True)
                if not a:
                    continue
                
                href = a.get("href", "")
                if "oferta" in href.lower() or re.search(r'-[0-9a-f]{6,}\.html?$', href, re.I):
                    text = limpiar(a.get_text())
                    if len(text) < 8:
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
                    stats["enlaces_encontrados"] += 1
            
            # ESTRATEGIA 2: Todos los links con "oferta"
            for a in soup.find_all("a", href=True):
                href = a.get("href", "")
                if "oferta" not in href.lower():
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
                stats["enlaces_encontrados"] += 1
            
            time.sleep(random.uniform(1, 2))
        
        set_source_ok(source)
        log.info(f"✅ [{source}] COMPLETADO: {len(out)} ofertas extraídas")
        
    except Exception as e:
        log.exception(f"❌ [{source}] ERROR: {e}")
        set_source_error(source, str(e))
    
    return dedup(out), stats

def parse_yapo() -> Tuple[List[Oferta], Dict[str, int]]:
    """Parser Yapo"""
    source = "Yapo"
    if should_cooldown(source):
        return [], {"error": "cooldown"}
    
    out: List[Oferta] = []
    stats = {"enlaces_encontrados": 0}
    seen: Set[str] = set()
    
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
            
            for a in soup.find_all("a", href=True):
                href = a.get("href", "")
                
                # Debe tener números (ID)
                if not re.search(r'\d{5,}', href):
                    continue
                
                # Debe ser empleo/trabajo
                if not any(x in href.lower() for x in ["/empleo", "/trabajo", "/oferta"]):
                    continue
                
                # Rechazar otras categorías
                if any(x in href.lower() for x in ["/arriendo", "/venta", "/auto", "/servicio", "/inmueble"]):
                    continue
                
                text = limpiar(a.get_text())
                if len(text) < 8:
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
                stats["enlaces_encontrados"] += 1
            
            time.sleep(random.uniform(1, 2))
        
        set_source_ok(source)
        log.info(f"✅ [{source}] COMPLETADO: {len(out)} ofertas extraídas")
        
    except Exception as e:
        log.exception(f"❌ [{source}] ERROR: {e}")
        set_source_error(source, str(e))
    
    return dedup(out), stats

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
            log.error(f"Error upsert {o.link}: {e}")
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
    
    cycle_stats = {"total_encontradas": 0, "nuevas": 0, "existentes": 0, "filtradas": 0, "por_fuente": {}}
    
    for nombre, parser in parsers:
        try:
            log.info(f"\n{'='*60}\n🚀 INICIANDO PARSER: {nombre}\n{'='*60}")
            ofertas, parser_stats = parser()
            cycle_stats["por_fuente"][nombre] = parser_stats
            
            log.info(f"📊 [{nombre}] Stats: {parser_stats}")
            
            nuevas_fuente = 0
            existentes_fuente = 0
            filtradas_fuente = 0
            
            for o in ofertas:
                cycle_stats["total_encontradas"] += 1
                
                if not pasa_filtros(o):
                    filtradas_fuente += 1
                    cycle_stats["filtradas"] += 1
                    continue
                
                job_id, status, op = upsert_job(o)
                
                if op == "inserted":
                    nuevas_fuente += 1
                    cycle_stats["nuevas"] += 1
                    
                    code = f"{o.source[:2].upper()}-{short_hash(o.link)}"
                    msg = formatear_oferta(o, code, status)
                    telegram_send(msg)
                    log.info(f"📨 Notificación enviada: {o.title}")
                else:
                    existentes_fuente += 1
                    cycle_stats["existentes"] += 1
            
            log.info(f"✅ [{nombre}] {nuevas_fuente} nuevas | {existentes_fuente} existentes | {filtradas_fuente} filtradas")
            
        except Exception as e:
            log.exception(f"❌ Parser {nombre} falló: {e}")
            cycle_stats["por_fuente"][nombre] = {"error": str(e)}
    
    cycle_num = get_state_int("cycle_counter", 0) + 1
    set_state_str("cycle_counter", str(cycle_num))
    
    log.info(f"\n{'='*60}")
    log.info(f"CICLO {cycle_num} COMPLETADO")
    log.info(f"Total: {cycle_stats['total_encontradas']} | Nuevas: {cycle_stats['nuevas']} | Existentes: {cycle_stats['existentes']} | Filtradas: {cycle_stats['filtradas']}")
    log.info(f"{'='*60}\n")
    
    if cycle_num % HEARTBEAT_EVERY_CYCLES == 0:
        msg = f"""🫀 <b>HEARTBEAT CICLO {cycle_num}</b>
━━━━━━━━━━━━━━━━━━━━
📥 Total encontradas: {cycle_stats['total_encontradas']}
🆕 Nuevas: {cycle_stats['nuevas']}
📚 Existentes: {cycle_stats['existentes']}
❌ Filtradas: {cycle_stats['filtradas']}
━━━━━━━━━━━━━━━━━━━━"""
        
        for fuente, stats in cycle_stats["por_fuente"].items():
            msg += f"\n\n<b>{fuente}</b>:"
            for k, v in stats.items():
                msg += f"\n• {k}: {v}"
        
        telegram_send(msg)
    
    return cycle_stats

def main():
    if not all([DATABASE_URL, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID]):
        raise RuntimeError("Faltan variables de entorno")
    
    init_db()
    telegram_send("🚀 <b>BOT OSORNO v5 ULTRA-ROBUSTO</b>\n━━━━━━━━━━━━━━━━━━━━\n✅ 5 fuentes activas\n✅ Múltiples estrategias por fuente\n✅ Parsers resilientes\n✅ Logging detallado DEBUG\n━━━━━━━━━━━━━━━━━━━━")
    
    while True:
        start = time.time()
        try:
            run_cycle()
        except Exception as e:
            log.exception(f"❌ Ciclo error: {e}")
            telegram_send(f"❌ <b>Error en ciclo:</b>\n{str(e)[:300]}")
        
        elapsed = time.time() - start
        sleep_time = max(15, INTERVALO - int(elapsed))
        log.info(f"⏰ Siguiente ciclo en {sleep_time}s\n")
        time.sleep(sleep_time)

if __name__ == "__main__":
    main()
