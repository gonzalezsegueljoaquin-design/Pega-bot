#!/usr/bin/env python3
"""
Bot Osorno v4 - Extracción Estructurada
- Parsea descripción completa
- Extrae requisitos automáticamente
- Genera resumen inteligente
- Formato Telegram mejorado
- Stats detalladas por página
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
MIN_SCORE_IMMEDIATE = int(os.getenv("MIN_SCORE_IMMEDIATE", "0"))  # 0 = mostrar todas
HEARTBEAT_EVERY_CYCLES = int(os.getenv("HEARTBEAT_EVERY_CYCLES", "10"))

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DATABASE_URL = os.getenv("DATABASE_URL")

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
]

OSORNO_SIGNALS = ["osorno", "los lagos", "región de los lagos", "5290"]
KEYWORDS_INCLUDE = [k.strip().lower() for k in os.getenv("KEYWORDS_INCLUDE", "").split(",") if k.strip()]
KEYWORDS_EXCLUDE = [k.strip().lower() for k in os.getenv("KEYWORDS_EXCLUDE", "").split(",") if k.strip()]

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format='%(asctime)s [%(levelname)s] %(message)s'
)
log = logging.getLogger("bot")

@dataclass
class Requisito:
    """Requisito extraído de la descripción"""
    tipo: str  # educacion, experiencia, habilidad, certificacion
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
    """Limpia espacios múltiples y whitespace"""
    return re.sub(r"\s+", " ", str(txt or "")).strip()

def extraer_requisitos(descripcion: str) -> List[Requisito]:
    """
    Extrae requisitos de la descripción.
    Busca secciones: Requisitos, Requerimientos, Perfil, etc.
    """
    requisitos = []
    
    # Patrones de secciones de requisitos
    patrones_seccion = [
        r"(?:requisitos?|requerimientos?|perfil|condiciones?|exigencias?)[\s:]+",
        r"(?:se requiere|buscamos|necesitamos)[\s:]+",
        r"(?:educaci[oó]n|formaci[oó]n|experiencia|habilidades)[\s:]+",
    ]
    
    texto = descripcion.lower()
    
    # Buscar secciones
    for patron in patrones_seccion:
        match = re.search(patron, texto, re.IGNORECASE)
        if match:
            # Extraer desde ahí hasta el final o siguiente sección
            inicio = match.end()
            resto = descripcion[inicio:]
            
            # Buscar bullets o líneas
            for linea in resto.split('\n'):
                linea = limpiar(linea)
                
                # Skip líneas muy cortas o de ruido
                if len(linea) < 15 or len(linea) > 200:
                    continue
                if any(noise in linea.lower() for noise in ['publicidad', 'postular', 'volver', 'guardar']):
                    continue
                
                # Clasificar tipo
                tipo = "general"
                if any(x in linea.lower() for x in ['título', 'estudios', 'carrera', 'universitari', 'técnic', 'media']):
                    tipo = "educacion"
                elif any(x in linea.lower() for x in ['años', 'experiencia', 'previo', 'desempeñ']):
                    tipo = "experiencia"
                elif any(x in linea.lower() for x in ['office', 'excel', 'inglés', 'licencia', 'computación']):
                    tipo = "habilidad"
                elif any(x in linea.lower() for x in ['certificado', 'certificación', 'licencia']):
                    tipo = "certificacion"
                
                # Limpiar bullets
                linea = re.sub(r'^[\-\•\*\+]+\s*', '', linea)
                linea = re.sub(r'^\d+[\.\)]\s*', '', linea)
                
                if linea and len(linea) > 10:
                    requisitos.append(Requisito(tipo=tipo, texto=linea))
                
                if len(requisitos) >= 8:  # Máximo 8 requisitos
                    break
            
            if requisitos:
                break  # Ya encontramos requisitos
    
    # Si no encontramos requisitos estructurados, buscar patrones directos
    if not requisitos:
        for pattern in [
            r'(?:se requiere|necesitamos|buscamos)\s+([^.]{20,150}\.)',
            r'(?:con|tener)\s+(?:experiencia|conocimientos?)\s+(?:en|de)\s+([^.]{15,100}\.)',
        ]:
            for match in re.finditer(pattern, descripcion, re.IGNORECASE):
                texto = limpiar(match.group(1) if match.lastindex else match.group(0))
                requisitos.append(Requisito(tipo="general", texto=texto))
                if len(requisitos) >= 5:
                    break
    
    return requisitos[:8]  # Máximo 8

def generar_resumen(descripcion: str, max_words: int = 50) -> str:
    """
    Genera un resumen de las primeras 2-3 oraciones significativas.
    """
    # Limpiar ruido
    desc = descripcion
    for noise in ["PUBLICIDAD", "Volver", "Buscar", "Postular", "Guardar", "Detalle"]:
        desc = desc.replace(noise, "")
    
    # Buscar primeras oraciones
    oraciones = re.split(r'[.!?]+', desc)
    significativas = []
    
    for oracion in oraciones:
        oracion = limpiar(oracion)
        
        # Skip vacías o muy cortas
        if len(oracion) < 30:
            continue
        
        # Skip navegación/metadata
        if any(x in oracion.lower() for x in ['fecha:', 'id:', 'salario:', 'ubicación:', 'categoría:']):
            continue
        
        significativas.append(oracion)
        
        if len(significativas) >= 2:
            break
    
    if not significativas:
        return "No disponible"
    
    resumen = '. '.join(significativas) + '.'
    
    # Truncar por palabras
    palabras = resumen.split()
    if len(palabras) > max_words:
        resumen = ' '.join(palabras[:max_words]) + '...'
    
    return resumen

def short_hash(*parts: str) -> str:
    return hashlib.sha1(limpiar("|".join(parts)).lower().encode()).hexdigest()[:10].upper()

def contiene_osorno(txt: str) -> bool:
    return any(sig in txt.lower() for sig in OSORNO_SIGNALS)

def pasa_filtros(o: Oferta) -> bool:
    """Filtros MÁS PERMISIVOS - solo excluir lo explícito"""
    txt = f"{o.title} {o.company} {o.description} {o.link}".lower()
    
    # Si está verificado por ubicación, pasar
    if o.location_verified:
        # Solo excluir keywords explícitas
        if KEYWORDS_EXCLUDE and any(k in txt for k in KEYWORDS_EXCLUDE):
            log.debug(f"❌ Rechazado por EXCLUDE: {o.title}")
            return False
        return True
    
    # Si no está verificado, debe contener osorno
    if not contiene_osorno(txt):
        log.debug(f"❌ Rechazado por ubicación: {o.title}")
        return False
    
    # Keywords include (si están configuradas)
    if KEYWORDS_INCLUDE and not any(k in txt for k in KEYWORDS_INCLUDE):
        log.debug(f"❌ Rechazado por INCLUDE: {o.title}")
        return False
    
    # Keywords exclude
    if KEYWORDS_EXCLUDE and any(k in txt for k in KEYWORDS_EXCLUDE):
        log.debug(f"❌ Rechazado por EXCLUDE: {o.title}")
        return False
    
    return True

def score_oferta(o: Oferta) -> int:
    """Score para priorizar notificaciones"""
    score = 1 if (o.location_verified or contiene_osorno(f"{o.title} {o.description}".lower())) else 0
    if o.salary != "No especificado":
        score += 1
    if o.jornada != "No especificada":
        score += 1
    if o.requisitos:
        score += 1
    return score

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
            
            log.debug(f"GET {url} → {r.status_code} ({len(r.text)} bytes)")
            
            if len(r.text) < 500:
                log.warning(f"Respuesta corta: {len(r.text)} bytes")
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
            data={"chat_id": TELEGRAM_CHAT_ID, "text": msg[:MAX_MSG], "parse_mode": "HTML"},
            timeout=10
        )
        if r.ok:
            return r.json().get("result", {}).get("message_id")
    except Exception as e:
        log.error(f"Telegram error: {e}")
    return None

def formatear_oferta(o: Oferta, code: str, status: str) -> str:
    """Formato amigable con secciones claras"""
    estado_emoji = {"applied": "✅", "not_applied": "❌", "unknown": "❓"}[status]
    
    # Header
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
    
    # Requisitos
    if o.requisitos:
        msg += "\n\n✅ <b>REQUISITOS</b>"
        for i, req in enumerate(o.requisitos[:5], 1):
            icon = {"educacion": "🎓", "experiencia": "💼", "habilidad": "⚡", "certificacion": "📜"}.get(req.tipo, "•")
            msg += f"\n{icon} {html.escape(req.texto[:100])}"
    
    # Link
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
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("SELECT consecutive_errors FROM source_health WHERE source=%s", (source,))
        row = cur.fetchone()
        return (row[0] >= 3) if row else False

# ==================== PARSERS ====================

def parse_chiletrabajos() -> Tuple[List[Oferta], Dict[str, int]]:
    """Parser Chiletrabajos con stats detalladas"""
    source = "Chiletrabajos"
    if should_cooldown(source):
        return [], {}
    
    out: List[Oferta] = []
    stats = {"paginas": 0, "enlaces_encontrados": 0, "ofertas_procesadas": 0, "ofertas_validas": 0}
    base = "https://www.chiletrabajos.cl"
    
    try:
        for offset in [0, 30, 60]:
            url = f"{base}/ciudad/osorno.html" + (f"/{offset}" if offset > 0 else "")
            log.info(f"📄 Scrapeando {url}")
            
            soup = get_soup(url, retries=3)
            if not soup:
                continue
            
            stats["paginas"] += 1
            
            # Buscar enlaces
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
                
                empresa = "No especificada"
                h3 = h2.find_next_sibling("h3")
                if h3:
                    empresa_text = limpiar(h3.get_text())
                    if empresa_text and not re.match(r'^\d{1,2}\.\d{3}', empresa_text):
                        empresa = empresa_text.split(",")[0]
                
                job_links.append((link, title, empresa))
            
            stats["enlaces_encontrados"] += len(job_links)
            log.info(f"📊 Página {stats['paginas']}: {len(job_links)} enlaces encontrados")
            
            # Procesar cada trabajo
            for link, title, empresa_base in job_links:
                stats["ofertas_procesadas"] += 1
                
                try:
                    det_soup = get_soup(link, retries=2)
                    if not det_soup:
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
                        elif "salario" in txt or "sueldo" in txt:
                            if val and val.lower() not in ["no especificado", "a convenir"]:
                                sueldo = val if val.startswith("$") else f"${val}"
                        elif "tipo" in txt or "jornada" in txt:
                            jornada = val
                        elif ("buscado" in txt or "empresa" in txt) and not re.match(r'\d{1,2}\.\d{3}', val):
                            empresa = val
                    
                    # DESCRIPCIÓN COMPLETA - sin truncar
                    desc_parts = []
                    for tag in det_soup.find_all(["p", "div", "li", "span"]):
                        txt = limpiar(tag.get_text())
                        if 20 < len(txt) < 1000:  # Permitir textos más largos
                            if not any(noise in txt for noise in ["PUBLICIDAD", "Volver", "Buscar", "Guardar", "Postular", "Detalle oferta"]):
                                desc_parts.append(txt)
                    
                    descripcion_completa = " ".join(desc_parts) if desc_parts else "No disponible"
                    
                    # Extraer requisitos
                    requisitos = extraer_requisitos(descripcion_completa)
                    
                    # Generar resumen
                    resumen = generar_resumen(descripcion_completa, max_words=50)
                    
                    oferta = Oferta(
                        source=source,
                        title=title,
                        link=link,
                        company=empresa,
                        date_text=fecha,
                        salary=sueldo,
                        jornada=jornada,
                        description=descripcion_completa,
                        requisitos=requisitos,
                        resumen=resumen,
                        location_verified=True
                    )
                    
                    out.append(oferta)
                    stats["ofertas_validas"] += 1
                    
                    time.sleep(random.uniform(0.3, 0.7))
                    
                except Exception as e:
                    log.warning(f"Error procesando {link}: {e}")
                    continue
            
            time.sleep(random.uniform(1, 2))
        
        set_source_ok(source)
        log.info(f"✅ {source}: {stats['ofertas_validas']} ofertas válidas de {stats['ofertas_procesadas']} procesadas")
        
    except Exception as e:
        log.exception(f"❌ {source} error: {e}")
        set_source_error(source, str(e))
    
    return dedup(out), stats

def parse_indeed() -> Tuple[List[Oferta], Dict[str, int]]:
    """Parser Indeed RSS"""
    source = "Indeed"
    stats = {"entradas_rss": 0, "ofertas_validas": 0}
    
    if should_cooldown(source):
        return [], stats
    
    out: List[Oferta] = []
    
    try:
        feed_url = f"https://cl.indeed.com/rss?q={quote_plus('osorno')}&l={quote_plus('Osorno, Los Lagos')}&sort=date&limit=50"
        feed = feedparser.parse(feed_url)
        
        stats["entradas_rss"] = len(feed.entries)
        log.info(f"📊 {source}: {stats['entradas_rss']} entradas RSS")
        
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
            resumen = generar_resumen(summary, max_words=50)
            
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
            stats["ofertas_validas"] += 1
        
        set_source_ok(source)
        log.info(f"✅ {source}: {stats['ofertas_validas']} ofertas válidas")
        
    except Exception as e:
        log.exception(f"❌ {source} error: {e}")
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
    """Guarda oferta con requisitos y resumen"""
    job_code = f"{o.source[:2].upper()}-{short_hash(o.link)}"
    fingerprint = short_hash(o.link)
    
    # Serializar requisitos
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
    """Ejecuta un ciclo completo con stats detalladas"""
    parsers = [
        ("Chiletrabajos", parse_chiletrabajos),
        ("Indeed", parse_indeed),
    ]
    
    cycle_stats = {"total_encontradas": 0, "nuevas": 0, "existentes": 0, "filtradas": 0, "por_fuente": {}}
    
    for nombre, parser in parsers:
        try:
            ofertas, parser_stats = parser()
            cycle_stats["por_fuente"][nombre] = parser_stats
            
            log.info(f"📊 {nombre} - Stats: {parser_stats}")
            
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
                    
                    if score_oferta(o) >= MIN_SCORE_IMMEDIATE:
                        msg = formatear_oferta(o, code, status)
                        telegram_send(msg)
                        log.info(f"📨 Notificación enviada: {o.title}")
                else:
                    existentes_fuente += 1
                    cycle_stats["existentes"] += 1
            
            log.info(f"✅ {nombre}: {nuevas_fuente} nuevas | {existentes_fuente} existentes | {filtradas_fuente} filtradas")
            
        except Exception as e:
            log.exception(f"❌ Parser {nombre} falló: {e}")
    
    cycle_num = get_state_int("cycle_counter", 0) + 1
    set_state_str("cycle_counter", str(cycle_num))
    
    log.info(f"═══════════════════════════════")
    log.info(f"CICLO {cycle_num} COMPLETADO")
    log.info(f"Total encontradas: {cycle_stats['total_encontradas']}")
    log.info(f"Nuevas: {cycle_stats['nuevas']}")
    log.info(f"Existentes: {cycle_stats['existentes']}")
    log.info(f"Filtradas: {cycle_stats['filtradas']}")
    log.info(f"═══════════════════════════════\n")
    
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

def test_extraction():
    """Testing de extracción"""
    print("\n" + "="*60)
    print("TESTING DE EXTRACCIÓN")
    print("="*60)
    
    # Test descripción
    test_desc = """
    Buscamos vendedor con experiencia para nuestro local en Osorno.
    
    Requisitos:
    - Enseñanza media completa
    - Experiencia mínima 1 año en ventas retail
    - Disponibilidad part-time
    - Manejo de Office básico
    - Buena presentación
    
    Se ofrece: Sueldo $450.000 + comisiones
    """
    
    print("\n1. EXTRACCIÓN DE REQUISITOS:")
    requisitos = extraer_requisitos(test_desc)
    for req in requisitos:
        print(f"  [{req.tipo}] {req.texto}")
    
    print("\n2. GENERACIÓN DE RESUMEN:")
    resumen = generar_resumen(test_desc)
    print(f"  {resumen}")
    
    print("\n3. FORMATO TELEGRAM:")
    oferta_test = Oferta(
        source="Test",
        title="Vendedor Part Time Osorno",
        link="https://test.com/123",
        company="Test Corp",
        date_text="2026-04-18",
        salary="$450.000",
        jornada="Part Time",
        description=test_desc,
        requisitos=requisitos,
        resumen=resumen,
        location_verified=True
    )
    
    msg = formatear_oferta(oferta_test, "TE-ABC123", "unknown")
    print(f"\n{msg}\n")
    
    print("="*60)
    print("✅ TESTING COMPLETADO")
    print("="*60 + "\n")

def main():
    if not all([DATABASE_URL, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID]):
        raise RuntimeError("Faltan variables de entorno")
    
    # Testing
    if os.getenv("TEST_MODE", "0") == "1":
        test_extraction()
        return
    
    init_db()
    telegram_send("🚀 <b>BOT OSORNO v4</b>\n━━━━━━━━━━━━━━━━━━━━\n✅ Extracción estructurada\n✅ Parser de requisitos\n✅ Resumen inteligente\n✅ Formato mejorado\n━━━━━━━━━━━━━━━━━━━━")
    
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
