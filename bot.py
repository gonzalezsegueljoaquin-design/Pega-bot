#!/usr/bin/env python3
"""
Bot Osorno v9
─────────────────────────────────────────────────────
FIXES v9:
  • CRÍTICO: upsert_job ahora usa INSERT ON CONFLICT DO NOTHING + rowcount
    (el patrón xmax=0 de v8 siempre era True → enviaba duplicados en cada ciclo)
  • enriquecer_computrabajo: elimina nav/footer del DOM antes de extraer,
    selectores actualizados, filtro agresivo de frases de ruido de sitio
  • limpiar_descripcion(): nueva función de filtrado de ruido
  • generar_resumen(): reescrito con scoring de relevancia y filtro de ruido
  • extraer_requisitos(): maneja texto continuo (coma/punto) y listas,
    corta al siguiente encabezado de sección
  • enriquecer_bne/yapo: aplican limpiar_descripcion
"""

import json
import logging
import os
import re
import time
import hashlib
import html
import random
import threading
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote_plus, urljoin

import feedparser
import psycopg2
import psycopg2.extras
import requests
from bs4 import BeautifulSoup

# ─── Config ───────────────────────────────────────────────────────────────────
INTERVALO              = int(os.getenv("INTERVALO", "90"))
MAX_MSG                = 4096
LOG_LEVEL              = os.getenv("LOG_LEVEL", "INFO").upper()
HEARTBEAT_EVERY_CYCLES = int(os.getenv("HEARTBEAT_EVERY_CYCLES", "10"))

TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DATABASE_URL     = os.getenv("DATABASE_URL")

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
]
OSORNO_SIGNALS   = ["osorno", "los lagos", "región de los lagos", "region de los lagos", "5290", "rahue"]
KEYWORDS_EXCLUDE = [k.strip().lower() for k in os.getenv("KEYWORDS_EXCLUDE", "").split(",") if k.strip()]

logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO),
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("bot")

# ─── Modelos ──────────────────────────────────────────────────────────────────
@dataclass
class Requisito:
    tipo:  str
    texto: str

@dataclass
class Oferta:
    source:            str
    title:             str
    link:              str
    company:           str = "No especificada"
    date_text:         str = "No especificada"
    salary:            str = "No especificado"
    jornada:           str = "No especificada"
    description:       str = ""
    requisitos:        List[Requisito] = field(default_factory=list)
    resumen:           str = ""
    location_verified: bool = False

# ─── Utilidades ───────────────────────────────────────────────────────────────
def limpiar(txt: str) -> str:
    return re.sub(r"\s+", " ", str(txt or "")).strip()

def short_hash(*parts: str) -> str:
    return hashlib.sha1(limpiar("|".join(parts)).lower().encode()).hexdigest()[:10].upper()

def make_code(source: str, link: str) -> str:
    return f"{source[:2].upper()}-{short_hash(link)}"

def contiene_osorno(txt: str) -> bool:
    return any(s in txt.lower() for s in OSORNO_SIGNALS)

def pasa_filtros(o: Oferta) -> bool:
    txt = f"{o.title} {o.company} {o.description} {o.link}".lower()
    if o.location_verified:
        return not (KEYWORDS_EXCLUDE and any(k in txt for k in KEYWORDS_EXCLUDE))
    if not contiene_osorno(txt):
        return False
    if KEYWORDS_EXCLUDE and any(k in txt for k in KEYWORDS_EXCLUDE):
        return False
    return True

# ─── Ruido de sitios (texto de navegación / footer que hay que ignorar) ──────
_NOISE_PHRASES = [
    # Computrabajo
    "ingresa y encuentra empleo", "encuentra empleo con una mejor", "mejores empresas para trabajar",
    "conoce los salarios en el mercado", "únete a nosotros y publica ofertas",
    "ocultaste esta oferta", "pulsa recuperar oferta", "recuperar oferta para verla",
    "consejos para encontrar empleo", "ver todas las ofertas", "ver oferta completa",
    "publicar oferta gratis", "crea tu cv gratis", "regístrate gratis",
    "no se han encontrado", "volver al inicio", "ver más empleos",
    "cómo funciona", "sobre nosotros", "política de privacidad",
    "términos y condiciones", "mapa del sitio", "quiénes somos",
    "síguenos en", "descarga la app", "empleos por categoría",
    "inicio empleos empresas", "empleos destacados",
    # BNE
    "bolsa nacional de empleo", "capacítate con nosotros",
    # Yapo
    "publicar aviso gratis", "vender en yapo",
    # Chiletrabajos
    "busca entre miles de empleos", "crea tu hoja de vida",
    # Genérico
    "javascript", "cookies", "© copyright", "todos los derechos",
    "lunes a viernes", "nuestros servicios", "contáctanos",
    "para más información visita", "haz click aquí",
]

def _es_ruido(texto: str) -> bool:
    """True si el texto es claramente navegación/footer y no contenido de oferta."""
    t = texto.lower()
    return any(n in t for n in _NOISE_PHRASES)

def limpiar_descripcion(texto: str) -> str:
    """
    Toma el texto crudo extraído del HTML y elimina ruido de navegación.
    Retorna el texto limpio listo para resumen/requisitos.
    """
    if not texto:
        return ""
    # Dividir en frases/oraciones (por . ! ? ; o salto de línea)
    partes = re.split(r"[.!?;\n]", texto)
    limpias = []
    for p in partes:
        p = limpiar(p)
        if len(p) < 20:
            continue
        if _es_ruido(p):
            continue
        limpias.append(p)
    return ". ".join(limpias) if limpias else texto[:500]

def extraer_requisitos(desc: str) -> List[Requisito]:
    """
    Extrae requisitos de la descripción de un trabajo.
    Maneja tanto texto con saltos de línea (listas) como texto continuo
    donde los ítems van separados por coma, punto o punto y coma.
    Filtra ruido de sitio antes de procesar.
    """
    if not desc:
        return []

    requisitos: List[Requisito] = []

    # ── Paso 1: encontrar sección de requisitos ────────────────────────────
    seccion_patron = re.compile(
        r"(?:requisitos?|requerimientos?|perfil\s+requerido|condiciones?\s+del?\s+cargo"
        r"|se\s+requiere|buscamos|necesitamos|postulantes?\s+deben"
        r"|lo\s+que\s+buscamos|exigencias?)\s*[:：]?\s*",
        re.IGNORECASE
    )
    # Encontrar el inicio de la sección
    m = seccion_patron.search(desc)
    if not m:
        return []

    seccion = desc[m.end():]

    # Cortar en el próximo encabezado de sección (ofrecemos, beneficios, funciones, etc.)
    corte = re.compile(
        r"\n\s*(?:funciones?|responsabilidades?|ofrecemos?|beneficios?|descripci[oó]n"
        r"|lo\s+que\s+ofrecemos|condiciones?\s+laborales?|informaci[oó]n\s+adicional)\s*[:：]",
        re.IGNORECASE
    )
    mc = corte.search(seccion)
    if mc:
        seccion = seccion[:mc.start()]

    # ── Paso 2: extraer ítems ────────────────────────────────────────────────
    def clasificar(linea: str) -> str:
        l = linea.lower()
        if any(x in l for x in ["título", "titulado", "egresado", "carrera", "estudios",
                                  "técnic", "profesional", "licenciado", "media", "bachiller"]):
            return "educacion"
        if any(x in l for x in ["año", "experiencia", "mínimo", "meses"]):
            return "experiencia"
        if any(x in l for x in ["office", "excel", "word", "inglés", "licencia", "manejo",
                                  "software", "sistema", "idioma"]):
            return "habilidad"
        return "general"

    def agregar(linea: str):
        linea = limpiar(re.sub(r"^[\-\•\*\+►▪◦→\d\.]+\s*", "", linea))
        if not linea or len(linea) < 10 or len(linea) > 220:
            return
        if _es_ruido(linea):
            return
        # Evitar duplicados similares
        for r in requisitos:
            if linea[:30].lower() in r.texto.lower():
                return
        requisitos.append(Requisito(tipo=clasificar(linea), texto=linea))

    # Intentar primero por líneas (formato lista)
    lineas = [l for l in seccion.split("\n") if limpiar(l)]
    items_linea = [l for l in lineas if len(limpiar(l)) >= 10]

    if len(items_linea) >= 2:
        for linea in items_linea:
            if len(requisitos) >= 7:
                break
            agregar(linea)
    else:
        # Texto continuo: dividir por coma, punto y coma, o "y "
        piezas = re.split(r"[,;]|\s+y\s+(?=[A-ZÁÉÍÓÚÑ])", seccion)
        for pieza in piezas:
            if len(requisitos) >= 7:
                break
            agregar(pieza)

    return requisitos[:7]

def generar_resumen(desc: str, max_palabras: int = 60) -> str:
    """
    Genera un resumen limpio del puesto a partir de la descripción.
    Filtra agresivamente el ruido de sitio y elige las oraciones más informativas.
    """
    if not desc:
        return ""

    # Limpiar ruido primero
    desc_limpia = limpiar_descripcion(desc)
    if not desc_limpia:
        return ""

    # Dividir en oraciones
    oraciones = [limpiar(s) for s in re.split(r"[.!?]+", desc_limpia)]

    # Palabras que indican contenido relevante del puesto
    _PALABRAS_RELEVANTES = [
        "busca", "requiere", "ofrece", "cargo", "puesto", "trabajo", "función",
        "responsabilidad", "contrato", "salario", "sueldo", "empresa", "postular",
        "candidato", "perfil", "experiencia", "años", "jornada", "beneficio",
        "incorporar", "incorporamos", "necesitamos", "buscamos",
    ]

    def puntaje(o: str) -> int:
        ol = o.lower()
        pts = sum(1 for w in _PALABRAS_RELEVANTES if w in ol)
        # Penalizar oraciones muy cortas o que empiecen con "Si" (condicionales)
        if len(o) < 40:
            pts -= 2
        if ol.startswith("si "):
            pts -= 1
        return pts

    # Ordenar por relevancia, luego tomar las 2 mejores
    candidatas = [(puntaje(o), i, o) for i, o in enumerate(oraciones) if 35 <= len(o) <= 350]
    candidatas.sort(key=lambda x: (-x[0], x[1]))  # mayor puntaje, menor índice
    mejores = sorted(candidatas[:2], key=lambda x: x[1])  # restaurar orden original

    if not mejores:
        # Fallback: primeras dos oraciones largas
        mejores_fb = [(0, i, o) for i, o in enumerate(oraciones) if len(o) >= 35][:2]
        mejores = sorted(mejores_fb, key=lambda x: x[1])

    if not mejores:
        return ""

    resumen = ". ".join(m[2] for m in mejores).strip()
    if not resumen.endswith("."):
        resumen += "."

    palabras = resumen.split()
    if len(palabras) > max_palabras:
        resumen = " ".join(palabras[:max_palabras]) + "…"

    return resumen

def dedup(items: List[Oferta]) -> List[Oferta]:
    seen, out = set(), []
    for o in items:
        k = o.link.strip().lower()
        if k and k not in seen:
            seen.add(k)
            out.append(o)
    return out

# ─── HTTP ─────────────────────────────────────────────────────────────────────
def get_soup(url: str, retries: int = 3, timeout: int = 12) -> Optional[BeautifulSoup]:
    for i in range(retries):
        try:
            s = requests.Session()
            s.headers.update({
                "User-Agent": random.choice(USER_AGENTS),
                "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
                "Accept-Language": "es-CL,es;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "DNT": "1", "Connection": "keep-alive",
            })
            time.sleep(random.uniform(0.5, 1.5))
            r = s.get(url, timeout=(timeout, timeout * 2), allow_redirects=True)
            r.raise_for_status()
            if len(r.text) < 300:
                if i < retries - 1:
                    continue
            log.debug(f"GET {url[:80]} → {r.status_code} ({len(r.text):,}b)")
            return BeautifulSoup(r.text, "html.parser")
        except requests.exceptions.ConnectionError as e:
            log.warning(f"❌ {i+1}/{retries} [conexión]: {str(e)[:120]}")
        except requests.exceptions.Timeout:
            log.warning(f"❌ {i+1}/{retries} [timeout]: {url[:70]}")
        except Exception as e:
            log.warning(f"❌ {i+1}/{retries}: {e}")
        if i < retries - 1:
            time.sleep(random.uniform(2, 4))
    log.error(f"🚫 FALLÓ: {url[:80]}")
    return None

# ─── DB ───────────────────────────────────────────────────────────────────────
def get_db():
    return psycopg2.connect(DATABASE_URL, sslmode="require")

def init_db() -> None:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id             BIGSERIAL PRIMARY KEY,
                job_code       TEXT UNIQUE NOT NULL,
                source         TEXT NOT NULL,
                title          TEXT NOT NULL,
                company        TEXT DEFAULT 'No especificada',
                link           TEXT UNIQUE NOT NULL,
                date_text      TEXT DEFAULT 'No especificada',
                salary         TEXT DEFAULT 'No especificado',
                jornada        TEXT DEFAULT 'No especificada',
                description    TEXT DEFAULT '',
                requisitos     JSONB DEFAULT '[]'::jsonb,
                resumen        TEXT DEFAULT '',
                fingerprint    TEXT NOT NULL,
                first_seen_at  TIMESTAMPTZ DEFAULT NOW(),
                last_seen_at   TIMESTAMPTZ DEFAULT NOW(),
                applied_status TEXT DEFAULT 'unknown'
            );
            CREATE UNIQUE INDEX IF NOT EXISTS jobs_link_idx ON jobs(link);
            CREATE        INDEX IF NOT EXISTS jobs_code_idx ON jobs(job_code);
            CREATE        INDEX IF NOT EXISTS jobs_status_idx ON jobs(applied_status);
            CREATE        INDEX IF NOT EXISTS jobs_seen_idx ON jobs(first_seen_at DESC);
            CREATE TABLE IF NOT EXISTS source_health (
                source             TEXT PRIMARY KEY,
                consecutive_errors INT DEFAULT 0,
                last_error         TEXT,
                last_success_at    TIMESTAMPTZ
            );
            CREATE TABLE IF NOT EXISTS bot_state (key TEXT PRIMARY KEY, value TEXT);
        """)
        conn.commit()

def upsert_job(o: Oferta) -> Tuple[int, str, str]:
    """
    Inserta o actualiza un trabajo.
    Retorna (id, applied_status, 'inserted' | 'updated' | 'error').

    FIX CRÍTICO v9: El patrón (xmax=0) AS inserted de PostgreSQL es SIEMPRE True
    en ON CONFLICT DO UPDATE porque RETURNING devuelve la nueva tupla cuyo xmax=0.
    Fix: dos pasos separados — INSERT ON CONFLICT DO NOTHING + rowcount check.
    """
    code        = make_code(o.source, o.link)
    fingerprint = short_hash(o.link)
    req_json    = json.dumps([{"tipo": r.tipo, "texto": r.texto} for r in o.requisitos])

    with get_db() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        try:
            # Paso 1: intentar insertar, ignorar si ya existe
            cur.execute("""
                INSERT INTO jobs(job_code, source, title, company, link, date_text, salary,
                                 jornada, description, requisitos, resumen, fingerprint)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (link) DO NOTHING
                RETURNING id, applied_status
            """, (code, o.source, o.title, o.company, o.link, o.date_text, o.salary,
                  o.jornada, o.description, req_json, o.resumen, fingerprint))

            row = cur.fetchone()
            conn.commit()

            if row:
                # rowcount > 0 → realmente fue insertada (NUEVA)
                return int(row["id"]), str(row["applied_status"]), "inserted"

            # Paso 2: ya existía → solo actualizar last_seen_at
            cur.execute("""
                UPDATE jobs SET last_seen_at = NOW()
                WHERE link = %s
                RETURNING id, applied_status
            """, (o.link,))
            row = cur.fetchone()
            conn.commit()
            if row:
                return int(row["id"]), str(row["applied_status"]), "updated"
            return 0, "unknown", "error"

        except Exception as e:
            log.error(f"upsert error {o.link[:60]}: {e}")
            conn.rollback()
            return 0, "unknown", "error"

def update_job_detail(job_id: int, o: Oferta) -> None:
    """Actualiza los campos de detalle de una oferta ya insertada."""
    req_json = json.dumps([{"tipo": r.tipo, "texto": r.texto} for r in o.requisitos])
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("""
            UPDATE jobs SET company=%s, salary=%s, jornada=%s, date_text=%s,
                            description=%s, requisitos=%s, resumen=%s
            WHERE id=%s
        """, (o.company, o.salary, o.jornada, o.date_text,
              o.description, req_json, o.resumen, job_id))
        conn.commit()

def update_applied_status(job_code: str, status: str) -> int:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("UPDATE jobs SET applied_status=%s WHERE job_code=%s",
                    (status, job_code.upper()))
        n = cur.rowcount
        conn.commit()
    return n

def get_job_info(job_code: str) -> Optional[dict]:
    with get_db() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT job_code, title, company, salary, jornada, link,
                   applied_status, first_seen_at, source
            FROM jobs WHERE job_code=%s
        """, (job_code.upper(),))
        row = cur.fetchone()
        return dict(row) if row else None

def get_recent_jobs(limit: int = 10) -> List[dict]:
    with get_db() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT job_code, title, company, salary, source, applied_status, first_seen_at
            FROM jobs ORDER BY first_seen_at DESC LIMIT %s
        """, (min(limit, 30),))
        return [dict(r) for r in cur.fetchall()]

def get_jobs_by_status(status: str, limit: int = 20) -> List[dict]:
    with get_db() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT job_code, title, company, salary, source, first_seen_at
            FROM jobs WHERE applied_status=%s
            ORDER BY first_seen_at DESC LIMIT %s
        """, (status, min(limit, 30)))
        return [dict(r) for r in cur.fetchall()]

def search_jobs(query: str, limit: int = 10) -> List[dict]:
    pattern = f"%{query.lower()}%"
    with get_db() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT job_code, title, company, salary, source, applied_status, first_seen_at
            FROM jobs
            WHERE LOWER(title) LIKE %s OR LOWER(company) LIKE %s
            ORDER BY first_seen_at DESC LIMIT %s
        """, (pattern, pattern, min(limit, 20)))
        return [dict(r) for r in cur.fetchall()]

def get_stats() -> dict:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN applied_status='applied'     THEN 1 ELSE 0 END) AS aplicados,
                SUM(CASE WHEN applied_status='not_applied' THEN 1 ELSE 0 END) AS no_aplicados,
                SUM(CASE WHEN applied_status='unknown'     THEN 1 ELSE 0 END) AS pendientes,
                MIN(first_seen_at) AS primer_trabajo,
                MAX(first_seen_at) AS ultimo_trabajo
            FROM jobs
        """)
        row = cur.fetchone()
        return {
            "total": row[0] or 0, "aplicados": row[1] or 0,
            "no_aplicados": row[2] or 0, "pendientes": row[3] or 0,
            "primer": row[4], "ultimo": row[5],
        }

def get_state_int(key: str, default: int = 0) -> int:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("SELECT value FROM bot_state WHERE key=%s", (key,))
        row = cur.fetchone()
        return int(row[0]) if row else default

def set_state_str(key: str, value: str) -> None:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("INSERT INTO bot_state(key,value) VALUES(%s,%s) "
                    "ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value", (key, value))
        conn.commit()

# ─── Source health ─────────────────────────────────────────────────────────────
def set_source_ok(source: str) -> None:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("INSERT INTO source_health(source,consecutive_errors,last_success_at) "
                    "VALUES(%s,0,NOW()) ON CONFLICT(source) DO UPDATE "
                    "SET consecutive_errors=0,last_error=NULL,last_success_at=NOW()", (source,))
        conn.commit()

def set_source_error(source: str, err: str) -> int:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("INSERT INTO source_health(source,consecutive_errors,last_error) "
                    "VALUES(%s,1,%s) ON CONFLICT(source) DO UPDATE "
                    "SET consecutive_errors=source_health.consecutive_errors+1,"
                    "last_error=EXCLUDED.last_error RETURNING consecutive_errors",
                    (source, err[:250]))
        n = cur.fetchone()[0]
        conn.commit()
    return n

def should_cooldown(source: str) -> bool:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("SELECT consecutive_errors FROM source_health WHERE source=%s", (source,))
        row = cur.fetchone()
        if row and row[0] >= 5:
            log.warning(f"🔥 {source} COOLDOWN ({row[0]} errores)")
            return True
    return False

# ─── Enriquecimiento de detalle ────────────────────────────────────────────────
def _texto_o_vacio(soup: BeautifulSoup, *selectors) -> str:
    """Intenta varios selectores CSS y retorna el primero que tenga texto."""
    for sel in selectors:
        try:
            el = soup.select_one(sel)
            if el:
                t = limpiar(el.get_text())
                if t and len(t) > 1:
                    return t
        except Exception:
            pass
    return ""

def enriquecer_computrabajo(o: Oferta) -> Oferta:
    """
    Visita la página de detalle de Computrabajo y extrae datos reales.
    FIX v9: selectores actualizados + filtro agresivo de ruido de sitio.
    """
    soup = get_soup(o.link, retries=2, timeout=14)
    if not soup:
        return o

    # ── Eliminar del árbol HTML los bloques que son definitivamente ruido ──
    for sel in ["nav", "footer", "header", ".sidebar", "#sidebar",
                "[class*='cookie']", "[class*='banner']", "[class*='publicidad']",
                "[class*='related']", "[class*='recomend']", "[class*='similar']",
                "[class*='footer']", "[class*='header']", "[class*='nav']",
                "script", "style", "noscript", "[class*='modal']"]:
        for el in soup.select(sel):
            el.decompose()

    texto_pagina = soup.get_text(" ")

    # ── Empresa ─────────────────────────────────────────────────────────────
    # Computrabajo pone la empresa en h2.subtitle > a, o en p con clase fc_base2
    company = ""
    for sel in ["h2.subtitle a", "h2.subtitle", "p.subtitle a", "p.subtitle",
                "[itemprop='hiringOrganization']", "[itemprop='name']",
                "a[href*='/empresa/']", "a[href*='/company/']",
                ".company_name", ".icoCompany + span", "h3.fc_base"]:
        el = soup.select_one(sel)
        if el:
            t = limpiar(el.get_text())
            # Descartar si parece un título de trabajo (muchas palabras en mayúsculas o verbos)
            if t and len(t) > 1 and len(t) < 80 and not re.search(r"\b(busca|requiere|solicita|necesita)\b", t, re.I):
                company = t
                break
    company = company or o.company

    # ── Salario ─────────────────────────────────────────────────────────────
    salary = ""
    for sel in ["[data-t='salary']", ".salary", "li.icon-salary",
                "[class*='salary']", "[class*='sueldo']", "[class*='remu']",
                "p.fs16.fc_base", "span.fs19"]:
        el = soup.select_one(sel)
        if el:
            t = limpiar(el.get_text())
            if t and "$" in t:
                salary = t
                break
    if not salary:
        m = re.search(r"\$\s?[\d\.,]+(?:\s*[-–]\s*\$\s?[\d\.,]+)?(?:\s*(?:netos?|brutos?|mensual|líquido|pesos))?",
                      texto_pagina, re.IGNORECASE)
        if m:
            salary = limpiar(m.group(0))
    salary = salary or "No especificado"

    # ── Jornada ─────────────────────────────────────────────────────────────
    jornada = ""
    for sel in ["[data-t='contract']", "[data-t='workday']", "li.icon-clock",
                "[class*='jornada']", "[class*='workday']", "[class*='contract']"]:
        el = soup.select_one(sel)
        if el:
            t = limpiar(el.get_text())
            if t and len(t) < 60:
                jornada = t
                break
    if not jornada:
        for kw in ["jornada completa", "part time", "part-time", "media jornada",
                   "tiempo completo", "tiempo parcial", "turnos rotativos"]:
            if kw in texto_pagina.lower():
                jornada = kw.title()
                break
    jornada = jornada or "No especificada"

    # ── Fecha ────────────────────────────────────────────────────────────────
    date_text = ""
    for sel in ["[data-t='date']", "time[datetime]", "span.timeago",
                "p.fc_aux", "[class*='posted']", "[class*='fecha']"]:
        el = soup.select_one(sel)
        if el:
            t = limpiar(el.get("datetime", "") or el.get_text())
            # Solo tomar si parece una fecha real (tiene dígitos y no es ruido)
            if t and re.search(r"\d", t) and len(t) < 50 and not _es_ruido(t):
                date_text = t
                break
    if not date_text:
        m = re.search(r"\d{1,2}\s*(?:de\s*)?(?:enero|febrero|marzo|abril|mayo|junio|julio|agosto|"
                      r"septiembre|octubre|noviembre|diciembre)\s*(?:de\s*)?\d{4}",
                      texto_pagina, re.IGNORECASE)
        if m:
            date_text = m.group(0)
    if not date_text:
        m = re.search(r"\d{1,2}[/-]\d{1,2}[/-]\d{2,4}", texto_pagina)
        if m:
            date_text = m.group(0)
    date_text = date_text or "No especificada"

    # ── Descripción (contenido real del puesto) ───────────────────────────
    desc_raw = ""
    # Intentar selectores específicos del contenedor principal de descripción
    for sel in ["#job_description", "div.box_description", "div.oferta_desc",
                "div[id*='description']", "div[class*='job_desc']",
                "section.box_detail", "div.cont_bloque", "article.box_oferta"]:
        el = soup.select_one(sel)
        if el:
            desc_raw = limpiar(el.get_text(" "))
            if len(desc_raw) > 100:
                break

    # Fallback: bloques de párrafos con texto sustancial
    if len(desc_raw) < 100:
        partes = []
        for tag in soup.find_all(["p", "li", "div"], recursive=True):
            # Solo tags directos con texto propio (no sus hijos)
            t = limpiar(tag.get_text(" "))
            if 40 < len(t) < 1000 and not _es_ruido(t):
                # Evitar duplicados
                if not any(t[:40] in p for p in partes):
                    partes.append(t)
                    if len(partes) >= 8:
                        break
        desc_raw = " ".join(partes)

    # Filtrar ruido del texto extraído
    descripcion = limpiar_descripcion(desc_raw)

    requisitos = extraer_requisitos(descripcion)
    resumen    = generar_resumen(descripcion)

    o.company     = company
    o.salary      = salary
    o.jornada     = jornada
    o.date_text   = date_text
    o.description = descripcion
    o.requisitos  = requisitos
    o.resumen     = resumen
    return o

def enriquecer_bne(o: Oferta) -> Oferta:
    soup = get_soup(o.link, retries=2, timeout=14)
    if not soup:
        return o

    # Eliminar ruido estructural
    for sel in ["nav", "footer", "header", ".sidebar", "script", "style",
                "[class*='related']", "[class*='similar']"]:
        for el in soup.select(sel):
            el.decompose()

    company = (_texto_o_vacio(soup,
        ".company-name", "h2.company", "[class*='company']",
        "a[href*='/empresa']", ".job-company") or o.company)

    salary = (_texto_o_vacio(soup, ".salary", "[class*='sueldo']", "[class*='salari']") or "")
    if not salary:
        m = re.search(r"\$\s?[\d\.,]+", soup.get_text())
        salary = m.group(0) if m else "No especificado"

    jornada = (_texto_o_vacio(soup, "[class*='jornada']", "[class*='contract']") or "No especificada")
    date_text = (_texto_o_vacio(soup, ".date", "time", "[class*='fecha']") or "No especificada")

    desc_el = (soup.select_one(".job-description") or soup.select_one("[class*='description']")
               or soup.select_one("article"))
    desc_raw = limpiar(desc_el.get_text(" ")) if desc_el else ""
    if not desc_raw:
        partes = [limpiar(p.get_text()) for p in soup.find_all("p")
                  if 40 < len(limpiar(p.get_text())) < 800 and not _es_ruido(limpiar(p.get_text()))][:5]
        desc_raw = " ".join(partes)

    descripcion = limpiar_descripcion(desc_raw)
    o.company    = company
    o.salary     = salary
    o.jornada    = jornada
    o.date_text  = date_text
    o.description = descripcion
    o.requisitos = extraer_requisitos(descripcion)
    o.resumen    = generar_resumen(descripcion)
    return o

def enriquecer_yapo(o: Oferta) -> Oferta:
    soup = get_soup(o.link, retries=2, timeout=14)
    if not soup:
        return o

    for sel in ["nav", "footer", "header", "script", "style", "[class*='related']"]:
        for el in soup.select(sel):
            el.decompose()

    company = (_texto_o_vacio(soup, ".seller-name", ".advertiser", "[class*='user']") or o.company)

    m = re.search(r"\$\s?[\d\.,]+(?:\s*[-–]\s*\$\s?[\d\.,]+)?", soup.get_text())
    salary = m.group(0) if m else "No especificado"

    desc_el = (soup.select_one(".description") or soup.select_one("[class*='detail']")
               or soup.select_one("article"))
    desc_raw = limpiar(desc_el.get_text(" ")) if desc_el else ""
    descripcion = limpiar_descripcion(desc_raw)

    o.company    = company
    o.salary     = salary
    o.description = descripcion
    o.requisitos = extraer_requisitos(descripcion)
    o.resumen    = generar_resumen(descripcion)
    return o

def enriquecer_oferta(o: Oferta) -> Oferta:
    """
    Dispatcher: enriquece la oferta según la fuente.
    Chiletrabajos = sin detalle (páginas de detalle dan timeout en servidor).
    """
    try:
        if o.source == "Computrabajo":
            return enriquecer_computrabajo(o)
        elif o.source == "BNE":
            return enriquecer_bne(o)
        elif o.source == "Yapo":
            return enriquecer_yapo(o)
        # Indeed ya trae descripción via RSS; Chiletrabajos no fetchea detalle
    except Exception as e:
        log.warning(f"⚠️ enriquecer {o.source} {o.link[:50]}: {e}")
    return o

# ─── Telegram ─────────────────────────────────────────────────────────────────
STATUS_LABEL = {
    "applied":     ("✅", "Ya postulaste"),
    "not_applied": ("❌", "No postulado"),
    "unknown":     ("❓", "Sin gestionar"),
}
STATUS_ICON = {"applied": "✅", "not_applied": "❌", "unknown": "❓"}

def telegram_send(msg: str, chat_id: str = None) -> Optional[int]:
    target = chat_id or TELEGRAM_CHAT_ID
    if not TELEGRAM_TOKEN or not target:
        return None
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data={"chat_id": target, "text": msg[:MAX_MSG],
                  "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=12,
        )
        if r.ok:
            return r.json().get("result", {}).get("message_id")
        log.warning(f"Telegram {r.status_code}: {r.text[:80]}")
    except Exception as e:
        log.error(f"Telegram error: {e}")
    return None

def formatear_oferta(o: Oferta, code: str, status: str) -> str:
    """Mensaje de Telegram para una oferta nueva."""
    emoji_s, label_s = STATUS_LABEL.get(status, ("❓", "Sin gestionar"))

    def fila(em, label, val):
        if val and val.lower() not in ("no especificado","no especificada","no disponible",""):
            return f"{em} <b>{label}:</b> {html.escape(val)}"
        return None

    lines = [
        f"<b>🆕 NUEVA OFERTA — {html.escape(o.source.upper())}</b>",
        "━━━━━━━━━━━━━━━━━━━━━━━━",
        f"<b>📌 {html.escape(o.title)}</b>",
        "",
        f"🆔 <code>{code}</code>",
    ]
    for r in filter(None, [
        fila("🏢", "Empresa", o.company),
        fila("💰", "Salario", o.salary),
        fila("🕒", "Jornada", o.jornada),
        fila("📅", "Fecha",   o.date_text),
    ]):
        lines.append(r)
    lines.append(f"📬 <b>Estado:</b> {emoji_s} {label_s}")

    if o.resumen:
        lines += ["", "─────────────────────────",
                  "<b>💼 Descripción</b>",
                  html.escape(o.resumen[:300])]

    if o.requisitos:
        lines += ["", "<b>📋 Requisitos</b>"]
        icons = {"educacion": "🎓", "experiencia": "🏆", "habilidad": "⚡", "general": "•"}
        for req in o.requisitos[:5]:
            lines.append(f"  {icons.get(req.tipo,'•')} {html.escape(req.texto[:100])}")

    lines += [
        "",
        "━━━━━━━━━━━━━━━━━━━━━━━━",
        f"🔗 <a href='{o.link}'>Ver oferta completa</a>",
        "",
        "¿Postulaste a esta oferta?",
        f"  /postule {code}     ✅ Sí",
        f"  /nopostule {code}   ❌ No",
    ]
    return "\n".join(lines)[:MAX_MSG]

def _fmt_fecha(dt) -> str:
    if not dt:
        return "?"
    if isinstance(dt, str):
        return dt[:10]
    return dt.strftime("%d/%m/%Y")

def _fmt_job_linea(r: dict, idx: int = None) -> str:
    """Una línea compacta para listas de trabajos."""
    icon = STATUS_ICON.get(r.get("applied_status", "unknown"), "❓")
    prefix = f"{idx}. " if idx else ""
    titulo = html.escape(r["title"][:50])
    empresa = html.escape((r.get("company") or "").strip()[:30]) or "—"
    code = r.get("job_code", "")
    fecha = _fmt_fecha(r.get("first_seen_at"))
    return (f"{prefix}{icon} <b>{titulo}</b>\n"
            f"   🏢 {empresa}  |  📅 {fecha}\n"
            f"   <code>{code}</code>")

# ─── Comandos Telegram ─────────────────────────────────────────────────────────
def cmd_listar(chat_id: str, args: List[str]) -> None:
    n = 10
    if args:
        try:
            n = max(1, min(int(args[0]), 25))
        except ValueError:
            pass
    jobs = get_recent_jobs(n)
    if not jobs:
        telegram_send("📭 No hay ofertas registradas aún.", chat_id)
        return
    lineas = [f"<b>📋 Últimas {len(jobs)} ofertas enviadas</b>",
              "━━━━━━━━━━━━━━━━━━━━━━━━"]
    for i, j in enumerate(jobs, 1):
        lineas.append(_fmt_job_linea(j, i))
        lineas.append("")
    lineas.append("<i>Usa /estado CÓDIGO para ver detalle.</i>")
    telegram_send("\n".join(lineas), chat_id)

def cmd_postulaciones(chat_id: str) -> None:
    jobs = get_jobs_by_status("applied")
    if not jobs:
        telegram_send("📭 No tienes trabajos marcados como postulados.", chat_id)
        return
    lineas = [f"<b>✅ Tus postulaciones ({len(jobs)})</b>",
              "━━━━━━━━━━━━━━━━━━━━━━━━"]
    for i, j in enumerate(jobs, 1):
        lineas.append(_fmt_job_linea(j, i))
        lineas.append("")
    telegram_send("\n".join(lineas), chat_id)

def cmd_pendientes(chat_id: str) -> None:
    jobs = get_jobs_by_status("unknown", limit=15)
    if not jobs:
        telegram_send("🎉 No tienes ofertas pendientes de gestionar.", chat_id)
        return
    lineas = [f"<b>❓ Pendientes de gestionar ({len(jobs)})</b>",
              "━━━━━━━━━━━━━━━━━━━━━━━━",
              "<i>Usa /postule o /nopostule CÓDIGO para cada una.</i>",
              ""]
    for i, j in enumerate(jobs, 1):
        lineas.append(_fmt_job_linea(j, i))
        lineas.append("")
    telegram_send("\n".join(lineas), chat_id)

def cmd_buscar(chat_id: str, query: str) -> None:
    if len(query) < 2:
        telegram_send("⚠️ Escribe al menos 2 caracteres para buscar.", chat_id)
        return
    jobs = search_jobs(query)
    if not jobs:
        telegram_send(f"🔍 Sin resultados para <b>{html.escape(query)}</b>.", chat_id)
        return
    lineas = [f"<b>🔍 Resultados para \"{html.escape(query)}\" ({len(jobs)})</b>",
              "━━━━━━━━━━━━━━━━━━━━━━━━"]
    for i, j in enumerate(jobs, 1):
        lineas.append(_fmt_job_linea(j, i))
        lineas.append("")
    telegram_send("\n".join(lineas), chat_id)

def cmd_resumen(chat_id: str) -> None:
    s = get_stats()
    msg = (
        f"<b>📊 Resumen general</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📦 Total trabajos detectados: <b>{s['total']}</b>\n\n"
        f"✅ Postulados:       <b>{s['aplicados']}</b>\n"
        f"❌ No postulados:   <b>{s['no_aplicados']}</b>\n"
        f"❓ Sin gestionar:   <b>{s['pendientes']}</b>\n\n"
        f"📅 Primer trabajo:  {_fmt_fecha(s['primer'])}\n"
        f"📅 Último trabajo:  {_fmt_fecha(s['ultimo'])}"
    )
    telegram_send(msg, chat_id)

def cmd_estado(chat_id: str, code: str) -> None:
    info = get_job_info(code)
    if not info:
        telegram_send(f"⚠️ Código no encontrado: <code>{code}</code>", chat_id)
        return
    emoji_s, label_s = STATUS_LABEL.get(info["applied_status"], ("❓", "Sin gestionar"))
    salary  = info.get("salary") or "No especificado"
    jornada = info.get("jornada") or "No especificada"
    msg = (
        f"<b>🔍 Detalle del trabajo</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🆔 <code>{code}</code>\n"
        f"📌 <b>{html.escape(info['title'][:80])}</b>\n"
        f"🏢 {html.escape(info.get('company') or 'No especificada')}\n"
        f"💰 {html.escape(salary)}\n"
        f"🕒 {html.escape(jornada)}\n"
        f"📅 Detectado: {_fmt_fecha(info['first_seen_at'])}\n"
        f"🌐 {html.escape(info.get('source','?'))}\n"
        f"📬 <b>Estado: {emoji_s} {label_s}</b>\n\n"
        f"🔗 <a href='{info['link']}'>Ver oferta completa</a>"
    )
    if info["applied_status"] != "applied":
        msg += f"\n\n/postule {code}   ✅ Sí postulé"
    if info["applied_status"] != "not_applied":
        msg += f"\n/nopostule {code}  ❌ No postulé"
    telegram_send(msg, chat_id)

def poll_telegram_commands() -> None:
    """Hilo demonio: long-polling de comandos Telegram."""
    log.info("📡 Iniciando handler de comandos Telegram")
    while True:
        try:
            offset = get_state_int("telegram_offset", 0)
            r = requests.get(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
                params={"offset": offset, "timeout": 30, "allowed_updates": ["message"]},
                timeout=38,
            )
            if not r.ok:
                time.sleep(5)
                continue

            for update in r.json().get("result", []):
                offset   = update["update_id"] + 1
                msg_data = update.get("message", {})
                text     = msg_data.get("text", "").strip()
                chat_id  = str(msg_data.get("chat", {}).get("id", ""))
                if not text or not chat_id:
                    continue

                parts = text.split()
                cmd   = parts[0].lower().split("@")[0]
                args  = parts[1:]
                code  = args[0].upper() if args else ""

                # ── /postule CODE ──────────────────────────────────────────
                if cmd == "/postule" and code:
                    n = update_applied_status(code, "applied")
                    if n > 0:
                        info = get_job_info(code)
                        titulo = html.escape(info["title"][:60]) if info else code
                        telegram_send(
                            f"✅ <b>¡Postulación registrada!</b>\n"
                            f"━━━━━━━━━━━━━━━━━━━━\n"
                            f"🆔 <code>{code}</code>\n"
                            f"📌 {titulo}\n"
                            f"📬 Estado: ✅ Ya postulaste\n\n"
                            f"<i>Usa /nopostule {code} si fue un error.</i>",
                            chat_id)
                    else:
                        telegram_send(f"⚠️ Código no encontrado: <code>{code}</code>", chat_id)

                # ── /nopostule CODE ────────────────────────────────────────
                elif cmd == "/nopostule" and code:
                    n = update_applied_status(code, "not_applied")
                    if n > 0:
                        info = get_job_info(code)
                        titulo = html.escape(info["title"][:60]) if info else code
                        telegram_send(
                            f"❌ <b>Marcado como no postulado</b>\n"
                            f"━━━━━━━━━━━━━━━━━━━━\n"
                            f"🆔 <code>{code}</code>\n"
                            f"📌 {titulo}\n\n"
                            f"<i>Usa /postule {code} si cambias de opinión.</i>",
                            chat_id)
                    else:
                        telegram_send(f"⚠️ Código no encontrado: <code>{code}</code>", chat_id)

                # ── /estado CODE ───────────────────────────────────────────
                elif cmd == "/estado" and code:
                    cmd_estado(chat_id, code)

                # ── /listar [N] ────────────────────────────────────────────
                elif cmd == "/listar":
                    cmd_listar(chat_id, args)

                # ── /postulaciones ─────────────────────────────────────────
                elif cmd in ("/postulaciones", "/aplicados"):
                    cmd_postulaciones(chat_id)

                # ── /pendientes ────────────────────────────────────────────
                elif cmd == "/pendientes":
                    cmd_pendientes(chat_id)

                # ── /buscar TEXTO ──────────────────────────────────────────
                elif cmd == "/buscar":
                    query = " ".join(args)
                    cmd_buscar(chat_id, query)

                # ── /resumen ───────────────────────────────────────────────
                elif cmd == "/resumen":
                    cmd_resumen(chat_id)

                # ── /ayuda / /start ────────────────────────────────────────
                elif cmd in ("/ayuda", "/start", "/help"):
                    telegram_send(
                        "<b>🤖 Comandos disponibles</b>\n"
                        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
                        "<b>Gestión de postulaciones:</b>\n"
                        "/postule <code>CÓDIGO</code>    — Marcar como postulado ✅\n"
                        "/nopostule <code>CÓDIGO</code>  — Marcar como no postulado ❌\n"
                        "/estado <code>CÓDIGO</code>     — Ver detalle de una oferta 🔍\n\n"
                        "<b>Historial:</b>\n"
                        "/listar <code>[N]</code>        — Últimas N ofertas (default 10)\n"
                        "/postulaciones                  — Trabajos donde postulé ✅\n"
                        "/pendientes                     — Trabajos sin gestionar ❓\n"
                        "/buscar <code>TEXTO</code>      — Buscar en título/empresa 🔎\n"
                        "/resumen                        — Estadísticas generales 📊\n\n"
                        "<i>El CÓDIGO aparece en cada oferta enviada.</i>",
                        chat_id)

            set_state_str("telegram_offset", str(offset))

        except requests.exceptions.Timeout:
            pass
        except Exception as e:
            log.error(f"poll_commands error: {e}")
            time.sleep(5)

# ─── Parsers ───────────────────────────────────────────────────────────────────

# ── Chiletrabajos ──────────────────────────────────────────────────────────────
def _extraer_meta_listing(contenedor) -> dict:
    datos = {"company": "No especificada", "date_text": "No especificada",
             "salary": "No especificado",  "jornada": "No especificada"}
    if not contenedor:
        return datos
    texto = limpiar(contenedor.get_text(" | "))
    for sel in ["span.empresa","span.company","div.empresa",".nombre-empresa",
                "[class*='empresa']","[class*='company']"]:
        el = contenedor.select_one(sel)
        if el:
            datos["company"] = limpiar(el.get_text())
            break
    m = re.search(r"\$[\d\.,]+(?:\s*[-–]\s*\$[\d\.,]+)?", texto)
    if m:
        datos["salary"] = m.group(0)
    m = re.search(r"\d{1,2}[/-]\d{1,2}[/-]\d{2,4}", texto)
    if m:
        datos["date_text"] = m.group(0)
    for kw in ["jornada completa","part.time","part time","media jornada","tiempo completo"]:
        if kw in texto.lower():
            datos["jornada"] = kw.title()
            break
    return datos

def parse_chiletrabajos() -> Tuple[List[Oferta], Dict]:
    """Sin fetch de detalle: páginas /trabajo/... dan timeout desde IPs de nube."""
    source = "Chiletrabajos"
    if should_cooldown(source):
        return [], {"error": "cooldown"}
    out   = []
    stats = {"paginas_ok": 0, "estrategias": {f"E{i}": 0 for i in range(1, 5)}, "total_enlaces": 0}
    base  = "https://www.chiletrabajos.cl"
    urls  = [f"{base}/ciudad/osorno.html",
             f"{base}/ciudad/osorno/2.html",
             f"{base}/ciudad/osorno/3.html"]
    try:
        for url in urls:
            log.info(f"🌐 [{source}] {url}")
            soup = get_soup(url, retries=2, timeout=10)
            if not soup:
                continue
            stats["paginas_ok"] += 1
            visto = set()

            def agregar_ct(link, title, contenedor, est):
                k = link.strip().lower()
                if not k or k in visto or len(title) < 5:
                    return
                visto.add(k)
                meta = _extraer_meta_listing(contenedor)
                out.append(Oferta(source=source, title=title, link=link,
                                  company=meta["company"], date_text=meta["date_text"],
                                  salary=meta["salary"], jornada=meta["jornada"],
                                  location_verified=True))
                stats["estrategias"][est] += 1

            for h in soup.find_all(["h2", "h3"]):
                a = h.find("a", href=True)
                if a and "/trabajo/" in a.get("href", ""):
                    href = a["href"]
                    link = href if href.startswith("http") else f"{base}{href}"
                    cont = h.find_parent(["article","div","li","section"]) or h
                    agregar_ct(link, limpiar(a.get_text()), cont,
                               "E1" if h.name == "h2" else "E2")
            for div in soup.find_all("div", class_=re.compile(r"(job|oferta|trabajo|empleo|card|item)", re.I)):
                a = div.find("a", href=True)
                if a and "/trabajo/" in a.get("href", ""):
                    href = a["href"]
                    link = href if href.startswith("http") else f"{base}{href}"
                    agregar_ct(link, limpiar(a.get_text()), div, "E3")
            if len(visto) < 10:
                for a in soup.find_all("a", href=True):
                    if "/trabajo/" in a.get("href", ""):
                        href = a["href"]
                        link = href if href.startswith("http") else f"{base}{href}"
                        cont = a.find_parent(["article","li","div"]) or a
                        agregar_ct(link, limpiar(a.get_text()), cont, "E4")

            stats["total_enlaces"] += len(visto)
            log.info(f"📊 [{source}] {len(visto)} ofertas")
            time.sleep(random.uniform(1, 2))

        if stats["paginas_ok"] > 0:
            set_source_ok(source)
        else:
            n = set_source_error(source, "timeout/bloqueo en todas las páginas")
            log.warning(f"⚠️ [{source}] 0 páginas ok — error #{n}")
        log.info(f"✅ [{source}] {len(out)} total")
    except Exception as e:
        log.exception(f"❌ [{source}]: {e}")
        set_source_error(source, str(e))
    return dedup(out), stats

# ── BNE ───────────────────────────────────────────────────────────────────────
def parse_bne() -> Tuple[List[Oferta], Dict]:
    source = "BNE"
    if should_cooldown(source):
        return [], {"error": "cooldown"}
    out   = []
    stats = {"estrategias": {f"E{i}": 0 for i in range(1, 6)}, "total_enlaces": 0}
    seen  = set()
    urls  = ["https://www.bne.cl/ofertas?ubicacion=Osorno",
             "https://www.bne.cl/ofertas?q=&ubicacion=Osorno&orden=reciente",
             "https://www.bne.cl/ofertas?ubicacion=Osorno&pagina=2",
             "https://www.bne.cl/ofertas?ubicacion=Osorno&pagina=3"]
    paginas_ok = 0
    try:
        for url in urls:
            log.info(f"🌐 [{source}] {url}")
            soup = get_soup(url, retries=2, timeout=12)
            if not soup:
                continue
            paginas_ok += 1

            def add_bne(link, text, est):
                k = link.strip().lower()
                if k not in seen and len(text) >= 8:
                    seen.add(k)
                    out.append(Oferta(source=source, title=text, link=link, location_verified=True))
                    stats["estrategias"][est] += 1

            for sel in ["a.job-link","a[href*='oferta']","a[href*='empleo']"]:
                for a in soup.select(sel):
                    href = a.get("href","")
                    link = href if href.startswith("http") else urljoin("https://www.bne.cl", href)
                    add_bne(link, limpiar(a.get_text()), "E1")
            for div in soup.find_all("div", class_=re.compile(r"(job|oferta|empleo|card)", re.I)):
                a = div.find("a", href=True)
                if a:
                    href = a.get("href","")
                    link = href if href.startswith("http") else urljoin("https://www.bne.cl", href)
                    add_bne(link, limpiar(a.get_text()), "E2")
            for article in soup.find_all("article"):
                a = article.find("a", href=True)
                if a:
                    href = a.get("href","")
                    link = href if href.startswith("http") else urljoin("https://www.bne.cl", href)
                    add_bne(link, limpiar(a.get_text()), "E3")
            if len(out) < 10:
                for a in soup.find_all("a", href=True):
                    href = a.get("href","")
                    if "oferta" in href.lower() or "empleo" in href.lower():
                        link = href if href.startswith("http") else urljoin("https://www.bne.cl", href)
                        add_bne(link, limpiar(a.get_text()), "E4")
            time.sleep(random.uniform(1, 2))

        stats["total_enlaces"] = len(out)
        if paginas_ok > 0:
            set_source_ok(source)
        else:
            n = set_source_error(source, "todas las páginas BNE fallaron")
            log.warning(f"⚠️ [{source}] 0 páginas ok — error #{n}")
        log.info(f"✅ [{source}] {len(out)} total")
    except Exception as e:
        log.exception(f"❌ [{source}]: {e}")
        set_source_error(source, str(e))
    return dedup(out), stats

# ── Indeed ────────────────────────────────────────────────────────────────────
def parse_indeed() -> Tuple[List[Oferta], Dict]:
    source = "Indeed"
    if should_cooldown(source):
        return [], {"error": "cooldown"}
    out   = []
    stats = {"rss_intentos": 0, "rss_ok": 0}
    seen  = set()
    rss_urls = [
        f"https://cl.indeed.com/rss?q=&l={quote_plus('Osorno')}&sort=date&limit=50",
        f"https://cl.indeed.com/rss?q={quote_plus('trabajo')}&l={quote_plus('Osorno, Los Lagos')}&sort=date",
        f"https://cl.indeed.com/rss?q=&l={quote_plus('Los Lagos, Chile')}&sort=date&limit=25",
    ]
    try:
        for feed_url in rss_urls:
            stats["rss_intentos"] += 1
            try:
                feed = feedparser.parse(feed_url)
                if not feed.entries:
                    continue
                stats["rss_ok"] += 1
                for e in feed.entries:
                    title   = limpiar(html.unescape(getattr(e, "title", "")))
                    link    = getattr(e, "link", "")
                    summary = limpiar(BeautifulSoup(getattr(e, "summary", ""), "html.parser").get_text())
                    if not title or not link or link in seen:
                        continue
                    seen.add(link)
                    company = "No especificada"
                    if " - " in title:
                        t, c = title.split(" - ", 1)
                        title, company = limpiar(t), limpiar(c)
                    out.append(Oferta(
                        source=source, title=title, company=company, link=link,
                        description=summary,
                        requisitos=extraer_requisitos(summary),
                        resumen=generar_resumen(summary),
                        location_verified=True,
                    ))
            except Exception as e:
                log.warning(f"⚠️ [{source}] RSS error: {e}")
            time.sleep(random.uniform(1, 2))

        if stats["rss_ok"] > 0:
            set_source_ok(source)
        log.info(f"✅ [{source}] {len(out)} ofertas via RSS")
    except Exception as e:
        log.exception(f"❌ [{source}]: {e}")
        set_source_error(source, str(e))
    return dedup(out), stats

# ── Computrabajo ──────────────────────────────────────────────────────────────
def parse_computrabajo() -> Tuple[List[Oferta], Dict]:
    source = "Computrabajo"
    if should_cooldown(source):
        return [], {"error": "cooldown"}
    out   = []
    stats = {"estrategias": {f"E{i}": 0 for i in range(1, 6)}, "total_enlaces": 0}
    seen  = set()
    base  = "https://cl.computrabajo.com"
    urls  = [f"{base}/empleos-en-los-lagos-en-osorno",
             f"{base}/empleos-en-los-lagos-en-osorno?p=2",
             f"{base}/empleos-en-los-lagos-en-osorno?p=3",
             f"{base}/trabajo-de-osorno"]
    paginas_ok = 0
    try:
        for url in urls:
            log.info(f"🌐 [{source}] {url}")
            soup = get_soup(url, retries=3, timeout=12)
            if not soup:
                continue
            paginas_ok += 1

            def add_ct(link, text, est):
                k = link.strip().lower()
                if k not in seen and len(text) >= 8:
                    seen.add(k)
                    out.append(Oferta(source=source, title=text, link=link, location_verified=True))
                    stats["estrategias"][est] += 1

            for article in soup.find_all("article"):
                a = article.find("a", href=True)
                if a and "oferta" in a.get("href","").lower():
                    text = limpiar(a.get_text())
                    if len(text) < 8:
                        for h in article.find_all(["h2","h3"]):
                            text = limpiar(h.get_text())
                            if len(text) >= 8:
                                break
                    link = a["href"] if a["href"].startswith("http") else f"{base}{a['href']}"
                    add_ct(link, text, "E1")
            for a in soup.find_all("a", href=True):
                if "oferta" in a["href"].lower():
                    link = a["href"] if a["href"].startswith("http") else f"{base}{a['href']}"
                    add_ct(link, limpiar(a.get_text()), "E2")
            for h in soup.find_all(["h2","h3"]):
                a = h.find("a", href=True)
                if a:
                    href = a["href"]
                    link = href if href.startswith("http") else f"{base}{href}"
                    if "oferta" in link.lower() or re.search(r"-[0-9a-fA-F]{6,}", link):
                        add_ct(link, limpiar(a.get_text()), "E3")
            if len(out) < 10:
                for a in soup.find_all("a", href=True):
                    if re.search(r"-[0-9a-fA-F]{6,}\.html?", a["href"]):
                        link = a["href"] if a["href"].startswith("http") else f"{base}{a['href']}"
                        add_ct(link, limpiar(a.get_text()), "E4")
            time.sleep(random.uniform(1, 2))

        stats["total_enlaces"] = len(out)
        if paginas_ok > 0:
            set_source_ok(source)
        else:
            n = set_source_error(source, "todas las páginas Computrabajo fallaron")
            log.warning(f"⚠️ [{source}] 0 páginas ok — error #{n}")
        log.info(f"✅ [{source}] {len(out)} total")
    except Exception as e:
        log.exception(f"❌ [{source}]: {e}")
        set_source_error(source, str(e))
    return dedup(out), stats

# ── Yapo ──────────────────────────────────────────────────────────────────────
def parse_yapo() -> Tuple[List[Oferta], Dict]:
    source = "Yapo"
    if should_cooldown(source):
        return [], {"error": "cooldown"}
    out   = []
    stats = {"estrategias": {f"E{i}": 0 for i in range(1, 5)}, "total_enlaces": 0}
    seen  = set()
    urls  = ["https://www.yapo.cl/empleos?ca=12_s&l=0&q=osorno",
             "https://www.yapo.cl/empleos?ca=12_s&l=0&q=osorno&o=25",
             "https://www.yapo.cl/empleos?ca=12_s&l=0&q=osorno&o=50",
             "https://www.yapo.cl/empleos?ca=12_s&l=14&q="]
    paginas_ok = 0
    try:
        for url in urls:
            log.info(f"🌐 [{source}] {url}")
            soup = get_soup(url, retries=2, timeout=12)
            if not soup:
                continue
            paginas_ok += 1

            def add_yapo(link, text, est):
                k = link.strip().lower()
                if k not in seen and len(text) >= 8:
                    seen.add(k)
                    out.append(Oferta(source=source, title=text, link=link, location_verified=True))
                    stats["estrategias"][est] += 1

            for a in soup.find_all("a", href=True):
                href = a["href"]
                if re.search(r"\d{5,}", href) and any(x in href.lower() for x in ["/empleo","/trabajo","/oferta"]):
                    if not any(x in href.lower() for x in ["/arriendo","/venta","/auto"]):
                        link = href if href.startswith("http") else urljoin("https://www.yapo.cl", href)
                        add_yapo(link, limpiar(a.get_text()), "E1")
            for div in soup.find_all("div", class_=re.compile(r"(ad|aviso|listing|item|card)", re.I)):
                a = div.find("a", href=True)
                if a and re.search(r"\d{5,}", a["href"]):
                    href = a["href"]
                    link = href if href.startswith("http") else urljoin("https://www.yapo.cl", href)
                    add_yapo(link, limpiar(a.get_text()), "E2")
            for article in soup.find_all("article"):
                a = article.find("a", href=True)
                if a and re.search(r"\d{5,}", a["href"]):
                    href = a["href"]
                    link = href if href.startswith("http") else urljoin("https://www.yapo.cl", href)
                    add_yapo(link, limpiar(a.get_text()), "E3")
            time.sleep(random.uniform(1, 2))

        stats["total_enlaces"] = len(out)
        if paginas_ok > 0:
            set_source_ok(source)
        else:
            n = set_source_error(source, "todas las páginas Yapo fallaron")
            log.warning(f"⚠️ [{source}] 0 páginas ok — error #{n}")
        log.info(f"✅ [{source}] {len(out)} total")
    except Exception as e:
        log.exception(f"❌ [{source}]: {e}")
        set_source_error(source, str(e))
    return dedup(out), stats

# ─── Ciclo principal ──────────────────────────────────────────────────────────
def run_cycle() -> Dict:
    parsers = [
        ("Chiletrabajos", parse_chiletrabajos),
        ("BNE",           parse_bne),
        ("Indeed",        parse_indeed),
        ("Computrabajo",  parse_computrabajo),
        ("Yapo",          parse_yapo),
    ]
    cycle_stats = {"total": 0, "nuevas": 0, "existentes": 0, "por_fuente": {}}

    for nombre, parser in parsers:
        try:
            log.info(f"\n{'═'*55}\n🚀 {nombre}\n{'═'*55}")
            ofertas, parser_stats = parser()
            cycle_stats["por_fuente"][nombre] = parser_stats
            nuevas = existentes = 0

            for o in ofertas:
                if not pasa_filtros(o):
                    continue
                cycle_stats["total"] += 1
                job_id, status, op = upsert_job(o)

                if op == "inserted":
                    # ── Enriquecer con detalle SOLO para nuevas ofertas ──────
                    log.info(f"🔍 [{nombre}] Enriqueciendo detalle: {o.title[:50]}")
                    o = enriquecer_oferta(o)
                    if job_id > 0:
                        update_job_detail(job_id, o)

                    nuevas += 1
                    cycle_stats["nuevas"] += 1
                    code = make_code(o.source, o.link)
                    msg  = formatear_oferta(o, code, status)
                    telegram_send(msg)
                    time.sleep(0.8)  # Rate-limit Telegram
                else:
                    existentes += 1
                    cycle_stats["existentes"] += 1

            cycle_stats["por_fuente"][nombre]["nuevas"]     = nuevas
            cycle_stats["por_fuente"][nombre]["existentes"] = existentes
            log.info(f"✅ {nombre}: {nuevas} nuevas | {existentes} existentes")

        except Exception as e:
            log.exception(f"❌ {nombre} falló: {e}")

    cycle_num = get_state_int("cycle_counter", 0) + 1
    set_state_str("cycle_counter", str(cycle_num))
    log.info(f"\n{'═'*55}\nCICLO {cycle_num} | Nuevas: {cycle_stats['nuevas']} / {cycle_stats['total']}\n{'═'*55}\n")

    # ── Heartbeat ─────────────────────────────────────────────────────────────
    if cycle_num % HEARTBEAT_EVERY_CYCLES == 0:
        s = get_stats()
        msg = (
            f"<b>🫀 CICLO {cycle_num}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 <b>ESTE CICLO</b>\n"
            f"  🆕 Nuevas:        {cycle_stats['nuevas']}\n"
            f"  ♻️  Existentes:  {cycle_stats['existentes']}\n"
            f"\n📦 <b>TOTAL ACUMULADO</b>\n"
            f"  Detectados:     {s['total']}\n"
            f"  ✅ Postulados:   {s['aplicados']}\n"
            f"  ❓ Pendientes:  {s['pendientes']}\n"
            f"\n━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📡 <b>FUENTES</b>"
        )
        for fuente, fstats in cycle_stats["por_fuente"].items():
            if not isinstance(fstats, dict):
                continue
            if "error" in fstats:
                msg += f"\n\n{fuente} ⛔ cooldown"
                continue
            total_f  = fstats.get("total_enlaces", 0)
            nuevas_f = fstats.get("nuevas", 0)
            msg += f"\n\n<b>{fuente}</b>  ✅ {total_f} encontradas  🆕 {nuevas_f} nuevas"
        telegram_send(msg)
    return cycle_stats

# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    if not all([DATABASE_URL, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID]):
        raise RuntimeError("Faltan variables: DATABASE_URL, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID")
    init_db()

    t = threading.Thread(target=poll_telegram_commands, name="cmd-handler", daemon=True)
    t.start()
    log.info("✅ Hilo de comandos iniciado")

    telegram_send(
        "<b>🚀 BOT v9 INICIADO</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "✅ Sin duplicados (upsert corregido)\n"
        "✅ Descripciones limpias (sin ruido de sitio)\n"
        "✅ Requisitos extraídos de la descripción\n"
        "✅ Resumen inteligente del puesto\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "Escribe /ayuda para ver todos los comandos."
    )

    while True:
        start = time.time()
        try:
            run_cycle()
        except Exception as e:
            log.exception(f"❌ Error ciclo: {e}")
        sleep_time = max(15, INTERVALO - int(time.time() - start))
        log.info(f"⏰ Próximo ciclo en {sleep_time}s")
        time.sleep(sleep_time)

if __name__ == "__main__":
    main()
