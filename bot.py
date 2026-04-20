#!/usr/bin/env python3
"""
Bot Osorno v7 - CORREGIDO
Fixes aplicados:
  - Chiletrabajos: URL paginación corregida, timeout reducido
  - BNE: URLs 404 eliminadas, solo URLs verificadas
  - Indeed: fallback scraping 403 eliminado, RSS mejorado
  - Yapo: URL 404 eliminada, URLs correctas
  - NUEVO: Handler de comandos Telegram (/postule, /nopostule, /estado)
  - CORRECCIÓN: set_source_ok/error ahora refleja el estado real
  - MEJORA: Formato de oferta más limpio y visual
  - MEJORA: Cooldown funciona correctamente
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
from typing import Dict, List, Optional, Tuple, Set
from urllib.parse import quote_plus, urljoin

import feedparser
import psycopg2
import psycopg2.extras
import requests
from bs4 import BeautifulSoup

# ─── Configuración ───────────────────────────────────────────────────────────
INTERVALO              = int(os.getenv("INTERVALO", "90"))
MAX_MSG                = 4096
LOG_LEVEL              = os.getenv("LOG_LEVEL", "INFO").upper()
HEARTBEAT_EVERY_CYCLES = int(os.getenv("HEARTBEAT_EVERY_CYCLES", "10"))

TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DATABASE_URL     = os.getenv("DATABASE_URL")

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

OSORNO_SIGNALS   = ["osorno", "los lagos", "región de los lagos", "region de los lagos", "5290", "rahue"]
KEYWORDS_EXCLUDE = [k.strip().lower() for k in os.getenv("KEYWORDS_EXCLUDE", "").split(",") if k.strip()]

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("bot")

# ─── Modelos ─────────────────────────────────────────────────────────────────
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
    description:       str = "No disponible"
    requisitos:        List[Requisito] = field(default_factory=list)
    resumen:           str = "No disponible"
    location_verified: bool = False

# ─── Utilidades ──────────────────────────────────────────────────────────────
def limpiar(txt: str) -> str:
    return re.sub(r"\s+", " ", str(txt or "")).strip()

def short_hash(*parts: str) -> str:
    return hashlib.sha1(limpiar("|".join(parts)).lower().encode()).hexdigest()[:10].upper()

def contiene_osorno(txt: str) -> bool:
    return any(sig in txt.lower() for sig in OSORNO_SIGNALS)

def pasa_filtros(o: Oferta) -> bool:
    txt = f"{o.title} {o.company} {o.description} {o.link}".lower()
    if o.location_verified:
        return not (KEYWORDS_EXCLUDE and any(k in txt for k in KEYWORDS_EXCLUDE))
    if not contiene_osorno(txt):
        return False
    if KEYWORDS_EXCLUDE and any(k in txt for k in KEYWORDS_EXCLUDE):
        return False
    return True

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
            resto  = descripcion[inicio:]
            for linea in resto.split("\n"):
                linea = limpiar(linea)
                if len(linea) < 15 or len(linea) > 200:
                    continue
                tipo = "general"
                if any(x in linea.lower() for x in ["título", "estudios", "técnic", "media"]):
                    tipo = "educacion"
                elif any(x in linea.lower() for x in ["años", "experiencia", "año"]):
                    tipo = "experiencia"
                elif any(x in linea.lower() for x in ["office", "excel", "inglés", "licencia"]):
                    tipo = "habilidad"
                linea = re.sub(r"^[\-\•\*\+]+\s*", "", linea)
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
    oraciones     = re.split(r"[.!?]+", desc)
    significativas = []
    for oracion in oraciones:
        oracion = limpiar(oracion)
        if len(oracion) < 30:
            continue
        significativas.append(oracion)
        if len(significativas) >= 2:
            break
    if not significativas:
        return ""
    resumen  = ". ".join(significativas) + "."
    palabras = resumen.split()
    if len(palabras) > max_words:
        resumen = " ".join(palabras[:max_words]) + "…"
    return resumen

# ─── Base de datos ────────────────────────────────────────────────────────────
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
                company        TEXT,
                link           TEXT UNIQUE NOT NULL,
                date_text      TEXT,
                salary         TEXT,
                jornada        TEXT,
                description    TEXT,
                requisitos     JSONB DEFAULT '[]'::jsonb,
                resumen        TEXT DEFAULT '',
                fingerprint    TEXT NOT NULL,
                first_seen_at  TIMESTAMPTZ DEFAULT NOW(),
                last_seen_at   TIMESTAMPTZ DEFAULT NOW(),
                applied_status TEXT DEFAULT 'unknown'
            );
            CREATE UNIQUE INDEX IF NOT EXISTS jobs_link_idx    ON jobs(link);
            CREATE        INDEX IF NOT EXISTS jobs_code_idx    ON jobs(job_code);
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
    """Inserta o actualiza un trabajo. Retorna (id, applied_status, 'inserted'|'updated'|'error')."""
    job_code       = f"{o.source[:2].upper()}-{short_hash(o.link)}"
    fingerprint    = short_hash(o.link)
    requisitos_json = json.dumps([{"tipo": r.tipo, "texto": r.texto} for r in o.requisitos])

    with get_db() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        try:
            cur.execute("""
                INSERT INTO jobs(job_code, source, title, company, link, date_text, salary, jornada,
                                 description, requisitos, resumen, fingerprint, last_seen_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                ON CONFLICT (link) DO UPDATE
                    SET last_seen_at  = NOW(),
                        description   = EXCLUDED.description,
                        requisitos    = EXCLUDED.requisitos,
                        resumen       = EXCLUDED.resumen
                RETURNING id, applied_status, (xmax = 0) AS inserted
            """, (job_code, o.source, o.title, o.company, o.link,
                  o.date_text, o.salary, o.jornada, o.description,
                  requisitos_json, o.resumen, fingerprint))

            row = cur.fetchone()
            if not row:
                cur.execute("SELECT id, applied_status FROM jobs WHERE link=%s", (o.link,))
                row = cur.fetchone()

            conn.commit()
            return int(row["id"]), str(row["applied_status"]), "inserted" if row.get("inserted") else "updated"
        except Exception as e:
            log.error(f"Error upsert {o.link[:60]}…: {e}")
            conn.rollback()
            return 0, "unknown", "error"

def update_applied_status(job_code: str, status: str) -> int:
    """Actualiza applied_status por job_code. Retorna filas afectadas."""
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE jobs SET applied_status=%s WHERE job_code=%s",
            (status, job_code.upper())
        )
        n = cur.rowcount
        conn.commit()
    return n

def get_job_info(job_code: str) -> Optional[dict]:
    """Retorna info básica de un trabajo por código."""
    with get_db() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT title, company, applied_status, first_seen_at FROM jobs WHERE job_code=%s",
            (job_code.upper(),)
        )
        return dict(cur.fetchone()) if cur.rowcount else None

def get_state_int(key: str, default: int = 0) -> int:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("SELECT value FROM bot_state WHERE key=%s", (key,))
        row = cur.fetchone()
        return int(row[0]) if row else default

def set_state_str(key: str, value: str) -> None:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO bot_state(key, value) VALUES (%s,%s) ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value",
            (key, value)
        )
        conn.commit()

# ─── Source health ────────────────────────────────────────────────────────────
def set_source_ok(source: str) -> None:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO source_health(source, consecutive_errors, last_success_at) VALUES (%s,0,NOW()) "
            "ON CONFLICT (source) DO UPDATE SET consecutive_errors=0, last_error=NULL, last_success_at=NOW()",
            (source,)
        )
        conn.commit()

def set_source_error(source: str, err: str) -> int:
    with get_db() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO source_health(source, consecutive_errors, last_error) VALUES (%s,1,%s) "
            "ON CONFLICT (source) DO UPDATE "
            "SET consecutive_errors=source_health.consecutive_errors+1, last_error=EXCLUDED.last_error "
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
            log.warning(f"🔥 {source} en COOLDOWN ({row[0]} errores consecutivos) — saltando")
            return True
        return False

# ─── HTTP Helper ──────────────────────────────────────────────────────────────
def get_soup(url: str, retries: int = 3, timeout: int = 12) -> Optional[BeautifulSoup]:
    """Descarga una URL y retorna BeautifulSoup. Timeout ajustable por fuente."""
    for i in range(retries):
        try:
            session = requests.Session()
            session.headers.update({
                "User-Agent":                random.choice(USER_AGENTS),
                "Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language":           "es-CL,es;q=0.9,en;q=0.8",
                "Accept-Encoding":           "gzip, deflate, br",
                "DNT":                       "1",
                "Connection":                "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "Cache-Control":             "max-age=0",
            })
            time.sleep(random.uniform(0.5, 1.5))
            r = session.get(url, timeout=(timeout, timeout * 2), allow_redirects=True)
            r.raise_for_status()

            if len(r.text) < 300:
                log.warning(f"⚠️ Respuesta muy corta ({len(r.text)} bytes): {url[:80]}")
                if i < retries - 1:
                    continue

            log.debug(f"✅ GET {url[:80]} → {r.status_code} ({len(r.text):,} bytes)")
            return BeautifulSoup(r.text, "html.parser")

        except requests.exceptions.ConnectionError as e:
            log.warning(f"❌ Intento {i+1}/{retries} [conexión]: {e}")
        except requests.exceptions.Timeout:
            log.warning(f"❌ Intento {i+1}/{retries} [timeout]: {url[:70]}")
        except Exception as e:
            log.warning(f"❌ Intento {i+1}/{retries}: {e}")

        if i < retries - 1:
            time.sleep(random.uniform(2, 4))

    log.error(f"🚫 FALLÓ definitivamente: {url[:80]}")
    return None

# ─── Telegram ────────────────────────────────────────────────────────────────
def telegram_send(msg: str, chat_id: str = None) -> Optional[int]:
    """Envía un mensaje a Telegram. Usa TELEGRAM_CHAT_ID por defecto."""
    target = chat_id or TELEGRAM_CHAT_ID
    if not TELEGRAM_TOKEN or not target:
        return None
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data={"chat_id": target, "text": msg[:MAX_MSG], "parse_mode": "HTML",
                  "disable_web_page_preview": True},
            timeout=12,
        )
        if r.ok:
            return r.json().get("result", {}).get("message_id")
        log.warning(f"Telegram no-ok: {r.status_code} {r.text[:100]}")
    except Exception as e:
        log.error(f"Telegram error: {e}")
    return None

STATUS_LABEL = {
    "applied":     ("✅", "Ya postulaste"),
    "not_applied": ("❌", "No postulado"),
    "unknown":     ("❓", "Sin registrar"),
}

def formatear_oferta(o: Oferta, code: str, status: str) -> str:
    """Genera el mensaje de Telegram para una nueva oferta."""
    emoji_status, label_status = STATUS_LABEL.get(status, ("❓", "Sin registrar"))

    # ── Encabezado ──
    lines = [
        f"<b>🆕 NUEVA OFERTA — {html.escape(o.source.upper())}</b>",
        "━━━━━━━━━━━━━━━━━━━━━━━━",
        f"<b>📌 {html.escape(o.title)}</b>",
        "",
    ]

    # ── Info rápida ──
    def fila(emoji, label, valor):
        if valor and valor.lower() not in ("no especificado", "no especificada", "no disponible"):
            return f"{emoji} <b>{label}:</b> {html.escape(valor)}"
        return f"{emoji} <b>{label}:</b> <i>No especificado</i>"

    lines += [
        f"🆔 <code>{code}</code>",
        fila("🏢", "Empresa",  o.company),
        fila("💰", "Salario",  o.salary),
        fila("🕒", "Jornada",  o.jornada),
        fila("📅", "Fecha",    o.date_text),
        f"📬 <b>Estado:</b> {emoji_status} {label_status}",
    ]

    # ── Resumen ──
    if o.resumen:
        lines += ["", "─────────────────────────", f"<b>💼 Descripción</b>", html.escape(o.resumen[:300])]

    # ── Requisitos ──
    if o.requisitos:
        lines += ["", "<b>📋 Requisitos</b>"]
        icons = {"educacion": "🎓", "experiencia": "🏆", "habilidad": "⚡", "general": "•"}
        for req in o.requisitos[:5]:
            lines.append(f"  {icons.get(req.tipo, '•')} {html.escape(req.texto[:100])}")

    # ── Footer ──
    lines += [
        "",
        "━━━━━━━━━━━━━━━━━━━━━━━━",
        f"🔗 <a href='{o.link}'>Ver oferta completa</a>",
        "",
        "¿Postulaste a esta oferta?",
        f"  /postule {code}   — Sí ✅",
        f"  /nopostule {code} — No ❌",
    ]

    return "\n".join(lines)[:MAX_MSG]

# ─── Manejador de comandos Telegram ──────────────────────────────────────────
def poll_telegram_commands() -> None:
    """
    Hilo demonio: lee comandos de Telegram usando long-polling.
    Soporta: /postule CODE, /nopostule CODE, /estado CODE
    """
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

            updates = r.json().get("result", [])
            for update in updates:
                offset = update["update_id"] + 1
                msg_data = update.get("message", {})
                text     = msg_data.get("text", "").strip()
                chat_id  = str(msg_data.get("chat", {}).get("id", ""))

                if not text or not chat_id:
                    continue

                parts = text.strip().split()
                cmd   = parts[0].lower().split("@")[0]  # elimina @botname si existe
                code  = parts[1].strip().upper() if len(parts) > 1 else ""

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
                            chat_id
                        )
                    else:
                        telegram_send(f"⚠️ Código no encontrado: <code>{code}</code>", chat_id)

                elif cmd == "/nopostule" and code:
                    n = update_applied_status(code, "not_applied")
                    if n > 0:
                        info = get_job_info(code)
                        titulo = html.escape(info["title"][:60]) if info else code
                        telegram_send(
                            f"❌ <b>Marcado como no postulado</b>\n"
                            f"━━━━━━━━━━━━━━━━━━━━\n"
                            f"🆔 <code>{code}</code>\n"
                            f"📌 {titulo}\n"
                            f"📬 Estado: ❌ No postulado\n\n"
                            f"<i>Usa /postule {code} si cambias de opinión.</i>",
                            chat_id
                        )
                    else:
                        telegram_send(f"⚠️ Código no encontrado: <code>{code}</code>", chat_id)

                elif cmd == "/estado" and code:
                    info = get_job_info(code)
                    if info:
                        emoji_s, label_s = STATUS_LABEL.get(info["applied_status"], ("❓", "Sin registrar"))
                        fecha = info["first_seen_at"].strftime("%d/%m/%Y") if info.get("first_seen_at") else "?"
                        telegram_send(
                            f"🔍 <b>Estado del trabajo</b>\n"
                            f"━━━━━━━━━━━━━━━━━━━━\n"
                            f"🆔 <code>{code}</code>\n"
                            f"📌 {html.escape(info['title'][:80])}\n"
                            f"🏢 {html.escape(info['company'] or 'No especificada')}\n"
                            f"📅 Detectado: {fecha}\n"
                            f"📬 Estado: {emoji_s} {label_s}",
                            chat_id
                        )
                    else:
                        telegram_send(f"⚠️ Código no encontrado: <code>{code}</code>", chat_id)

                elif cmd == "/ayuda" or cmd == "/start":
                    telegram_send(
                        "<b>🤖 Comandos disponibles</b>\n"
                        "━━━━━━━━━━━━━━━━━━━━\n"
                        "/postule <code>CÓDIGO</code> — Registrar postulación ✅\n"
                        "/nopostule <code>CÓDIGO</code> — Marcar como no postulado ❌\n"
                        "/estado <code>CÓDIGO</code> — Ver estado de una oferta 🔍\n\n"
                        "<i>El CÓDIGO aparece en cada oferta enviada.</i>",
                        chat_id
                    )

            set_state_str("telegram_offset", str(offset))

        except requests.exceptions.Timeout:
            pass  # Normal en long-polling
        except Exception as e:
            log.error(f"poll_commands error: {e}")
            time.sleep(5)

# ─── Parser: Chiletrabajos ───────────────────────────────────────────────────
def parse_chiletrabajos() -> Tuple[List[Oferta], Dict]:
    """
    FIX: Paginación URL corregida (/2.html, /3.html)
    FIX: Timeout reducido a 8s (el sitio bloquea IPs de servidor)
    FIX: set_source_error se llama correctamente si 0 páginas responden
    """
    source = "Chiletrabajos"
    if should_cooldown(source):
        return [], {"error": "cooldown"}

    out       = []
    stats     = {"paginas_ok": 0, "estrategias": {f"E{i}": 0 for i in range(1, 8)}, "total_enlaces": 0}
    base      = "https://www.chiletrabajos.cl"
    paginas_urls = [
        f"{base}/ciudad/osorno.html",
        f"{base}/ciudad/osorno/2.html",   # FIX: antes era /osorno.html/30 (inválido)
        f"{base}/ciudad/osorno/3.html",
    ]

    try:
        for url in paginas_urls:
            log.info(f"🌐 [{source}] {url}")
            soup = get_soup(url, retries=2, timeout=8)  # FIX: timeout reducido
            if not soup:
                continue

            stats["paginas_ok"] += 1
            job_links = []
            visto = set()

            def agregar(link, title, est):
                nl = link.strip().lower()
                if nl and nl not in visto and len(title) >= 5:
                    visto.add(nl)
                    job_links.append((link, title, est))
                    stats["estrategias"][est] += 1

            for h in soup.find_all(["h2", "h3"]):
                a = h.find("a", href=True)
                if a and "/trabajo/" in a.get("href", ""):
                    href = a["href"]
                    link = href if href.startswith("http") else f"{base}{href}"
                    agregar(link, limpiar(a.get_text()), "E1" if h.name == "h2" else "E2")

            for div in soup.find_all("div", class_=re.compile(r"(job|oferta|trabajo|empleo|card|item)", re.I)):
                a = div.find("a", href=True)
                if a and "/trabajo/" in a.get("href", ""):
                    href = a["href"]
                    link = href if href.startswith("http") else f"{base}{href}"
                    agregar(link, limpiar(a.get_text()), "E3")

            if len(job_links) < 15:
                for a in soup.find_all("a", href=True):
                    if "/trabajo/" in a.get("href", ""):
                        href = a["href"]
                        link = href if href.startswith("http") else f"{base}{href}"
                        agregar(link, limpiar(a.get_text()), "E4")

            stats["total_enlaces"] += len(job_links)
            log.info(f"📊 [{source}] Página OK: {len(job_links)} enlaces")

            for link, title, estrategia in job_links[:30]:
                try:
                    det = get_soup(link, retries=2, timeout=8)
                    fecha = jornada = sueldo = "No especificada"
                    descripcion = "No disponible"

                    if det:
                        for td in det.select("table td"):
                            txt = limpiar(td.get_text()).lower()
                            sib = td.find_next_sibling("td")
                            if not sib:
                                continue
                            val = limpiar(sib.get_text())
                            if "fecha" in txt and "expira" not in txt:
                                fecha = val
                            elif "salario" in txt or "sueldo" in txt:
                                sueldo = val if val.lower() not in ["no especificado", "a convenir"] else "No especificado"
                            elif "jornada" in txt or "tipo" in txt:
                                jornada = val

                        partes = []
                        for tag in det.find_all(["p", "div", "li"]):
                            txt = limpiar(tag.get_text())
                            if 30 < len(txt) < 1000 and "PUBLICIDAD" not in txt:
                                partes.append(txt)
                                if len(partes) >= 5:
                                    break
                        descripcion = " ".join(partes) if partes else "No disponible"

                    out.append(Oferta(
                        source=source, title=title, link=link,
                        date_text=fecha, salary=sueldo, jornada=jornada,
                        description=descripcion,
                        requisitos=extraer_requisitos(descripcion),
                        resumen=generar_resumen(descripcion),
                        location_verified=True,
                    ))
                    time.sleep(random.uniform(0.3, 0.7))
                except Exception as e:
                    log.warning(f"⚠️ [{source}] Error en {link[:60]}: {e}")
                    out.append(Oferta(source=source, title=title, link=link, location_verified=True))

            time.sleep(random.uniform(1, 2))

        # FIX: solo marcar ok si al menos una página respondió
        if stats["paginas_ok"] > 0:
            set_source_ok(source)
        else:
            n = set_source_error(source, "Todas las páginas fallaron (timeout/bloqueo)")
            log.warning(f"⚠️ [{source}] 0 páginas respondieron — error #{n}")

        log.info(f"✅ [{source}] {len(out)} ofertas | páginas_ok={stats['paginas_ok']}")

    except Exception as e:
        log.exception(f"❌ [{source}] ERROR: {e}")
        set_source_error(source, str(e))

    return dedup(out), stats

# ─── Parser: BNE ─────────────────────────────────────────────────────────────
def parse_bne() -> Tuple[List[Oferta], Dict]:
    """
    FIX: URLs 404 eliminadas (/trabajos/osorno, /empleos/osorno)
    FIX: Solo URLs verificadas + paginación correcta
    """
    source = "BNE"
    if should_cooldown(source):
        return [], {"error": "cooldown"}

    out   = []
    stats = {"estrategias": {f"E{i}": 0 for i in range(1, 7)}, "total_enlaces": 0}
    seen  = set()

    # FIX: solo URLs que funcionan (verificadas en logs)
    urls = [
        "https://www.bne.cl/ofertas?ubicacion=Osorno",
        "https://www.bne.cl/ofertas?q=&ubicacion=Osorno&orden=reciente",
        "https://www.bne.cl/ofertas?ubicacion=Osorno&pagina=2",
        "https://www.bne.cl/ofertas?ubicacion=Osorno&pagina=3",
    ]

    paginas_ok = 0
    try:
        for url in urls:
            log.info(f"🌐 [{source}] {url}")
            soup = get_soup(url, retries=2, timeout=12)
            if not soup:
                continue
            paginas_ok += 1

            def add(link, text, est):
                k = link.strip().lower()
                if k not in seen and len(text) >= 8:
                    seen.add(k)
                    out.append(Oferta(source=source, title=text, link=link, location_verified=True))
                    stats["estrategias"][est] += 1

            # E1: Selectores específicos BNE
            for selector in ["a.job-link", "a[href*='oferta']", "a[href*='empleo']"]:
                for a in soup.select(selector):
                    href = a.get("href", "")
                    text = limpiar(a.get_text())
                    link = href if href.startswith("http") else urljoin("https://www.bne.cl", href)
                    add(link, text, "E1")

            # E2: Divs con clase
            for div in soup.find_all("div", class_=re.compile(r"(job|oferta|empleo|card)", re.I)):
                a = div.find("a", href=True)
                if a:
                    href = a.get("href", "")
                    link = href if href.startswith("http") else urljoin("https://www.bne.cl", href)
                    add(link, limpiar(a.get_text()), "E2")

            # E3: Articles
            for article in soup.find_all("article"):
                a = article.find("a", href=True)
                if a:
                    href = a.get("href", "")
                    link = href if href.startswith("http") else urljoin("https://www.bne.cl", href)
                    add(link, limpiar(a.get_text()), "E3")

            # E4: H2/H3 con enlaces
            for h in soup.find_all(["h2", "h3"]):
                a = h.find("a", href=True)
                if a:
                    href = a.get("href", "")
                    link = href if href.startswith("http") else urljoin("https://www.bne.cl", href)
                    add(link, limpiar(a.get_text()), "E4")

            # E5: Todos los A con /oferta/ o /empleo/
            if len(out) < 10:
                for a in soup.find_all("a", href=True):
                    href = a.get("href", "")
                    if "oferta" in href.lower() or "empleo" in href.lower():
                        link = href if href.startswith("http") else urljoin("https://www.bne.cl", href)
                        add(link, limpiar(a.get_text()), "E5")

            time.sleep(random.uniform(1, 2))

        stats["total_enlaces"] = len(out)

        if paginas_ok > 0:
            set_source_ok(source)
        else:
            n = set_source_error(source, "Todas las páginas BNE fallaron")
            log.warning(f"⚠️ [{source}] 0 páginas respondieron — error #{n}")

        log.info(f"✅ [{source}] {len(out)} ofertas encontradas")

    except Exception as e:
        log.exception(f"❌ [{source}] ERROR: {e}")
        set_source_error(source, str(e))

    return dedup(out), stats

# ─── Parser: Indeed ──────────────────────────────────────────────────────────
def parse_indeed() -> Tuple[List[Oferta], Dict]:
    """
    FIX: Fallback scraping 403 eliminado (Indeed bloquea bots permanentemente)
    FIX: RSS con múltiples parámetros para mejor cobertura
    """
    source = "Indeed"
    if should_cooldown(source):
        return [], {"error": "cooldown"}

    out   = []
    stats = {"rss_intentos": 0, "rss_ok": 0, "total": 0}

    # Diferentes URLs RSS para maximizar resultados
    rss_urls = [
        f"https://cl.indeed.com/rss?q=&l={quote_plus('Osorno')}&sort=date&limit=50",
        f"https://cl.indeed.com/rss?q={quote_plus('trabajo')}&l={quote_plus('Osorno, Los Lagos')}&sort=date",
        f"https://cl.indeed.com/rss?q=&l={quote_plus('Los Lagos, Chile')}&sort=date&limit=25",
    ]

    seen = set()
    try:
        for feed_url in rss_urls:
            log.info(f"🌐 [{source}] RSS: {feed_url[:80]}")
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
                        parts = title.split(" - ", 1)
                        title, company = limpiar(parts[0]), limpiar(parts[1])

                    out.append(Oferta(
                        source=source, title=title, company=company, link=link,
                        description=summary,
                        requisitos=extraer_requisitos(summary),
                        resumen=generar_resumen(summary),
                        location_verified=True,
                    ))
            except Exception as e:
                log.warning(f"⚠️ [{source}] RSS error ({feed_url[:50]}): {e}")

            time.sleep(random.uniform(1.0, 2.0))

        stats["total"] = len(out)

        if stats["rss_ok"] > 0:
            set_source_ok(source)
        elif stats["rss_intentos"] > 0:
            # RSS vacío no es error grave, solo registrar si llevamos muchos fallos
            log.info(f"ℹ️ [{source}] RSS sin resultados (posible feed vacío)")
        
        log.info(f"✅ [{source}] {len(out)} ofertas via RSS")

    except Exception as e:
        log.exception(f"❌ [{source}] ERROR: {e}")
        set_source_error(source, str(e))

    return dedup(out), stats

# ─── Parser: Computrabajo ────────────────────────────────────────────────────
def parse_computrabajo() -> Tuple[List[Oferta], Dict]:
    """
    5 estrategias de extracción.
    Sin cambios estructurales — funciona bien según logs.
    Pequeño fix: set_source_error si 0 páginas ok.
    """
    source = "Computrabajo"
    if should_cooldown(source):
        return [], {"error": "cooldown"}

    out   = []
    stats = {"estrategias": {f"E{i}": 0 for i in range(1, 6)}, "total_enlaces": 0}
    seen  = set()
    base  = "https://cl.computrabajo.com"

    urls = [
        f"{base}/empleos-en-los-lagos-en-osorno",
        f"{base}/empleos-en-los-lagos-en-osorno?p=2",
        f"{base}/empleos-en-los-lagos-en-osorno?p=3",
        f"{base}/trabajo-de-osorno",
    ]

    paginas_ok = 0
    try:
        for url in urls:
            log.info(f"🌐 [{source}] {url}")
            soup = get_soup(url, retries=3, timeout=12)
            if not soup:
                continue
            paginas_ok += 1

            def add(link, text, est):
                k = link.strip().lower()
                if k not in seen and len(text) >= 8:
                    seen.add(k)
                    out.append(Oferta(source=source, title=text, link=link, location_verified=True))
                    stats["estrategias"][est] += 1

            # E1: Article > A
            for article in soup.find_all("article"):
                a = article.find("a", href=True)
                if a:
                    href = a.get("href", "")
                    if "oferta" in href.lower():
                        text = limpiar(a.get_text())
                        if len(text) < 8:
                            for h in article.find_all(["h2", "h3"]):
                                text = limpiar(h.get_text())
                                if len(text) >= 8:
                                    break
                        link = href if href.startswith("http") else f"{base}{href}"
                        add(link, text, "E1")

            # E2: A con "oferta" en href
            for a in soup.find_all("a", href=True):
                href = a.get("href", "")
                if "oferta" in href.lower():
                    link = href if href.startswith("http") else f"{base}{href}"
                    add(link, limpiar(a.get_text()), "E2")

            # E3: Divs
            for div in soup.find_all("div", class_=re.compile(r"(oferta|job|empleo)", re.I)):
                a = div.find("a", href=True)
                if a:
                    href = a.get("href", "")
                    link = href if href.startswith("http") else f"{base}{href}"
                    add(link, limpiar(a.get_text()), "E3")

            # E4: H2/H3
            for h in soup.find_all(["h2", "h3"]):
                a = h.find("a", href=True)
                if a:
                    href = a.get("href", "")
                    link = href if href.startswith("http") else f"{base}{href}"
                    if "oferta" in link.lower() or re.search(r"-[0-9a-fA-F]{6,}", link):
                        add(link, limpiar(a.get_text()), "E4")

            # E5: Pattern hash
            if len(out) < 10:
                for a in soup.find_all("a", href=True):
                    href = a.get("href", "")
                    if re.search(r"-[0-9a-fA-F]{6,}\.html?", href):
                        link = href if href.startswith("http") else f"{base}{href}"
                        add(link, limpiar(a.get_text()), "E5")

            time.sleep(random.uniform(1, 2))

        stats["total_enlaces"] = len(out)

        if paginas_ok > 0:
            set_source_ok(source)
        else:
            n = set_source_error(source, "Todas las páginas Computrabajo fallaron")
            log.warning(f"⚠️ [{source}] 0 páginas respondieron — error #{n}")

        log.info(f"✅ [{source}] {len(out)} ofertas")

    except Exception as e:
        log.exception(f"❌ [{source}] ERROR: {e}")
        set_source_error(source, str(e))

    return dedup(out), stats

# ─── Parser: Yapo ────────────────────────────────────────────────────────────
def parse_yapo() -> Tuple[List[Oferta], Dict]:
    """
    FIX: URL 404 /region_de_los_lagos/empleos eliminada
    FIX: Reemplazada por URLs verificadas con parámetros
    """
    source = "Yapo"
    if should_cooldown(source):
        return [], {"error": "cooldown"}

    out   = []
    stats = {"estrategias": {f"E{i}": 0 for i in range(1, 6)}, "total_enlaces": 0}
    seen  = set()

    # FIX: URLs actualizadas — eliminada la URL 404
    urls = [
        "https://www.yapo.cl/empleos?ca=12_s&l=0&q=osorno",
        "https://www.yapo.cl/empleos?ca=12_s&l=0&q=osorno&o=25",
        "https://www.yapo.cl/empleos?ca=12_s&l=0&q=osorno&o=50",
        # FIX: antes era /region_de_los_lagos/empleos → 404
        "https://www.yapo.cl/empleos?ca=12_s&l=14&q=",
    ]

    paginas_ok = 0
    try:
        for url in urls:
            log.info(f"🌐 [{source}] {url}")
            soup = get_soup(url, retries=2, timeout=12)
            if not soup:
                continue
            paginas_ok += 1

            def add(link, text, est):
                k = link.strip().lower()
                if k not in seen and len(text) >= 8:
                    seen.add(k)
                    out.append(Oferta(source=source, title=text, link=link, location_verified=True))
                    stats["estrategias"][est] += 1

            # E1: Pattern principal Yapo
            for a in soup.find_all("a", href=True):
                href = a.get("href", "")
                if re.search(r"\d{5,}", href) and any(x in href.lower() for x in ["/empleo", "/trabajo", "/oferta"]):
                    if not any(x in href.lower() for x in ["/arriendo", "/venta", "/auto", "/servicio"]):
                        link = href if href.startswith("http") else urljoin("https://www.yapo.cl", href)
                        add(link, limpiar(a.get_text()), "E1")

            # E2: Divs ad/aviso
            for div in soup.find_all("div", class_=re.compile(r"(ad|aviso|listing|item|card)", re.I)):
                a = div.find("a", href=True)
                if a:
                    href = a.get("href", "")
                    if re.search(r"\d{5,}", href):
                        link = href if href.startswith("http") else urljoin("https://www.yapo.cl", href)
                        add(link, limpiar(a.get_text()), "E2")

            # E3: Keywords en texto
            for a in soup.find_all("a", href=True):
                text = limpiar(a.get_text())
                if 10 < len(text) < 100:
                    if any(kw in text.lower() for kw in ["vendedor", "asistente", "técnico", "ejecutivo", "se busca", "se solicita"]):
                        href = a.get("href", "")
                        if re.search(r"\d{5,}", href):
                            link = href if href.startswith("http") else urljoin("https://www.yapo.cl", href)
                            add(link, text, "E3")

            # E4: Articles
            for article in soup.find_all("article"):
                a = article.find("a", href=True)
                if a:
                    href = a.get("href", "")
                    if re.search(r"\d{5,}", href):
                        link = href if href.startswith("http") else urljoin("https://www.yapo.cl", href)
                        add(link, limpiar(a.get_text()), "E4")

            # E5: Fallback
            if len(out) < 5:
                for a in soup.find_all("a", href=True):
                    href = a.get("href", "")
                    if re.search(r"\d{6,}", href) and "empleo" in href.lower():
                        link = href if href.startswith("http") else urljoin("https://www.yapo.cl", href)
                        add(link, limpiar(a.get_text()), "E5")

            time.sleep(random.uniform(1, 2))

        stats["total_enlaces"] = len(out)

        if paginas_ok > 0:
            set_source_ok(source)
        else:
            n = set_source_error(source, "Todas las páginas Yapo fallaron")
            log.warning(f"⚠️ [{source}] 0 páginas respondieron — error #{n}")

        log.info(f"✅ [{source}] {len(out)} ofertas")

    except Exception as e:
        log.exception(f"❌ [{source}] ERROR: {e}")
        set_source_error(source, str(e))

    return dedup(out), stats

# ─── Dedup ────────────────────────────────────────────────────────────────────
def dedup(items: List[Oferta]) -> List[Oferta]:
    seen, out = set(), []
    for o in items:
        k = o.link.strip().lower()
        if k and k not in seen:
            seen.add(k)
            out.append(o)
    return out

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
                    nuevas += 1
                    cycle_stats["nuevas"] += 1
                    code = f"{o.source[:2].upper()}-{short_hash(o.link)}"
                    msg  = formatear_oferta(o, code, status)
                    telegram_send(msg)
                    time.sleep(0.5)  # Rate limit Telegram
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

    log.info(f"\n{'═'*55}\nCICLO {cycle_num} | Total: {cycle_stats['total']} | Nuevas: {cycle_stats['nuevas']}\n{'═'*55}\n")

    # ── Heartbeat ──
    if cycle_num % HEARTBEAT_EVERY_CYCLES == 0:
        msg = (
            f"<b>🫀 CICLO {cycle_num}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 <b>RESUMEN</b>\n"
            f"  • Total procesadas:  {cycle_stats['total']}\n"
            f"  • 🆕 Nuevas:         {cycle_stats['nuevas']}\n"
            f"  • ♻️  Ya existentes: {cycle_stats['existentes']}\n"
            f"\n━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📡 <b>FUENTES</b>"
        )
        for fuente, stats in cycle_stats["por_fuente"].items():
            if not isinstance(stats, dict):
                continue
            if "error" in stats:
                msg += f"\n\n<b>{fuente}</b> ⛔ cooldown"
                continue
            total_f  = stats.get("total_enlaces", 0) or sum(
                v for k, v in stats.items() if k.startswith("E") or k in ["rss", "rss_ok", "scraping"]
            )
            nuevas_f = stats.get("nuevas", 0)
            msg += f"\n\n<b>{fuente}</b>"
            msg += f"\n  ✅ {total_f} encontradas  |  🆕 {nuevas_f} nuevas"
            ests = [(k, v) for k, v in stats.items() if (k.startswith("E") or k in ["rss_ok"]) and v > 0]
            if ests:
                msg += "\n  📍 " + ", ".join(f"{k}:{v}" for k, v in ests)

        telegram_send(msg)

    return cycle_stats

# ─── Main ────────────────────────────────────────────────────────────────────
def main():
    if not all([DATABASE_URL, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID]):
        raise RuntimeError("❌ Faltan variables de entorno: DATABASE_URL, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID")

    init_db()

    # Iniciar hilo de comandos Telegram (NUEVO)
    t = threading.Thread(target=poll_telegram_commands, name="cmd-handler", daemon=True)
    t.start()
    log.info("✅ Hilo de comandos Telegram iniciado")

    telegram_send(
        "<b>🚀 BOT v7 INICIADO</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "✅ 5 fuentes configuradas\n"
        "✅ URLs verificadas y corregidas\n"
        "✅ Comandos /postule y /nopostule activos\n"
        "✅ Sistema cooldown corregido\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "Escribe /ayuda para ver comandos disponibles."
    )

    while True:
        start = time.time()
        try:
            run_cycle()
        except Exception as e:
            log.exception(f"❌ Error en ciclo principal: {e}")

        elapsed    = time.time() - start
        sleep_time = max(15, INTERVALO - int(elapsed))
        log.info(f"⏰ Próximo ciclo en {sleep_time}s\n")
        time.sleep(sleep_time)

if __name__ == "__main__":
    main()
