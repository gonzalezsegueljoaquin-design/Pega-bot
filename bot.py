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


INTERVALO = int(os.getenv("INTERVALO", "180"))
TIMEOUT = int(os.getenv("TIMEOUT", "20"))
MAX_DESC = 700
MAX_MSG = 4096
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

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

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
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
    parts = [
        f"🆕 NUEVA OFERTA [{o.source}]",
        "────────────────────────────",
        f"🆔 Codigo: {job_code}",
        f"📌 {o.title}",
        f"🏢 Empresa: {o.company}",
        f"📅 Publicado: {o.date_text}",
        f"🕒 Jornada: {o.jornada}",
        f"💰 Sueldo: {o.salary}",
        f"📮 Estado postulacion: {format_estado(applied_status)}",
        "────────────────────────────",
        limpiar(o.description)[:MAX_DESC],
        f"🔗 {o.link}",
        "────────────────────────────",
        f"Comandos: /postule {job_code} | /nopostule {job_code} | /estado {job_code}",
    ]
    return "\n".join(parts)


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
    for i in range(1, retries + 1):
        try:
            r = SESSION.get(url, timeout=TIMEOUT)
            r.raise_for_status()
            return BeautifulSoup(r.text, "html.parser")
        except requests.RequestException as e:
            log.warning("GET [%s/%s] %s: %s", i, retries, url, e)
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
        for p in pages:
            soup = get_soup(p)
            if not soup:
                continue
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
                empresa = limpiar((h3.get_text(" ") if h3 else "").split(",")[0]) or "No especificada"
                out.append(Oferta(source=source, title=title, link=link, company=empresa))
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


def parse_command(text: str) -> Tuple[str, str]:
    t = limpiar(text)
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


def run_cycle() -> int:
    sources = [
        parse_chiletrabajos,
        parse_bne,
        parse_indeed_rss,
        parse_computrabajo,
        parse_yapo,
    ]
    found: List[Oferta] = []
    for fn in sources:
        try:
            found.extend(fn())
        except Exception as e:
            log.exception("Fuente fallo %s: %s", fn.__name__, e)

    new_count = 0
    for o in dedup(found):
        job_id, applied_status, op = upsert_job(o)
        if op != "inserted":
            continue
        msg = formatear_oferta(o, f"{o.source[:2].upper()}-{short_hash(o.link)}", applied_status)
        message_id = enviar(msg)
        register_notification(job_id, message_id)
        new_count += 1
    return new_count


def main() -> None:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID or not DATABASE_URL:
        raise RuntimeError("Config incompleta. Requiere TELEGRAM_TOKEN, TELEGRAM_CHAT_ID y DATABASE_URL")
    init_db()
    enviar(
        "🚀 Bot empleos Osorno activo\n"
        "Fuentes: Chiletrabajos, BNE, Indeed, Computrabajo, Yapo\n"
        "Comandos: /postule CODIGO | /nopostule CODIGO | /estado CODIGO"
    )
    while True:
        start = time.time()
        try:
            process_telegram_commands()
            nuevos = run_cycle()
            elapsed = time.time() - start
            log.info("Ciclo OK | nuevos=%s | %.1fs", nuevos, elapsed)
        except Exception as e:
            log.exception("Error ciclo: %s", e)
            enviar(f"❌ Error ciclo: {str(e)[:200]}")
            elapsed = time.time() - start
        time.sleep(max(10, INTERVALO - int(elapsed)))


if __name__ == "__main__":
    main()
