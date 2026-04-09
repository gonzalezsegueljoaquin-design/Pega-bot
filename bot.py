import requests
import time
import hashlib
import os
import logging
import sqlite3
from datetime import datetime, timedelta
import feedparser
from bs4 import BeautifulSoup

# ---------------- LOGGING ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("bot.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ---------------- CONFIG ----------------
TOKEN       = os.getenv("TOKEN")
CHAT_ID     = os.getenv("CHAT_ID")
TWILIO_SID  = os.getenv("TWILIO_SID")
TWILIO_AUTH = os.getenv("TWILIO_AUTH")
TWILIO_FROM = os.getenv("TWILIO_FROM")
TWILIO_TO   = os.getenv("TWILIO_TO")

CIUDAD = "Osorno"

KEYWORDS = [
    "bodega", "operario", "logistica", "reponedor",
    "chofer", "peoneta", "auxiliar", "produccion",
    "ayudante", "asistente", "ventas", "tienda",
    "aseo", "limpieza", "repartidor", "almacen"
]

REQUEST_TIMEOUT = 10
INTERVALO       = 60
DIAS_RETENCION  = 30

# ---------------- PERSISTENCIA (SQLite) ----------------
def init_db():
    con = sqlite3.connect("seen.db")
    con.execute("""
        CREATE TABLE IF NOT EXISTS vistos (
            clave TEXT PRIMARY KEY,
            fecha TEXT NOT NULL
        )
    """)
    con.commit()
    return con

def cargar_vistos(con):
    rows = con.execute("SELECT clave FROM vistos").fetchall()
    return {r[0] for r in rows}

def guardar_visto(con, clave):
    con.execute(
        "INSERT OR IGNORE INTO vistos (clave, fecha) VALUES (?, ?)",
        (clave, datetime.now().isoformat())
    )
    con.commit()

def limpiar_viejos(con):
    limite = (datetime.now() - timedelta(days=DIAS_RETENCION)).isoformat()
    eliminados = con.execute(
        "DELETE FROM vistos WHERE fecha < ?", (limite,)
    ).rowcount
    con.commit()
    if eliminados:
        log.info(f"Limpieza: {eliminados} registros viejos eliminados")

# ---------------- UTIL ----------------
def hash_item(texto: str) -> str:
    return hashlib.md5(texto.encode()).hexdigest()

def get_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"})
    return s

# ---------------- NOTIFICACIONES ----------------
def telegram(msg: str):
    """
    FIX: sin parse_mode para evitar el error 400 Bad Request.
    Telegram rechaza Markdown mal formateado (asteriscos sueltos, etc).
    """
    try:
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        r   = requests.post(
            url,
            data={"chat_id": CHAT_ID, "text": msg},
            timeout=REQUEST_TIMEOUT
        )
        r.raise_for_status()
    except Exception as e:
        log.error(f"Error Telegram: {e}")

def whatsapp(msg: str):
    try:
        url  = f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_SID}/Messages.json"
        data = {
            "From": f"whatsapp:{TWILIO_FROM}",
            "To":   f"whatsapp:{TWILIO_TO}",
            "Body": msg
        }
        r = requests.post(url, data=data, auth=(TWILIO_SID, TWILIO_AUTH), timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
    except Exception as e:
        log.error(f"Error WhatsApp: {e}")

def notificar(msg: str):
    telegram(msg)
    whatsapp(msg)

# ---------------- FUENTES ----------------
def indeed() -> list[tuple]:
    """
    FIX: el RSS de Indeed devuelve 0 sin keywords.
    Se consultan 3 combinaciones para cubrir mas cargos.
    """
    jobs         = []
    vistos_links = set()
    urls = [
        f"https://cl.indeed.com/rss?q=trabajo&l={CIUDAD}",
        f"https://cl.indeed.com/rss?q=operario+bodega&l={CIUDAD}",
        f"https://cl.indeed.com/rss?q=chofer+auxiliar&l={CIUDAD}",
    ]
    try:
        for url in urls:
            feed = feedparser.parse(url)
            for e in feed.entries:
                if e.link not in vistos_links:
                    vistos_links.add(e.link)
                    jobs.append((e.title, e.link, "Indeed"))
        log.info(f"Indeed: {len(jobs)} avisos")
        return jobs
    except Exception as e:
        log.error(f"Error Indeed: {e}")
        return []

def chiletrabajos() -> list[tuple]:
    """
    FIX: prueba multiples slugs por si el RSS no responde a uno solo.
    """
    jobs         = []
    vistos_links = set()
    urls = [
        "https://www.chiletrabajos.cl/rss/trabajos/osorno",
        "https://www.chiletrabajos.cl/rss/trabajos/los-lagos",
    ]
    try:
        for url in urls:
            feed = feedparser.parse(url)
            for e in feed.entries:
                if e.link not in vistos_links:
                    vistos_links.add(e.link)
                    jobs.append((e.title, e.link, "Chiletrabajos"))
        log.info(f"Chiletrabajos: {len(jobs)} avisos")
        return jobs
    except Exception as e:
        log.error(f"Error Chiletrabajos: {e}")
        return []

def yapo(session: requests.Session) -> list[tuple]:
    """
    FIX: URL corregida a la estructura actual de Yapo Chile.
    Antes:  /region_de_los_lagos/empleos?q=Osorno  (404)
    Ahora:  /empleos-ofertas-de-trabajos/los-lagos-osorno
    """
    jobs         = []
    vistos_links = set()
    urls = [
        "https://www.yapo.cl/empleos-ofertas-de-trabajos/los-lagos-osorno",
        "https://www.yapo.cl/empleos-ofertas-de-trabajos/los-lagos-osorno.2",
    ]
    try:
        for url in urls:
            r = session.get(url, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")

            for item in soup.select("a[href*='/avisos/']"):
                titulo = item.get_text(strip=True)
                href   = item.get("href", "")

                if not href.startswith("http"):
                    href = "https://www.yapo.cl" + href

                if not titulo or len(titulo) < 8 or href in vistos_links:
                    continue

                vistos_links.add(href)
                jobs.append((titulo, href, "Yapo"))

        log.info(f"Yapo: {len(jobs)} avisos")
        return jobs
    except Exception as e:
        log.error(f"Error Yapo: {e}")
        return []

def facebook() -> list[tuple]:
    try:
        url  = "https://rsshub.app/facebook/page/empleos.osorno"
        feed = feedparser.parse(url)
        jobs = [(e.title, e.link, "Facebook") for e in feed.entries]
        log.info(f"Facebook: {len(jobs)} avisos")
        return jobs
    except Exception as e:
        log.error(f"Error Facebook: {e}")
        return []

# ---------------- FILTRO ----------------
def filtrar(trabajos: list[tuple]) -> list[tuple]:
    filtrados = []
    for titulo, link, fuente in trabajos:
        t = titulo.lower()
        if any(k in t for k in KEYWORDS):
            filtrados.append((titulo, link, fuente))
    return filtrados

# ---------------- MENSAJE ----------------
def mensaje(titulo: str, link: str, fuente: str) -> str:
    # Sin asteriscos ni caracteres Markdown para evitar errores de Telegram
    return (
        f"OFERTA DETECTADA\n\n"
        f"{titulo}\n"
        f"Fuente: {fuente}\n\n"
        f"Postula aqui:\n{link}"
    )

# ---------------- MAIN ----------------
def main():
    con     = init_db()
    vistos  = cargar_vistos(con)
    session = get_session()

    log.info("Bot iniciado")
    notificar("BOT ACTIVADO - Buscando empleos en Osorno")

    ciclo = 0
    while True:
        try:
            ciclo += 1
            log.info(f"--- Ciclo {ciclo} ---")

            trabajos  = []
            trabajos += indeed()
            trabajos += chiletrabajos()
            trabajos += yapo(session)
            trabajos += facebook()

            log.info(f"Total antes de filtro: {len(trabajos)}")
            trabajos = filtrar(trabajos)
            log.info(f"Total despues de filtro: {len(trabajos)}")

            nuevos = 0
            for titulo, link, fuente in trabajos:
                clave = hash_item(link)
                if clave not in vistos:
                    msg = mensaje(titulo, link, fuente)
                    notificar(msg)
                    guardar_visto(con, clave)
                    vistos.add(clave)
                    nuevos += 1

            log.info(f"Nuevos avisos enviados: {nuevos}")

            if ciclo % 10080 == 0:
                limpiar_viejos(con)

        except Exception as e:
            log.error(f"Error inesperado en ciclo: {e}")
            telegram(f"Error inesperado: {e}")

        time.sleep(INTERVALO)

if __name__ == "__main__":
    main()
