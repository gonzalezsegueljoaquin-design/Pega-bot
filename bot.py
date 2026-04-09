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
    "ayudante", "asistente", "ventas", "tienda"
]

REQUEST_TIMEOUT = 10       # segundos por request
INTERVALO       = 60       # segundos entre ciclos
DIAS_RETENCION  = 30       # días para limpiar registros viejos

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
    s.headers.update({"User-Agent": "Mozilla/5.0"})
    return s

# ---------------- NOTIFICACIONES ----------------
def telegram(msg: str):
    try:
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        r = requests.post(
            url,
            data={"chat_id": CHAT_ID, "text": msg, "parse_mode": "Markdown"},
            timeout=REQUEST_TIMEOUT
        )
        r.raise_for_status()
    except Exception as e:
        log.error(f"Error Telegram: {e}")

def whatsapp(msg: str):
    try:
        url = f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_SID}/Messages.json"
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
    try:
        url  = f"https://cl.indeed.com/rss?q=&l={CIUDAD}"
        feed = feedparser.parse(url)
        jobs = [(e.title, e.link, "Indeed") for e in feed.entries]
        log.info(f"Indeed: {len(jobs)} avisos")
        return jobs
    except Exception as e:
        log.error(f"Error Indeed: {e}")
        return []

def chiletrabajos() -> list[tuple]:
    try:
        url  = f"https://www.chiletrabajos.cl/rss/trabajos/{CIUDAD.lower()}"
        feed = feedparser.parse(url)
        jobs = [(e.title, e.link, "Chiletrabajos") for e in feed.entries]
        log.info(f"Chiletrabajos: {len(jobs)} avisos")
        return jobs
    except Exception as e:
        log.error(f"Error Chiletrabajos: {e}")
        return []

def yapo(session: requests.Session) -> list[tuple]:
    try:
        url = f"https://www.yapo.cl/region_de_los_lagos/empleos?q={CIUDAD}"
        r   = session.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        jobs = []
        vistos_yapo = set()
        for item in soup.select("a[href*='/avisos/']"):
            titulo = item.get_text(strip=True)
            link   = "https://www.yapo.cl" + item["href"]

            # Filtrar links de navegación y títulos muy cortos
            if not titulo or len(titulo) < 5 or link in vistos_yapo:
                continue

            vistos_yapo.add(link)
            jobs.append((titulo, link, "Yapo"))

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
        # Requiere al menos 1 keyword del oficio (no solo ciudad)
        if any(k in t for k in KEYWORDS):
            filtrados.append((titulo, link, fuente))
    return filtrados

# ---------------- MENSAJE ----------------
def mensaje(titulo: str, link: str, fuente: str) -> str:
    return (
        f"🚨 *OFERTA DETECTADA*\n\n"
        f"📌 *{titulo}*\n"
        f"🌐 {fuente}\n\n"
        f"🔗 *Postula aquí:*\n{link}"
    )

# ---------------- MAIN ----------------
def main():
    con     = init_db()
    vistos  = cargar_vistos(con)
    session = get_session()

    log.info("Bot iniciado")
    notificar("🔥 *BOT MODO DIOS ACTIVADO*")

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
            log.info(f"Total después de filtro: {len(trabajos)}")

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

            # Limpieza semanal (cada 10080 ciclos de 60s ≈ 7 días)
            if ciclo % 10080 == 0:
                limpiar_viejos(con)

        except Exception as e:
            log.error(f"Error inesperado en ciclo: {e}")
            telegram(f"⚠️ Error inesperado: {e}")

        time.sleep(INTERVALO)

if __name__ == "__main__":
    main()
