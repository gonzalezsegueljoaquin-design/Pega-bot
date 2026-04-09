import requests
import time
import hashlib
import os
import feedparser
from bs4 import BeautifulSoup

# ---------------- CONFIG ----------------
TOKEN = os.getenv("TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

TWILIO_SID = os.getenv("TWILIO_SID")
TWILIO_AUTH = os.getenv("TWILIO_AUTH")
TWILIO_FROM = os.getenv("TWILIO_FROM")
TWILIO_TO = os.getenv("TWILIO_TO")

CIUDAD = "Osorno"

KEYWORDS = [
    "bodega", "operario", "logistica", "reponedor",
    "chofer", "peoneta", "auxiliar", "produccion",
    "ayudante", "asistente", "ventas", "tienda"
]

# ---------------- PERSISTENCIA ----------------
def cargar_vistos():
    try:
        with open("seen.txt", "r") as f:
            return set(f.read().splitlines())
    except:
        return set()

def guardar_visto(item):
    with open("seen.txt", "a") as f:
        f.write(item + "\n")

vistos = cargar_vistos()

# ---------------- UTIL ----------------
def hash_item(texto):
    return hashlib.md5(texto.encode()).hexdigest()

# ---------------- NOTIFICACIONES ----------------
def telegram(msg):
    try:
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        requests.post(url, data={"chat_id": CHAT_ID, "text": msg})
    except:
        print("Error Telegram")

def whatsapp(msg):
    try:
        url = f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_SID}/Messages.json"
        data = {
            "From": f"whatsapp:{TWILIO_FROM}",
            "To": f"whatsapp:{TWILIO_TO}",
            "Body": msg
        }
        requests.post(url, data=data, auth=(TWILIO_SID, TWILIO_AUTH))
    except:
        print("Error WhatsApp")

# ---------------- INDEED RSS ----------------
def indeed():
    try:
        url = f"https://cl.indeed.com/rss?q=&l={CIUDAD}"
        feed = feedparser.parse(url)

        jobs = [(e.title, e.link, "Indeed") for e in feed.entries]
        print(f"Indeed: {len(jobs)}")
        return jobs
    except:
        return []

# ---------------- CHILETRABAJOS ----------------
def chiletrabajos():
    try:
        url = f"https://www.chiletrabajos.cl/rss/trabajos/{CIUDAD.lower()}"
        feed = feedparser.parse(url)

        jobs = [(e.title, e.link, "Chiletrabajos") for e in feed.entries]
        print(f"Chiletrabajos: {len(jobs)}")
        return jobs
    except:
        return []

# ---------------- YAPO ----------------
def yapo():
    try:
        url = f"https://www.yapo.cl/region_de_los_lagos/empleos?q={CIUDAD}"
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(r.text, "html.parser")

        jobs = []
        for item in soup.select("a[href*='/avisos/']"):
            titulo = item.get_text(strip=True)
            link = "https://www.yapo.cl" + item["href"]

            if titulo:
                jobs.append((titulo, link, "Yapo"))

        print(f"Yapo: {len(jobs)}")
        return jobs
    except:
        return []

# ---------------- FACEBOOK (RSS PÚBLICO) ----------------
def facebook():
    try:
        # Puedes cambiar este link por grupos públicos
        url = "https://rsshub.app/facebook/page/empleos.osorno"
        feed = feedparser.parse(url)

        jobs = [(e.title, e.link, "Facebook") for e in feed.entries]
        print(f"Facebook: {len(jobs)}")
        return jobs
    except:
        return []

# ---------------- FILTRO ----------------
def filtrar(trabajos):
    filtrados = []
    for titulo, link, fuente in trabajos:
        t = titulo.lower()

        if any(k in t for k in KEYWORDS) or CIUDAD.lower() in t:
            filtrados.append((titulo, link, fuente))

    return filtrados

# ---------------- MENSAJE ----------------
def mensaje(titulo, link, fuente):
    return f"""🚨 *OFERTA DETECTADA*

📌 *{titulo}*
🌐 {fuente}

🔗 *Postula aquí:*
{link}
"""

# ---------------- INICIO ----------------
telegram("🔥 BOT MODO DIOS ACTIVADO")
whatsapp("🔥 BOT MODO DIOS ACTIVADO")

# ---------------- LOOP ----------------
while True:
    try:
        print("🟢 Buscando...")

        trabajos = []
        trabajos += indeed()
        trabajos += chiletrabajos()
        trabajos += yapo()
        trabajos += facebook()

        print("TOTAL:", len(trabajos))

        trabajos = filtrar(trabajos)

        nuevos = 0

        for titulo, link, fuente in trabajos:
            clave = hash_item(link)

            if clave not in vistos:
                msg = mensaje(titulo, link, fuente)

                telegram(msg)
                whatsapp(msg)

                guardar_visto(clave)
                vistos.add(clave)
                nuevos += 1

        print(f"📩 Nuevos: {nuevos}")

    except Exception as e:
        print("Error:", e)
        telegram(f"⚠️ Error: {e}")

    time.sleep(60)
