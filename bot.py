import requests
from bs4 import BeautifulSoup
import time
import os
import json
import logging
from datetime import datetime

# ================= CONFIG =================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

WHATSAPP_TOKEN = os.getenv("WHATSAPP_TOKEN")
WHATSAPP_INSTANCE = os.getenv("WHATSAPP_INSTANCE")
WHATSAPP_PHONE = os.getenv("WHATSAPP_PHONE")

INTERVALO = 600

logging.basicConfig(level=logging.INFO)

# ================= ARCHIVOS =================
ENVIADOS_FILE = "enviados.json"

def cargar_enviados():
    if os.path.exists(ENVIADOS_FILE):
        with open(ENVIADOS_FILE, "r") as f:
            return set(json.load(f))
    return set()

def guardar_enviados(data):
    with open(ENVIADOS_FILE, "w") as f:
        json.dump(list(data), f)

# ================= UTIL =================
def hash_item(texto):
    return str(hash(texto))

def es_reciente(texto):
    texto = texto.lower()
    return any(x in texto for x in ["hoy", "ayer", "justo ahora", "reciente"])

# ================= TELEGRAM =================
def enviar_telegram(msg):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, data={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": msg
        }, timeout=10)
        logging.info("Telegram OK")
    except Exception as e:
        logging.error(f"Telegram error: {e}")

# ================= WHATSAPP =================
def enviar_whatsapp(msg):
    try:
        if not WHATSAPP_TOKEN:
            return

        url = f"https://api.ultramsg.com/{WHATSAPP_INSTANCE}/messages/chat"
        payload = {
            "token": WHATSAPP_TOKEN,
            "to": WHATSAPP_PHONE,
            "body": msg
        }
        requests.post(url, data=payload, timeout=10)
        logging.info("WhatsApp OK")
    except Exception as e:
        logging.error(f"WhatsApp error: {e}")

def enviar(msg):
    enviar_telegram(msg)
    enviar_whatsapp(msg)

# ================= SCRAPERS =================
HEADERS = {"User-Agent": "Mozilla/5.0"}

def chiletrabajos():
    lista = []
    try:
        url = "https://www.chiletrabajos.cl/busqueda?2=Osorno"
        r = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")

        for a in soup.select("a"):
            titulo = a.get_text(strip=True)
            link = a.get("href")

            if titulo and link and "trabajo" in link:
                lista.append((titulo, link, "Chiletrabajos"))
    except:
        pass
    return lista

def computrabajo():
    lista = []
    try:
        url = "https://cl.computrabajo.com/trabajo-de-osorno"
        r = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")

        for a in soup.select("a.js-o-link"):
            titulo = a.get_text(strip=True)
            link = "https://cl.computrabajo.com" + a.get("href")

            if titulo:
                lista.append((titulo, link, "Computrabajo"))
    except:
        pass
    return lista

def yapo():
    lista = []
    try:
        url = "https://www.yapo.cl/region_de_los_lagos/empleos?ca=12_s&l=0&q=osorno"
        r = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")

        for a in soup.select("a"):
            titulo = a.get_text(strip=True)
            link = a.get("href")

            if titulo and link and "empleos" in link:
                lista.append((titulo, link, "Yapo"))
    except:
        pass
    return lista

def facebook():
    lista = []
    try:
        url = "https://www.facebook.com/search/posts/?q=trabajo%20osorno"
        r = requests.get(url, headers=HEADERS, timeout=10)

        if "trabajo" in r.text.lower():
            lista.append(("Publicaciones trabajo en Facebook", url, "Facebook"))
    except:
        pass
    return lista

# ================= LOOP =================
def main():
    enviados = cargar_enviados()

    while True:
        logging.info("Buscando trabajos...")

        trabajos = []
        trabajos += chiletrabajos()
        trabajos += computrabajo()
        trabajos += yapo()
        trabajos += facebook()

        logging.info(f"Total encontrados: {len(trabajos)}")

        nuevos = 0

        for titulo, link, fuente in trabajos:

            if not es_reciente(titulo):
                continue

            clave = hash_item(link)

            if clave in enviados:
                continue

            msg = f"""💼 NUEVO TRABAJO

📌 {titulo}
🌐 {fuente}
🔗 {link}
📍 Osorno
"""

            enviar(msg)

            enviados.add(clave)
            nuevos += 1

        guardar_enviados(enviados)

        logging.info(f"Nuevos enviados: {nuevos}")

        time.sleep(INTERVALO)

if __name__ == "__main__":
    main()
