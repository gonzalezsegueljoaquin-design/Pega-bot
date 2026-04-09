import requests
from bs4 import BeautifulSoup
import time
import hashlib
import os
import json

# ================= CONFIG =================
CIUDAD = "Osorno"

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

# WhatsApp (opcional)
ULTRAMSG_TOKEN = os.getenv("ULTRAMSG_TOKEN")
ULTRAMSG_INSTANCE = os.getenv("ULTRAMSG_INSTANCE")
WHATSAPP_TO = os.getenv("WHATSAPP_TO")

HEADERS = {"User-Agent": "Mozilla/5.0"}
ARCHIVO = "vistos.json"

# ================= UTIL =================
def cargar_vistos():
    if os.path.exists(ARCHIVO):
        with open(ARCHIVO, "r") as f:
            return set(json.load(f))
    return set()

def guardar_vistos(vistos):
    with open(ARCHIVO, "w") as f:
        json.dump(list(vistos), f)

def hash_item(texto):
    return hashlib.md5(texto.encode()).hexdigest()

# ================= MENSAJE =================
def formatear(titulo, link, fuente):
    return f"""🔥 NUEVA OFERTA

📌 {titulo}

🌐 Fuente: {fuente}
🔗 {link}
"""

# ================= TELEGRAM =================
def enviar_telegram(msg):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        r = requests.post(url, data={"chat_id": CHAT_ID, "text": msg})

        print("Telegram:", r.status_code, r.text)

        return r.status_code == 200
    except Exception as e:
        print("Error Telegram:", e)
        return False

# ================= WHATSAPP =================
def enviar_whatsapp(msg):
    try:
        if not ULTRAMSG_TOKEN:
            return False

        url = f"https://api.ultramsg.com/{ULTRAMSG_INSTANCE}/messages/chat"
        data = {
            "token": ULTRAMSG_TOKEN,
            "to": WHATSAPP_TO,
            "body": msg
        }

        r = requests.post(url, data=data)
        print("WhatsApp:", r.status_code)

        return r.status_code == 200

    except Exception as e:
        print("Error WhatsApp:", e)
        return False

# ================= SISTEMA ANTI-FALLOS =================
def enviar_notificacion(msg):
    print("📤 Enviando...")

    ok_tel = enviar_telegram(msg)

    if not ok_tel:
        print("⚠️ Telegram falló, reintentando...")
        time.sleep(5)
        ok_tel = enviar_telegram(msg)

    ok_wsp = enviar_whatsapp(msg)

    if ok_wsp:
        print("✅ WhatsApp OK")
    else:
        print("⚠️ WhatsApp no disponible (no crítico)")

# ================= FUENTES =================

def fuente_indeed():
    jobs = []
    try:
        url = "https://cl.indeed.com/jobs?q=&l=Osorno"
        r = requests.get(url, headers=HEADERS)
        soup = BeautifulSoup(r.text, "html.parser")

        for job in soup.select("a.tapItem"):
            titulo = job.get_text(strip=True)
            link = "https://cl.indeed.com" + job.get("href")
            jobs.append((titulo, link, "Indeed"))
    except Exception as e:
        print("Indeed error:", e)

    print("Indeed:", len(jobs))
    return jobs


def fuente_yapo():
    jobs = []
    try:
        url = "https://www.yapo.cl/region_de_los_lagos/empleos"
        r = requests.get(url, headers=HEADERS)
        soup = BeautifulSoup(r.text, "html.parser")

        for item in soup.select("a"):
            titulo = item.get_text(strip=True)
            link = item.get("href")

            if link and "yapo.cl" in link:
                jobs.append((titulo, link, "Yapo"))
    except Exception as e:
        print("Yapo error:", e)

    print("Yapo:", len(jobs))
    return jobs


def fuente_google():
    jobs = []
    try:
        url = "https://www.google.com/search?q=empleos+en+Osorno"
        r = requests.get(url, headers=HEADERS)
        soup = BeautifulSoup(r.text, "html.parser")

        for g in soup.select("a"):
            titulo = g.get_text(strip=True)
            link = g.get("href")

            if titulo and "http" in str(link):
                jobs.append((titulo, link, "Google"))
    except Exception as e:
        print("Google error:", e)

    print("Google:", len(jobs))
    return jobs


def fuente_facebook():
    jobs = []
    try:
        url = "https://www.google.com/search?q=trabajo+osorno+facebook"
        r = requests.get(url, headers=HEADERS)
        soup = BeautifulSoup(r.text, "html.parser")

        for a in soup.select("a"):
            link = a.get("href")
            titulo = a.get_text(strip=True)

            if "facebook.com" in str(link):
                jobs.append((titulo, link, "Facebook"))
    except Exception as e:
        print("Facebook error:", e)

    print("Facebook:", len(jobs))
    return jobs


def fuente_computrabajo():
    jobs = []
    try:
        url = "https://www.computrabajo.cl/trabajo-de-osorno"
        r = requests.get(url, headers=HEADERS)
        soup = BeautifulSoup(r.text, "html.parser")

        for item in soup.select("article"):
            tag = item.select_one("h2 a")
            if tag:
                titulo = tag.get_text(strip=True)
                link = "https://www.computrabajo.cl" + tag.get("href")
                jobs.append((titulo, link, "Computrabajo"))
    except Exception as e:
        print("Computrabajo error:", e)

    print("Computrabajo:", len(jobs))
    return jobs


def fuente_chiletrabajos():
    jobs = []
    try:
        url = "https://www.chiletrabajos.cl/busqueda/?q=osorno"
        r = requests.get(url, headers=HEADERS)
        soup = BeautifulSoup(r.text, "html.parser")

        for item in soup.select("a"):
            titulo = item.get_text(strip=True)
            link = item.get("href")

            if link and "/oferta/" in str(link):
                jobs.append((titulo, link, "Chiletrabajos"))
    except Exception as e:
        print("Chiletrabajos error:", e)

    print("Chiletrabajos:", len(jobs))
    return jobs


# ================= MAIN =================
vistos = cargar_vistos()

while True:
    try:
        print("\n🔎 Buscando trabajos...")

        trabajos = []
        trabajos += fuente_indeed()
        trabajos += fuente_yapo()
        trabajos += fuente_google()
        trabajos += fuente_facebook()
        trabajos += fuente_computrabajo()
        trabajos += fuente_chiletrabajos()

        print("TOTAL:", len(trabajos))

        nuevos = 0

        for titulo, link, fuente in trabajos:
            clave = hash_item(link)

            if clave not in vistos:
                msg = formatear(titulo, link, fuente)

                enviar_notificacion(msg)

                vistos.add(clave)
                guardar_vistos(vistos)

                nuevos += 1

        print("📩 Nuevos enviados:", nuevos)

    except Exception as e:
        print("ERROR GENERAL:", e)

    time.sleep(180)
