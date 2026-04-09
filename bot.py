import requests
from bs4 import BeautifulSoup
import time
import hashlib
import os
import json

# ================= CONFIG =================
CIUDAD = "Osorno"
KEYWORDS = ["trabajo", "operario", "bodega", "reponedor", "auxiliar"]

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

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

def guardar_visto(vistos):
    with open(ARCHIVO, "w") as f:
        json.dump(list(vistos), f)

def hash_item(texto):
    return hashlib.md5(texto.encode()).hexdigest()

def contiene_keyword(texto):
    texto = texto.lower()
    return any(k in texto for k in KEYWORDS)

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
        requests.post(url, data={"chat_id": CHAT_ID, "text": msg})
    except Exception as e:
        print("Error Telegram:", e)

# ================= WHATSAPP =================
def enviar_whatsapp(msg):
    try:
        url = f"https://api.ultramsg.com/{ULTRAMSG_INSTANCE}/messages/chat"
        data = {
            "token": ULTRAMSG_TOKEN,
            "to": WHATSAPP_TO,
            "body": msg
        }
        requests.post(url, data=data)
    except Exception as e:
        print("Error WhatsApp:", e)

# ================= FUENTES =================

# 1. INDEED (HTML fallback)
def fuente_indeed():
    jobs = []
    try:
        url = "https://cl.indeed.com/jobs?q=trabajo&l=Osorno"
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


# 2. YAPO
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


# 3. GOOGLE JOBS (MUY POTENTE)
def fuente_google():
    jobs = []
    try:
        url = "https://www.google.com/search?q=trabajos+osorno"
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


# 4. FACEBOOK INDIRECTO
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


# ================= MOTOR =================
def obtener_todo():
    trabajos = []
    trabajos += fuente_indeed()
    trabajos += fuente_yapo()
    trabajos += fuente_google()
    trabajos += fuente_facebook()

    print("TOTAL bruto:", len(trabajos))

    # filtro inteligente
    filtrados = []
    for t, l, f in trabajos:
        if contiene_keyword(t) and CIUDAD.lower() in t.lower():
            filtrados.append((t, l, f))

    print("TOTAL filtrado:", len(filtrados))
    return filtrados


# ================= MAIN LOOP =================
vistos = cargar_vistos()

while True:
    try:
        print("\n🔎 Buscando trabajos...")

        trabajos = obtener_todo()

        nuevos = 0

        for titulo, link, fuente in trabajos:
            clave = hash_item(link)

            if clave not in vistos:
                msg = formatear(titulo, link, fuente)

                enviar_telegram(msg)
                enviar_whatsapp(msg)

                vistos.add(clave)
                guardar_visto(vistos)

                nuevos += 1

        print("Nuevos enviados:", nuevos)

    except Exception as e:
        print("ERROR GENERAL:", e)

    time.sleep(180)
