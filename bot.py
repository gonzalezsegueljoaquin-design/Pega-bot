import requests
from bs4 import BeautifulSoup
import time
import hashlib
import os

# -------- CONFIG --------
TOKEN = os.getenv("TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

TWILIO_SID = os.getenv("TWILIO_SID")
TWILIO_AUTH = os.getenv("TWILIO_AUTH")
TWILIO_FROM = os.getenv("TWILIO_FROM")
TWILIO_TO = os.getenv("TWILIO_TO")

CIUDAD = "Osorno"

vistos = set()

# -------- HASH --------
def hash_item(texto):
    return hashlib.md5(texto.encode()).hexdigest()

# -------- TELEGRAM --------
def telegram(msg):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    requests.post(url, data={"chat_id": CHAT_ID, "text": msg})

# -------- WHATSAPP --------
def whatsapp(msg):
    url = f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_SID}/Messages.json"
    data = {
        "From": f"whatsapp:{TWILIO_FROM}",
        "To": f"whatsapp:{TWILIO_TO}",
        "Body": msg
    }
    requests.post(url, data=data, auth=(TWILIO_SID, TWILIO_AUTH))

# -------- SCRAPERS --------
def indeed():
    url = f"https://cl.indeed.com/jobs?q=&l={CIUDAD}"
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
    soup = BeautifulSoup(r.text, "html.parser")

    jobs = []
    for j in soup.select("h2.jobTitle"):
        titulo = j.get_text(strip=True)
        link = "https://cl.indeed.com" + j.find("a")["href"]
        jobs.append((titulo, link, "Indeed"))
    return jobs

def chiletrabajos():
    url = f"https://www.chiletrabajos.cl/trabajos/en/{CIUDAD.lower()}/"
    r = requests.get(url)
    soup = BeautifulSoup(r.text, "html.parser")

    jobs = []
    for j in soup.select(".job-title a"):
        jobs.append((j.text.strip(), j["href"], "Chiletrabajos"))
    return jobs

def computrabajo():
    url = f"https://www.computrabajo.cl/trabajo-en-{CIUDAD.lower()}"
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
    soup = BeautifulSoup(r.text, "html.parser")

    jobs = []
    for j in soup.select("h2 a"):
        jobs.append((j.text.strip(), j["href"], "Computrabajo"))
    return jobs

# -------- LOOP REALTIME --------

telegram("✅ Bot iniciado correctamente")
whatsapp("✅ Bot iniciado correctamente")

while True:
    try:
        trabajos = []
        trabajos += indeed()
        trabajos += chiletrabajos()
        trabajos += computrabajo()

        for titulo, link, fuente in trabajos:
            clave = hash_item(link)

            if clave not in vistos:
                msg = f"🚨 NUEVO TRABAJO ({fuente})\n\n{titulo}\n{link}"

                telegram(msg)
                whatsapp(msg)

                vistos.add(clave)

        print("Chequeo OK")

    except Exception as e:
        telegram(f"Error: {e}")

    time.sleep(30)  # 🔥 REALTIME (30 segundos)
