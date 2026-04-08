import requests
from bs4 import BeautifulSoup
import time
import hashlib
import os

# ---------------- CONFIG ----------------
TOKEN = os.getenv("TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

TWILIO_SID = os.getenv("TWILIO_SID")
TWILIO_AUTH = os.getenv("TWILIO_AUTH")
TWILIO_FROM = os.getenv("TWILIO_FROM")
TWILIO_TO = os.getenv("TWILIO_TO")

CIUDAD = "Osorno"

# ---------------- FILTROS ----------------
KEYWORDS = [
    "bodega", "operario", "logistica", "reponedor",
    "chofer", "peoneta", "auxiliar", "produccion"
]

EXCLUDE = [
    "practica", "práctica", "voluntario"
]

vistos = set()

# ---------------- UTIL ----------------
def hash_item(texto):
    return hashlib.md5(texto.encode()).hexdigest()

def limpiar(texto):
    return texto.replace("\n", " ").strip()

# ---------------- TELEGRAM ----------------
def telegram(msg):
    try:
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        requests.post(url, data={"chat_id": CHAT_ID, "text": msg})
    except:
        print("Error Telegram")

# ---------------- WHATSAPP ----------------
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

# ---------------- EXTRAER DETALLE ----------------
def extraer_detalle(link):
    try:
        r = requests.get(link, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")

        texto = soup.get_text(separator=" ").lower()

        empresa = "No especificado"
        ubicacion = "Osorno"
        sueldo = "No indicado"
        jornada = "No indicada"

        if "$" in texto or "clp" in texto:
            sueldo = "Posible sueldo indicado"

        if "full time" in texto or "tiempo completo" in texto:
            jornada = "Tiempo completo"
        elif "part time" in texto:
            jornada = "Part time"

        descripcion = limpiar(soup.get_text()[:400])

        return empresa, ubicacion, sueldo, jornada, descripcion

    except:
        return ("Error", "Error", "Error", "Error", "No se pudo cargar")

# ---------------- FILTRO ----------------
def filtrar(trabajos):
    filtrados = []
    for titulo, link, fuente in trabajos:
        t = titulo.lower()

        if any(k in t for k in KEYWORDS) and not any(e in t for e in EXCLUDE):
            filtrados.append((titulo, link, fuente))

    return filtrados

# ---------------- SCRAPERS ----------------
def indeed():
    try:
        url = f"https://cl.indeed.com/jobs?q=&l={CIUDAD}"
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(r.text, "html.parser")

        jobs = []
        for j in soup.select("h2.jobTitle"):
            titulo = j.get_text(strip=True)
            link = "https://cl.indeed.com" + j.find("a")["href"]
            jobs.append((titulo, link, "Indeed"))
        return jobs
    except:
        return []

def computrabajo():
    try:
        url = f"https://www.computrabajo.cl/trabajo-en-{CIUDAD.lower()}"
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(r.text, "html.parser")

        jobs = []
        for j in soup.select("h2 a"):
            jobs.append((j.text.strip(), j["href"], "Computrabajo"))
        return jobs
    except:
        return []

def chiletrabajos():
    try:
        url = f"https://www.chiletrabajos.cl/trabajos/en/{CIUDAD.lower()}/"
        r = requests.get(url)
        soup = BeautifulSoup(r.text, "html.parser")

        jobs = []
        for j in soup.select(".job-title a"):
            jobs.append((j.text.strip(), j["href"], "Chiletrabajos"))
        return jobs
    except:
        return []

# ---------------- MENSAJE ----------------
def crear_mensaje(titulo, link, fuente):
    empresa, ubicacion, sueldo, jornada, descripcion = extraer_detalle(link)

    return f"""🚨 *NUEVA OFERTA EN OSORNO*

📌 *Cargo:* {titulo}
🏢 *Empresa:* {empresa}
📍 *Ubicación:* {ubicacion}
💰 *Sueldo:* {sueldo}
🕒 *Jornada:* {jornada}
🌐 *Fuente:* {fuente}

📝 *Resumen:*
{descripcion[:160]}...

🔗 *Postula aquí:*
{link}
"""

# ---------------- INICIO ----------------
telegram("🧠 Bot inteligente activado")
whatsapp("🧠 Bot inteligente activado")

# ---------------- LOOP ----------------
while True:
    try:
        print("🟢 Analizando ofertas...")

        trabajos = []
        trabajos += indeed()
        trabajos += computrabajo()
        trabajos += chiletrabajos()

        trabajos = filtrar(trabajos)

        nuevos = 0

        for titulo, link, fuente in trabajos:
            clave = hash_item(link)

            if clave not in vistos:
                msg = crear_mensaje(titulo, link, fuente)

                telegram(msg)
                whatsapp(msg)

                vistos.add(clave)
                nuevos += 1

        print(f"🔎 {nuevos} nuevos")

    except Exception as e:
        print("Error:", e)
        telegram(f"⚠️ Error: {e}")

    time.sleep(20)
