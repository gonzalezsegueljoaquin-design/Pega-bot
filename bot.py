import requests
from bs4 import BeautifulSoup
import time
import os
import json
import logging
import re

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

def titulo_valido(titulo):
    if not titulo:
        return False

    titulo = titulo.lower()
    basura = ["here","click","retry","enablejs","javascript","error","httpservice"]

    if any(b in titulo for b in basura):
        return False

    return len(titulo) > 8

def link_valido(link):
    if not link:
        return False

    if not link.startswith("http"):
        return False

    basura = ["google","enablejs","retry","httpservice"]

    if any(b in link for b in basura):
        return False

    return True

# ================= EXTRACCIÓN INTELIGENTE =================
def extraer_info(url):
    try:
        r = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(r.text, "html.parser")

        texto = soup.get_text(" ", strip=True)

        # SUELDO
        sueldo = "No especificado"
        match = re.search(r"\$[\d\.\,]+", texto)
        if match:
            sueldo = match.group()

        # EMPRESA
        empresa = "No especificada"
        posibles = ["empresa", "compañía", "company"]
        for p in posibles:
            if p in texto.lower():
                empresa = p
                break

        # UBICACIÓN
        ubicacion = "Osorno"
        if "puerto montt" in texto.lower():
            ubicacion = "Puerto Montt"

        # JORNADA
        jornada = "No especificada"
        if "turno" in texto.lower():
            jornada = "Turnos"
        elif "full time" in texto.lower():
            jornada = "Full Time"
        elif "part time" in texto.lower():
            jornada = "Part Time"

        # REQUISITOS (texto resumido)
        requisitos = "No especificados"
        if "requisitos" in texto.lower():
            requisitos = "Incluye requisitos (ver link)"

        return {
            "sueldo": sueldo,
            "empresa": empresa,
            "ubicacion": ubicacion,
            "jornada": jornada,
            "requisitos": requisitos
        }

    except Exception as e:
        logging.error(f"Error extrayendo info: {e}")
        return {
            "sueldo": "No disponible",
            "empresa": "No disponible",
            "ubicacion": "Osorno",
            "jornada": "No disponible",
            "requisitos": "No disponible"
        }

# ================= NOTIFICACIONES =================
def enviar_telegram(msg):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, data={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": msg
        }, timeout=10)
    except Exception as e:
        logging.error(f"Telegram error: {e}")

def enviar_whatsapp(msg):
    try:
        if not WHATSAPP_TOKEN:
            return

        url = f"https://api.ultramsg.com/{WHATSAPP_INSTANCE}/messages/chat"
        requests.post(url, data={
            "token": WHATSAPP_TOKEN,
            "to": WHATSAPP_PHONE,
            "body": msg
        }, timeout=10)
    except Exception as e:
        logging.error(f"WhatsApp error: {e}")

def enviar(msg):
    enviar_telegram(msg)
    enviar_whatsapp(msg)

# ================= SCRAPERS =================
def chiletrabajos():
    lista = []
    try:
        r = requests.get("https://www.chiletrabajos.cl/busqueda?2=Osorno", timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")

        for a in soup.select("a"):
            titulo = a.get_text(strip=True)
            link = a.get("href")

            if titulo and link and "trabajo" in link:
                if not link.startswith("http"):
                    link = "https://www.chiletrabajos.cl" + link
                lista.append((titulo, link, "Chiletrabajos"))
    except:
        pass
    return lista

def computrabajo():
    lista = []
    try:
        r = requests.get("https://cl.computrabajo.com/trabajo-de-osorno", timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")

        for a in soup.select("a.js-o-link"):
            titulo = a.get_text(strip=True)
            link = a.get("href")

            if titulo and link:
                if not link.startswith("http"):
                    link = "https://cl.computrabajo.com" + link
                lista.append((titulo, link, "Computrabajo"))
    except:
        pass
    return lista

def yapo():
    lista = []
    try:
        r = requests.get("https://www.yapo.cl/region_de_los_lagos/empleos?ca=12_s&l=0&q=osorno", timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")

        for a in soup.select("a"):
            titulo = a.get_text(strip=True)
            link = a.get("href")

            if titulo and link and "empleos" in link:
                if not link.startswith("http"):
                    link = "https://www.yapo.cl" + link
                lista.append((titulo, link, "Yapo"))
    except:
        pass
    return lista

# ================= LOOP =================
def main():
    enviados = cargar_enviados()

    while True:
        trabajos = []
        trabajos += chiletrabajos()
        trabajos += computrabajo()
        trabajos += yapo()

        for titulo, link, fuente in trabajos:

            if not titulo_valido(titulo):
                continue

            if not link_valido(link):
                continue

            if not es_reciente(titulo):
                continue

            clave = hash_item(link)

            if clave in enviados:
                continue

            info = extraer_info(link)

            msg = f"""💼 NUEVA OFERTA

📌 {titulo}
🏢 Empresa: {info['empresa']}
📍 Ubicación: {info['ubicacion']}
💰 Sueldo: {info['sueldo']}
⏰ Jornada: {info['jornada']}
📋 {info['requisitos']}

🌐 Fuente: {fuente}
🔗 {link}
"""

            enviar(msg)

            enviados.add(clave)

        guardar_enviados(enviados)

        time.sleep(INTERVALO)

if __name__ == "__main__":
    main()
