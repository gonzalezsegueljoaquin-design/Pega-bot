import requests
from bs4 import BeautifulSoup
import time
import os
import json
import logging
import re
from datetime import datetime

# ================== CONFIG ==================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

WHATSAPP_TOKEN = os.getenv("WHATSAPP_TOKEN")
WHATSAPP_INSTANCE = os.getenv("WHATSAPP_INSTANCE")
WHATSAPP_PHONE = os.getenv("WHATSAPP_PHONE")

INTERVALO = int(os.getenv("INTERVALO", "600"))  # 10 min default
HEARTBEAT = int(os.getenv("HEARTBEAT", "300"))  # log cada 5 min

HEADERS = {"User-Agent": "Mozilla/5.0"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ================== ARCHIVOS ==================
ENVIADOS_FILE = "enviados.json"

def cargar_enviados():
    try:
        if os.path.exists(ENVIADOS_FILE):
            with open(ENVIADOS_FILE, "r") as f:
                return set(json.load(f))
    except Exception as e:
        logging.error(f"Error leyendo enviados.json: {e}")
    return set()

def guardar_enviados(data):
    try:
        with open(ENVIADOS_FILE, "w") as f:
            json.dump(list(data), f)
    except Exception as e:
        logging.error(f"Error guardando enviados.json: {e}")

# ================== UTIL ==================
def hash_item(texto):
    return str(hash(texto))

def es_reciente(texto):
    t = (texto or "").lower()
    return any(x in t for x in ["hoy", "ayer", "justo ahora", "reciente"])

def titulo_valido(titulo):
    if not titulo:
        return False
    t = titulo.lower()
    basura = ["here","click","retry","enablejs","javascript","error","httpservice"]
    if any(b in t for b in basura):
        return False
    return len(t.strip()) > 8

def link_valido(link):
    if not link:
        return False
    if not link.startswith("http"):
        return False
    basura = ["google", "enablejs", "retry", "httpservice"]
    if any(b in link for b in basura):
        return False
    return True

# ================== EXTRACCIÓN ==================
def extraer_info(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")
        texto = soup.get_text(" ", strip=True)

        sueldo = "No especificado"
        m = re.search(r"\$\s?[\d\.\,]+", texto)
        if m:
            sueldo = m.group()

        empresa = "No especificada"
        if "empresa" in texto.lower():
            empresa = "Empresa detectada (ver link)"

        ubicacion = "Osorno"
        if "puerto montt" in texto.lower():
            ubicacion = "Puerto Montt"

        jornada = "No especificada"
        tl = texto.lower()
        if "turno" in tl:
            jornada = "Turnos"
        elif "full time" in tl:
            jornada = "Full Time"
        elif "part time" in tl:
            jornada = "Part Time"

        requisitos = "Ver publicación"
        if "requisitos" in tl:
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

# ================== NOTIFICACIONES ==================
def enviar_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logging.error("Faltan variables de Telegram")
        return

    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        r = requests.post(url, data={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": msg
        }, timeout=10)

        if r.status_code == 200:
            logging.info("Telegram OK")
        else:
            logging.error(f"Telegram error {r.status_code}: {r.text}")

    except Exception as e:
        logging.error(f"Telegram fallo: {e}")

def enviar_whatsapp(msg):
    try:
        if not WHATSAPP_TOKEN or not WHATSAPP_INSTANCE or not WHATSAPP_PHONE:
            logging.info("WhatsApp no configurado (se omite)")
            return

        url = f"https://api.ultramsg.com/{WHATSAPP_INSTANCE}/messages/chat"
        r = requests.post(url, data={
            "token": WHATSAPP_TOKEN,
            "to": WHATSAPP_PHONE,
            "body": msg
        }, timeout=10)

        logging.info(f"WhatsApp status: {r.status_code}")

    except Exception as e:
        logging.error(f"WhatsApp fallo: {e}")

def enviar(msg):
    enviar_telegram(msg)
    enviar_whatsapp(msg)

# ================== SCRAPERS ==================
def chiletrabajos():
    lista = []
    try:
        r = requests.get("https://www.chiletrabajos.cl/busqueda?2=Osorno", headers=HEADERS, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")

        for a in soup.select("a"):
            titulo = a.get_text(strip=True)
            link = a.get("href")

            if titulo and link and "trabajo" in link:
                if not link.startswith("http"):
                    link = "https://www.chiletrabajos.cl" + link
                lista.append((titulo, link, "Chiletrabajos"))
    except Exception as e:
        logging.error(f"Chiletrabajos error: {e}")
    return lista

def computrabajo():
    lista = []
    try:
        r = requests.get("https://cl.computrabajo.com/trabajo-de-osorno", headers=HEADERS, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")

        for a in soup.select("a.js-o-link"):
            titulo = a.get_text(strip=True)
            link = a.get("href")

            if titulo and link:
                if not link.startswith("http"):
                    link = "https://cl.computrabajo.com" + link
                lista.append((titulo, link, "Computrabajo"))
    except Exception as e:
        logging.error(f"Computrabajo error: {e}")
    return lista

def yapo():
    lista = []
    try:
        r = requests.get("https://www.yapo.cl/region_de_los_lagos/empleos?ca=12_s&l=0&q=osorno", headers=HEADERS, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")

        for a in soup.select("a"):
            titulo = a.get_text(strip=True)
            link = a.get("href")

            if titulo and link and "empleos" in link:
                if not link.startswith("http"):
                    link = "https://www.yapo.cl" + link
                lista.append((titulo, link, "Yapo"))
    except Exception as e:
        logging.error(f"Yapo error: {e}")
    return lista

# ================== MAIN ==================
def main():
    logging.info("🚀 BOT INICIANDO (DEBUG MODO DIOS)")

    # Test inicial SIEMPRE
    enviar("🚀 Bot iniciado correctamente (debug activo)")

    enviados = cargar_enviados()
    last_heartbeat = time.time()

    while True:
        try:
            logging.info("🔎 Buscando trabajos...")

            trabajos = []
            trabajos += chiletrabajos()
            trabajos += computrabajo()
            trabajos += yapo()

            logging.info(f"Total encontrados: {len(trabajos)}")

            nuevos = 0

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
🏢 {info['empresa']}
📍 {info['ubicacion']}
💰 {info['sueldo']}
⏰ {info['jornada']}
📋 {info['requisitos']}

🌐 {fuente}
🔗 {link}
"""

                enviar(msg)

                enviados.add(clave)
                nuevos += 1

            guardar_enviados(enviados)

            logging.info(f"📤 Nuevos enviados: {nuevos}")

            # HEARTBEAT (para saber que no está pegado)
            if time.time() - last_heartbeat > HEARTBEAT:
                logging.info("💓 BOT ACTIVO (heartbeat)")
                last_heartbeat = time.time()

        except Exception as e:
            logging.error(f"💥 ERROR GLOBAL: {e}")

        time.sleep(INTERVALO)

if __name__ == "__main__":
    main()
