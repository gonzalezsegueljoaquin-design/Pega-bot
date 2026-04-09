import requests
from bs4 import BeautifulSoup
import time
import logging
import os
import re
from datetime import datetime

# ==============================
# ⚙️ CONFIG
# ==============================
BUSQUEDA = "trabajo osorno"
INTERVALO = 600  # 10 minutos
ENVIADOS_FILE = "enviados.txt"

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

WHATSAPP_TOKEN = os.getenv("WHATSAPP_TOKEN")
WHATSAPP_PHONE_ID = os.getenv("WHATSAPP_PHONE_ID")
WHATSAPP_TO = os.getenv("WHATSAPP_TO")

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

# ==============================
# 🧠 LOGS MODO DIOS
# ==============================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ==============================
# 📦 UTILIDADES
# ==============================
def limpiar(texto):
    return re.sub(r"\s+", " ", texto).strip()

def es_link_valido(link):
    if not link:
        return False
    if "google" in link and "http" not in link:
        return False
    return True

# ==============================
# 🧠 FILTRO INTELIGENTE (NO AGRESIVO)
# ==============================
def es_reciente(texto):
    texto = texto.lower()

    if "hoy" in texto or "ayer" in texto:
        return True

    if "hora" in texto:
        return True

    match = re.search(r"(\d+)\s*d[ií]a", texto)
    if match:
        dias = int(match.group(1))
        return dias <= 2

    return True  # 🔥 no perder ofertas

# ==============================
# 📂 CONTROL DUPLICADOS
# ==============================
def cargar_enviados():
    try:
        with open(ENVIADOS_FILE, "r") as f:
            return set(f.read().splitlines())
    except:
        return set()

def guardar_enviado(link):
    with open(ENVIADOS_FILE, "a") as f:
        f.write(link + "\n")

# ==============================
# 📤 TELEGRAM
# ==============================
def enviar_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logging.error("Faltan variables de Telegram")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"

    try:
        r = requests.post(url, data={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": msg
        })
        logging.info(f"Telegram status: {r.status_code}")
    except Exception as e:
        logging.error(f"Error Telegram: {e}")

# ==============================
# 📤 WHATSAPP (META API)
# ==============================
def enviar_whatsapp(msg):
    if not WHATSAPP_TOKEN or not WHATSAPP_PHONE_ID or not WHATSAPP_TO:
        logging.info("WhatsApp no configurado (se omite)")
        return

    url = f"https://graph.facebook.com/v18.0/{WHATSAPP_PHONE_ID}/messages"

    headers = {
        "Authorization": f"Bearer {WHATSAPP_TOKEN}",
        "Content-Type": "application/json"
    }

    data = {
        "messaging_product": "whatsapp",
        "to": WHATSAPP_TO,
        "type": "text",
        "text": {"body": msg}
    }

    try:
        r = requests.post(url, headers=headers, json=data)
        logging.info(f"WhatsApp status: {r.status_code}")
    except Exception as e:
        logging.error(f"Error WhatsApp: {e}")

# ==============================
# 📤 ENVÍO GENERAL
# ==============================
def enviar(msg):
    enviar_telegram(msg)
    enviar_whatsapp(msg)

# ==============================
# 🧠 EXTRAER INFO (BÁSICO PERO ÚTIL)
# ==============================
def extraer_info(texto):
    texto_lower = texto.lower()

    sueldo = "No especificado"
    empresa = "No especificada"
    ubicacion = "Osorno"

    # sueldo simple
    match = re.search(r"\$[\d\.\,]+", texto)
    if match:
        sueldo = match.group(0)

    # empresa heurística
    if "empresa" in texto_lower:
        empresa = "Mencionada en aviso"

    return empresa, sueldo, ubicacion

# ==============================
# 🔎 SCRAP GOOGLE JOBS (MEJORADO)
# ==============================
def buscar_google():
    trabajos = []
    url = f"https://www.google.com/search?q={BUSQUEDA.replace(' ', '+')}"

    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")

        for g in soup.select("a"):
            titulo = limpiar(g.text)
            link = g.get("href")

            if not titulo or len(titulo) < 15:
                continue

            if not es_link_valido(link):
                continue

            trabajos.append({
                "titulo": titulo,
                "link": link,
                "fuente": "Google"
            })

    except Exception as e:
        logging.error(f"Error Google: {e}")

    logging.info(f"Google: {len(trabajos)} avisos")
    return trabajos

# ==============================
# 🔎 YAPO (BÁSICO)
# ==============================
def buscar_yapo():
    trabajos = []
    url = "https://www.yapo.cl/region_de_los_lagos/empleos"

    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")

        for item in soup.select("a"):
            titulo = limpiar(item.text)
            link = item.get("href")

            if not titulo or len(titulo) < 10:
                continue

            if not link:
                continue

            trabajos.append({
                "titulo": titulo,
                "link": "https://www.yapo.cl" + link,
                "fuente": "Yapo"
            })

    except Exception as e:
        logging.error(f"Error Yapo: {e}")

    logging.info(f"Yapo: {len(trabajos)} avisos")
    return trabajos

# ==============================
# 🔎 FACEBOOK (LIMITADO PERO INCLUIDO)
# ==============================
def buscar_facebook():
    trabajos = []
    # Facebook bloquea scraping → placeholder inteligente
    logging.info("Facebook: scraping limitado (se omite automático)")
    return trabajos

# ==============================
# 🧠 PROCESAMIENTO CENTRAL
# ==============================
def procesar(trabajos, enviados):
    nuevos = 0

    for t in trabajos:
        titulo = t["titulo"]
        link = t["link"]

        if link in enviados:
            continue

        if not es_reciente(titulo):
            continue

        empresa, sueldo, ubicacion = extraer_info(titulo)

        mensaje = f"""🔥 NUEVA OFERTA

📌 {titulo}

🏢 Empresa: {empresa}
📍 Lugar: {ubicacion}
💰 Sueldo: {sueldo}

🌐 Fuente: {t['fuente']}
🔗 {link}
"""

        enviar(mensaje)
        guardar_enviado(link)
        nuevos += 1

    return nuevos

# ==============================
# 🚀 MAIN LOOP
# ==============================
def main():
    logging.info("🚀 BOT INICIANDO (DEBUG MODO DIOS)")
    enviar("🚀 Bot activo y monitoreando trabajos en Osorno")

    while True:
        try:
            logging.info("🔎 Buscando trabajos...")
            enviados = cargar_enviados()

            trabajos = []
            trabajos += buscar_google()
            trabajos += buscar_yapo()
            trabajos += buscar_facebook()

            logging.info(f"Total encontrados: {len(trabajos)}")

            nuevos = procesar(trabajos, enviados)

            logging.info(f"📤 Nuevos enviados: {nuevos}")
            logging.info("⏳ Esperando siguiente ciclo...\n")

        except Exception as e:
            logging.error(f"💥 ERROR CRÍTICO: {e}")
            enviar(f"⚠️ ERROR BOT: {e}")

        time.sleep(INTERVALO)

# ==============================
# ▶️ START
# ==============================
if __name__ == "__main__":
    main()
