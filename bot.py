import requests
from bs4 import BeautifulSoup
import time
import logging
import os
import re

# ==============================
# ⚙️ CONFIG
# ==============================
BUSQUEDA = "empleos osorno chile"
INTERVALO = 600
ENVIADOS_FILE = "enviados.txt"

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

# ==============================
# LOGS
# ==============================
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ==============================
# UTILIDADES
# ==============================
def limpiar(texto):
    return re.sub(r"\s+", " ", texto).strip()

def es_basura(texto):
    texto = texto.lower()
    basura = ["términos", "privacidad", "login", "registr", "contacto", "cookies"]
    return any(p in texto for p in basura)

# ==============================
# DUPLICADOS
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
# TELEGRAM
# ==============================
def enviar(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logging.error("Faltan variables de Telegram")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"

    try:
        requests.post(url, data={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": msg
        })
    except Exception as e:
        logging.error(f"Error Telegram: {e}")

# ==============================
# EXTRAER INFO
# ==============================
def extraer_info(texto):
    sueldo = "No especificado"
    empresa = "No especificada"

    match = re.search(r"\$[\d\.\,]+", texto)
    if match:
        sueldo = match.group(0)

    if "empresa" in texto.lower():
        empresa = "Indicada en aviso"

    return empresa, sueldo

# ==============================
# SCRAPERS REALES
# ==============================

# 🟢 CHILETRABAJOS
def buscar_chiletrabajos():
    trabajos = []
    url = "https://www.chiletrabajos.cl/trabajo/?q=osorno"

    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")

        for item in soup.select("article"):
            titulo = limpiar(item.text)
            link_tag = item.find("a")

            if not link_tag:
                continue

            link = link_tag.get("href")

            if not titulo or len(titulo) < 15 or es_basura(titulo):
                continue

            trabajos.append({
                "titulo": titulo,
                "link": link,
                "fuente": "Chiletrabajos"
            })

    except Exception as e:
        logging.error(f"Chiletrabajos error: {e}")

    logging.info(f"Chiletrabajos: {len(trabajos)}")
    return trabajos


# 🟢 COMPUTRABAJO
def buscar_computrabajo():
    trabajos = []
    url = "https://www.computrabajo.cl/trabajo-de-osorno"

    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")

        for item in soup.select("article"):
            titulo = limpiar(item.text)
            link_tag = item.find("a")

            if not link_tag:
                continue

            link = "https://www.computrabajo.cl" + link_tag.get("href")

            if not titulo or len(titulo) < 15 or es_basura(titulo):
                continue

            trabajos.append({
                "titulo": titulo,
                "link": link,
                "fuente": "Computrabajo"
            })

    except Exception as e:
        logging.error(f"Computrabajo error: {e}")

    logging.info(f"Computrabajo: {len(trabajos)}")
    return trabajos


# 🟢 BNE
def buscar_bne():
    trabajos = []
    url = "https://www.bne.cl/ofertas?textoBusqueda=osorno"

    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")

        for item in soup.select("a"):
            titulo = limpiar(item.text)
            link = item.get("href")

            if not titulo or len(titulo) < 20:
                continue

            if "/oferta/" not in str(link):
                continue

            link = "https://www.bne.cl" + link

            trabajos.append({
                "titulo": titulo,
                "link": link,
                "fuente": "BNE"
            })

    except Exception as e:
        logging.error(f"BNE error: {e}")

    logging.info(f"BNE: {len(trabajos)}")
    return trabajos


# 🟡 YAPO (filtrado)
def buscar_yapo():
    trabajos = []
    url = "https://www.yapo.cl/region_de_los_lagos/empleos"

    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")

        for item in soup.select("li"):
            titulo = limpiar(item.text)
            link_tag = item.find("a")

            if not link_tag:
                continue

            link = link_tag.get("href")

            if not titulo or len(titulo) < 20 or es_basura(titulo):
                continue

            if "/empleos" not in str(link):
                continue

            link = "https://www.yapo.cl" + link

            trabajos.append({
                "titulo": titulo,
                "link": link,
                "fuente": "Yapo"
            })

    except Exception as e:
        logging.error(f"Yapo error: {e}")

    logging.info(f"Yapo: {len(trabajos)}")
    return trabajos


# ==============================
# PROCESAR
# ==============================
def procesar(trabajos, enviados):
    nuevos = 0

    for t in trabajos:
        if t["link"] in enviados:
            continue

        empresa, sueldo = extraer_info(t["titulo"])

        mensaje = f"""🔥 NUEVA OFERTA

📌 {t['titulo']}

🏢 Empresa: {empresa}
💰 Sueldo: {sueldo}

🌐 Fuente: {t['fuente']}
🔗 {t['link']}
"""

        enviar(mensaje)
        guardar_enviado(t["link"])
        nuevos += 1

    return nuevos


# ==============================
# MAIN
# ==============================
def main():
    logging.info("🚀 BOT PROFESIONAL INICIADO")
    enviar("🚀 Bot activo buscando trabajos reales en Osorno")

    while True:
        try:
            enviados = cargar_enviados()

            trabajos = []
            trabajos += buscar_chiletrabajos()
            trabajos += buscar_computrabajo()
            trabajos += buscar_bne()
            trabajos += buscar_yapo()

            logging.info(f"Total encontrados: {len(trabajos)}")

            nuevos = procesar(trabajos, enviados)

            logging.info(f"Nuevos enviados: {nuevos}")

        except Exception as e:
            logging.error(f"ERROR: {e}")

        time.sleep(INTERVALO)


if __name__ == "__main__":
    main()
