import requests
from bs4 import BeautifulSoup
import time
import logging
import os
import re

# ==============================
# CONFIG
# ==============================
BUSQUEDA = "empleos osorno chile"
INTERVALO = 600
ENVIADOS_FILE = "enviados.txt"

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

HEADERS = {"User-Agent": "Mozilla/5.0"}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ==============================
# UTILIDADES
# ==============================
def limpiar(texto):
    return re.sub(r"\s+", " ", texto).strip()

def limpiar_titulo(texto):
    basura = ["Postulado", "Vista", "Guardar", "Denunciar", "Ocultar", "Mostrar"]
    for b in basura:
        texto = texto.replace(b, "")
    return limpiar(texto)

def detectar_postulado(texto):
    return "✅ YA POSTULASTE" if "postulado" in texto.lower() else "🆕 NUEVO"

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
# EXTRAER DESCRIPCIÓN COMPLETA
# ==============================
def obtener_descripcion(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")

        # intentar varios selectores comunes
        posibles = [
            ".description",
            ".job-description",
            "#jobDescription",
            "article",
            ".contenido"
        ]

        for sel in posibles:
            bloque = soup.select_one(sel)
            if bloque:
                texto = limpiar(bloque.text)
                if len(texto) > 100:
                    return texto[:800]  # limitar largo

        return "Descripción no disponible"

    except:
        return "No se pudo cargar descripción"

# ==============================
# EXTRAER INFO
# ==============================
def extraer_info(texto):
    sueldo = "No especificado"
    jornada = ""
    
    if "part time" in texto.lower():
        jornada = "Part Time"
    elif "full time" in texto.lower():
        jornada = "Full Time"

    match = re.search(r"\$[\d\.\,]+", texto)
    if match:
        sueldo = match.group(0)

    return sueldo, jornada

# ==============================
# SCRAPERS
# ==============================

def buscar_chiletrabajos():
    trabajos = []
    url = "https://www.chiletrabajos.cl/trabajo/?q=osorno"

    try:
        r = requests.get(url, headers=HEADERS)
        soup = BeautifulSoup(r.text, "html.parser")

        for item in soup.select("article"):
            link_tag = item.find("a")
            if not link_tag:
                continue

            titulo = limpiar(item.text)
            link = link_tag.get("href")

            if len(titulo) < 20:
                continue

            trabajos.append({"titulo": titulo, "link": link, "fuente": "Chiletrabajos"})
    except:
        pass

    return trabajos


def buscar_computrabajo():
    trabajos = []
    url = "https://www.computrabajo.cl/trabajo-de-osorno"

    try:
        r = requests.get(url, headers=HEADERS)
        soup = BeautifulSoup(r.text, "html.parser")

        for item in soup.select("article"):
            link_tag = item.find("a")
            if not link_tag:
                continue

            titulo = limpiar(item.text)
            link = "https://www.computrabajo.cl" + link_tag.get("href")

            if len(titulo) < 20:
                continue

            trabajos.append({"titulo": titulo, "link": link, "fuente": "Computrabajo"})
    except:
        pass

    return trabajos


def buscar_bne():
    trabajos = []
    url = "https://www.bne.cl/ofertas?textoBusqueda=osorno"

    try:
        r = requests.get(url, headers=HEADERS)
        soup = BeautifulSoup(r.text, "html.parser")

        for a in soup.select("a"):
            titulo = limpiar(a.text)
            link = a.get("href")

            if "/oferta/" not in str(link):
                continue

            if len(titulo) < 20:
                continue

            trabajos.append({
                "titulo": titulo,
                "link": "https://www.bne.cl" + link,
                "fuente": "BNE"
            })
    except:
        pass

    return trabajos


# ==============================
# PROCESAR
# ==============================
def procesar(trabajos, enviados):
    nuevos = 0

    for t in trabajos:
        if t["link"] in enviados:
            continue

        titulo_limpio = limpiar_titulo(t["titulo"])
        estado = detectar_postulado(t["titulo"])
        sueldo, jornada = extraer_info(t["titulo"])

        descripcion = obtener_descripcion(t["link"])

        mensaje = f"""🔥 OFERTA DE TRABAJO

📌 {titulo_limpio[:120]}

{estado}

🕒 Jornada: {jornada}
💰 Sueldo: {sueldo}

📝 Descripción:
{descripcion[:500]}

🌐 {t['fuente']}
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
    logging.info("🚀 BOT PRO CON DESCRIPCIÓN COMPLETA")
    enviar("🚀 Bot activo con lectura completa de ofertas")

    while True:
        try:
            enviados = cargar_enviados()

            trabajos = []
            trabajos += buscar_chiletrabajos()
            trabajos += buscar_computrabajo()
            trabajos += buscar_bne()

            logging.info(f"Total encontrados: {len(trabajos)}")

            nuevos = procesar(trabajos, enviados)

            logging.info(f"Nuevos enviados: {nuevos}")

        except Exception as e:
            logging.error(f"ERROR: {e}")

        time.sleep(INTERVALO)


if __name__ == "__main__":
    main()
