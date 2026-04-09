import requests
from bs4 import BeautifulSoup
import time
import logging
import os
import re
from datetime import datetime, timedelta

# ==============================
# CONFIG
# ==============================
INTERVALO = 600          # segundos entre ciclos
MAX_ENVIADOS = 2000      # límite de links guardados (rota el archivo)
TIMEOUT = 10             # segundos por request HTTP
DELAY_ENTRE_REQUESTS = 1.5  # pausa entre fetches para no ser bloqueado

ENVIADOS_FILE = "enviados.txt"

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("bot_empleos.log", encoding="utf-8"),
    ]
)
log = logging.getLogger(__name__)


# ==============================
# UTILIDADES DE TEXTO
# ==============================

def limpiar(texto: str) -> str:
    return re.sub(r"\s+", " ", texto).strip()

BASURA_TITULO = {"Postulado", "Vista", "Guardar", "Denunciar", "Ocultar", "Mostrar"}

def limpiar_titulo(texto: str) -> str:
    for b in BASURA_TITULO:
        texto = texto.replace(b, "")
    return limpiar(texto)

def extraer_info(texto: str) -> tuple[str, str]:
    texto_lower = texto.lower()
    if "part time" in texto_lower:
        jornada = "Part Time"
    elif "full time" in texto_lower:
        jornada = "Full Time"
    else:
        jornada = "No especificada"

    match = re.search(r"\$[\d\.\,]+", texto)
    sueldo = match.group(0) if match else "No especificado"

    return sueldo, jornada

def escape_telegram(texto: str) -> str:
    """Escapa caracteres que pueden romper mensajes de Telegram."""
    # Para parse_mode=None (texto plano) no es necesario, pero protege contra
    # ampersands u otros chars que algunos clientes muestran raro
    return texto.replace("&", "&amp;")


# ==============================
# DUPLICADOS (con rotación)
# ==============================

def cargar_enviados() -> set:
    try:
        with open(ENVIADOS_FILE, "r", encoding="utf-8") as f:
            return set(f.read().splitlines())
    except FileNotFoundError:
        return set()

def guardar_enviado(link: str, enviados: set) -> None:
    enviados.add(link)
    # Rotar archivo si supera el límite
    lines = list(enviados)
    if len(lines) > MAX_ENVIADOS:
        lines = lines[-MAX_ENVIADOS:]
        enviados.clear()
        enviados.update(lines)
    with open(ENVIADOS_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


# ==============================
# TELEGRAM
# ==============================

def enviar(msg: str, reintentos: int = 3) -> bool:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.error("Faltan variables de entorno TELEGRAM_TOKEN o TELEGRAM_CHAT_ID")
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": msg}

    for intento in range(1, reintentos + 1):
        try:
            r = requests.post(url, data=payload, timeout=TIMEOUT)
            if r.status_code == 200:
                return True
            log.warning(f"Telegram devolvió {r.status_code} (intento {intento}): {r.text[:200]}")
        except requests.RequestException as e:
            log.warning(f"Error de red enviando a Telegram (intento {intento}): {e}")
        time.sleep(2 * intento)

    log.error(f"No se pudo enviar mensaje a Telegram tras {reintentos} intentos")
    return False


# ==============================
# SCRAPING CON REINTENTOS
# ==============================

def get_soup(url: str, reintentos: int = 2) -> BeautifulSoup | None:
    for intento in range(1, reintentos + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            r.raise_for_status()
            return BeautifulSoup(r.text, "html.parser")
        except requests.RequestException as e:
            log.warning(f"Error GET {url} (intento {intento}): {e}")
            time.sleep(DELAY_ENTRE_REQUESTS * intento)
    return None


def obtener_descripcion(url: str) -> str:
    time.sleep(DELAY_ENTRE_REQUESTS)  # rate limiting
    soup = get_soup(url)
    if not soup:
        return "No se pudo cargar descripción"

    selectores = [
        ".description", ".job-description", "#jobDescription",
        "article", ".contenido", "[class*='description']", "main"
    ]
    for sel in selectores:
        bloque = soup.select_one(sel)
        if bloque:
            texto = limpiar(bloque.get_text(separator=" "))
            if len(texto) > 100:
                return texto[:600]

    # fallback: body completo truncado
    body = soup.find("body")
    if body:
        texto = limpiar(body.get_text(separator=" "))
        if len(texto) > 100:
            return texto[:400]

    return "Descripción no disponible"


# ==============================
# SCRAPERS
# ==============================

def buscar_chiletrabajos() -> list[dict]:
    trabajos = []
    soup = get_soup("https://www.chiletrabajos.cl/trabajo/?q=osorno")
    if not soup:
        log.warning("Chiletrabajos: no se pudo conectar")
        return trabajos

    for item in soup.select("article"):
        link_tag = item.find("a", href=True)
        if not link_tag:
            continue
        titulo = limpiar(item.get_text(separator=" "))
        if len(titulo) < 20:
            continue
        href = link_tag["href"]
        if not href.startswith("http"):
            href = "https://www.chiletrabajos.cl" + href
        trabajos.append({"titulo": titulo, "link": href, "fuente": "Chiletrabajos"})

    log.info(f"Chiletrabajos: {len(trabajos)} ofertas encontradas")
    return trabajos


def buscar_computrabajo() -> list[dict]:
    trabajos = []
    soup = get_soup("https://www.computrabajo.cl/trabajo-de-osorno")
    if not soup:
        log.warning("Computrabajo: no se pudo conectar")
        return trabajos

    for item in soup.select("article"):
        link_tag = item.find("a", href=True)
        if not link_tag:
            continue
        titulo = limpiar(item.get_text(separator=" "))
        if len(titulo) < 20:
            continue
        href = link_tag["href"]
        if not href.startswith("http"):
            href = "https://www.computrabajo.cl" + href
        trabajos.append({"titulo": titulo, "link": href, "fuente": "Computrabajo"})

    log.info(f"Computrabajo: {len(trabajos)} ofertas encontradas")
    return trabajos


def buscar_bne() -> list[dict]:
    trabajos = []
    soup = get_soup("https://www.bne.cl/ofertas?textoBusqueda=osorno")
    if not soup:
        log.warning("BNE: no se pudo conectar")
        return trabajos

    for a in soup.select("a[href]"):
        if "/oferta/" not in a["href"]:
            continue
        titulo = limpiar(a.get_text(separator=" "))
        if len(titulo) < 20:
            continue
        trabajos.append({
            "titulo": titulo,
            "link": "https://www.bne.cl" + a["href"],
            "fuente": "BNE"
        })

    log.info(f"BNE: {len(trabajos)} ofertas encontradas")
    return trabajos


# ==============================
# PROCESAR Y ENVIAR
# ==============================

def procesar(trabajos: list[dict], enviados: set) -> int:
    nuevos = 0

    for t in trabajos:
        link = t["link"]
        if link in enviados:
            continue

        titulo_limpio = limpiar_titulo(t["titulo"])[:120]
        sueldo, jornada = extraer_info(t["titulo"])
        descripcion = obtener_descripcion(link)

        mensaje = (
            f"🔥 OFERTA DE TRABAJO\n\n"
            f"📌 {escape_telegram(titulo_limpio)}\n\n"
            f"🕒 Jornada: {jornada}\n"
            f"💰 Sueldo: {sueldo}\n\n"
            f"📝 Descripción:\n{escape_telegram(descripcion[:500])}\n\n"
            f"🌐 {t['fuente']}\n"
            f"🔗 {link}"
        )

        if enviar(mensaje):
            guardar_enviado(link, enviados)
            nuevos += 1
            log.info(f"Enviado: {titulo_limpio[:60]}")
        else:
            log.error(f"No se pudo enviar oferta: {link}")

    return nuevos


# ==============================
# MAIN
# ==============================

def main() -> None:
    log.info("Bot de empleos iniciado")

    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.error("TELEGRAM_TOKEN y TELEGRAM_CHAT_ID son requeridos. Saliendo.")
        return

    enviar("🚀 Bot de empleos Osorno activo")

    while True:
        try:
            inicio = time.time()
            enviados = cargar_enviados()

            trabajos: list[dict] = []
            trabajos += buscar_chiletrabajos()
            trabajos += buscar_computrabajo()
            trabajos += buscar_bne()

            # Eliminar duplicados por link dentro del mismo ciclo
            vistos = set()
            trabajos_unicos = []
            for t in trabajos:
                if t["link"] not in vistos:
                    vistos.add(t["link"])
                    trabajos_unicos.append(t)

            log.info(f"Total encontrados: {len(trabajos_unicos)} (sin duplicates)")

            nuevos = procesar(trabajos_unicos, enviados)
            log.info(f"Nuevos enviados: {nuevos}")

            elapsed = time.time() - inicio
            espera = max(0, INTERVALO - elapsed)
            log.info(f"Ciclo terminado en {elapsed:.1f}s. Próxima búsqueda en {espera:.0f}s")
            time.sleep(espera)

        except KeyboardInterrupt:
            log.info("Bot detenido por el usuario")
            break
        except Exception as e:
            log.exception(f"Error inesperado en el ciclo principal: {e}")
            time.sleep(60)  # esperar antes de reintentar


if __name__ == "__main__":
    main()
