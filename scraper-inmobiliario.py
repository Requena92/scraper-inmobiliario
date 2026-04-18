import os
import re
import sys
import time
import random
import logging
import smtplib
from datetime import date
from email.message import EmailMessage
from urllib.parse import urljoin
import requests
import pandas as pd
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

BASE_URL = "https://www.infocasas.com.uy"
SEARCH_URL_BASE = "https://www.infocasas.com.uy/venta/casas-y-apartamentos/montevideo/buceo-y-en-puerto-buceo-y-en-pocitos-nuevo-y-en-punta-carretas-y-en-pocitos-y-en-parque-batlle/2-dormitorios/2-o-mas-banos/hasta-260000/dolares"
TODAY_CSV = "infocasas_hoy.csv"
HIST_CSV = "infocasas_historico.csv"

MAX_RETRIES = 3
BACKOFF_BASE = 4  # seconds  (backoff = BACKOFF_BASE * attempt)
SLEEP_MIN = 1.5
SLEEP_MAX = 4.0


def _require_env() -> dict:
    return {
        "SMTP_USER": os.getenv("SMTP_USER", ""),
        "SMTP_PASS": os.getenv("SMTP_PASS", ""),
        "EMAIL_TO": os.getenv("EMAIL_TO", ""),
    }


_SESSION = requests.Session()
_SESSION.headers["User-Agent"] = (
    "Mozilla/5.0 (compatible; RealEstateBot/1.0; +https://tusitio.com)"
)


def get_page(url: str) -> BeautifulSoup | None:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = _SESSION.get(url, timeout=20)
            r.raise_for_status()
            return BeautifulSoup(r.text, "html.parser")
        except requests.RequestException as exc:
            wait = BACKOFF_BASE * attempt
            log.warning(
                "Intento %d/%d falló para %s — %s. Reintentando en %ds…",
                attempt,
                MAX_RETRIES,
                url,
                exc,
                wait,
            )
            time.sleep(wait)
    log.error("No se pudo obtener %s tras %d intentos. Se omite.", url, MAX_RETRIES)
    return None


_ID_RE = re.compile(r"[-/](\d{5,})(?:[/?#]|$)")


def _extract_id(relative_url: str | None) -> str | None:
    if not relative_url:
        return None
    m = _ID_RE.search(relative_url)
    return m.group(1) if m else None


def parse_listing(card, scraped_date: str) -> dict:
    link_tag = card.select_one("a.lc-data")
    relative_url = link_tag["href"] if link_tag and link_tag.has_attr("href") else None
    url = urljoin(BASE_URL, relative_url) if relative_url else None

    price_tag = card.select_one(".property-price-tag p.main-price")
    gc_tag = card.select_one(".property-price-tag span.commonExpenses")
    loc_tag = card.select_one("strong.lc-location")
    title_tag = card.select_one("h2.lc-title")
    desc_tag = card.select_one("p.lc-description")
    owner_tag = card.select_one(".lc-owner-name")

    dorms = banos = m2 = None
    for item in card.select(".lc-typologyTag__item"):
        txt = item.get_text(" ", strip=True).lower()
        if "dorm" in txt:
            dorms = txt
        elif "baño" in txt:
            banos = txt
        elif "m²" in txt or "m2" in txt:
            m2 = txt

    return {
        "anuncio_id": _extract_id(relative_url),
        "url": url,
        "precio": price_tag.get_text(strip=True) if price_tag else None,
        "gastos_comunes": gc_tag.get_text(strip=True) if gc_tag else None,
        "ubicacion": loc_tag.get_text(strip=True) if loc_tag else None,
        "titulo": title_tag.get_text(strip=True) if title_tag else None,
        "descripcion": desc_tag.get_text(" ", strip=True) if desc_tag else None,
        "dormitorios": dorms,
        "banos": banos,
        "m2": m2,
        "inmobiliaria": owner_tag.get_text(strip=True) if owner_tag else None,
        "fecha_scraping": scraped_date,
    }


def scrape_all_pages(max_pages: int = 20) -> pd.DataFrame:
    today = date.today().isoformat()
    data = []

    for page in range(1, max_pages + 1):
        url = SEARCH_URL_BASE if page == 1 else f"{SEARCH_URL_BASE}/pagina{page}"
        log.info("Scrapeando página %d: %s", page, url)

        soup = get_page(url)
        if soup is None:
            log.warning("Página %d omitida por error HTTP.", page)
            continue

        cards = soup.select("div.listingCard")
        if not cards:
            log.info("Página %d sin cards — fin de resultados.", page)
            break

        for card in cards:
            item = parse_listing(card, today)
            if item.get("url"):
                data.append(item)

        log.info("  → %d avisos acumulados hasta ahora.", len(data))
        time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))

    df = pd.DataFrame(data)

    if df.empty:
        log.warning(
            "No se encontró ningún aviso. Verificar si cambió el HTML del sitio."
        )
        return df

    # Normalizar anuncio_id: descartar "None" string y duplicados reales
    df["anuncio_id"] = df["anuncio_id"].where(df["anuncio_id"].notna())
    sin_id = df["anuncio_id"].isna().sum()
    if sin_id:
        log.warning(
            "%d avisos sin anuncio_id — no podrán deduplicarse correctamente.", sin_id
        )

    return df


def enviar_email_nuevos(
    df_nuevos: pd.DataFrame,
    env: dict,
    adjuntar_csv: bool = True,
    csv_path: str = TODAY_CSV,
) -> None:
    if df_nuevos.empty:
        log.info("Sin propiedades nuevas — no se envía email.")
        return

    filas = [
        f"- {row['titulo']} | {row['precio']} | {row['ubicacion']} | {row['url']}"
        for _, row in df_nuevos.iterrows()
    ]
    body = (
        f"Inmuebles nuevos que cumplen los filtros ({date.today()}):\n\n"
        + "\n".join(filas)
    )

    msg = EmailMessage()
    msg["Subject"] = f"Nuevos inmuebles InfoCasas — {date.today()}"
    msg["From"] = env["SMTP_USER"]
    msg["To"] = env["EMAIL_TO"]
    msg.set_content(body)

    if adjuntar_csv and os.path.exists(csv_path):
        with open(csv_path, "rb") as f:
            msg.add_attachment(
                f.read(),
                maintype="text",
                subtype="csv",
                filename=os.path.basename(csv_path),
            )

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(env["SMTP_USER"], env["SMTP_PASS"])
            server.send_message(msg)
        log.info(
            "Email enviado a %s con %d propiedad(es) nueva(s).",
            env["EMAIL_TO"],
            len(df_nuevos),
        )
    except smtplib.SMTPException as exc:
        log.error("Error al enviar email: %s", exc)


def main() -> None:
    env = _require_env()

    # 1. Scraping
    df_hoy = scrape_all_pages(max_pages=20)

    if df_hoy.empty:
        log.error("Scraping retornó vacío — se aborta para no corromper el histórico.")
        sys.exit(1)

    # 2. Guardar CSV del día (solo después de confirmar que hay datos)
    df_hoy.to_csv(TODAY_CSV, index=False, encoding="utf-8-sig")
    log.info("CSV de hoy guardado en %s (%d filas).", TODAY_CSV, len(df_hoy))

    # 3. Comparar contra histórico para detectar novedades
    ids_hoy = set(df_hoy["anuncio_id"].dropna().astype(str))

    if os.path.exists(HIST_CSV):
        df_hist = pd.read_csv(HIST_CSV, dtype={"anuncio_id": str})
        ids_hist = set(df_hist["anuncio_id"].dropna())

        nuevas_claves = ids_hoy - ids_hist
        df_nuevos = df_hoy[df_hoy["anuncio_id"].astype(str).isin(nuevas_claves)].copy()

        # Agregar solo las filas genuinamente nuevas al histórico
        df_hist_total = pd.concat([df_hist, df_nuevos], ignore_index=True)
    else:
        log.info(
            "No existe histórico previo — todas las propiedades de hoy son 'nuevas'."
        )
        df_nuevos = df_hoy.copy()
        df_hist_total = df_hoy.copy()

    # 4. Deduplicar histórico (mantener la primera aparición)
    df_hist_total.drop_duplicates(subset=["anuncio_id"], keep="first", inplace=True)
    df_hist_total.to_csv(HIST_CSV, index=False, encoding="utf-8-sig")
    log.info(
        "Histórico actualizado en %s (%d filas totales, %d nuevas hoy).",
        HIST_CSV,
        len(df_hist_total),
        len(df_nuevos),
    )

    # 5. Notificación
    enviar_email_nuevos(df_nuevos, env, adjuntar_csv=True, csv_path=TODAY_CSV)


if __name__ == "__main__":
    main()
