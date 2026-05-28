import os
import re
import sys
import time
import random
import logging
from datetime import date
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
BASE_URL = "https://www.infocasas.com.uy"
SEARCH_URL_BASE = (
    "https://www.infocasas.com.uy/venta/casas-y-apartamentos/2-o-mas-dormitorios/2-o-mas-banos/baratos/con-garaje/hasta-260000/dolares/m2-desde-60/edificados"
)

TODAY_CSV = "infocasas_hoy.csv"
HIST_CSV  = "infocasas_historico.csv"

MAX_RETRIES  = 3
BACKOFF_BASE = 4
SLEEP_MIN    = 1.5
SLEEP_MAX    = 4.0


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------
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
                attempt, MAX_RETRIES, url, exc, wait,
            )
            time.sleep(wait)
    log.error("No se pudo obtener %s tras %d intentos. Se omite.", url, MAX_RETRIES)
    return None


# ---------------------------------------------------------------------------
# ID extraction
# ---------------------------------------------------------------------------
_ID_RE = re.compile(r"[-/](\d{5,})(?:[/?#]|$)")


def _extract_id(relative_url: str | None) -> str | None:
    if not relative_url:
        return None
    m = _ID_RE.search(relative_url)
    return m.group(1) if m else None


# ---------------------------------------------------------------------------
# Parseo de una card individual
# ---------------------------------------------------------------------------
def parse_listing(card, scraped_date: str) -> dict:
    link_tag     = card.select_one("a.lc-data")
    relative_url = link_tag["href"] if link_tag and link_tag.has_attr("href") else None
    url          = urljoin(BASE_URL, relative_url) if relative_url else None

    price_tag = card.select_one(".property-price-tag p.main-price")
    gc_tag    = card.select_one(".property-price-tag span.commonExpenses")
    loc_tag   = card.select_one("strong.lc-location")
    title_tag = card.select_one("h2.lc-title")
    desc_tag  = card.select_one("p.lc-description")
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
        "anuncio_id":     _extract_id(relative_url),
        "url":            url,
        "precio":         price_tag.get_text(strip=True)     if price_tag else None,
        "gastos_comunes": gc_tag.get_text(strip=True)        if gc_tag    else None,
        "ubicacion":      loc_tag.get_text(strip=True)       if loc_tag   else None,
        "titulo":         title_tag.get_text(strip=True)     if title_tag else None,
        "descripcion":    desc_tag.get_text(" ", strip=True) if desc_tag  else None,
        "dormitorios":    dorms,
        "banos":          banos,
        "m2":             m2,
        "inmobiliaria":   owner_tag.get_text(strip=True)     if owner_tag else None,
        "fecha_scraping": scraped_date,
    }


# ---------------------------------------------------------------------------
# Scraping de todas las páginas
# ---------------------------------------------------------------------------
def scrape_all_pages(max_pages: int = 20) -> pd.DataFrame:
    today = date.today().isoformat()
    data  = []

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
        log.warning("No se encontró ningún aviso. Verificar si cambió el HTML del sitio.")
        return df

    df["anuncio_id"] = df["anuncio_id"].where(df["anuncio_id"].notna())
    sin_id = df["anuncio_id"].isna().sum()
    if sin_id:
        log.warning("%d avisos sin anuncio_id — no podrán deduplicarse correctamente.", sin_id)

    return df


# ---------------------------------------------------------------------------
# Notificación Slack
# ---------------------------------------------------------------------------
def enviar_slack(df_nuevos: pd.DataFrame) -> None:
    slack_webhook = os.getenv("SLACK_WEBHOOK_URL")

    if not slack_webhook:
        log.warning("SLACK_WEBHOOK_URL no configurada — no se envió notificación Slack.")
        return

    if df_nuevos.empty:
        log.info("Sin propiedades nuevas — no se envía notificación Slack.")
        return

    filas = [
        f"• {row['titulo']} | {row['precio']} | {row['ubicacion']}\n  {row['url']}"
        for _, row in df_nuevos.iterrows()
    ]
    mensaje = (
        f"*🏠 {len(df_nuevos)} propiedad(es) nueva(s) en InfoCasas — {date.today()}*\n\n"
        + "\n\n".join(filas)
    )

    try:
        requests.post(slack_webhook, json={"text": mensaje}, timeout=10)
        log.info("Notificación Slack enviada con %d propiedad(es).", len(df_nuevos))
    except requests.RequestException as exc:
        log.error("Error al enviar a Slack: %s", exc)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    # 1. Scraping
    df_hoy = scrape_all_pages(max_pages=20)

    if df_hoy.empty:
        log.error("Scraping retornó vacío — se aborta para no corromper el histórico.")
        sys.exit(1)

    # 2. Guardar CSV del día
    df_hoy.to_csv(TODAY_CSV, index=False, encoding="utf-8-sig")
    log.info("CSV de hoy guardado en %s (%d filas).", TODAY_CSV, len(df_hoy))

    # 3. Comparar contra histórico para detectar novedades
    ids_hoy = set(df_hoy["anuncio_id"].dropna().astype(str))

    if os.path.exists(HIST_CSV):
        df_hist       = pd.read_csv(HIST_CSV, dtype={"anuncio_id": str})
        ids_hist      = set(df_hist["anuncio_id"].dropna())
        nuevas_claves = ids_hoy - ids_hist
        df_nuevos     = df_hoy[df_hoy["anuncio_id"].astype(str).isin(nuevas_claves)].copy()
        df_hist_total = pd.concat([df_hist, df_nuevos], ignore_index=True)
    else:
        log.info("No existe histórico previo — todas las propiedades de hoy son 'nuevas'.")
        df_nuevos     = df_hoy.copy()
        df_hist_total = df_hoy.copy()

    # 4. Deduplicar histórico y guardar
    df_hist_total.drop_duplicates(subset=["anuncio_id"], keep="first", inplace=True)
    df_hist_total.to_csv(HIST_CSV, index=False, encoding="utf-8-sig")
    log.info(
        "Histórico actualizado en %s (%d filas totales, %d nuevas hoy).",
        HIST_CSV, len(df_hist_total), len(df_nuevos),
    )

    # 5. Notificación Slack
    enviar_slack(df_nuevos)


if __name__ == "__main__":
    main()
