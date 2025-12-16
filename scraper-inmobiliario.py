import os
import time
import smtplib
import requests
import pandas as pd
from bs4 import BeautifulSoup
from email.message import EmailMessage
from urllib.parse import urljoin

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
USE_SLACK = os.getenv("USE_SLACK", "true").lower() == "true"
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
EMAIL_TO = os.getenv("EMAIL_TO")

BASE_URL = "https://www.infocasas.com.uy"
SEARCH_URL_BASE = "https://www.infocasas.com.uy/venta/casas-y-apartamentos/montevideo/buceo-y-en-puerto-buceo-y-en-pocitos-nuevo-y-en-punta-carretas-y-en-pocitos-y-en-parque-batlle/2-dormitorios/2-o-mas-banos/hasta-260000/dolares"

TODAY_CSV = "infocasas_hoy.csv"
HIST_CSV = "infocasas_historico.csv"


def get_page(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; RealEstateBot/1.0; +https://tusitio.com)"
    }
    r = requests.get(url, headers=headers, timeout=20)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")


def parse_listing(card):
    link_tag = card.select_one("a.lc-data")
    relative_url = link_tag["href"] if link_tag and link_tag.has_attr("href") else None
    url = urljoin(BASE_URL, relative_url) if relative_url else None

    price_tag = card.select_one(".property-price-tag p.main-price")
    price = price_tag.get_text(strip=True) if price_tag else None

    gc_tag = card.select_one(".property-price-tag span.commonExpenses")
    gastos_comunes = gc_tag.get_text(strip=True) if gc_tag else None

    loc_tag = card.select_one("strong.lc-location")
    ubicacion = loc_tag.get_text(strip=True) if loc_tag else None

    title_tag = card.select_one("h2.lc-title")
    titulo = title_tag.get_text(strip=True) if title_tag else None

    desc_tag = card.select_one("p.lc-description")
    descripcion = desc_tag.get_text(" ", strip=True) if desc_tag else None

    owner_tag = card.select_one(".lc-owner-name")
    inmobiliaria = owner_tag.get_text(strip=True) if owner_tag else None

    dorms = banos = m2 = None
    for item in card.select(".lc-typologyTag__item"):
        txt = item.get_text(" ", strip=True).lower()
        if "dorm" in txt:
            dorms = txt
        elif "baño" in txt:
            banos = txt
        elif "m²" in txt or "m2" in txt:
            m2 = txt

    anuncio_id = None
    if relative_url and relative_url.strip("/"):
        parts = relative_url.strip("/").split("/")
        if parts and parts[-1].isdigit():
            anuncio_id = parts[-1]

    return {
        "anuncio_id": anuncio_id,
        "url": url,
        "precio": price,
        "gastos_comunes": gastos_comunes,
        "ubicacion": ubicacion,
        "titulo": titulo,
        "descripcion": descripcion,
        "dormitorios": dorms,
        "banos": banos,
        "m2": m2,
        "inmobiliaria": inmobiliaria,
    }


def scrape_all_pages(max_pages=20):
    """Itera /pagina2, /pagina3... hasta que no haya avisos o llegue a max_pages."""
    data = []

    for page in range(1, max_pages + 1):
        if page == 1:
            url = SEARCH_URL_BASE
        else:
            url = f"{SEARCH_URL_BASE}/pagina{page}"

        print(f"Scrapeando página {page}: {url}")
        soup = get_page(url)
        cards = soup.select("div.listingCard")
        if not cards:
            print("Sin cards, se asume fin de resultados.")
            break

        for card in cards:
            item = parse_listing(card)
            if item.get("url"):
                data.append(item)

        time.sleep(2)

    return data


def enviar_email_nuevos(df_nuevos, adjuntar_csv=True, csv_path="infocasas_hoy.csv"):
    if df_nuevos.empty:
        print("No hay propiedades nuevas, no se envía email.")
        return

    if not SMTP_USER or not SMTP_PASS:
        print("SMTP_USER o SMTP_PASS no están definidos; se omite envío de email.")
        return

    filas = [
        f"- {row['titulo']} | {row['precio']} | {row['ubicacion']} | {row['url']}"
        for _, row in df_nuevos.iterrows()
    ]
    body = "Inmuebles nuevos que cumplen los filtros:\n\n" + "\n".join(filas)

    msg = EmailMessage()
    msg["Subject"] = "Nuevos inmuebles InfoCasas (filtros diarios)"
    msg["From"] = SMTP_USER
    msg["To"] = ", ".join([m.strip() for m in EMAIL_TO.split(",")])
    msg.set_content(body)

    if adjuntar_csv and os.path.exists(csv_path):
        with open(csv_path, "rb") as f:
            data = f.read()
        msg.add_attachment(
            data, maintype="text", subtype="csv", filename=os.path.basename(csv_path)
        )

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=20) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        server.send_message(msg)

    print("Email enviado.")

    print(
        "Email enviado con propiedades nuevas y adjunto."
        if adjuntar_csv
        else "Email enviado."
    )
    
def enviar_slack_nuevos(df_nuevos):
    if not USE_SLACK:
        print("Slack desactivado (USE_SLACK=false).")
        return
    if df_nuevos.empty:
        print("No hay propiedades nuevas, no se envía mensaje a Slack.")
        return
    if not SLACK_WEBHOOK_URL:
        print("SLACK_WEBHOOK_URL no está definido.")
        return

    lineas = []
    for _, row in df_nuevos.iterrows():
        lineas.append(
            f"• {row['titulo']} | {row['precio']} | {row['ubicacion']} | {row['url']}"
        )

    texto = "*Nuevos inmuebles InfoCasas (filtros diarios)*\n" + "\n".join(lineas[:20])

    resp = requests.post(SLACK_WEBHOOK_URL, json={"text": texto}, timeout=15)
    if resp.status_code != 200:
        raise RuntimeError(f"Error al enviar a Slack: {resp.status_code} - {resp.text}")
    print("Mensaje enviado a Slack.")

    
def main():
    data = scrape_all_pages(max_pages=20)
    df_hoy = pd.DataFrame(data)
    df_hoy.to_csv(TODAY_CSV, index=False, encoding="utf-8-sig")
    print(f"Guardado CSV de hoy en {TODAY_CSV}")

    if os.path.exists(HIST_CSV):
        df_hist = pd.read_csv(HIST_CSV)
        claves_hoy = set(df_hoy["anuncio_id"].dropna().astype(str))
        claves_hist = set(df_hist["anuncio_id"].dropna().astype(str))
        nuevas_claves = claves_hoy - claves_hist
        df_nuevos = df_hoy[df_hoy["anuncio_id"].astype(str).isin(nuevas_claves)]
        df_hist_total = pd.concat([df_hist, df_nuevos], ignore_index=True)
    else:
        df_nuevos = df_hoy.copy()
        df_hist_total = df_hoy.copy()

    df_hist_total.drop_duplicates(subset=["anuncio_id"], inplace=True)
    df_hist_total.to_csv(HIST_CSV, index=False, encoding="utf-8-sig")
    print(f"Histórico actualizado en {HIST_CSV}")
    
    enviar_slack_nuevos(df_nuevos)
    
    # enviar_email_nuevos(df_nuevos, adjuntar_csv=True, csv_path=TODAY_CSV)

if __name__ == "__main__":
    main()