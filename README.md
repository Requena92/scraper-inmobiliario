# 🏠 Scraper Inmobiliario — Montevideo

Sistema automatizado de recolección de avisos de apartamentos en Montevideo desde [InfoCasas](https://www.infocasas.com.uy). Corre diariamente vía GitHub Actions, detecta propiedades nuevas y envía notificaciones por email.

---

## ¿Qué hace?

- Scrapea hasta 20 páginas de resultados filtrados por zona, dormitorios, baños y precio máximo
- Guarda los avisos del día en un CSV (`infocasas_hoy.csv`)
- Compara contra un histórico acumulado (`infocasas_historico.csv`) para detectar novedades
- Envía un email con las propiedades nuevas, adjuntando el CSV del día
- Ejecuta automáticamente todos los días a las 12:00 UTC

---

## Filtros aplicados

| Parámetro | Valor |
|---|---|
| Ciudad | Montevideo |
| Zonas | Buceo, Puerto Buceo, Pocitos Nuevo, Punta Carretas, Pocitos, Parque Batlle |
| Dormitorios | 2 |
| Baños | 2 o más |
| Precio máximo | USD 260.000 |

Para cambiar los filtros, modificar la variable `SEARCH_URL_BASE` en `scraper-inmobiliario.py`.

---

## Estructura del proyecto

```
scraper-inmobiliario/
├── scraper-inmobiliario.py     # Script principal
├── requirements.txt            # Dependencias Python
└── .github/
    └── workflows/
        └── daily-scraper.yml   # Workflow de GitHub Actions
```

---

## Datos recolectados por aviso

Cada propiedad se guarda con los siguientes campos:

| Campo | Descripción |
|---|---|
| `anuncio_id` | ID único del aviso en InfoCasas |
| `url` | Link directo al aviso |
| `precio` | Precio publicado |
| `gastos_comunes` | Gastos comunes (si aplica) |
| `ubicacion` | Barrio / dirección |
| `titulo` | Título del aviso |
| `descripcion` | Descripción breve |
| `dormitorios` | Cantidad de dormitorios |
| `banos` | Cantidad de baños |
| `m2` | Superficie |
| `inmobiliaria` | Nombre de la inmobiliaria o propietario |
| `fecha_scraping` | Fecha en que fue recolectado |

---

## Configuración

### Variables de entorno requeridas

El script requiere tres variables para el envío de emails. En local se pueden definir en un archivo `.env`. En GitHub Actions se configuran como secrets.

| Variable | Descripción |
|---|---|
| `SMTP_USER` | Email desde el que se envían las notificaciones (Gmail) |
| `SMTP_PASS` | Contraseña de aplicación de Gmail (ver abajo) |
| `EMAIL_TO` | Email destinatario de las notificaciones |

> **⚠️ Importante:** `SMTP_PASS` no es tu contraseña de Gmail normal. Es una *App Password* que se genera en [myaccount.google.com](https://myaccount.google.com) → Seguridad → Verificación en dos pasos → Contraseñas de aplicación.

### Variable opcional

| Variable | Descripción |
|---|---|
| `SLACK_WEBHOOK_URL` | Webhook de Slack para notificaciones adicionales |

---

## Configuración en GitHub Actions

1. Ir al repositorio → **Settings → Secrets and variables → Actions**
2. Crear los siguientes secrets con **New repository secret**:

```
SMTP_USER      → tu-email@gmail.com
SMTP_PASS      → contraseña de aplicación de Gmail
EMAIL_TO       → destinatario@email.com
SLACK_WEBHOOK_URL → https://hooks.slack.com/services/...  (opcional)
```

El workflow se ejecuta automáticamente cada día a las 12:00 UTC. También puede correrse manualmente desde la pestaña **Actions → Run scraper daily → Run workflow**.

---

## Ejecución local

```bash
# Clonar el repo
git clone https://github.com/Requena92/scraper-inmobiliario.git
cd scraper-inmobiliario

# Instalar dependencias
pip install -r requirements.txt

# Crear archivo .env con las variables
echo "SMTP_USER=tu-email@gmail.com" >> .env
echo "SMTP_PASS=tu-app-password" >> .env
echo "EMAIL_TO=destinatario@email.com" >> .env

# Correr el scraper
python scraper-inmobiliario.py
```

---

## Dependencias

- `requests` — peticiones HTTP
- `beautifulsoup4` — parsing de HTML
- `pandas` — manejo de datos y CSVs
- `python-dotenv` — carga de variables de entorno desde `.env`

---

## Notas técnicas

- El scraper incluye reintentos automáticos (hasta 3 intentos con backoff) ante errores HTTP
- Se agrega un delay aleatorio entre páginas para no sobrecargar el servidor
- La deduplicación se basa en el `anuncio_id` extraído de la URL de cada aviso
- El histórico acumulado nunca se sobreescribe, solo se le agregan filas nuevas
