# Terna API Documentation (Source Doc Summary)

Source name: **terna**.  
Italian electricity system data: Load, Generation, Transmission, and related domains.  
This document covers the initial table scope: `total_load`, `actual_generation`, `renewable_generation`, `physical_foreign_flow`.

---

## Base URL

- **Data APIs (Load, Generation):** `https://api.terna.it`
- **Transmission (Physical Foreign Flow):** Documentation uses `https://api-intra.servizi.prv` for the transmission API. The path is `/transmission/v2.0/physical-foreign-flow`. For public/sandbox, confirm with Terna whether transmission is also served under `https://api.terna.it` (e.g. `https://api.terna.it/transmission/v2.0/physical-foreign-flow`).

---

## Authorization

### OAuth 2.0 (Load and Generation APIs)

Terna Public APIs use **OAuth 2.0** with the **Client Credentials** grant. The connector does **not** run user-facing OAuth flows; it stores `client_id` and `client_secret` and obtains access tokens at runtime.

**Token endpoint (preferred in current docs):**

- `POST https://api.terna.it/public-api/access-token`

**Legacy token endpoint (also documented):**

- `POST https://api.terna.it/transparency/oauth/accessToken`

**Request:**

- **Method:** POST  
- **Content-Type:** `application/x-www-form-urlencoded`  
- **Body parameters:**

| Parameter       | Required | Description                |
|----------------|----------|----------------------------|
| `grant_type`   | Yes      | `client_credentials`       |
| `client_id`    | Yes      | Application key (API Key)  |
| `client_secret`| Yes      | Application secret         |

**Response:**

| Field          | Description                          |
|----------------|--------------------------------------|
| `access_token` | Bearer token for API calls           |
| `token_type`   | `bearer`                             |
| `expires_in`   | Token lifetime in seconds (e.g. 300)|

**Using the token:**

- **Header:** `Authorization: Bearer <access_token>`
- Optional: `Accept: application/json`
- Optional: `businessID` (UUID) for support/tracing

**Example – get token:**

```bash
curl --location 'https://api.terna.it/public-api/access-token' \
  --header 'Content-Type: application/x-www-form-urlencoded' \
  --data-urlencode 'grant_type=client_credentials' \
  --data-urlencode 'client_id=YOUR_CLIENT_ID' \
  --data-urlencode 'client_secret=YOUR_CLIENT_SECRET'
```

**Example – call API with token:**

```bash
curl --location 'https://api.terna.it/load/v2.0/total-load?dateFrom=10/05/2022&dateTo=11/05/2022' \
  --header 'Authorization: Bearer <access_token>'
```

Credentials are obtained by [registering](https://developer.terna.it/member/register) and [creating an application](https://developer.terna.it/docs/read/How_to_register_an_application) on the Terna developer portal.

### API Key (Physical Foreign Flow only)

The **Physical Foreign Flow** endpoint is documented with **API Key** auth instead of OAuth:

- **Header:** `x-api-key: <token>`
- Optional: `businessID`, `Accept: application/json`

If Terna exposes this endpoint on the public base URL with OAuth, the connector can use the same OAuth token; otherwise it should support an API key for this resource.

---

## Endpoint list (scoped tables)

| Table / resource           | HTTP method | Path (relative to base)                    | Auth        | Base URL (per docs)        |
|----------------------------|-------------|-------------------------------------------|-------------|----------------------------|
| total_load                 | GET         | `/load/v2.0/total-load`                   | OAuth2 Bearer | https://api.terna.it       |
| actual_generation          | GET         | `/generation/v2.0/actual-generation`      | OAuth2 Bearer | https://api.terna.it       |
| renewable_generation       | GET         | `/generation/v2.0/renewable-generation`    | OAuth2 Bearer | https://api.terna.it       |
| physical_foreign_flow      | GET         | `/transmission/v2.0/physical-foreign-flow`| x-api-key   | https://api-intra.servizi.prv |

---

## Request/response and parameters by table

### 1. total_load

- **Description:** Trend of Italian electricity system total demand (and forecast).
- **Method:** GET  
- **Path:** `https://api.terna.it/load/v2.0/total-load`

**Query parameters:**

| Parameter     | Type   | Required | Description                    | Example   |
|---------------|--------|----------|--------------------------------|-----------|
| dateFrom      | string | Yes      | Start date (dd/mm/yyyy)        | 15/2/2021 |
| dateTo        | string | Yes      | End date (dd/mm/yyyy)          | 15/2/2021 |
| biddingZone   | string | No (0..n)| Bidding zone(s); repeat for multiple | South, NORD |

**Bidding zones (v2.0):** `North`, `Centre-North`, `South`, `Centre-South`, `Sardinia`, `Sicily`, `Calabria`, `Italy`.  
Invalid values can result in an empty body without an error message.

**Response shape:**

- `result`: `{ "message": string, "status": "Completed" }`
- `total_load`: array of objects:
  - `date` (string, yyyy-mm-dd hh:mm:ss)
  - `date_tz` (string, e.g. Europe/Rome)
  - `date_offset` (string, e.g. +02:00)
  - `total_load_MW` (string)
  - `forecast_total_load_MW` (string)
  - `bidding_zone` (string)

**Example request:**

```bash
curl --location --request GET 'https://api.terna.it/load/v2.0/total-load?dateFrom=10/05/2022&dateTo=10/06/2022&biddingZone=Calabria&biddingZone=Sicily' \
  --header 'Authorization: Bearer <token>'
```

---

### 2. actual_generation

- **Description:** National net hourly generation by primary source (including self-consumption).
- **Method:** GET  
- **Path:** `https://api.terna.it/generation/v2.0/actual-generation`

**Query parameters:**

| Parameter | Type   | Required | Description             | Example |
|-----------|--------|----------|-------------------------|---------|
| dateFrom  | string | Yes      | Start date (dd/mm/yyyy) | 2/3/2019 |
| dateTo    | string | Yes      | End date (dd/mm/yyyy)   | 20/3/2019 |
| type      | string | No (0..n)| Primary source type(s)  | Wind, Hydro |

**Type list:** `Thermal`, `Wind`, `Geothermal`, `Photovoltaic`, `Self-consumption`, `Hydro`.  
Invalid values can result in an empty body or “No results found”.

**Response shape:**

- `result`: `{ "message": string, "status": "Completed" }`
- `actual_generation`: array of objects:
  - `date`, `date_tz`, `date_offset` (as above)
  - `actual_generation_GWh` (string)
  - `primary_source` (string)

**Example request:**

```bash
curl --location --request GET 'https://api.terna.it/generation/v2.0/actual-generation?dateFrom=23/04/2022&dateTo=23/04/2022' \
  --header 'Authorization: Bearer <token>'
```

---

### 3. renewable_generation

- **Description:** Hourly production from renewable sources (hydro, geothermal, biomass, wind, solar).
- **Method:** GET  
- **Path:** `https://api.terna.it/generation/v2.0/renewable-generation`

**Query parameters:** Same as actual_generation: `dateFrom`, `dateTo`, `type` (optional, same allowed values).

**Response shape:**

- `result`: `{ "message": string, "status": "Completed" }`
- `renewable_generation`: array of objects:
  - `date`, `date_tz`, `date_offset`
  - `renewable_generation_GWh` (string)
  - `energy_source` (string)

**Example request:**

```bash
curl --location --request GET 'https://api.terna.it/generation/v2.0/renewable-generation?dateFrom=23/04/2022&dateTo=23/04/2022' \
  --header 'Authorization: Bearer <token>'
```

---

### 4. physical_foreign_flow

- **Description:** Physical exchange of electricity between Italy and neighbouring TSOs (Corsica, Malta, etc.).
- **Method:** GET  
- **Path (per docs):** `https://api-intra.servizi.prv/transmission/v2.0/physical-foreign-flow`  
  (Note: doc once had typo `physical-foreig-flow`; correct path is `physical-foreign-flow`.)

**Query parameters:**

| Parameter | Type   | Required | Description             | Example   |
|-----------|--------|----------|-------------------------|-----------|
| dateFrom  | string | Yes      | Start date (dd/mm/yyyy) | 15/2/2021 |
| dateTo    | string | Yes      | End date (dd/mm/yyyy)   | 15/2/2021 |

**Request header:** `x-api-key: <token>` (and optionally `businessID`, `Accept: application/json`).

**Response shape:**

- `result`: `{ "message": string, "status": "Completed" }`
- `physical_foreign_flow`: array of objects:
  - `date`, `date_tz`, `date_offset`
  - `country` (string)
  - `import` (string)
  - `export` (string)
  - `physical_foreign_flow_MW` (string)

**Example request:**

```bash
curl --location --request GET 'https://api-intra.servizi.prv/transmission/v2.0/physical-foreign-flow?dateFrom=23/04/2022&dateTo=23/04/2022' \
  --header 'x-api-key: <token>'
```

---

## Pagination and date ranges

- The documented endpoints do **not** describe cursor-based pagination or `limit`/`offset`.
- Data is requested by **date range** (`dateFrom`, `dateTo`). The connector should request data in chunks (e.g. by day or month) to avoid oversized responses and to support incremental sync by date.
- All four scoped tables return a single JSON object with a `result` block and one data array per response; there is no “next page” link in the response.

---

## Rate limits

- Terna’s public documentation does **not** specify rate limits (requests per minute/hour).
- Tokens expire in **300 seconds**; the connector must refresh the access token before expiry and should not assume any specific request-rate ceiling without checking with Terna or observing 429 responses.

---

## Object list and schema (connector view)

For the Lakeflow connector, the object list is **static** for the initial scope:

| Object (table)           | Description                                      | Ingestion type |
|--------------------------|--------------------------------------------------|-----------------|
| total_load               | Total demand trend (and forecast) by bidding zone | append          |
| actual_generation        | Net hourly generation by primary source          | append          |
| renewable_generation     | Hourly renewable generation by source           | append          |
| physical_foreign_flow    | Physical cross-border flows by country          | append          |

- **Primary keys / uniqueness:** Natural keys are combinations of (date, bidding_zone) for total_load; (date, primary_source) for actual_generation; (date, energy_source) for renewable_generation; (date, country) for physical_foreign_flow. No explicit “primary key” API; treat as above for deduplication.
- **Schema:** Inferred from the response structures above. No separate schema discovery endpoint; fields and types are as in the tables in this doc. Numeric values are often returned as strings (e.g. `total_load_MW`, `actual_generation_GWh`).
- **Ingestion type:** All four are time-series, append-oriented (no deletes or change feed described). Use **append** with date-range incremental reads.

---

## Field type mapping (summary)

| API field / pattern     | Suggested type  | Notes                                      |
|-------------------------|-----------------|--------------------------------------------|
| date                    | timestamp/string| yyyy-mm-dd hh:mm:ss; store with timezone  |
| date_tz, date_offset    | string          | For interpretation only                    |
| *_MW, *_GWh, import, export | decimal/string | API returns strings; parse for numeric use |
| bidding_zone, country, primary_source, energy_source | string | Enums / categories                         |
| result.message, result.status | string    | Metadata only                              |

---

## Sources and references

| Source | URL | Confidence |
|--------|-----|------------|
| APIs Catalog | https://developer.terna.it/docs/read/apis_catalog | Official – high |
| OAuth2 / OIDC | https://developer.terna.it/docs/read/getting_started/Oauth2, OIDC | Official – high |
| Access Token | https://developer.terna.it/docs/read/Access_Token | Official – high |
| How to make a request | https://developer.terna.it/docs/read/How_to_make_a_request | Official – high |
| Total Load | https://developer.terna.it/docs/read/apis_catalog/load/Total_Load | Official – high |
| Actual Generation | https://developer.terna.it/docs/read/apis_catalog/generation/Actual_Generation | Official – high |
| Renewable Generation | https://developer.terna.it/docs/read/apis_catalog/generation/Renewable_Generation | Official – high |
| Physical Foreign Flow | https://developer.terna.it/docs/read/apis_catalog/transmission/Physical_Foreign_Flow | Official – high |

All content is taken from the Terna developer portal public documentation; no third-party connector implementations were used.
