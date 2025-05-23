# RuniUni Agent Pipeline

End‑to‑end tooling that **discovers** live events on the internet, **enriches** them with AI, and **publishes** fully‑formed objects to the RuniUni platform – no manual data entry required.

---

## 1  Why it exists

RuniUni’s mission is to surface every interesting local happening. The Agent Pipeline automates the tedious parts:

| Stage               | What happens                              | Tech                                 |
| ------------------- | ----------------------------------------- | ------------------------------------ |
| **Search / Scrape** | Find candidate events by keyword or URL   | GPT‑4o, Requests‑HTML                |
| **Enrich**          | Fill gaps (images, geo‑coords, copy)      | Google CSE, Google Geocoding, GPT‑4o |
| **Validate**        | Ensure required fields, dedupe, flag NSFW | Pydantic rules                       |
| **Publish**         | Push to RuniUni REST API with JWT auth    | `RuniuniJWTClient`                   |

> **Two entry points:**
>
> 1. **Location‑Based** – “Find concerts in Pensacola tonight.”
> 2. **URL‑Based** – “Import everything on eventbrite.com/d/fl‑miami/events.”

---

## 2  Key Features

* 🔍 **AI‑driven discovery** – Large‑language‑model prompts generate precise search queries per locale.
* 🖼 **Auto image attach** – Grabs high‑resolution hero shots via Google Custom Search.
* 📍 **Geo enhancement** – Converts free‑form venues into structured lat/lng + hierarchical location fields.
* ✨ **Copy polishing** – GPT writes engaging, SEO‑ready descriptions.
* 🛡 **Strict validation** – No bad data hits production; the pipeline fails‑fast with helpful JSON diffs.
* 🏗 **Composable** – Each step is a class with a clear `run()` signature; swap or extend as needed.

---

## 3  Quick Start

### 3.1 Prerequisites

* Python ≥ 3.9
* Accounts / keys for:

  * **OpenAI** – LLM calls
  * **Google CSE** – image search
  * **Google Geocoding / Places** – address resolution
  * **RuniUni** – API username + password (creates short‑lived JWT)

### 3.2 Installation

```bash
# 1 Clone
$ git clone https://github.com/runiuni/runiuni-agent-pipeline.git
$ cd runiuni-agent-pipeline

# 2 Create venv
$ python -m venv .venv && source .venv/bin/activate  # Windows: .venv\Scripts\activate

# 3 Install deps
$ pip install -r requirements.txt

# 4 Configure secrets
$ cp .env.example .env  &&  $EDITOR .env  # add your keys
```

### 3.3 Running

* **Location pipeline**

  ```bash
  python run_pipeline_location.py \
    --locations "Pensacola, FL" "Miami, FL" \
    --max-events 5           # per location
  ```
* **URL pipeline**

  ```bash
  python run_pipeline_urls.py \
    --urls "https://example.com/events" "https://another.com/calendar" \
    --dry-run                # skip final POST
  ```

Pass `--help` on either script for the full CLI.

---

## 4  Configuration Reference

| Env Var                                 | Required | Description                                 |
| --------------------------------------- | -------- | ------------------------------------------- |
| `OPENAI_API_KEY`                        | ✔        | GPT‑4o access                               |
| `GOOGLE_API_KEY`                        | ✔        | Shared key for CSE & Geocoding              |
| `SEARCH_ENGINE_ID`                      | ✔        | Programmable Search Engine ID               |
| `RUNIUNI_USERNAME` / `RUNIUNI_PASSWORD` | ✔        | Auth for event POSTs                        |
| `GOOGLE_PLACES_API_KEY`                 | ✖︎       | Only if `GOOGLE_API_KEY` lacks Places scope |

---

## 5  Extending the Pipeline

1. **Add a new data source**
   Implement `NewSourceAgent(EventURLAgent)` → return standard event JSON.
2. **Custom validators**
   Drop a `pydantic` model into `validators/` and reference it from `EventValidationChecker`.
3. **Alternative publish target**
   Swap `RuniuniJWTClient` for your own client – the pipeline only expects `.create_event(payload)`.

---

## 6  Testing & CI

* Unit tests live in `tests/` and cover every agent class.
* GitHub Actions run lint + pytest on push.
* Add the secret vars in your repo settings to test live POSTs.

---

## 7  Troubleshooting

| Symptom                | Likely Cause                 | Fix                                       |
| ---------------------- | ---------------------------- | ----------------------------------------- |
| `ImageNotFoundError`   | CSE quota exhausted          | Verify billing; rotate key                |
| `InvalidLocationError` |  Geocoding API disabled      | Enable **Geocoding API** in GCP console   |
| `401 Unauthorized`     | Bad or expired RuniUni creds |  Double‑check `.env`; regenerate password |

---

## 8  License

Released under the MIT License – see `LICENSE` for details.

---

Made with 💜 by the RuniUni team. Ship fast, connect local.
