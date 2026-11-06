# LEGO Sets Analytics – Databricks Medallion Pipeline

End-to-end data pipeline on Databricks that ingests LEGO set data from two public APIs, cleans and joins it using the medallion architecture (bronze → silver → gold), and serves it to a Databricks dashboard.

**Scope:** Star Wars, Ninjago (up to 2013) and Botanical Collection sets.

**Questions the dashboard answers:**
- How have set prices, piece counts and price per piece evolved over time across themes?
- Which are the largest and most expensive sets in each theme?
- How much does the price of a set rise with the number of minifigures – and is it more than just the set size?

## Dashboard

**Star Wars sets** – number of sets, price (average vs. median), pieces and minifigures per year

![Star Wars sets – overview](docs/dashboard_star_wars_1.png)
![Star Wars sets – totals and price per piece](docs/dashboard_star_wars_2.png)

**Minifigure catalog** – search a minifigure and see every set it appears in

![Minifigure catalog](docs/dashboard_minifigs.png)

> Prices are available from 2006 (Brickset only provides LEGO.com retail prices from that year).

## Architecture

```
Rebrickable API ─┐
Brickset API ────┼─► BRONZE (raw) ─► SILVER (cleaned, joined) ─► GOLD (dashboard tables) ─► Dashboard
Rebrickable CSV ─┤
ČNB FX rates ────┘
```

| Layer | Schema | Tables |
|---|---|---|
| Bronze | `lego_bronze` | `rebrickable_sets`, `brickset_sets`, `rb_inventories`, `rb_inventory_minifigs`, `rb_minifigs`, `cnb_fx_rates` |
| Silver | `lego_silver` | `lego_sets`, `set_minifigs`, `set_minifig_counts` |
| Gold | `lego_gold` | `sets_dashboard`, `theme_year_stats`, `theme_summary`, `sets_minifig_price`, `price_by_minifig_count`, `minifig_price_effect` |

## Notebooks

| Notebook | Layer | Description |
|---|---|---|
| `00_setup_schemas` | – | Creates bronze/silver/gold schemas (idempotent) |
| `10_bronze_rebrickable_sets` | Bronze | Sets from Rebrickable API (paginated, rate-limit aware, retry on 429) |
| `11_bronze_brickset_sets` | Bronze | Sets incl. retail prices (EUR/USD) from Brickset API, raw JSON kept |
| `12_bronze_rebrickable_minifigs` | Bronze | Minifigure inventories from Rebrickable daily CSV export (no API rate limit) |
| `13_bronze_cnb_fx_rates` | Bronze | Daily CZK exchange rates from the Czech National Bank (1999 – today) |
| `20_silver_lego_sets` | Silver | Typed, deduplicated sets joined with prices, converted to CZK + data quality checks |
| `21_silver_set_minifigs` | Silver | Minifigures per set (counting quantities, not just distinct figures) |
| `30_gold_dashboard_tables` | Gold | Set details, theme × year stats, theme KPIs |
| `31_gold_minifig_price` | Gold | Price vs. minifigure count, linear regression per theme |

Numbering by tens leaves room for new notebooks within a layer.

## Orchestration

Daily Databricks Job (serverless), 06:00 Europe/Prague:

```
setup_schemas ─┬─► bronze_rebrickable ─┐
               ├─► bronze_brickset ────┴─► silver_lego_sets ─► gold_dashboard ──┐
               ├─► bronze_cnb_fx ──────┘
               └─► bronze_minifigs ──────────────────────────► silver_minifigs ─┴─► gold_minifig_price
```

- Bronze tasks run in parallel; API tasks have retries configured.
- The minifigure branch is independent – if it fails, the main dashboard tables still refresh.

## Setup

1. **API keys** – get free keys at [Rebrickable](https://rebrickable.com/api/) and [Brickset](https://brickset.com/tools/webservices/requestkey), then store them as Databricks secrets (never in code):

   ```bash
   databricks secrets create-scope lego
   databricks secrets put-secret lego rebrickable_api_key
   databricks secrets put-secret lego brickset_api_key
   ```

2. **Catalog** – notebooks use the `workspace` catalog. Change `CATALOG` / `USE CATALOG` if yours differs.

3. **Run** – clone this repo as a Databricks Git folder and run notebooks in order `00 → 30`, or create a job with the dependencies above.

## Design decisions

- **Bronze stores raw data as strings** plus ingestion metadata (`_source`, `_ingested_at`); typing happens in silver with `TRY_CAST`, so a malformed value never breaks ingestion.
- **Full theme tree is downloaded** – Rebrickable `theme_id` filter does not include sub-themes (e.g. Star Wars → Ultimate Collector Series), so all descendant themes and themes named after the franchise (e.g. Technic → Star Wars) are queried.
- **Theme is captured at ingestion** (`query_tema`) rather than derived from `theme_id`, which is unreliable when the API returns sub-themes.
- **Deduplication in silver** – Botanical sets are queried through several Brickset theme configurations, so duplicates are removed before the join.
- **Minifigures via CSV export instead of per-set API calls** – reduces runtime from ~25 minutes to seconds.
- **All prices in CZK, converted with the historical rate of the release year** – EUR (DE retail) price first, USD as fallback; yearly average of daily Czech National Bank rates. The source currency and rate used are kept (`cena_czk_zdroj`, `pouzity_kurz`). US prices exclude sales tax while the EUR prices are German retail incl. 19 % VAT, so USD-based prices get +19 % (`pricitana_dph`) to stay comparable.
- **Price per piece next to price** – sets with more minifigures are also bigger, so price per piece partially controls for set size.

## Tech stack

Databricks (Unity Catalog, Delta Lake, Jobs, serverless compute, Dashboards) · Python · PySpark · SQL · REST APIs

## Data sources

- [Rebrickable](https://rebrickable.com/) – sets, themes, minifigure inventories
- [Brickset](https://brickset.com/) – retail prices, ratings
- [Czech National Bank](https://www.cnb.cz/) – CZK exchange rates

LEGO® is a trademark of the LEGO Group, which does not sponsor or endorse this project.
