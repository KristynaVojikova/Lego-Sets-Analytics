# Databricks notebook source
# MAGIC %md
# MAGIC # 10 – Bronze: Rebrickable sets
# MAGIC Stáhne sety pro Star Wars, Ninjago (do roku 2013) a všechna Botanical témata z Rebrickable API.
# MAGIC Data ukládá beze změn (vše jako string) + metadata o tom, z jakého dotazu přišla.
# MAGIC
# MAGIC **Výstup:** `workspace.lego_bronze.rebrickable_sets` (full refresh)

# COMMAND ----------

import time
import requests
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

CATALOG = "workspace"
TARGET_TABLE = f"{CATALOG}.lego_bronze.rebrickable_sets"

# Klíč patří do Databricks secrets, ne do kódu (hlavně když to půjde na Git)
API_KEY = dbutils.secrets.get(scope="lego", key="rebrickable_api_key")
HEADERS = {"Authorization": f"key {API_KEY}", "Accept": "application/json"}
BASE_URL = "https://rebrickable.com/api/v3/lego"
SLEEP_S = 1.2  # limit API ~1 požadavek/s

# COMMAND ----------

def get_json(url, params=None, max_retries=5):
    """GET s retry na 429 (rate limit). Při jiné chybě job spadne – je to lepší než tiše uložit neúplná data."""
    for attempt in range(max_retries):
        r = requests.get(url, headers=HEADERS, params=params, timeout=30)
        if r.status_code == 429:
            time.sleep(5 * (attempt + 1))
            continue
        r.raise_for_status()
        time.sleep(SLEEP_S)
        return r.json()
    raise RuntimeError(f"Rate limit i po {max_retries} pokusech: {url}")


def get_all_pages(url, params=None):
    data = get_json(url, params)
    results = list(data.get("results", []))
    while data.get("next"):
        data = get_json(data["next"])
        results.extend(data.get("results", []))
    return results

# COMMAND ----------

# MAGIC %md
# MAGIC ## Konfigurace dotazů
# MAGIC Rebrickable `sets/?theme_id=X` vrací jen sety přímo v tématu X, **ne v podtématech**
# MAGIC (např. Star Wars → Ultimate Collector Series). Proto se stahuje celý strom:
# MAGIC kořenové téma + všechna témata s klíčovým slovem v názvu (např. Technic → Star Wars)
# MAGIC + všechna jejich podtémata. `tema` se ukládá k datům už tady.

# COMMAND ----------

themes = get_all_pages(f"{BASE_URL}/themes/", {"page_size": 1000})
theme_names = {t["id"]: t["name"] for t in themes}
children = {}
for t in themes:
    children.setdefault(t["parent_id"], []).append(t["id"])


def theme_tree(root_ids, keyword):
    """Kořenová témata + témata s keyword v názvu + všechna jejich podtémata (rekurzivně)."""
    roots = set(root_ids) | {t["id"] for t in themes if keyword in t["name"].lower()}
    result, stack = set(), list(roots)
    while stack:
        theme_id = stack.pop()
        if theme_id in result:
            continue
        result.add(theme_id)
        stack.extend(children.get(theme_id, []))
    return sorted(result)


TEMA_CONFIG = [
    {"tema": "Star Wars",            "roots": [158], "keyword": "star wars", "max_year": None},
    {"tema": "Ninjago",              "roots": [435], "keyword": "ninjago",   "max_year": 2013},
    {"tema": "Botanical Collection", "roots": [],    "keyword": "botanical", "max_year": None},
]

query_configs = []
for cfg in TEMA_CONFIG:
    ids = theme_tree(cfg["roots"], cfg["keyword"])
    if not ids:
        raise ValueError(f"Nenašlo se žádné téma pro {cfg['tema']} – zkontroluj názvy témat v API.")
    print(f"{cfg['tema']}: {[f'{i} {theme_names[i]}' for i in ids]}")
    query_configs += [{"tema": cfg["tema"], "theme_id": i, "max_year": cfg["max_year"]} for i in ids]

# COMMAND ----------

rows = []
for cfg in query_configs:
    params = {"theme_id": cfg["theme_id"], "page_size": 1000}
    if cfg["max_year"] is not None:
        params["max_year"] = cfg["max_year"]

    sets = get_all_pages(f"{BASE_URL}/sets/", params)
    for s in sets:
        s["query_tema"] = cfg["tema"]
        s["query_theme_id"] = cfg["theme_id"]
    rows.extend(sets)
    print(f"{cfg['tema']} (theme_id {cfg['theme_id']}): {len(sets)} setů")

if not rows:
    raise ValueError("API nevrátilo žádné sety.")

# COMMAND ----------

# Vše jako string, None zůstává NULL (ne text 'None' jako u pandas astype(str))
columns = sorted({k for r in rows for k in r})
schema = StructType([StructField(c, StringType()) for c in columns])
data = [tuple(None if r.get(c) is None else str(r.get(c)) for c in columns) for r in rows]

df = (
    spark.createDataFrame(data, schema)
    .withColumn("_source", F.lit("rebrickable_api_v3"))
    .withColumn("_ingested_at", F.current_timestamp())
)

(df.write.mode("overwrite")
   .option("overwriteSchema", "true")
   .saveAsTable(TARGET_TABLE))

print(f"Uloženo {df.count()} řádků do {TARGET_TABLE}")
