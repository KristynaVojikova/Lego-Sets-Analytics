# Databricks notebook source
# MAGIC %md
# MAGIC # 40 – Data quality tests
# MAGIC Poslední task jobu. Spustí všechny kontroly, vypíše tabulku výsledků a pokud některá selže,
# MAGIC **shodí job** (→ notifikace e-mailem). Dashboard tak nikdy tiše neukáže rozbitá data.
# MAGIC
# MAGIC | Oblast | Co se kontroluje |
# MAGIC |---|---|
# MAGIC | Bronze | tabulky nejsou prázdné, data jsou čerstvá, kurzy ČNB pokrývají všechny roky |
# MAGIC | Silver | unikátní sety, povinná pole, povolená témata, rozsah roků, převod cen do CZK, pokrytí cen |
# MAGIC | Gold | JOINy nezdvojily řádky |
# MAGIC
# MAGIC Prahy (např. min. počet Star Wars setů) jsou nastavené s rezervou – při změně scope je uprav.

# COMMAND ----------

import datetime

CATALOG = "workspace"
spark.sql(f"USE CATALOG {CATALOG}")

CURRENT_YEAR = datetime.date.today().year
results = []


def check(name, sql, condition, expected):
    """Spustí SQL vracející jednu hodnotu a vyhodnotí podmínku."""
    value = spark.sql(sql).collect()[0][0]
    try:
        ok = bool(condition(value))
    except TypeError:  # např. None
        ok = False
    results.append((name, str(value), expected, "OK" if ok else "FAIL"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze

# COMMAND ----------

BRONZE_TABLES = [
    "rebrickable_sets", "brickset_sets", "rb_inventories",
    "rb_inventory_minifigs", "rb_minifigs", "cnb_fx_rates",
]

for table in BRONZE_TABLES:
    check(f"bronze.{table}: není prázdná",
          f"SELECT COUNT(*) FROM lego_bronze.{table}",
          lambda v: v > 0, "> 0")
    check(f"bronze.{table}: načteno za posledních 48 h",
          f"SELECT MAX(_ingested_at) >= current_timestamp() - INTERVAL 48 HOURS FROM lego_bronze.{table}",
          lambda v: v is True, "true")

# Rok, který má kurz EUR i USD. Tolerance 1 rok – první dny v lednu ČNB ještě nový rok nemá.
check("bronze.cnb_fx_rates: kurzy EUR i USD pro všechny roky od 1999",
      """
      SELECT COUNT(*) FROM (
          SELECT YEAR(TO_DATE(datum, 'dd.MM.yyyy')) AS rok
          FROM lego_bronze.cnb_fx_rates
          WHERE mena IN ('EUR', 'USD')
          GROUP BY 1
          HAVING COUNT(DISTINCT mena) = 2
      )
      """,
      lambda v: v >= CURRENT_YEAR - 1999, f">= {CURRENT_YEAR - 1999} let")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver

# COMMAND ----------

check("silver.lego_sets: žádné duplicitní sety",
      "SELECT COUNT(*) - COUNT(DISTINCT originalni_cislo) FROM lego_silver.lego_sets",
      lambda v: v == 0, "0")

check("silver.lego_sets: vyplněné číslo, název a téma",
      """SELECT COUNT(*) FROM lego_silver.lego_sets
         WHERE originalni_cislo IS NULL OR nazev_stavebnice IS NULL OR tema IS NULL""",
      lambda v: v == 0, "0")

check("silver.lego_sets: jen povolená témata",
      """SELECT COUNT(*) FROM lego_silver.lego_sets
         WHERE tema NOT IN ('Star Wars', 'Ninjago', 'Botanical Collection')""",
      lambda v: v == 0, "0")

check("silver.lego_sets: rok vydání 1999 – příští rok",
      f"""SELECT COUNT(*) FROM lego_silver.lego_sets
          WHERE rok_vydani IS NULL OR rok_vydani < 1999 OR rok_vydani > {CURRENT_YEAR + 1}""",
      lambda v: v == 0, "0")

# Hlídá, že se neztratila podtémata (UCS apod.) – po opravě stahování ~1 100 setů
check("silver.lego_sets: min. 1 000 Star Wars setů",
      "SELECT COUNT(*) FROM lego_silver.lego_sets WHERE tema = 'Star Wars'",
      lambda v: v >= 1000, ">= 1000")

check("silver.lego_sets: Ninjago jen do roku 2013",
      "SELECT COUNT(*) FROM lego_silver.lego_sets WHERE tema = 'Ninjago' AND rok_vydani > 2013",
      lambda v: v == 0, "0")

check("silver.lego_sets: každý set s cenou má cenu v CZK",
      "SELECT COUNT(*) FROM lego_silver.lego_sets WHERE ma_cenu AND cena_czk IS NULL",
      lambda v: v == 0, "0")

check("silver.lego_sets: použitý kurz 15–45 Kč",
      """SELECT COUNT(*) FROM lego_silver.lego_sets
         WHERE pouzity_kurz IS NOT NULL AND (pouzity_kurz < 15 OR pouzity_kurz > 45)""",
      lambda v: v == 0, "0")

check("silver.lego_sets: žádné záporné ceny ani dílky",
      "SELECT COUNT(*) FROM lego_silver.lego_sets WHERE cena_czk < 0 OR pocet_dilku < 0",
      lambda v: v == 0, "0")

# Star Wars od 2006 má cenu u ~64 % setů; pokles pod 50 % = problém s Brickset API
check("silver.lego_sets: pokrytí cen Star Wars od 2006 >= 50 %",
      """SELECT ROUND(AVG(CASE WHEN ma_cenu THEN 1.0 ELSE 0.0 END) * 100, 1)
         FROM lego_silver.lego_sets WHERE tema = 'Star Wars' AND rok_vydani >= 2006""",
      lambda v: v >= 50, ">= 50 %")

check("silver.set_minifig_counts: žádné duplicitní sety",
      "SELECT COUNT(*) - COUNT(DISTINCT originalni_cislo) FROM lego_silver.set_minifig_counts",
      lambda v: v == 0, "0")

check("silver.set_minifig_counts: pokrytí figurek Star Wars >= 50 %",
      """SELECT ROUND(AVG(CASE WHEN c.pocet_figurek IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100, 1)
         FROM lego_silver.lego_sets s
         JOIN lego_silver.set_minifig_counts c ON c.originalni_cislo = s.originalni_cislo
         WHERE s.tema = 'Star Wars'""",
      lambda v: v >= 50, ">= 50 %")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold

# COMMAND ----------

check("gold.sets_overview: JOIN nezdvojil řádky",
      "SELECT COUNT(*) - COUNT(DISTINCT originalni_cislo) FROM lego_gold.sets_overview",
      lambda v: v == 0, "0")

check("gold.sets_overview: stejný počet setů jako gold.sets_dashboard",
      """SELECT (SELECT COUNT(*) FROM lego_gold.sets_overview)
              - (SELECT COUNT(*) FROM lego_gold.sets_dashboard)""",
      lambda v: v == 0, "0")

check("gold.theme_summary: všechna 3 témata mají data",
      "SELECT COUNT(DISTINCT tema) FROM lego_gold.theme_summary",
      lambda v: v == 3, "3")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Výsledek

# COMMAND ----------

df_results = spark.createDataFrame(results, "test STRING, hodnota STRING, ocekavano STRING, vysledek STRING")
display(df_results)

failed = [r for r in results if r[3] == "FAIL"]
if failed:
    lines = "\n".join(f"- {name}: {value} (očekáváno {expected})" for name, value, expected, _ in failed)
    raise AssertionError(f"{len(failed)} z {len(results)} testů selhalo:\n{lines}")

print(f"Všech {len(results)} testů prošlo.")
