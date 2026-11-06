-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 21 – Silver: minifigurky v setech
-- MAGIC Vyfiltruje figurky jen pro sety z `lego_silver.lego_sets` a spočítá jejich počet.
-- MAGIC
-- MAGIC - pokud má set víc verzí inventáře, bere se verze 1 (původní vydání)
-- MAGIC - `pocet_figurek` = součet `quantity` (3 stejní klony = 3), `pocet_unikatnich_figurek` = počet různých
-- MAGIC - set s inventářem bez figurek → 0, set bez inventáře v Rebrickable → NULL
-- MAGIC
-- MAGIC **Vstup:** `lego_silver.lego_sets`, `lego_bronze.rb_*`
-- MAGIC **Výstup:** `lego_silver.set_minifigs` (detail), `lego_silver.set_minifig_counts` (počty na set)

-- COMMAND ----------

USE CATALOG workspace;

-- COMMAND ----------

CREATE OR REPLACE TABLE lego_silver.set_minifigs
COMMENT 'Jeden řádek = jedna figurka v setu'
AS
WITH inv AS (
    SELECT
        set_num,
        id AS inventory_id
    FROM lego_bronze.rb_inventories
    QUALIFY ROW_NUMBER() OVER (PARTITION BY set_num ORDER BY TRY_CAST(version AS INT)) = 1
)
SELECT
    s.originalni_cislo,
    s.tema,
    im.fig_num                        AS cislo_figurky,
    TRIM(m.name)                      AS nazev_figurky,
    TRY_CAST(im.quantity AS INT)      AS pocet_kusu,
    TRY_CAST(m.num_parts AS INT)      AS dilku_figurky,
    m.img_url                         AS obrazek_figurky
FROM lego_silver.lego_sets s
JOIN inv
    ON inv.set_num = s.originalni_cislo
JOIN lego_bronze.rb_inventory_minifigs im
    ON im.inventory_id = inv.inventory_id
LEFT JOIN lego_bronze.rb_minifigs m
    ON m.fig_num = im.fig_num;

-- COMMAND ----------

CREATE OR REPLACE TABLE lego_silver.set_minifig_counts
COMMENT 'Počet figurek na set'
AS
WITH inv AS (
    SELECT DISTINCT set_num
    FROM lego_bronze.rb_inventories
)
SELECT
    s.originalni_cislo,
    CASE WHEN inv.set_num IS NULL THEN NULL
         ELSE COALESCE(SUM(f.pocet_kusu), 0) END                 AS pocet_figurek,
    CASE WHEN inv.set_num IS NULL THEN NULL
         ELSE COUNT(DISTINCT f.cislo_figurky) END                AS pocet_unikatnich_figurek
FROM lego_silver.lego_sets s
LEFT JOIN inv
    ON inv.set_num = s.originalni_cislo
LEFT JOIN lego_silver.set_minifigs f
    ON f.originalni_cislo = s.originalni_cislo
GROUP BY s.originalni_cislo, inv.set_num;

-- COMMAND ----------

-- Kontrola: porovnání s počtem figurek z Brickset (měly by se většinou shodovat)
SELECT
    s.tema,
    COUNT(*)                                                         AS setu,
    COUNT(c.pocet_figurek)                                           AS s_poctem_rebrickable,
    SUM(CASE WHEN c.pocet_figurek = s.pocet_minifigurek THEN 1 END)  AS shoda_s_brickset
FROM lego_silver.lego_sets s
LEFT JOIN lego_silver.set_minifig_counts c
    ON c.originalni_cislo = s.originalni_cislo
GROUP BY s.tema;
