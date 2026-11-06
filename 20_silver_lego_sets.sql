-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 20 – Silver: LEGO sets
-- MAGIC Otypuje, deduplikuje a spojí Rebrickable sety s cenami z Brickset.
-- MAGIC
-- MAGIC - téma se bere z `query_tema` (uložené při stahování), ne z `CASE` nad `theme_id`
-- MAGIC - Brickset je deduplikovaný – Botanical se stahuje přes víc konfigurací, bez toho by `LEFT JOIN` zdvojoval řádky
-- MAGIC - `TRY_CAST` místo `CAST`, aby prázdné hodnoty nerozbily běh v ANSI módu
-- MAGIC
-- MAGIC - `cena_czk`: cena v CZK – primárně z EUR, jinak z USD, ročním průměrným kurzem ČNB v roce vydání
-- MAGIC
-- MAGIC **Vstup:** `lego_bronze.rebrickable_sets`, `lego_bronze.brickset_sets`, `lego_bronze.cnb_fx_rates`
-- MAGIC **Výstup:** `lego_silver.lego_sets`

-- COMMAND ----------

USE CATALOG workspace;

-- COMMAND ----------

CREATE OR REPLACE TABLE lego_silver.lego_sets
COMMENT 'LEGO sety (Star Wars, Ninjago do 2013, Botanical) s původní cenou'
AS
WITH rebrickable AS (
    SELECT *
    FROM lego_bronze.rebrickable_sets
    QUALIFY ROW_NUMBER() OVER (PARTITION BY set_num ORDER BY _ingested_at DESC) = 1
),
brickset AS (
    SELECT
        set_num,
        subtheme,
        TRY_CAST(retail_price_eur AS DECIMAL(10, 2)) AS puvodni_cena_eur,
        TRY_CAST(retail_price_usd AS DECIMAL(10, 2)) AS puvodni_cena_usd,
        TRY_CAST(rating AS DOUBLE)                    AS hodnoceni,
        TRY_CAST(minifigs AS INT)                     AS pocet_minifigurek
    FROM lego_bronze.brickset_sets
    -- při duplicitě preferuj záznam, který má cenu
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY set_num
        ORDER BY (retail_price_eur IS NULL), (retail_price_usd IS NULL)
    ) = 1
),
joined AS (
    SELECT
        r.set_num                          AS originalni_cislo,
        split_part(r.set_num, '-', 1)      AS cislo_setu,
        TRIM(r.name)                       AS nazev_stavebnice,
        TRY_CAST(r.year AS INT)            AS rok_vydani,
        r.query_tema                       AS tema,
        b.subtheme                         AS podtema,
        TRY_CAST(r.num_parts AS INT)       AS pocet_dilku,
        b.pocet_minifigurek,
        b.puvodni_cena_eur,
        b.puvodni_cena_usd,
        b.hodnoceni,
        r.set_url,
        r.set_img_url
    FROM rebrickable r
    LEFT JOIN brickset b
        ON r.set_num = b.set_num
),
-- Roční průměrný kurz ČNB (CZK za 1 jednotku měny) z denních kurzů
fx_rok AS (
    SELECT
        YEAR(TO_DATE(datum, 'dd.MM.yyyy'))                                          AS rok,
        mena,
        AVG(TRY_CAST(REPLACE(kurz, ',', '.') AS DOUBLE) / TRY_CAST(mnozstvi AS INT)) AS kurz
    FROM lego_bronze.cnb_fx_rates
    WHERE mena IN ('EUR', 'USD')
    GROUP BY ALL
),
-- Záloha pro sety z roku, za který ještě kurz není (např. ohlášené sety na příští rok)
fx_posledni AS (
    SELECT mena, MAX_BY(kurz, rok) AS kurz
    FROM fx_rok
    GROUP BY mena
),
prevod AS (
    SELECT
        j.*,
        COALESCE(eur.kurz, eur_p.kurz) AS kurz_eur,
        COALESCE(usd.kurz, usd_p.kurz) AS kurz_usd
    FROM joined j
    LEFT JOIN fx_rok eur      ON eur.rok = j.rok_vydani AND eur.mena = 'EUR'
    LEFT JOIN fx_rok usd      ON usd.rok = j.rok_vydani AND usd.mena = 'USD'
    LEFT JOIN fx_posledni eur_p ON eur_p.mena = 'EUR'
    LEFT JOIN fx_posledni usd_p ON usd_p.mena = 'USD'
)
SELECT
    p.* EXCEPT (kurz_eur, kurz_usd),
    -- cena_czk: primárně z EUR, když EUR chybí, tak z USD; kurz = roční průměr ČNB v roce vydání.
    -- US ceny jsou bez daně, EUR ceny jsou německé s DPH 19 % → k USD ceně se přičte 19 %,
    -- aby byly obě varianty srovnatelné.
    CAST(ROUND(CASE
        WHEN p.puvodni_cena_eur IS NOT NULL THEN p.puvodni_cena_eur * p.kurz_eur
        WHEN p.puvodni_cena_usd IS NOT NULL THEN p.puvodni_cena_usd * p.kurz_usd * 1.19
    END, 0) AS DECIMAL(12, 0))                                        AS cena_czk,
    CASE WHEN p.puvodni_cena_eur IS NULL AND p.puvodni_cena_usd IS NOT NULL
         THEN 1.19 END                                                AS pricitana_dph,
    CASE
        WHEN p.puvodni_cena_eur IS NOT NULL THEN 'EUR'
        WHEN p.puvodni_cena_usd IS NOT NULL THEN 'USD'
    END                                                               AS cena_czk_zdroj,
    ROUND(CASE
        WHEN p.puvodni_cena_eur IS NOT NULL THEN p.kurz_eur
        WHEN p.puvodni_cena_usd IS NOT NULL THEN p.kurz_usd
    END, 3)                                                           AS pouzity_kurz,
    -- TRY_CAST výše už převedl prázdný text i 'null' na NULL, stačí tedy IS NOT NULL
    (p.puvodni_cena_eur IS NOT NULL OR p.puvodni_cena_usd IS NOT NULL) AS ma_cenu,
    current_timestamp()                                               AS _updated_at
FROM prevod p;

-- COMMAND ----------

-- Rychlá kontrola kvality: kolik setů má cenu a odkud
SELECT
    tema,
    COUNT(*)                                                         AS pocet_setu,
    SUM(CASE WHEN cena_czk_zdroj = 'EUR' THEN 1 ELSE 0 END)          AS z_eur,
    SUM(CASE WHEN cena_czk_zdroj = 'USD' THEN 1 ELSE 0 END)          AS z_usd,
    ROUND(COUNT(cena_czk) / COUNT(*) * 100)                          AS pokryti_cen_pct
FROM lego_silver.lego_sets
GROUP BY tema
ORDER BY tema;

-- COMMAND ----------

-- Kontrola DPH korekce: u setů, které mají obě ceny, porovná CZK z EUR a CZK z USD (+19 %).
-- Medián kolem 1.0 = korekce sedí; výrazně nad 1.0 = LEGO má v Evropě ceny vyšší i nad rámec DPH.
SELECT
    tema,
    COUNT(*) AS paru,
    ROUND(PERCENTILE(
        (puvodni_cena_eur * kurz_eur) / (puvodni_cena_usd * kurz_usd * 1.19), 0.5), 3) AS median_pomer
FROM (
    SELECT
        s.*,
        e.kurz AS kurz_eur,
        u.kurz AS kurz_usd
    FROM lego_silver.lego_sets s
    JOIN (SELECT YEAR(TO_DATE(datum, 'dd.MM.yyyy')) AS rok, AVG(TRY_CAST(REPLACE(kurz, ',', '.') AS DOUBLE)) AS kurz
          FROM lego_bronze.cnb_fx_rates WHERE mena = 'EUR' GROUP BY 1) e ON e.rok = s.rok_vydani
    JOIN (SELECT YEAR(TO_DATE(datum, 'dd.MM.yyyy')) AS rok, AVG(TRY_CAST(REPLACE(kurz, ',', '.') AS DOUBLE)) AS kurz
          FROM lego_bronze.cnb_fx_rates WHERE mena = 'USD' GROUP BY 1) u ON u.rok = s.rok_vydani
    WHERE s.puvodni_cena_eur > 0 AND s.puvodni_cena_usd > 0
)
GROUP BY tema;

-- COMMAND ----------

-- Kontrola použitých kurzů podle roku (EUR cca 24–36 CZK, USD cca 18–40 CZK)
SELECT
    rok_vydani,
    cena_czk_zdroj,
    COUNT(*)          AS setu,
    MIN(pouzity_kurz) AS kurz
FROM lego_silver.lego_sets
WHERE cena_czk IS NOT NULL
GROUP BY ALL
ORDER BY rok_vydani, cena_czk_zdroj;
