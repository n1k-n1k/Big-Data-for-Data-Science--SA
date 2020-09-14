-- 2. Разобраться, почему случился такой скачок 2019-04-05?
-- Каких событий стало больше?
-- У всех объявлений или только у некоторых?

SELECT date,
       ad_id,
       countIf(event == 'click') AS clicks,
       countIf(event == 'view') AS views
FROM ads_data
GROUP BY date,
         ad_id
ORDER BY clicks DESC
LIMIT 10;


WITH 112583 AS anormal_ad_id
SELECT date,
       sum(event == 'click') AS clicks,
       sum(event == 'click' AND ad_id == anormal_ad_id) AS clicks_anormal_ad,
       round(clicks_anormal_ad / clicks * 100, 2) AS clicks_anormal_ad_perc,
       sum(event == 'view') AS views,
       sum(event == 'view' AND ad_id == anormal_ad_id) AS views_anormal_ad,
       round(views_anormal_ad / views * 100, 2) AS views_anormal_ad_perc
FROM ads_data
GROUP BY date
-- HAVING clicks_anormal_ad != 0
ORDER BY date;
