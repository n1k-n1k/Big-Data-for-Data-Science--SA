-- 1. Получить статистику по дням.
-- Просто посчитать число всех событий по дням, число показов, число кликов,
-- число уникальных объявлений и уникальных кампаний.

SELECT date,
       count() AS events_all,
       countIf(event == 'view') AS views,
       countIf(event == 'click') AS clicks,
       countIf(ad_id) AS unique_ads,
       countIf(campaign_union_id) AS unique_campaigns
FROM ads_data
GROUP BY date
ORDER BY date;
