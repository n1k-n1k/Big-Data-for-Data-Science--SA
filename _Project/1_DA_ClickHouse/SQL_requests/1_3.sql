-- 3. Найти топ 10 объявлений по CTR за все время.
-- CTR — это отношение всех кликов объявлений к просмотрам.
-- Например, если у объявления было 100 показов и 2 клика, CTR = 0.02.
-- Различается ли средний и медианный CTR объявлений в наших данных?

SELECT ad_id,
       sum(event == 'click') AS clicks,
       sum(event == 'view') AS views,
       round(clicks / views, 3) AS CTR
FROM ads_data
GROUP BY ad_id
HAVING views != 0
ORDER BY CTR DESC
LIMIT 10;


SELECT avg(CTR),
       median(CTR)
FROM (
    SELECT sum(event == 'click') AS clicks,
           sum(event == 'view') AS views,
           round(clicks / views, 3) AS CTR
    FROM ads_data
    GROUP BY ad_id
    HAVING views != 0
    ORDER BY CTR DESC
     );
