-- 4. Похоже, в наших логах есть баг, объявления приходят с кликами, но без показов!
-- Сколько таких объявлений, есть ли какие-то закономерности?
-- Эта проблема наблюдается на всех платформах?

SELECT count()
FROM (
    SELECT sum(event == 'click') AS clicks,
           sum(event == 'view') AS views
    FROM ads_data
    GROUP BY ad_id
    HAVING views = 0
       AND clicks != 0
     );


SELECT ad_id,
       count(ad_id) AS click_count,
       platform
FROM ads_data
WHERE ad_id GLOBAL IN (
    SELECT ad_id
    FROM ads_data
    GROUP BY ad_id
    HAVING sum(event == 'view') = 0
       AND sum(event == 'click') != 0
    )
GROUP BY platform,
         ad_id
ORDER BY ad_id;


SELECT ad_id
FROM ads_data
WHERE ad_id GLOBAL IN (
    SELECT ad_id
    FROM ads_data
    GROUP BY ad_id
    HAVING sum(event == 'view') = 0
       AND sum(event == 'click') != 0
    )
GROUP BY ad_id;


SELECT *
FROM ads_data
WHERE ad_id IN (115825,120536,117364,45418,120431,120796,41500,19223,26204)
  AND has_video == 1
ORDER BY time;