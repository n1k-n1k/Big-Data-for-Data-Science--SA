-- 5. Есть ли различия в CTR у объявлений с видео и без?
-- А чему равняется 95 процентиль CTR по всем объявлениям за 2019-04-04?

SELECT avg(CTR),
       median(CTR)
FROM (
    SELECT countIf(event == 'click') AS clicks,
           countIf(event == 'view') AS views,
           round(clicks / views, 3) AS CTR
    FROM ads_data
    GROUP BY ad_id, has_video
    HAVING views != 0
       AND has_video == 1
    ORDER BY CTR DESC
     );

SELECT avg(CTR),
       median(CTR)
FROM (
    SELECT countIf(event == 'click') AS clicks,
           countIf(event == 'view') AS views,
           round(clicks / views, 3) AS CTR
    FROM ads_data
    GROUP BY ad_id, has_video
    HAVING views != 0
       AND has_video == 0
    ORDER BY CTR DESC
     );

SELECT quantile(0.95)(CTR)
FROM (
    SELECT countIf(event == 'click') AS clicks,
           countIf(event == 'view') AS views,
           round(clicks / views, 3) AS CTR
    FROM ads_data
    WHERE date == '2019-04-04'
    GROUP BY ad_id
    ORDER BY CTR DESC
     );
