-- 7. Какая платформа самая популярная для размещения рекламных объявлений?
-- Сколько процентов показов приходится на каждую из платформ (колонка platform)?

SELECT platform AS most_popular_platform
FROM ads_data
GROUP BY platform
ORDER BY count() DESC
LIMIT 1;


SELECT count() AS views_cnt,
       round(countIf(platform == 'web') / views_cnt * 100, 2) AS web_cnt_perc,
       round(countIf(platform == 'ios') / views_cnt * 100, 2) AS ios_cnt_perc,
       round(countIf(platform == 'android') / views_cnt * 100, 2) AS android_cnt_perc
FROM ads_data
WHERE event == 'view';