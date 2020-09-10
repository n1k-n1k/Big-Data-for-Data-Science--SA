-- 1.13.2.
-- Перед вами запрос, с которым мы попробуем разобраться в нескольких степах.
-- Цель этого запроса - определить все возможные сочетания платформ пользователей,
-- с которых они могут смотреть объявления и определить наиболее частое сочетание.
--
-- with (select 1 from ads_data) as sum_ads
--
-- select platforms, 2 as ads, sum_ads, ads / sum_ads * 100 as perc_ads
-- from (select ad_id, 4(3(platform)) as platforms
--       from ads_data
--       where event = 'view'
--       group by ad_id)
-- group by platforms
-- order by perc_ads desc

-- При помощи каких выражений можно посчитать точное уникальное количество объявлений
-- в таблице ads_data на месте пропусков и 1 , и 2?

-- count(ad_id)
-- uniq(ad_id)
-- count(distinct ad_id)
-- uniqExact(ad_id)

-- При помощи каких выражений можно сагрегировать платформы, с которых было просмотрено объявление,
-- в виде массива уникальных значений в пропуске 3?

-- groupUniqArray(platform)
-- arrayDistinct(groupArray(platform))
-- groupArray(platform)
-- groupArray(distinct platform)

-- При помощи какой функции можно отсортировать значения платформ внутри сагрегированных массивов в пропуске 4?
--
-- sort()
-- orderby()
-- arraySort()


-- Итоговый запрос:

WITH (SELECT count(DISTINCT ad_id) FROM ads_data) AS sum_ads

SELECT platforms, count(DISTINCT ad_id) AS ads, sum_ads, ads / sum_ads * 100 AS perc_ads
FROM (SELECT ad_id, arraySort(arrayDistinct(groupArray(platform))) AS platforms
      FROM ads_data
      WHERE event = 'view'
      GROUP BY ad_id)
GROUP BY platforms
ORDER BY perc_ads DESC;
