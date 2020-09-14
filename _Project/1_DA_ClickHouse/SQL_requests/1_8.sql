-- 8. А есть ли такие объявления, по которым сначала произошел клик, а только потом показ?

SELECT ad_id,
       minIf(time, event == 'click') AS first_click,
       minIf(time, event == 'view') AS first_view
FROM ads_data
GROUP BY ad_id
HAVING first_click < first_view
   AND first_click != '1970-01-01 00:00:00'
ORDER BY ad_id;
