-- 6. Для финансового отчета нужно рассчитать наш заработок по дням.
-- В какой день мы заработали больше всего? В какой меньше?
-- Мы списываем с клиентов деньги, если произошел клик по CPC объявлению,
-- и мы списываем деньги за каждый показ CPM объявления,
-- если у CPM объявления цена - 200 рублей, то за один показ мы зарабатываем 200 / 1000.

SELECT date,
       round(sumIf(ad_cost, ad_cost_type == 'CPC' AND event == 'click'), 2) AS cpc_sum_cost,
       round(sumIf(ad_cost / 1000, ad_cost_type == 'CPM' AND event == 'view'), 2) AS cpm_sum_cost,
       round(cpc_sum_cost + cpm_sum_cost, 2) AS total_cost
FROM ads_data
GROUP BY date
ORDER BY total_cost DESC;
