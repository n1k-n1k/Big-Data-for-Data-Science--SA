-- 1.13.3.
-- В таблице ads_clients_data хранятся данные обо всех клиентах, которые существовали на платформе 7 мая 2020 года.
-- Подобные логи называют дампами. Они показывают как бы слепок системы в каком-то разрезе за определенный день.
-- В данном случае это слепок всех клиентов рекламного кабинета за конкретный день.
--
-- Мы знаем,  что не все кабинеты запускают рекламу на нашей платформе.
-- Некоторые из них вообще не имеют когда-либо запущенных или активных рекламных объявлений.
--
-- В этом задании, исключите из таблицы ads_clients_data всех клиентов, у которых когда-либо были активные объявления
-- (для этого понадобится таблица ads_data) и заполните пропуски ниже.


WITH (SELECT count(DISTINCT client_union_id) FROM ads_clients_data) AS allclients

SELECT allclients,
       count(DISTINCT client_union_id) AS notactive,
       round(notactive / allclients * 100) nonactive_perc,
       min(create_date) AS first_nonactive_client_date
FROM ads_clients_data
WHERE client_union_id GLOBAL NOT IN
      (SELECT client_union_id FROM ads_data);
