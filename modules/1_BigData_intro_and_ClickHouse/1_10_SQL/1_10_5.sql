-- 5.
-- Анализируя нашу базу данных, найдите топ 5 самых популярных позиций среди всех сотрудников.

SELECT job_title FROM employee
GROUP BY job_title
ORDER BY count(job_title) DESC
LIMIT 5;
