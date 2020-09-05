-- 8.
-- Анализируя нашу базу данных, найдите дату (формата ГГГГ-ММ-ДД),
-- в которую произошло самое большое число увольнений сотрудников.

SELECT end_date FROM employee
WHERE end_date IS NOT NULL
GROUP BY end_date
ORDER BY count(end_date) DESC
LIMIT 1;
