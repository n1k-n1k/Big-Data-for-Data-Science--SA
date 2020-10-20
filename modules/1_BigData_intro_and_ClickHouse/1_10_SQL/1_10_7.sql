-- 7.
-- Анализируя нашу базу данных, найдите идентификатор (id),
-- должность и число рабочих дней сотрудника, проработавшего наименьшее время.

SELECT id, job_title, end_date-start_date AS working_days FROM employee
WHERE end_date IS NOT NULL
ORDER BY working_days
LIMIT 1;
