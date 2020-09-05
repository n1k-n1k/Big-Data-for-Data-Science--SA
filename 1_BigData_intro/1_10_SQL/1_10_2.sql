-- 2.
-- Анализируя нашу базу данных, найдите среднюю зарплату среди специалистов с должность Data Science.
-- Ответ округлите до целых (round).

SELECT round(avg(salary)) FROM employee
WHERE job_title = 'Data Science';
