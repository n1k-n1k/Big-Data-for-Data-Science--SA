-- 4.
-- Анализируя нашу базу данных, найдите число уволенных сотрудников в каждой компании:

SELECT c.name, count(e.name) FROM employee e
JOIN company c ON c.id = e.company_id
WHERE end_date IS NOT NULL
GROUP BY c.name;
