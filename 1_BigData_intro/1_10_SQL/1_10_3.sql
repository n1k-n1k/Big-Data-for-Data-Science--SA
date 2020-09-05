-- 3.
-- Анализируя нашу базу данных, найдите самое популярное имя среди сотрудников компании GoGoogle.

-- джоин:
SELECT e.name FROM employee e
JOIN company c ON c.id = e.company_id
WHERE c.name = 'GoGoogle'
GROUP BY e.name
ORDER BY count(e.name) DESC
LIMIT 1;

-- или вложенный запрос без джоина:
SELECT name FROM employee
WHERE company_id IN (SELECT id FROM company WHERE name = 'GoGoogle')
GROUP BY name
ORDER BY count(name) DESC
LIMIT 1;
