-- 6.
-- Анализируя нашу базу данных, найдите в какой из стран за все время было наибольшее число DevOps сотрудников всех уровней
-- (Junior, Middle, Senior)? В ответе укажите страну и число сотрудников.

SELECT country, count(name) AS devops_count FROM employee e, office o
WHERE lower(job_title) like '%devops%'
  AND e.office_id = o.id
GROUP BY country
ORDER BY devops_count DESC
LIMIT 1;
