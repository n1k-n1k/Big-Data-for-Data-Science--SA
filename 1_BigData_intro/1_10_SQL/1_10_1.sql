-- 1.
-- Анализируя нашу базу данных, найдите сумму зарплат сотрудников Rick Sanchez и Morty Smith.
-- Истинный только Rick C-137.

SELECT sum(salary) FROM employee
WHERE (name = 'Rick' AND surname like 'Sanchez%137')
   OR (name = 'Morty' AND surname = 'Smith');
