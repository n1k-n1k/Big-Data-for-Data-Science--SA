-- 1.
-- Анализируя нашу базу данных, найдите сумму зарплат сотрудников Rick Sanchez и Morty Smith.
-- Истинный только Rick C-137.

select sum(salary) from employee
where (name = 'Rick' and surname like 'Sanchez%137')
   or (name = 'Morty' and surname = 'Smith');
