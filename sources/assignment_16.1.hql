
DROP DATABASE IF EXISTS assignment_16_1 CASCADE;

CREATE DATABASE IF NOT EXISTS assignment_16_1;

USE assignment_16_1;

DROP TABLE IF EXISTS emp_details;
-- create the emp_details table
create EXTERNAL table IF NOT EXISTS emp_details
(
	id string,
	name string,
	sal float,
	unit string
)
row format delimited
fields terminated by '\t'
LOCATION '/user/arvind/hive/acadgild/assignments/assignment_16.1/input/';

select * from emp_details;

set mapreduce.job.reduces=1;
-- in task 1 we need to find those employees who have salary less than 100 and whose salary is maximum in the unit
-- so filetr by sal < 100 in the where clause and use max and over to find the maximum
-- salary per unit. Finally select from this table all rows whose salary = max salary
INSERT OVERWRITE LOCAL DIRECTORY '/home/arvind/hive/acadgild/assignments/assignment_16.1/output/task_1'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
select * FROM
(select id,name,unit,sal, MAX(sal) over(partition by unit) AS max_sal FROM emp_details where sal < 100) tmp where sal = max_sal;

-- in task 2 we need to find those employees whose salary is greater than the average salary
-- for the unit
-- so partition by unit and calculate the average salary for each unit
-- and then put the condition that salary should be greater than the average salary
INSERT OVERWRITE LOCAL DIRECTORY '/home/arvind/hive/acadgild/assignments/assignment_16.1/output/task_2'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
select id,name,sal,unit,avg_sal FROM
(select id,name,sal,unit,AVG(sal) over (partition by unit) AS avg_sal FROM emp_details) tmp
WHERE sal > avg_sal;
