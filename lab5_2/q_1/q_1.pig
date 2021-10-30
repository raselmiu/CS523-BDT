users = LOAD '/Users/russell/Desktop/BDT-PIG/lab5_2/users.txt' USING PigStorage('|') AS (userId:int, age:int, gender:chararray, job_title: chararray, zip:chararray); -- load users

filtered = FILTER users BY job_title == 'lawyer' AND gender == 'M'; -- filter male lawyers

/* group all male lawyers */
group_all = GROUP filtered ALL;

-- count number of male lawyers
counts = FOREACH group_all GENERATE COUNT(filtered);

--dump counts;
STORE counts INTO '/Users/russell/Desktop/BDT-PIG/lab5_2/q_1/output';



