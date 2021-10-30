users = LOAD '/Users/russell/Desktop/BDT-PIG/lab5_2/users.txt' USING PigStorage('|') AS (userId:int, age:int, gender:chararray, job_title: chararray, zip:chararray); -- load users

filtered = FILTER users BY job_title == 'lawyer' AND gender == 'F'; -- filter female lawyers

ordered_desc = ORDER filtered BY age DESC; -- order male lawyers by age

oldest_f_lawyer = LIMIT ordered_desc 1;

lawyer_Id = FOREACH oldest_f_lawyer GENERATE userId;

STORE lawyer_Id INTO '/Users/russell/Desktop/BDT-PIG/lab5_2/q_2/output';
