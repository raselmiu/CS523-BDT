users = LOAD '/Users/russell/Desktop/BDT-PIG/lab5/users.csv' USING PigStorage (',') AS (name:chararray, age:int);
dump users;
filtered_ages = FILTER users BY age < 26 AND age > 17;
dump filtered_ages;
--describe filtered_ages;
pages = LOAD '/Users/russell/Desktop/BDT-PIG/lab5/pages.csv' USING PigStorage (',') AS (username:chararray, website:chararray);
--describe pages;
dump pages;
joined_data = JOIN filtered_ages BY name, pages BY username;
--jump joined_data;
sites_result = FOREACH joined_data GENERATE $3;

dump sites_result;
grouped_sites = GROUP sites_result BY $0;
--describe grouped_sites;
dump grouped_sites;

recordcount = FOREACH grouped_sites GENERATE group, COUNT ($1);
dump recordcount; -- TO order these data, it will need to be stored with a schema (word:chararray, count:int) and order by count

--describe recordcount;

orderBySitesCount = ORDER recordcount BY $1 DESC;
dump orderBySitesCount;

--describe orderBySitesCount;

topFive = LIMIT orderBySitesCount 5;
dump topFive;

STORE topFive INTO '/Users/russell/Desktop/BDT-PIG/output';
