REGISTER /Users/russell/Desktop/BDT-PIG/piggybank.jar;

movies = LOAD '/Users/russell/Desktop/BDT-PIG/lab5_2/movies.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',') AS (movieId: int, title: chararray, genres: chararray) ;

token = FOREACH movies GENERATE $0, $1, TOKENIZE($2, '|'); /*AS genres*/; -- extract 0th field (movieId), 1th field (movie title), and create for the 3rd field a tokenize that will be a bag of genres

flat_data = FOREACH token GENERATE $0, $1, FLATTEN($2); -- generate 0th field, 1st field and for each of this previous its genre. This is done through flatten

filtered = FILTER flat_data BY STARTSWITH($1, 'A') OR STARTSWITH($1, 'a'); -- filter movies starting with 'A' or 'a'

grouped = GROUP filtered BY $2; -- group movies by genres

--describe grouped;
counts = FOREACH grouped GENERATE group, COUNT(filtered); -- for each genre count the number of movies

result = ORDER counts BY group; -- order by the previous group

STORE result INTO '/Users/russell/Desktop/BDT-PIG/lab5_2/q_3/output/';

