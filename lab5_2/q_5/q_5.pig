REGISTER /Users/russell/Desktop/BDT-PIG/piggybank.jar;

movies = LOAD '/Users/russell/Desktop/BDT-PIG/lab5_2/movies.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',') AS (movieId: int, title: chararray, genres: chararray);

token = FOREACH movies GENERATE $0, $1, TOKENIZE($2, '|');

flat_data = FOREACH token GENERATE $0, $1, FLATTEN($2);

rating = LOAD '/Users/russell/Desktop/BDT-PIG/lab5_2/rating.txt' USING PigStorage('\t') AS (userId: int, movieId: int, rating: int, timestamp: chararray);

join_movies_rating = JOIN flat_data BY $0, rating BY $1;

adventure_movies = FILTER join_movies_rating BY $2 == 'Adventure';

adventure_movies_top_rated = FILTER adventure_movies BY $5 == 5;

movies_organized = FOREACH adventure_movies_top_rated GENERATE $0 as MovieId, $2 as Genre, $5 as Rating, $1 as Title;

distincts = DISTINCT movies_organized;

ordered_movies = ORDER distincts BY $0;

top20 = LIMIT ordered_movies 20;

STORE top20 INTO '/Users/russell/Desktop/BDT-PIG/lab5_2/q_5/output/' USING org.apache.pig.piggybank.storage.CSVExcelStorage('\t', 'NO_MULTILINE', 'WINDOWS', 'WRITE_OUTPUT_HEADER');
