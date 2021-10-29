records = LOAD '/Users/russell/Desktop/BDT-PIG/input/InputForWC.txt' using TextLoader AS (wordline:chararray);
--dump records;
tBag = FOREACH records GENERATE TOKENIZE($0, '\t ') AS wordsBag; -- now use tokenize for tab, space
--dump tBag;
flatData = FOREACH tBag GENERATE flatten($0);
--dump flatData;
grecords = GROUP flatData BY token;
--dump grecords;
recordcount = FOREACH grecords GENERATE group, COUNT (flatData);
dump recordcount; -- TO order these data, it will need to be stored with a schema (word:chararray, count:int) and order by count
STORE recordcount INTO '/Users/russell/Desktop/BDT-PIG/output';