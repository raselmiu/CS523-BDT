package lab9.q1;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkWordCount
{
	
	public static void main(String[] args) throws Exception
	{
		
		
		Configuration conf = new Configuration();
		//delete the output directory if exist

		new Path(args[1]).getFileSystem(conf).delete(new Path(args[1]), true);
		
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("wordCount").setMaster("local"));
       	
		int threshold=Integer.parseInt(args[2]);
		
		// Load our input data
		JavaRDD<String> lines = sc.textFile(args[0]);

		// Calculate word count
		JavaPairRDD<String, Integer> wordCounts = lines
					.flatMap(line -> Arrays.asList(line.split(" ")))
					.mapToPair(w -> new Tuple2<String, Integer>(w, 1))
					.reduceByKey((x, y) -> x + y)
					.filter(f -> f._2 > threshold)
					.flatMap(w ->  Arrays.asList(w._1.split("")).stream().map(m->new Tuple2<String, Integer>(m, w._2)).collect(Collectors.toList()))
					.mapToPair(f->f)
					.reduceByKey((x, y) -> x + y);
		

		// Save the word count back out to a text file, causing evaluation
		wordCounts.saveAsTextFile(args[1]);

		sc.close();
	}
}

