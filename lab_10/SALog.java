package lab10;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

public class SALog {

    public static void main(String[] args) throws Exception {
        // Create a Java Spark Context
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("apacheLog").setMaster("local"));

        JavaRDD<String> logEntryLines = sc.textFile(args[0]).cache();

        // take query date range
        LocalDate start = parseDateString(args[1]);
        LocalDate end = parseDateString(args[2]);

        // predicate to check date range
        Function<LocalDate, Boolean> isWithinRange = date -> ( ! date.isBefore( start ) ) && ( date.isBefore( end ) );

        // convert log lines to LogEntry RDD
        JavaRDD<Entry> logEntries = logEntryLines
                .map(Entry::parseFromLogLine)
                .filter(Optional::isPresent)
                .map(Optional::get);

        // Q1: count 401 status code within date range
        JavaPairRDD<Integer, Integer> fourZeroOneCounts = logEntries
                .filter(le1 -> isWithinRange.call(le1.getLogDate()))
                .filter(le2 -> le2.getResponseCode() == 401)
                .mapToPair(le3 -> new Tuple2<>(401, 1))
                .reduceByKey(Integer::sum);

        // Q2: all the IPs that visited more than 20 times
        JavaPairRDD<String, Integer> accessCountByeIP = logEntries
                .mapToPair(logEntry -> new Tuple2<>(logEntry.getIpAddress(), 1))
                .reduceByKey(Integer::sum)
                .filter(t -> t._2 > 20);

        // save 401 count
        fourZeroOneCounts.saveAsTextFile(args[3]);

        // save ip address access count
        accessCountByeIP.saveAsTextFile(args[4]);

        sc.close();
    }

    private static LocalDate parseDateString(String dateString) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy");
        return LocalDate.parse(dateString, formatter);
    }
}
