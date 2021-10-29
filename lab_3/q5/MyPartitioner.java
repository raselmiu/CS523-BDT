package lab3.q5;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<IntWritable, MyPairWritable> {
    public int getPartition(IntWritable key, MyPairWritable value, int numReduceTasks) {
        return key.get() < 1930 ? 0 : 1;
    }
}
