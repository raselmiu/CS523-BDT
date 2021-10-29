package lab3.q2;

import lab3.q1.AverageTempPerYear;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


public class AverageUsingCombiner extends Configured implements Tool {

    public static class AverageTemperatureMapper extends Mapper<LongWritable, Text, IntWritable, MyPairWritable> {
        private final IntWritable year = new IntWritable();

        @Override
        public void map(LongWritable lineOffset, Text line, Context context) throws IOException, InterruptedException {
            String record = line.toString();
            // parse year from the line data
            year.set(Integer.parseInt(record.substring(15, 19)));
            // parse temperature from the line data
            double temperature = Double.parseDouble(line.toString().substring(87, 92)) / 10;
            context.write(year, new MyPairWritable(temperature, 1));
        }
    }

    public static class AverageTemperatureCombiner extends Reducer<IntWritable, MyPairWritable, IntWritable, MyPairWritable> {

        @Override
        public void reduce(IntWritable year, Iterable<MyPairWritable> temperatures, Context context) throws IOException, InterruptedException {
            int count = 0;
            double sum = 0;
            for (MyPairWritable temperature : temperatures) {
                count += temperature.getCount();
                sum += temperature.getSum();
            }
            context.write(year, new MyPairWritable(sum, count));
        }
    }

    public static class AverageTemperatureReducer extends Reducer<IntWritable, MyPairWritable, IntWritable, DoubleWritable> {
        private final DoubleWritable average = new DoubleWritable();

        @Override
        public void reduce(IntWritable year, Iterable<MyPairWritable> temperatures, Context context) throws IOException, InterruptedException {
            int count = 0;
            double sum = 0;
            for (MyPairWritable temp : temperatures) {
                count += temp.getCount();
                sum += temp.getSum();
            }
            average.set(sum / count);
            context.write(year, average);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf(), "AverageUsingCombiner");
        job.setJarByClass(AverageUsingCombiner.class);

        job.setMapperClass(AverageTemperatureMapper.class);
        job.setReducerClass(AverageTemperatureReducer.class);
        job.setCombinerClass(AverageTemperatureCombiner.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(MyPairWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileSystem fs = FileSystem.get(new Configuration());

        fs.delete(new Path(args[1]), true);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Path path = new Path(args[1]); // folder location from the argument
        FileSystem fs = path.getFileSystem(conf);

        // Check if the file exists
        if (fs.exists(path)) {
            fs.delete(path, true);
            System.out.println("Output folder deleted!");
        }

        int runner = ToolRunner.run(conf, new AverageUsingCombiner(), args);

        System.exit(runner);
    }

}
