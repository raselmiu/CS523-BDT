package lab3.q1;

import java.io.IOException;

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


public class AverageTempPerYear extends Configured implements Tool {

    public static class AverageTempPerYearMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {

        private IntWritable year = new IntWritable();
        private DoubleWritable temperature = new DoubleWritable();


        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            year.set(Integer.parseInt(line.substring(15, 19)));
            temperature.set(Double.parseDouble(line.substring(87, 92)) / 10);

            context.write(year, temperature);

        }
    }

    public static class AverageTempPerYearReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();


        @Override
        public void reduce(IntWritable year, Iterable<DoubleWritable> temperatures, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            for (DoubleWritable temperature : temperatures) {
                sum += temperature.get();
                count += 1;
            }
            result.set(sum / count);
            context.write(year, result);

        }

    }

    @Override
    public int run(String[] args) throws Exception {


        Job job = new Job(getConf(), "AverageTempPerYear");
        job.setJarByClass(AverageTempPerYear.class);

        job.setMapperClass(AverageTempPerYear.AverageTempPerYearMapper.class);
        job.setReducerClass(AverageTempPerYear.AverageTempPerYearReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

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

        int runner = ToolRunner.run(conf, new AverageTempPerYear(), args);

        System.exit(runner);
    }

}
