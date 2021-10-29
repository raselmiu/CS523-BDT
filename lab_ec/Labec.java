package labec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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

public class Labec extends Configured implements Tool {

    public static class MyEcMapper extends Mapper<LongWritable, Text, MyWritableComparable, NullWritable> {
        @Override
        public void map(LongWritable lineOffset, Text line, Context context) throws IOException, InterruptedException {
            String record = line.toString();
            String stationId = record.substring(4, 10) + "-" + record.substring(10, 15);
            int year = Integer.parseInt(record.substring(15, 19));
            double temperature = Double.parseDouble(line.toString().substring(87, 92)) / 10;
            MyWritableComparable compositeKey = new MyWritableComparable(stationId, temperature, year);
            context.write(compositeKey, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String location = args[1];
        Path path = new Path(location);
        FileSystem fs = path.getFileSystem(conf);
        if (fs.exists(path)) {
            fs.delete(path, true);
            System.out.println("Output directory deleted!");
        }
        int res = ToolRunner.run(conf, new Labec(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf(), "ECLabJob");
        job.setJarByClass(Labec.class);

        job.setMapperClass(Labec.MyEcMapper.class);

        job.setMapOutputKeyClass(MyWritableComparable.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(MyWritableComparable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileSystem fs = FileSystem.get(new Configuration());

        fs.delete(new Path(args[1]), true);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
