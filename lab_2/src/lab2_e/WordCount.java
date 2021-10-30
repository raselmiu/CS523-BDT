package lab2_e;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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


public class WordCount extends Configured implements Tool
{

	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{

			for (String token : value.toString().split("\\s+"))
			{
				//token = token.toLowerCase();
				//if(token.equals("Hadoop")  || token.equals("Java")){

				word.set(token);
				context.write(word, one);
				//}
			}
		}
	}

	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		//private IntWritable result = new IntWritable();
		int c = 0;

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
//			int sum = 0;
//			for (IntWritable val : values)
//			{
//				sum += val.get();
//			}
//			
//			//if(sum >= 25){
//			result.set(sum);
//			context.write(new Text(key), result);
			//}
			c += 1;
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException {
			String txt = "Number of Unique Words = ";
			Text t = new Text();
			t.set(txt);
			IntWritable i = new IntWritable(c);
			context.write(t, i);
		}
	}
	
	 
	


	public static void main(String[] args) throws Exception
	{
		
		
		
		Configuration conf = new Configuration();
		
		// Check first if the folder exist
		String location = args[1]; // folder location
		Path path = new Path(location);
		FileSystem fs = path.getFileSystem(conf);
		
		if(fs.exists(path)) {
		    fs.delete(path, true);
		    System.out.println("Folder deleted!");
		}

		int res = ToolRunner.run(conf, new WordCount(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception
	{
		

		Job job = new Job(getConf(), "WordCount");
		job.setJarByClass(WordCount.class);

		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		//job.setNumReduceTasks(2);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
