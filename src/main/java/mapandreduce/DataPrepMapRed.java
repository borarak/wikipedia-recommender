package mapandreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.apache.log4j.Logger;

public class DataPrepMapRed extends Configured implements Tool{
	
	private static final Logger log = Logger.getLogger(DataPrepMapRed.class);
	
	public DataPrepMapRed(){
	}
	
	public static class DataMap extends Mapper<LongWritable,Text,TextPairKey,IntWritable>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String[] dataTokens = value.toString().split("\t");
			String id = dataTokens[0];
			String username = dataTokens[1];
			String editDate = dataTokens[2];
			String editedTitle = dataTokens[3];
			log.info(id + "\t" + editedTitle);
			
			context.write(new TextPairKey(id,editedTitle), new IntWritable(1));
		}
	}
	
	public static class DataRed extends Reducer<TextPairKey, IntWritable, Text, IntWritable>{
		public void reduce(TextPairKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int editCount = 0;
			for(IntWritable val:values){
				editCount += val.get();
			}
			context.write(new Text(new String(key.getFirstElement() + "\t" + key.getSecondElement())), new IntWritable(editCount));
			
		}
	}
	
	public int run(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException{
		
		Job job = Job.getInstance(getConf());
		job.setJarByClass(DataPrepMapRed.class);
		job.setJobName("Dataprep");
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(DataMap.class);
		job.setMapOutputKeyClass(TextPairKey.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(DataRed.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		return job.waitForCompletion(true)?0:1;
		
	}
	
	public static void main(String[] args) throws Exception{
		int retCode = ToolRunner.run(new DataPrepMapRed() , args);
		System.exit(retCode);
		

	}

}
