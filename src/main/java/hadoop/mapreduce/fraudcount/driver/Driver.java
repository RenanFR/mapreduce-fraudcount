package hadoop.mapreduce.fraudcount.driver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import hadoop.mapreduce.fraudcount.mapper.FraudCountMapper;
import hadoop.mapreduce.fraudcount.pojo.CustomerFraud;
import hadoop.mapreduce.fraudcount.reducer.FraudCountReducer;

public class Driver {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Path inputDataset = new Path(args[0]);
		Path outputDir = new Path(args[1]);
		
		Configuration configuration = new Configuration();
		@SuppressWarnings("deprecation")
		Job job = new Job(configuration, "ECOMMERCE FRAUD ANALYSIS");
		
		job.setJarByClass(Driver.class);
		job.setMapperClass(FraudCountMapper.class);
		job.setReducerClass(FraudCountReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(CustomerFraud.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, inputDataset);
		FileOutputFormat.setOutputPath(job, outputDir);
		outputDir.getFileSystem(job.getConfiguration()).delete(outputDir, true);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}
