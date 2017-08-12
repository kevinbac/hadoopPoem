package com.kevin.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


public class TangPoem {
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{

		public final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			String[] lines = line.split("");
			for (String str : lines) {
				word.set(str);
				context.write(word, one);
			}
			
		}
		
	}
	
	public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{

		private IntWritable result = new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			result.set(sum);
			context.write(key, new IntWritable(sum));
		}
		
	}
	
	public static class IntWritableDecreasingComparator extends IntWritable.Comparator {
        public int compare(WritableComparable a, WritableComparable b) {
          return -super.compare(a, b);
        }
        
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

	public static void main(String[] args) throws Exception {
		
		Configuration configuration = new Configuration();
		
		Job job = new Job(configuration, "tang poem analyse");
		job.setJarByClass(TangPoem.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		Path inPath = new Path("E:\\poem300.txt");
		Path middlePath = new Path("E:\\middle2");
		Path outPath = new Path("E:\\out2");
		FileInputFormat.addInputPath(job, inPath);
		FileOutputFormat.setOutputPath(job, middlePath);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		//System.exit(job.waitForCompletion(true) ? 0 : 1);
		if (job.waitForCompletion(true)) {
			Job sortJob = new Job(configuration, "second sort");
			sortJob.setJarByClass(WordCount.class);
			sortJob.setMapperClass(InverseMapper.class);
			FileInputFormat.addInputPath(sortJob, middlePath);
			sortJob.setInputFormatClass(SequenceFileInputFormat.class);
			FileOutputFormat.setOutputPath(sortJob, outPath);
			sortJob.setOutputKeyClass(IntWritable.class);
			sortJob.setOutputValueClass(Text.class);
			sortJob.setSortComparatorClass(IntWritableDecreasingComparator.class);
			System.exit(sortJob.waitForCompletion(true) ? 0 : 1);
		}
	}

}
