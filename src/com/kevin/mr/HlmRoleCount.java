package com.kevin.mr;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;

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
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;


public class HlmRoleCount {
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{

		private IKSegmenter iks;
		private Lexeme t;
		public final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
				iks = new IKSegmenter(new StringReader(line), true);
				while ((t = iks.next()) != null) {
					word.set(t.getLexemeText());
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
			context.write(key, result);
			
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
		job.setJarByClass(HlmRoleCount.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		Path inPath = new Path("E:\\hlm.txt");
		Path middlePath = new Path("E:\\hlmmid2");
		Path outPath = new Path("E:\\hlmout2");
		FileInputFormat.addInputPath(job, inPath);
		FileOutputFormat.setOutputPath(job, middlePath);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		if (job.waitForCompletion(true)) {
			Job sortJob = new Job(configuration, "second sort");
			sortJob.setJarByClass(HlmRoleCount.class);
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
