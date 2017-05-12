/*=============================================================================
|   Assignment:  Individual assignment: Programming - 2
|       Author:  Sampath Kumar Gunasekaran(sgunase2@uncc.edu)
|       Grader:  Walid Shalaby
|
|       Course:  ITCS 6190
|   Instructor:  Srinivas Akella
|     Due Date:  Feb 16 at 11:59PM
|
|     Language:  Java 
|	  Version :  1.8.0_101
|                
| Deficiencies:  No logical errors.
 *===========================================================================*/

package org.myorg;

import java.io.IOException;
import java.io.*;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * TermFrequency class includes main and run methods, and the inner classes Map
 * and Reduce. It outputs the logarithmic Term Frequency WF(t,d) for each
 * distinct word in each file and saves the results to the output location in
 * HDFS.
 * 
 * @author Sampath Kumar
 * @version 1.0
 * @since 2017-10-02
 */

public class TermFrequency extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(TermFrequency.class);

	/*
	 * The main method invokes ToolRunner, which creates and runs a new instance
	 * of TermFrequency, passing the command line arguments
	 */

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new TermFrequency(), args);
		System.exit(res);
	}

	/*
	 * The run method configures the job, starts the job, waits for the job to
	 * complete, and then returns an integer value as the success flag.
	 */

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), " TermFrequency ");
		job.setJarByClass(this.getClass());

		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	/*
	 * The Map class (an extension of Mapper) transforms key/value input into
	 * intermediate key/value pairs to be sent to the Reducer.
	 */

	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		/*
		 * Create a regular expression pattern to parse each line of input text
		 * on word boundaries ("\b"). Word boundaries include spaces, tabs, and
		 * punctuation.
		 */
		private static final Pattern WORD_BOUNDARY = Pattern
				.compile("\\s*\\b\\s*");

		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {

			String line = lineText.toString().toLowerCase();
			String key = new String();
			Text currentWord = new Text();
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String fileName = fileSplit.getPath().getName();
			for (String word : WORD_BOUNDARY.split(line)) {
				if (word.isEmpty()) {
					continue;
				}
				key = word + "#####" + fileName;
				currentWord = new Text(key);
				context.write(currentWord, one);
			}
		}
	}

	/*
	 * The reducer processes each pair, adding one to the count for the current
	 * word in the key/value pair to the overall count of that word from all
	 * mappers
	 */

	public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {
		@Override
		public void reduce(Text word, Iterable<IntWritable> counts,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			double termFrequency = 0.0;
			String weightedTermFrequency = new String();
			Text wtf = new Text();
			for (IntWritable count : counts) {
				sum += count.get();
			}
			if (sum > 0) {
				termFrequency = 1 + Math.log10(sum);
			}
			weightedTermFrequency = Double.toString(termFrequency);
			wtf = new Text(weightedTermFrequency);
			context.write(word, wtf);
		}
	}
}
