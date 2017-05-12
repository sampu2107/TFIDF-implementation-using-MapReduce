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
import java.util.StringTokenizer;
import java.util.HashMap;
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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.conf.Configuration;

/**
 * TFIDF class includes main and run methods, and the inner classes Map and
 * Reduce. It outputs the TF-IDF values for each distinct word in each file and
 * saves the results to the output location in HDFS.
 * 
 * @author Sampath Kumar
 * @version 1.0
 * @since 2016-09-09
 */

public class TFIDF extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(TFIDF.class);
	private static long TOTAL_NO_OF_DOCS = 0;

	/*
	 * The main method invokes the TermFrequency ToolRunner, which creates and
	 * runs a new instance of TermFrequency job first, and the next TFIDF job
	 * takes the output files of the first job upon successful completion and
	 * runs a new instance of TFIDF passing the command line arguments
	 */

	public static void main(String[] args) throws Exception {
		int termFreq = ToolRunner.run(new TermFrequency(), args);
		if (termFreq == 0) {
			int res = ToolRunner.run(new TFIDF(), args);
			System.exit(res);
		}
	}

	/*
	 * The run method configures the job, starts the job, waits for the job to
	 * complete, and then returns an integer value as the success flag. Total
	 * number of files in the collection is passed to the reducer by setting the
	 * parameter in the configuration instance and getting it in the reducer
	 * class through the context object.
	 */

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		Path input = new Path(args[0]);
		ContentSummary cs = fs.getContentSummary(input);
		TOTAL_NO_OF_DOCS = cs.getFileCount();
		Configuration config = new Configuration();
		config.set("TotalDocumentsCount", String.valueOf(TOTAL_NO_OF_DOCS));
		Job tfJob = Job.getInstance(config, " TFIDF ");
		tfJob.setJarByClass(this.getClass());
		FileInputFormat.addInputPath(tfJob, new Path(args[1]));
		FileOutputFormat.setOutputPath(tfJob, new Path(args[2]));
		tfJob.setMapperClass(TFIDFMap.class);
		tfJob.setReducerClass(TFIDFReduce.class);
		tfJob.setOutputKeyClass(Text.class);
		tfJob.setOutputValueClass(Text.class);
		return tfJob.waitForCompletion(true) ? 0 : 1;
	}

	/*
	 * The Map class (an extension of Mapper) transforms key/value input into
	 * intermediate key/value pairs to be sent to the Reducer.
	 */

	public static class TFIDFMap extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {

			String line = lineText.toString().toLowerCase();
			String[] tokens = line.split("#####");
			String key = tokens[0];
			String fullValue = tokens[1];
			String[] value = fullValue.split("\t");
			String fileName = value[0];
			String termFreq = value[1];
			Text currentWord = new Text();
			Text currentValue = new Text();
			String finalValue = fileName + "=" + termFreq;
			currentWord = new Text(key);
			currentValue = new Text(finalValue);
			LOG.info("TFIDF Mapper key***" + currentWord + "TFIDF Mapper value***"
					+ currentValue);
			context.write(currentWord, currentValue);
		}
	}

	/*
	 * The reducer processes each pair, adding one to the count for the current
	 * word in the key/value pair to the overall count of that word from all
	 * mappers
	 */

	public static class TFIDFReduce extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text word, Iterable<Text> postingsList,
				Context context) throws IOException, InterruptedException {
			long noOfTermDocs = 0;
			double inverseDocFreq = 0;
			double tfIDF = 0;
			double termFreq = 0;
			String key = new String();
			String[] value;
			java.util.Map<String, Double> tfMap = new HashMap<String, Double>();
			Configuration con = context.getConfiguration();
			long totalDocuments = Long.valueOf(con.get("TotalDocumentsCount"));
			key = word.toString();
			for (Text curr : postingsList) {
				noOfTermDocs++;
				value = curr.toString().split("=");
				key = word.toString() + "#####" + value[0];
				termFreq = Double.valueOf(value[1]);
				tfMap.put(key, termFreq);
			}
			inverseDocFreq = Math.log10(1 + (totalDocuments / noOfTermDocs)); // IDF is calculated
			for (java.util.Map.Entry<String, Double> entry : tfMap.entrySet()) {
				tfIDF = entry.getValue() * inverseDocFreq; // tfidf is calculated
				key = entry.getKey();
				context.write(new Text(key), new Text(Double.toString(tfIDF)));
				LOG.info("Wrote TFIDF Content in HDFS " + key + " " + tfIDF);
			}
		}
	}
}
