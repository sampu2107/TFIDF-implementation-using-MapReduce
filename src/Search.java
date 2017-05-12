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
import java.util.Scanner;
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
 * Search class includes main and run methods, and the inner classes Map and
 * Reduce. It accepts the user query and outputs a list of documents with scores
 * that best matches the query. It saves the results to the output location in
 * HDFS.
 * 
 * @author Sampath Kumar
 * @version 1.0
 * @since 2017-10-02
 */

public class Search extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(Search.class);
	private static String userQuery = "";

	/*
	 * The main method accepts the user query and invokes ToolRunner, which
	 * creates and runs a new instance of Search, passing the command line
	 * arguments
	 */

	public static void main(String[] args) throws Exception {
		System.out.println("Enter your query");
		Scanner sc = new Scanner(System.in);
		userQuery = sc.nextLine();
		int res = ToolRunner.run(new Search(), args);
		System.exit(res);
	}

	/*
	 * The run method configures the job, starts the job, waits for the job to
	 * complete, and then returns an integer value as the success flag. User
	 * query is passed to the mapper through Configuration object.
	 */

	public int run(String[] args) throws Exception {
		Configuration config = new Configuration();
		config.set("UserQuery", userQuery);
		Job job = Job.getInstance(config, " Search ");
		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	/*
	 * The Map class (an extension of Mapper) transforms key/value input into
	 * intermediate key/value pairs to be sent to the Reducer.
	 */

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {

			String line = lineText.toString();
			String[] tokens = line.split("#####");
			String key = tokens[0];
			Configuration con = context.getConfiguration();
			String usrQry = con.get("UserQuery");
			String fullValue = tokens[1];
			String[] value = fullValue.split("\t");
			String fileName = value[0];
			String termFreq = value[1];
			for (String query : usrQry.split("\\s+")) {
				if (query.isEmpty()) {
					continue;
				}
				if (query.equalsIgnoreCase(key)) {
					context.write(new Text(fileName), new Text(termFreq));
				}
			}
		}
	}

	/*
	 * The reducer processes each pair, adding one to the count for the current
	 * word in the key/value pair to the overall count of that word from all
	 * mappers
	 */

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text word, Iterable<Text> postingsList,
				Context context) throws IOException, InterruptedException {
			double tfIDFScoring = 0;
			String key = new String();
			String value = new String();
			double termFreq = 0;
			key = word.toString();
			for (Text curr : postingsList) {
				value = curr.toString();
				termFreq = Double.valueOf(value);
				tfIDFScoring += termFreq;
			}
			LOG.info("TFIDFScoring**" + tfIDFScoring);
			context.write(new Text(key),
					new Text(Double.toString(tfIDFScoring)));
		}
	}
}
