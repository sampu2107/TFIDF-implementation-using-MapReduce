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
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
 * Rank class includes main and run methods, and the inner classes Map and
 * Reduce. It ranks the search hits in descending order by their accumulated
 * tf-idf score and saves the results to the output location in HDFS.
 * 
 * @author Sampath Kumar
 * @version 1.0
 * @since 2017-10-02
 */

public class Rank extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(Rank.class);

	/*
	 * The main method invokes ToolRunner, which creates and runs a new instance
	 * of Rank, passing the command line arguments
	 */

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Rank(), args);
		System.exit(res);
	}

	/*
	 * The run method configures the job, starts the job, waits for the job to
	 * complete, and then returns an integer value as the success flag.
	 */

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), " Rank ");
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
			String key = "Rank";
			context.write(new Text(key), new Text(line));
			LOG.info("Rank Mapper key***" + new Text(key) + "Rank Mapper value***"
					+ new Text(line));
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
			String key = new String();
			String value = new String();
			double tdIDF = 0;
			String[] tokens;
			java.util.Map<String, Double> rankMap = new HashMap<String, Double>();
			for (Text curr : postingsList) {
				tokens = curr.toString().split("\t");
				key = tokens[0];
				tdIDF = Double.valueOf(tokens[1]);
				rankMap.put(key, tdIDF);
				LOG.info("Before sorting Map key" + key + "Value" + tdIDF);
			}
			// sorting hashmap by values using Comparator
			java.util.List list = new LinkedList(rankMap.entrySet());
			java.util.Collections.sort(list, new Comparator() {
				public int compare(Object o1, Object o2) {
					return ((Comparable) ((java.util.Map.Entry) (o2))
							.getValue()).compareTo(((java.util.Map.Entry) (o1))
							.getValue());
				}
			});
			// Sorted Hashmap is copied using LinkedHashMap to preserve the
			// insertion order
			java.util.HashMap sortedHashMap = new LinkedHashMap();
			for (Iterator it = list.iterator(); it.hasNext();) {
				java.util.Map.Entry entry = (java.util.Map.Entry) it.next();
				sortedHashMap.put(entry.getKey(), entry.getValue());
				LOG.info("After sorting Map key" + entry.getKey() + "Value"
						+ entry.getValue());
			}
			java.util.Set set = sortedHashMap.entrySet();
			Iterator iterator = set.iterator();
			while (iterator.hasNext()) {
				java.util.Map.Entry me = (java.util.Map.Entry) iterator.next();
				context.write(new Text(String.valueOf(me.getKey())), new Text(
						String.valueOf(me.getValue())));
			}
		}
	}
}
