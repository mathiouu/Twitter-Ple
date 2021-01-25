package bigdata.hashtags;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import scala.Tuple2;

public class TopKHashtags {

	public static FlatMapFunction<String,Tuple2<String,Integer>> getHashtags = new FlatMapFunction<String,Tuple2<String,Integer>>() {

		private static final long serialVersionUID = 1L;

		@Override
		public Iterator<Tuple2<String,Integer>> call(String line) {
			List<Tuple2<String,Integer>> res = new ArrayList<Tuple2<String,Integer>>();
			Gson gson = new Gson();
			try {
				JsonObject tweet = gson.fromJson(line, JsonElement.class).getAsJsonObject();
				
				JsonArray hashtags = tweet.getAsJsonObject("entities").getAsJsonArray("hashtags");
				for (int i = 0; i < hashtags.size(); i++) {
					res.add(new Tuple2<String,Integer>(hashtags.get(i).getAsJsonObject().get("text").getAsString().toLowerCase(),1));
				}
				return res.iterator();
			}

			catch (Exception e) {
				return res.iterator();
			}
		}

		
	};
    public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("Usage: TPSpark <directory path> <day selected> ");
			System.exit(1);
		}

		SparkConf conf = new SparkConf().setAppName("TP Spark");
		JavaSparkContext context = new JavaSparkContext(conf);

		
		JavaRDD<String> lines = context.textFile(args[0]);
		JavaRDD<Tuple2<String,Integer>> tweets = lines.flatMap(getHashtags) ;
		JavaPairRDD<String,Integer> hashtags = tweets.mapToPair(tweet->tweet).partitionBy(new HashPartitioner(30));
		hashtags = hashtags.reduceByKey((a,b)-> a+b);
		List<Tuple2<String,Integer>> sortedList = hashtags.mapToPair(x -> x.swap()).sortByKey(false).mapToPair(x -> x.swap()).take(10000);
		
		// hashtags.saveAsNewAPIHadoopFile("/users/smelezan/testSpark",Text.class,
		// Text.class,TextOutputFormat.class);
		// JavaRDD<String> example =
		// context.textFile("/users/smelezan/testSpark/part-r-00000", 4);
		// createHBaseTable(sortedList, context);
		//System.out.println(context.parallelizePairs(sortedList).first());
		//createHBaseTablev2(context.parallelizePairs(sortedList), context);
		createHBaseTable(sortedList, context);
		context.stop();
	}
	public static void createHBaseTable(List<Tuple2<String,Integer>> sortedRdd,JavaSparkContext context) {
		Configuration hbConf = HBaseConfiguration.create(context.hadoopConfiguration());
		// Information about the declaration table
		Table table = null;
		String tableName = "seb-mat-tophashtagsbyday"; 
		byte[] familyName = Bytes.toBytes("hashtags");
		Connection connection = null;
		try {
			// Obtain the HBase connection.
			connection = ConnectionFactory.createConnection(hbConf);
			// Obtain the table object.
			table = connection.getTable(TableName.valueOf(tableName));
			
			Integer i = 0;
			for (Tuple2<String,Integer> line : sortedRdd) {
				Put put = new Put(Bytes.toBytes(String.format("row-%s",i)));
				put.addColumn(familyName, Bytes.toBytes("times"), Bytes.toBytes(String.format("%s",line._2())));
				put.addColumn(familyName, Bytes.toBytes("hashtags"), Bytes.toBytes(line._1()));
				i += 1;
				table.put(put);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (table != null) {
				try {
					// Close the table object.
					table.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			if (connection != null) {
				try {
					// Close the HBase connection.
					connection.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
	public static void createHBaseTablev2(JavaPairRDD<String,Integer> sortedRdd,JavaSparkContext context) {
		Configuration conf = HBaseConfiguration.create(context.hadoopConfiguration());
		// Information about the declaration table
		try {
			sortedRdd.mapToPair(line-> new Tuple2<Text,Text>(new Text(line._1) ,new Text(Integer.toString(line._2)  ))).saveAsHadoopFile(
            "/user/smelezan/sequence_file",
            Text.class,
            Text.class,
            SequenceFileOutputFormat.class
			);
		
			// hbConf.set(TableInputFormat.INPUT_TABLE, "testSmelezan");
			// Job newAPIJobConfiguration1 = Job.getInstance(hbConf);
			// newAPIJobConfiguration1.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "testSmelezan");
			// newAPIJobConfiguration1.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);
			//Configuration conf = new HBaseConfiguration();
            Job job = Job.getInstance(conf, "spark TP");
            job.setJarByClass(SequenceToHBase.class);
            SequenceFileInputFormat.addInputPath(job, new Path("sequence_file"));
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setNumReduceTasks(15); //to ensure we have some parallel reducers, you can try changing this number if you want
            TableMapReduceUtil.initTableReducerJob("testSmelezan", SequenceToHBase.WriteReducer.class, job);
            job.waitForCompletion(true);
			// JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = sortedRdd.mapToPair( line -> {
			// 	System.out.println(line);
			// 	Put put = new Put(Bytes.toBytes(line._1()));
			// 	put.addColumn(Bytes.toBytes("hashtags"), Bytes.toBytes("times"), Bytes.toBytes(Integer.toString(line._2())));				
			// 	return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);    
			// });
			
			
			//hbasePuts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration1.getConfiguration());	
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}
	public static class SequenceToHBase {
		public static class WriteReducer extends TableReducer<Text, Text, Text> {    
	
			@Override
			public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
				int i = 0;
				for (Text val : values) {  
					//here we use the key from the pairs as the key for the row in the table, but that is not mandatory
					Put put = new Put(Bytes.toBytes(String.format("row-%s", i)));
					//for this example there is only one column family, called "line". The column will be "line:content". Multiple columns could be created here using multiple calls to put.add, but the column families must be created beforehand (see the createTable function)
					put.addColumn(Bytes.toBytes("hashtags"), Bytes.toBytes("times"), val.toString().getBytes());
					put.addColumn(Bytes.toBytes("hashtags"), Bytes.toBytes("hashtag"), key.toString().getBytes());
					context.write(new Text(key.toString()), put);
					i++;
				}
			}
			
		}
	}
}
