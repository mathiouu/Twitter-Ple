package bigdata.hashtags;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;


public class HashtagsCount {
    public static PairFlatMapFunction<String,String,Integer> getHashtags = new PairFlatMapFunction<String,String,Integer>() {

		private static final long serialVersionUID = 1L;

		@Override
		public Iterator<Tuple2<String, Integer>> call(String line) {
			List<Tuple2<String, Integer>> res = new ArrayList<Tuple2<String, Integer>>();
			Gson gson = new Gson();
			try {
				JsonObject tweet = gson.fromJson(line, JsonElement.class).getAsJsonObject();
				String user = tweet.getAsJsonObject("user").get("screen_name").getAsString() ;
				JsonArray hashtags = tweet.getAsJsonObject("entities").getAsJsonArray("hashtags");
				for (int i = 0; i < hashtags.size(); i++) {
					String hashtag = hashtags.get(i).getAsJsonObject().get("text").getAsString().toLowerCase();
					
					Tuple2<String, Integer> objToAdd = new Tuple2<String, Integer> (hashtag,1);
					res.add(objToAdd);
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
		JavaPairRDD<String, Integer> tweets = lines.flatMapToPair(getHashtags);
		tweets = tweets.repartition(123);
		JavaPairRDD<String,Integer> aggr = tweets.reduceByKey((a,b)-> a+b);
		
		//System.out.println(aggr.first());
		createHBaseTable(aggr, context);
		

		context.stop();
	}

	
	public static void createHBaseTable(JavaPairRDD<String, Integer> sortedRdd,JavaSparkContext context) {
		Configuration conf = HBaseConfiguration.create(context.hadoopConfiguration());
		// Information about the declaration table
		try {
			sortedRdd.mapToPair(line->{
				Text hashtag = new Text(line._1());
				Text times = new Text(Integer.toString(line._2()));
				return new Tuple2<Text,Text>(hashtag,times);
			}).saveAsHadoopFile(
            "/user/smelezan/sequence_file",
            Text.class,
            Text.class,
            SequenceFileOutputFormat.class
			);
		
            Job job = Job.getInstance(conf, "spark TP");
            job.setJarByClass(SequenceToHBase.class);
            SequenceFileInputFormat.addInputPath(job, new Path("sequence_file"));
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setNumReduceTasks(15); //to ensure we have some parallel reducers, you can try changing this number if you want
            TableMapReduceUtil.initTableReducerJob("seb-mat-hashtags_by_users", SequenceToHBase.WriteReducer.class, job);
            job.waitForCompletion(true);
			
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}
	public static class SequenceToHBase {
		public static class WriteReducer extends TableReducer<Text, Text ,Text> {    
	
			@Override
			public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
				for (Text val : values) {  
					//here we use the key from the pairs as the key for the row in the table, but that is not mandatory
					Put put = new Put(Bytes.toBytes(key.toString()));
					//for this example there is only one column family, called "line". The column will be "line:content". Multiple columns could be created here using multiple calls to put.add, but the column families must be created beforehand (see the createTable function)
					put.addColumn(Bytes.toBytes("hashtags"), Bytes.toBytes("times"), val.toString().getBytes());
					context.write(new Text(key.toString()), put);
				}
			}
			
		}
	}
}
