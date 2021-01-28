package bigdata.q2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import bigdata.utils.Utils;
import bigdata.utils.Lang;
import scala.Tuple2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.fs.FileSystem;

public class TweetNbByLang {

	public static FlatMapFunction<String, Tuple2<String, Integer>> getNbTweetByLang = new FlatMapFunction<String, Tuple2<String, Integer>>(){

		private static final long serialVersionUID = 1L;
	
		@Override
		public Iterator<Tuple2<String, Integer>> call(String line) {

			List<Tuple2<String, Integer>> res = new ArrayList<Tuple2<String, Integer>>();
			Gson gson = new Gson();

			try{
				JsonObject tweet = gson.fromJson(line, JsonElement.class).getAsJsonObject();
				String lang = tweet.get("lang").getAsString();

				Lang langParser = new Lang(lang);
				String key = langParser.getConvertedLang();

				Integer value = 1;

				Tuple2<String, Integer> tuple = new Tuple2<String, Integer>(key, value);

				res.add(tuple);
				return res.iterator();
			} catch (Exception e){
				return res.iterator();
			}
		}
	};
    
    public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("TweetNbByLang Spark");
		JavaSparkContext context = new JavaSparkContext(conf);
		
		// String filePath = "/raw_data/tweet_01_03_2020_first10000.nljson";

		List<JavaPairRDD<String, Integer>> listOfRdd = new ArrayList<JavaPairRDD<String, Integer>>();

		int nbDaySelected = Integer.parseInt(args[1]);
		for(int i = 1; i <= nbDaySelected; i++){

			String filePath = Utils.getTweetFile(args[0], Integer.toString(i));
			
			JavaRDD<String> lines = context.textFile(filePath, 4);

			JavaRDD<Tuple2<String, Integer>> tweets = lines.flatMap(getNbTweetByLang);
			JavaPairRDD<String, Integer> pairRddNbTweetByLang = tweets.mapToPair(tweet-> tweet).partitionBy(new HashPartitioner(30));

			pairRddNbTweetByLang.repartition(20);
			
			JavaPairRDD<String, Integer> resRDDNbTweetByLang = pairRddNbTweetByLang.reduceByKey((a,b) -> a + b).partitionBy(new HashPartitioner(30));

			listOfRdd.add(resRDDNbTweetByLang);
		}

		
		JavaPairRDD<String, Integer> rdd = listOfRdd.get(0);
		for(int i = 1; i < listOfRdd.size() ; i++){
			rdd = rdd.union(listOfRdd.get(i)).reduceByKey((a, b) -> a + b).partitionBy(new HashPartitioner(30));
			// rdd = rdd.union(listOfRdd.get(i)).distinct();
		}

		// ArrayList<String> columns = new ArrayList<String>();
		// columns.add("lang"); columns.add("times");
		// Utils.fillHBaseTable(rdd, context, "seb-mat-tweetByLang", Bytes.toBytes("tweetByLang"), columns);
		String tableName = "seb-mat-tweetByLang";
		fillHbaseWithSequenceFile(rdd, context, tableName);
		
		context.stop();
	}
		
	public static void fillHbaseWithSequenceFile(JavaPairRDD<String, Integer> rdd, JavaSparkContext context, String tableName) {
		Configuration conf = HBaseConfiguration.create(context.hadoopConfiguration());
		try {
			String path = "/user/mduban/";
			String fileName = "sequence-file-TweetNbByLang";
			String pathHadoop = path + fileName;

			// Delete repo
			FileSystem.get(conf).delete(new Path(pathHadoop), true);

			rdd.mapToPair(line->{
				Text lang = new Text(line._1());
				Text times = new Text(Integer.toString(line._2()));
				return new Tuple2<Text, Text>(lang, times);
			}).saveAsHadoopFile(pathHadoop, Text.class, Text.class, SequenceFileOutputFormat.class);
		
            Job job = Job.getInstance(conf, "TweetNbByLang");
            job.setJarByClass(SequenceToHBase.class);
            SequenceFileInputFormat.addInputPath(job, new Path(fileName));
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
			job.setNumReduceTasks(15);
			
			// Table init
            TableMapReduceUtil.initTableReducerJob(tableName, SequenceToHBase.WriteReducer.class, job);
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
					String keyString = key.toString();

					Put row = new Put(Bytes.toBytes(keyString));
					row.addColumn(Bytes.toBytes("tweetByLang"), Bytes.toBytes("lang"), key.toString().getBytes());
					row.addColumn(Bytes.toBytes("tweetByLang"), Bytes.toBytes("times"), val.toString().getBytes());
					context.write(new Text(key.toString()), row);
				}
			}
			
		}
	}
	    
}