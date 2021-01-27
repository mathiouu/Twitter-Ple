package bigdata.q2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
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

public class UsersHashtags {

	public static FlatMapFunction<String, Tuple2<Long, Tuple2<String, List<String>>>> getUserHashtags = new FlatMapFunction<String, Tuple2<Long, Tuple2<String, List<String>>>>(){
		private static final long serialVersionUID = 1L;
	
		@Override
		public Iterator<Tuple2<Long, Tuple2<String, List<String>>>> call(String line) {

			List<Tuple2<Long, Tuple2<String, List<String>>>> res = new ArrayList<Tuple2<Long, Tuple2<String, List<String>>>>();
			Gson gson = new Gson();

			try{
				JsonObject tweet = gson.fromJson(line, JsonElement.class).getAsJsonObject();
				Long key = tweet.getAsJsonObject("user").get("id_str").getAsLong();
				String userName = tweet.getAsJsonObject("user").get("screen_name").getAsString();

				List<String> hashtagsList = new ArrayList<String>();
				JsonArray hashtags = tweet.getAsJsonObject("entities").getAsJsonArray("hashtags");
				if(hashtags.size() < 1){
					throw new Exception("test exception");
				}
				for(int i = 0 ; i < hashtags.size(); i++){

					String hashtagsText = hashtags.get(i).getAsJsonObject().get("text").getAsString();
					hashtagsList.add(hashtagsText.toLowerCase());
				}
				
				Tuple2<String, List<String>> value = new Tuple2<String, List<String>>(userName, hashtagsList);
				Tuple2<Long, Tuple2<String, List<String>>> tuple = new Tuple2<Long, Tuple2<String, List<String>>>(key, value);

				res.add(tuple);
				return res.iterator();
			} catch (Exception e){
				return res.iterator();
			}
		}
	};

    public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("TP Spark");
		JavaSparkContext context = new JavaSparkContext(conf);	

		// String filePath = "/raw_data/tweet_01_03_2020_first10000.nljson";

		List<JavaPairRDD<String, String>> listOfRdd = new ArrayList<JavaPairRDD<String, String>>();

		int nbDaySelected = Integer.parseInt(args[1]);
		for(int i = 1; i <= nbDaySelected; i++){

			String filePath = Utils.getTweetFile(args[0], Integer.toString(i));

			JavaRDD<String> lines = context.textFile(filePath, 4);

			JavaRDD<Tuple2<Long, Tuple2<String, List<String>>>> tweets = lines.flatMap(getUserHashtags);
			JavaPairRDD<Long, Tuple2<String, List<String>>> pairRddUserHashtags = tweets.mapToPair(tweet-> tweet).partitionBy(new HashPartitioner(30));

			pairRddUserHashtags.repartition(20);

			JavaPairRDD<String, String> userHashTags = getUsersHashtags(pairRddUserHashtags);

			listOfRdd.add(userHashTags);
		}

		JavaPairRDD<String, String> rdd = listOfRdd.get(0);
		for(int i = 1; i < listOfRdd.size() ; i++){
			rdd = rdd.union(listOfRdd.get(i)).reduceByKey((a, b) -> a + b).partitionBy(new HashPartitioner(30));
			// rdd = rdd.union(listOfRdd.get(i)).distinct();
		}

		// ArrayList<String> columns = new ArrayList<String>();
		// columns.add("user");
		// columns.add("hashTags");
		// Utils.fillHBaseTable1(rdd, context, "seb-mat-userHashtags", Bytes.toBytes("userHashtags"), columns);
	
		String tableName = "seb-mat-userHashtags";
		fillHbaseWithSequenceFile(rdd, context, tableName);
		
		context.stop();
	}
	
	public static void fillHbaseWithSequenceFile(JavaPairRDD<String, String> rdd, JavaSparkContext context, String tableName) {
		Configuration conf = HBaseConfiguration.create(context.hadoopConfiguration());
		try {
			String path = "/user/mduban/";
			String fileName = "sequence-file-UserHashtags";
			String pathHadoop = path + fileName;

			// Delete repo
			FileSystem.get(conf).delete(new Path(pathHadoop), true);

			rdd.mapToPair(line->{
				Text user = new Text(line._1());
				Text hashtags = new Text(line._2());
				return new Tuple2<Text, Text>(user, hashtags);
			}).saveAsHadoopFile(pathHadoop, Text.class, Text.class, SequenceFileOutputFormat.class);
		
            Job job = Job.getInstance(conf, "UserNbTweet");
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
					row.addColumn(Bytes.toBytes("userHashtags"), Bytes.toBytes("user"), key.toString().getBytes());
					row.addColumn(Bytes.toBytes("userHashtags"), Bytes.toBytes("hashtags"), val.toString().getBytes());
					context.write(new Text(key.toString()), row);
				}
			}
			
		}
	}


	public static JavaPairRDD<String, String> getUsersHashtags(JavaPairRDD<Long, Tuple2<String, List<String>>> userHashtags) {

		// Reducer (without duplicates)
		JavaPairRDD<Long, Tuple2<String, List<String>>> reducer = userHashtags.reduceByKey((a,b) -> {
			List<String> hashTagsListA = a._2();
			List<String> hashTagsListB = b._2();

			hashTagsListA.addAll(hashTagsListB);
			
			StringBuilder str = new StringBuilder();
			str.append(a._1());
			str.append(",");
			str.append(b._1());
			String tuple_1 = str.toString();
			List<String> tuple_2 = new ArrayList<>(new HashSet<>(hashTagsListA));

			Tuple2<String, List<String>> res = new Tuple2<String, List<String>>(tuple_1, tuple_2);
			return res;
		}).partitionBy(new HashPartitioner(30));

		JavaPairRDD<String, String> pairRDDRes = reducer.mapToPair((tuple) -> {
			try{
				String userNameList = tuple._2()._1();
				String splitted[] = userNameList.split(",");

				String key = '@' + splitted[splitted.length - 1];

				List<String> hashtagsWithoutDuplicates = tuple._2()._2();
				StringBuilder value = new StringBuilder();

				for(String hashtag : hashtagsWithoutDuplicates){
					value.append(hashtag);
					value.append(",");
				}

				Tuple2<String, String> res = new Tuple2<String, String>(key, value.toString());
				return res;
			} catch (Exception e){
				String key = "unvalid userName";
				String value = "unvalid #";
				Tuple2<String, String> res = new Tuple2<String, String>(key, value);
				return res;
			}
		}).partitionBy(new HashPartitioner(30));

		return pairRDDRes;
	}
}