package bigdata.influencers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

public class Triplets {
	public static PairFlatMapFunction<String,String, Tuple2<ArrayList<String>,Integer>> getHashtags = new PairFlatMapFunction<String,String, Tuple2<ArrayList<String>,Integer>>() {

		private static final long serialVersionUID = 1L;

		@Override
		public Iterator<Tuple2<String, Tuple2<ArrayList<String>, Integer>>> call(String line) {
			List<Tuple2<String,  Tuple2<ArrayList<String>, Integer>>> res = new ArrayList<Tuple2<String,  Tuple2<ArrayList<String>, Integer>>>();
			Gson gson = new Gson();
			try {
				JsonObject tweet = gson.fromJson(line, JsonElement.class).getAsJsonObject();
				String user = tweet.getAsJsonObject("user").get("screen_name").getAsString() ;
				JsonArray hashtags = tweet.getAsJsonObject("entities").getAsJsonArray("hashtags");
				for(String hashtag: extractsAllTriplets(hashtags)){ 
					ArrayList<String> userInList = new ArrayList<String>();
					userInList.add(user);
					 Tuple2<ArrayList<String>, Integer> tuple = new  Tuple2<ArrayList<String>, Integer>(userInList,1);
					Tuple2<String,  Tuple2<ArrayList<String>, Integer>> result = new Tuple2<String, Tuple2<ArrayList<String>, Integer>>(hashtag,tuple);
					res.add(result);
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
		JavaPairRDD<String, Tuple2<ArrayList<String>,Integer>> tweets = lines.flatMapToPair(getHashtags);
		tweets  = tweets.reduceByKey((a,b) ->{
			ArrayList<String> tuple_list = new ArrayList<String>();
			tuple_list.addAll(a._1());
			tuple_list.addAll(b._1());
			return new Tuple2<ArrayList<String>,Integer>(tuple_list, a._2() + b._2());
		});
		JavaPairRDD<String, Tuple2<HashMap<String,Integer>,Integer>> aggr = tweets.mapToPair(line ->{
			HashMap<String,Integer> map = new HashMap<String,Integer>();
			for(String user : line._2()._1()){
				if(map.containsKey(user)){
					map.put(user, map.get(user) + 1);
				}
				else{
					map.put(user,1);
				}
			}
			Tuple2<HashMap<String,Integer>,Integer> tuple = new Tuple2<HashMap<String,Integer>,Integer>(map,line._2()._2());
			return new Tuple2<String, Tuple2<HashMap<String,Integer>,Integer>>(line._1(), tuple);
		});
		List<Tuple2<Tuple2<String,HashMap<String,Integer>>,Integer>> sortedRdd = aggr.mapToPair(line -> new Tuple2<Tuple2<String,HashMap<String,Integer>>,Integer>( new Tuple2<String,HashMap<String,Integer>>(line._1(),line._2()._1()), line._2()._2())).mapToPair(x -> x.swap()).sortByKey(false).mapToPair(x -> x.swap()).take(1000);
		
		JavaPairRDD<Tuple2<String,HashMap<String,Integer>>,Integer> newAggr = context.parallelizePairs(sortedRdd,30);
		newAggr = newAggr.cache();
		createHBaseTable(newAggr, context);

		JavaPairRDD<Tuple2<String,String>,Integer> influencers = newAggr.flatMapToPair(line -> {
			ArrayList<Tuple2<Tuple2<String,String>,Integer>> res = new ArrayList<Tuple2<Tuple2<String,String>,Integer>>();
			
			for(Map.Entry<String, Integer> entry : line._1()._2().entrySet())
			{
				Tuple2<String,String> hashtags_user = new Tuple2<String,String>(line._1()._1(), entry.getKey());
				res.add(new Tuple2<Tuple2<String,String>,Integer>(hashtags_user,entry.getValue()));
			}
			return res.iterator();
		}); 

		List<Tuple2<Tuple2<String, String>, Integer>> sortedInfluencers = influencers.mapToPair(x -> x.swap()).sortByKey(false).mapToPair(x -> x.swap()).take(1000);
		createHBaseTable2(context.parallelizePairs(sortedInfluencers), context);

		context.stop();
	}
	public static ArrayList<String> extractsAllTriplets(JsonArray hashtags){
		ArrayList<String> result = new ArrayList<String>();
        for (int i = 0; i <= hashtags.size()-3;i++){
			String hashtag1 = hashtags.get(i).getAsJsonObject().get("text").getAsString().toLowerCase();
            for(int j = i+1 ; j<= hashtags.size()-2;j++){	
				String hashtag2 = hashtags.get(j).getAsJsonObject().get("text").getAsString().toLowerCase();
				if(hashtag1.equals(hashtag2)) continue;
                for(int k = j+1 ; k<= hashtags.size()-1;k++){
					String hashtag3 = hashtags.get(k).getAsJsonObject().get("text").getAsString().toLowerCase();
					if(hashtag1.equals(hashtag3) || hashtag2.equals(hashtag3)) continue;
					ArrayList<String> tuples = new ArrayList<String>();
					tuples.add(hashtag1);
					tuples.add(hashtag2);
					tuples.add(hashtag3);
					Collections.sort(tuples);
                    result.add(tuples.get(0)+","+tuples.get(1)+","+tuples.get(2));
                }
            }
        }
        return result;
    }

	public static void createHBaseTable(JavaPairRDD<Tuple2<String,HashMap<String,Integer>>,Integer> sortedRdd,JavaSparkContext context) {
		Configuration conf = HBaseConfiguration.create(context.hadoopConfiguration());
		// Information about the declaration table
		try {
			sortedRdd.mapToPair(line->{
				HashMap<String,Object> map = new HashMap<String,Object>();
				Text hashtags = new Text(line._1()._1());
				Gson g = new Gson();
				map.put("users", line._1()._2());
				map.put("times",line._2());
				String infosString = g.toJson(map);
				Text infos = new Text(infosString);
				return new Tuple2<Text,Text>(hashtags,infos);
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
            TableMapReduceUtil.initTableReducerJob("seb-mat-triplets", SequenceToHBase.WriteReducer.class, job);
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
					put.addColumn(Bytes.toBytes("users"), Bytes.toBytes("infos"), val.toString().getBytes());
					context.write(new Text(key.toString()), put);
				}
			}
			
		}
	}
	public static void createHBaseTable2(JavaPairRDD<Tuple2<String, String>, Integer> sortedRdd,JavaSparkContext context) {
		Configuration conf = HBaseConfiguration.create(context.hadoopConfiguration());
		// Information about the declaration table
		try {
			sortedRdd.mapToPair(line->{
				HashMap<String,Object> map = new HashMap<String,Object>();
				Text user =new Text(line._1()._2());
				Gson g = new Gson();
				map.put("hashtags", line._1()._1());
				map.put("times",line._2());
				String infosString = g.toJson(map);
				Text infos = new Text(infosString);
				return new Tuple2<Text,Text>(user,infos);
			}).saveAsHadoopFile(
            "/user/smelezan/sequence_file2",
            Text.class,
            Text.class,
            SequenceFileOutputFormat.class
			);
		
            Job job = Job.getInstance(conf, "spark TP");
            job.setJarByClass(SequenceToHBase.class);
            SequenceFileInputFormat.addInputPath(job, new Path("sequence_file2"));
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setNumReduceTasks(15); //to ensure we have some parallel reducers, you can try changing this number if you want
            TableMapReduceUtil.initTableReducerJob("seb-mat-topkinfluencers", SequenceToHBase2.WriteReducer.class, job);
            job.waitForCompletion(true);
			
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}
	public static class SequenceToHBase2 {
		public static class WriteReducer extends TableReducer<Text, Text ,Text> {    
	
			@Override
			public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
				for (Text val : values) {  
					//here we use the key from the pairs as the key for the row in the table, but that is not mandatory
					Put put = new Put(Bytes.toBytes(key.toString()));
					//for this example there is only one column family, called "line". The column will be "line:content". Multiple columns could be created here using multiple calls to put.add, but the column families must be created beforehand (see the createTable function)
					put.addColumn(Bytes.toBytes("users"), Bytes.toBytes("infos"), val.toString().getBytes());
					context.write(new Text(key.toString()), put);
				}
			}
			
		}
	}
}
