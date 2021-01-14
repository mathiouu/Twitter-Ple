package bigdata;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.nio.charset.StandardCharsets;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.util.StatCounter;
import java.util.regex.*;
import bigdata.tweetTest.*;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import bigdata.comparators.*;

public class HashtagsTriplets {

    public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("Usage: TPSpark <directory path> <day selected> ");
			System.exit(1);
		}
		
		SparkConf conf = new SparkConf().setAppName("TP Spark");
		JavaSparkContext context = new JavaSparkContext(conf);
        // String tweetFile = getTweetFile(args[0], args[1]);

        // Pattern p = Pattern.compile("(.*tweet_)(.+)(.nljson)");

        // Matcher m = p.matcher(tweetFile);
        // String date ;
        // if(m.find()){
        //     date = m.group(2);
        // }
        // else{
        //     date = "row";
        // }
		JavaRDD<String> lines = context.textFile("/raw_data/tweet_01_03_2020.nljson,/raw_data/tweet_02_03_2020.nljson,/raw_data/tweet_03_03_2020.nljson,/raw_data/tweet_04_03_2020.nljson", 12);
        JavaRDD<JsonObject> tweets = convertLinesToTweets(lines);
		JavaPairRDD<Tuple3<String,String,String>,String> newTriplets = getTriplets(tweets);
		JavaPairRDD<Tuple3<String,String,String>,Tuple2<ArrayList<String>, Integer>> hashtagsByUsers = getHashtagsByListOfUser(newTriplets);
		
		JavaPairRDD<Tuple4<String,String,String,ArrayList<String> >,Integer> newRdd = hashtagsByUsers.mapToPair(line -> 
			new Tuple2<Tuple4<String,String,String,ArrayList<String> >,Integer>(
				new Tuple4<String,String,String,ArrayList<String>>(line._1()._1(),line._1()._2(),line._1()._3(),line._2()._1()),
					line._2._2 )
				);
		List<Tuple2<Tuple4<String,String,String,ArrayList<String> >,Integer>> sortedRdd = newRdd.mapToPair(x -> x.swap()).sortByKey(false).mapToPair(x -> x.swap()).take(1000);
		createHBaseTable(sortedRdd,context);
		
		context.stop();
	}

	public static JavaPairRDD<Tuple3<String,String,String>,String> getTriplets (JavaRDD<JsonObject> tweets){
		JavaRDD<Tuple2<Tuple3<String,String,String>,String>> triplets = tweets.flatMap(tweet->{
			ArrayList<Tuple2<Tuple3<String,String,String>,String>> res = new ArrayList<Tuple2<Tuple3<String,String,String>,String>>();
			try {
				JsonArray hashtags = tweet.getAsJsonObject("entities").getAsJsonArray("hashtags");
				if(hashtags.size()< 3){
					return res.iterator();
				}
				
				ArrayList<Tuple3<String,String,String>> tripletsHashtags = extractsAllTriplets(hashtags);
				
				String user = tweet.getAsJsonObject("user").get("screen_name").getAsString();
				for(Tuple3<String,String,String> hashtag : tripletsHashtags){
					res.add(new Tuple2<Tuple3<String,String,String>,String>(hashtag,user));
				}
				
				return res.iterator();

			} catch (Exception e) {
				return res.iterator();
			}
		});

		 return triplets.mapToPair(triplet->triplet);
	}


	public static JavaPairRDD<Tuple3<String,String,String>,Tuple2<ArrayList<String>, Integer>> getHashtagsByListOfUser(JavaPairRDD<Tuple3<String,String,String>,String> newTriplets){
		Tuple2<ArrayList<String>,Integer> zeroValue= new Tuple2<ArrayList<String>,Integer>(new ArrayList<String>(),0);
		Function2<Tuple2<ArrayList<String>,Integer> ,String, Tuple2<ArrayList<String>, Integer> > seqOp = new Function2<Tuple2<ArrayList<String>,Integer> ,String, Tuple2<ArrayList<String>, Integer>>(){
			
			@Override
			public Tuple2<ArrayList<String>, Integer> call(Tuple2<ArrayList<String>, Integer> accumulator,String element){
				ArrayList<String> userList = new ArrayList<String>(accumulator._1);
				if(!userList.contains(element)){
					userList.add(element);
				}
				Tuple2<ArrayList<String>, Integer> newAccumulator = new Tuple2<ArrayList<String>, Integer>(userList, accumulator._2 + 1); 
				return newAccumulator;
			}
        };
    
        Function2<Tuple2<ArrayList<String>, Integer>, Tuple2<ArrayList<String>, Integer>, Tuple2<ArrayList<String>, Integer>> combOp = new Function2<Tuple2<ArrayList<String>, Integer>, Tuple2<ArrayList<String>, Integer>, Tuple2<ArrayList<String>,Integer>>(){
			@Override
			public Tuple2<ArrayList<String>, Integer> call(Tuple2<ArrayList<String>, Integer> accumulator1, Tuple2<ArrayList<String>, Integer> accumulator2){
				ArrayList<String> userList = new ArrayList<String>();
				for(int i = 0; i < accumulator1._1.size(); i++){
					if(!userList.contains(accumulator1._1.get(i))){
						userList.add(accumulator1._1.get(i));
					}
				}
				
				for(int i = 0; i < accumulator2._1.size(); i++){
					if(!userList.contains(accumulator2._1.get(i))){
						userList.add(accumulator2._1.get(i));
					}
				}
				Tuple2<ArrayList<String>, Integer> result = new Tuple2<ArrayList<String>, Integer>(userList, accumulator1._2 + accumulator2._2);
				return result;
			}
		};
		
		return newTriplets.aggregateByKey(zeroValue,seqOp,combOp);
	}

    

    public static ArrayList<Tuple3<String,String,String>> extractsAllTriplets(JsonArray hashtags){
		ArrayList<Tuple3<String,String,String>> result = new ArrayList<Tuple3<String,String,String>>();
        for (int i = 0; i <= hashtags.size()-3;i++){
			String hashtag1 = hashtags.get(i).getAsJsonObject().get("text").getAsString();
            for(int j = i+1 ; j<= hashtags.size()-2;j++){	
				String hashtag2 = hashtags.get(j).getAsJsonObject().get("text").getAsString();
				if(hashtag1.equals(hashtag2)) continue;
                for(int k = j+1 ; k<= hashtags.size()-1;k++){
					String hashtag3 = hashtags.get(k).getAsJsonObject().get("text").getAsString();
					if(hashtag1.equals(hashtag3) || hashtag2.equals(hashtag3)) continue;
					ArrayList<String> tuples = new ArrayList<String>();
					tuples.add(hashtag1);
					tuples.add(hashtag2);
					tuples.add(hashtag3);
					Collections.sort(tuples);
                    result.add(new Tuple3<String,String,String>(tuples.get(0),tuples.get(1),tuples.get(2)));
                }
            }
        }
        return result;
    }

	public static String getTweetFile(String directory, String dayInArg){
		
		String dataDirectory = directory;
		int daySelected = Integer.parseInt(dayInArg);
        if(daySelected==0){
            return "/raw_data/tweet_01_03_2020_first10000.nljson";
        }
		if(daySelected < 1 || daySelected > 21){
			System.err.println("Day are included between 1 and 21");
			System.exit(1);
		}
		
		String tweetStartFilePath = dataDirectory + "/tweet_";
		String tweetEndFilePath = "_03_2020.nljson";
		StringBuilder day = new StringBuilder();
		if(daySelected < 10){
			day.append(tweetStartFilePath);
			day.append("0");
		}
		else{
			day.append(tweetStartFilePath);
		}
		return day.toString() + daySelected + tweetEndFilePath;
	}

	public static JavaRDD<JsonObject> convertLinesToTweets(JavaRDD<String> lines) {
		JavaRDD<JsonObject> tweets = lines.flatMap(line-> {
			ArrayList<JsonObject> res = new ArrayList<JsonObject>();
			Gson gson = new Gson();
			try{
				res.add(gson.fromJson(line, JsonElement.class).getAsJsonObject());
				return res.iterator();
			}
			catch(Exception e){
				return res.iterator();
			}
		});
		return tweets;
	}

	public static void createHBaseTable(List<Tuple2<Tuple4<String,String,String,ArrayList<String> >,Integer>> sortedRdd ,JavaSparkContext context) {
		Configuration hbConf = HBaseConfiguration.create(context.hadoopConfiguration());
		// Information about the declaration table
		Table table = null;
		String tableName = "seb-mat-hashtagsTriplets"; 
		byte[] familyName = Bytes.toBytes("hashtags");
		Connection connection = null;
		try {
			// Obtain the HBase connection.
			connection = ConnectionFactory.createConnection(hbConf);
			// Obtain the table object.
			table = connection.getTable(TableName.valueOf(tableName));
			
			Integer i = 0;
			for (Tuple2<Tuple4<String,String,String,ArrayList<String> >,Integer> line : sortedRdd) {
				Put put = new Put(Bytes.toBytes("row"+i));
				put.addColumn(familyName, Bytes.toBytes("times"), Bytes.toBytes(String.format("%s",line._2)));
				put.addColumn(familyName, Bytes.toBytes("hashtag_1"), Bytes.toBytes(line._1()._1()));
				put.addColumn(familyName, Bytes.toBytes("hashtag_2"), Bytes.toBytes(line._1()._2()));
				put.addColumn(familyName, Bytes.toBytes("hashtag_3"), Bytes.toBytes(line._1()._3()));
				Gson g = new Gson();
				
				put.addColumn(familyName, Bytes.toBytes("users"), Bytes.toBytes(g.toJson(line._1()._4())));

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
}
