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
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import bigdata.comparators.*;
import bigdata.tweet.*;


public class HashtagsTriplets {

    public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("Usage: TPSpark <directory path> <day selected> ");
			System.exit(1);
		}
		
		SparkConf conf = new SparkConf().setAppName("TP Spark");
		JavaSparkContext context = new JavaSparkContext(conf);
        
		JavaRDD<String> lines = context.textFile("/raw_data/tweet_01_03_2020.nljson", 8);
        JavaRDD<JsonObject> tweets = convertLinesToTweets(lines);
		JavaPairRDD<String,String> newTriplets = getTriplets(tweets);
		
		

		JavaPairRDD<String,Tuple2<ArrayList<Tuple2<String,Integer>>, Integer>> hashtagsByUsers = getHashtagsByListOfUser(newTriplets);
		
		JavaPairRDD<Tuple2<String,ArrayList<Tuple2<String,Integer>>>,Integer> newRdd = hashtagsByUsers.mapToPair(line -> 
			new Tuple2<Tuple2<String,ArrayList<Tuple2<String,Integer>> >,Integer>(
				new Tuple2<String,ArrayList<Tuple2<String,Integer>>>(line._1(),line._2()._1()),
					line._2()._2() )
				);
		List<Tuple2<Tuple2<String,ArrayList<Tuple2<String,Integer>> >,Integer>> sortedRdd = newRdd.mapToPair(x -> x.swap()).sortByKey(false).mapToPair(x -> x.swap()).take(100);
		System.out.println(sortedRdd.get(99));
		//createHBaseTable(sortedRdd,context);
		
		context.stop();
	}

	public static JavaPairRDD<String,String> getTriplets (JavaRDD<JsonObject> tweets){
		JavaRDD<Tuple2<String,String>> triplets = tweets.flatMap(tweet->{
			ArrayList<Tuple2<String,String>> res = new ArrayList<Tuple2<String,String>>();
			try {
				JsonArray hashtags = tweet.getAsJsonObject("entities").getAsJsonArray("hashtags");
				if(hashtags.size()< 3){
					return res.iterator();
				}
				
				ArrayList<String> tripletsHashtags = extractsAllTriplets(hashtags);
				Gson gson = new Gson();
				String user = tweet.getAsJsonObject("user").get("screen_name").getAsString();
				for(String hashtag : tripletsHashtags){
					res.add(new Tuple2<String,String>(hashtag,user));
				}
				
				return res.iterator();

			} catch (Exception e) {
				return res.iterator();
			}
		});

		 return triplets.mapToPair(triplet->triplet);
	}
	
	
	public static JavaPairRDD<String,Tuple2<ArrayList<Tuple2<String,Integer>>, Integer>> getHashtagsByListOfUser(JavaPairRDD<String,String> newTriplets){
		
		Tuple2<ArrayList<Tuple2<String,Integer>>,Integer> zeroValue= new Tuple2<ArrayList<Tuple2<String,Integer>>,Integer>(new ArrayList<Tuple2<String,Integer>>(),0);
		Function2<Tuple2<ArrayList<Tuple2<String,Integer>>,Integer> ,String, Tuple2<ArrayList<Tuple2<String,Integer>>, Integer> > seqOp = new Function2<Tuple2<ArrayList<Tuple2<String,Integer>>,Integer> ,String, Tuple2<ArrayList<Tuple2<String,Integer>>, Integer>>(){
			
			@Override
			public Tuple2<ArrayList<Tuple2<String,Integer>>, Integer> call(Tuple2<ArrayList<Tuple2<String,Integer>>, Integer> accumulator,String element){
				ArrayList<Tuple2<String,Integer>> userList = new ArrayList<Tuple2<String,Integer>>(accumulator._1);
				int nbTime=0;
				for( Tuple2<String,Integer> user : userList){
					if(user._1()== element){
						user = new Tuple2<String,Integer>(user._1(), user._2()+1);
						nbTime =user._2()+ 1;
						// System.out.println(element+" -> "+ nbTime);
						break;
					}
				}
				if(nbTime==0){
					userList.add(new Tuple2<String,Integer>(element,1) );
				}

				Tuple2<ArrayList<Tuple2<String,Integer>>, Integer> newAccumulator = new Tuple2<ArrayList<Tuple2<String,Integer>>, Integer>(userList, accumulator._2 + 1); 
				return newAccumulator;
			}
        };
    
        Function2<Tuple2<ArrayList<Tuple2<String,Integer>>, Integer>, Tuple2<ArrayList<Tuple2<String,Integer>>, Integer>, Tuple2<ArrayList<Tuple2<String,Integer>>, Integer>> combOp = new Function2<Tuple2<ArrayList<Tuple2<String,Integer>>, Integer>, Tuple2<ArrayList<Tuple2<String,Integer>>, Integer>, Tuple2<ArrayList<Tuple2<String,Integer>>,Integer>>(){
			@Override
			public Tuple2<ArrayList<Tuple2<String,Integer>>, Integer> call(Tuple2<ArrayList<Tuple2<String,Integer>>, Integer> accumulator1, Tuple2<ArrayList<Tuple2<String,Integer>>, Integer> accumulator2){
				ArrayList<Tuple2<String,Integer>> userList = new ArrayList<Tuple2<String,Integer>>( accumulator1._1());

				boolean isIn = false;
				for(int i = 0; i < accumulator2._1.size(); i++){
					for(Tuple2<String,Integer> user : userList){
						if(user._1()== accumulator2._1().get(i)._1()){
							user = new Tuple2<String,Integer>(user._1(), user._2()+ accumulator2._1().get(i)._2());
							isIn =true;
							break;
						}
					}
					if(!isIn){
						userList.add(accumulator2._1.get(i));
					}
					// if(!userList.contains(accumulator2._1.get(i))){
					// 	userList.add(accumulator2._1.get(i));
					// }
				}
				Tuple2<ArrayList<Tuple2<String,Integer>>, Integer> result = new Tuple2<ArrayList<Tuple2<String,Integer>>, Integer>(userList, accumulator1._2 + accumulator2._2);
				return result;
			}
		};
		return newTriplets.aggregateByKey(zeroValue,seqOp,combOp);
	}

    

    public static ArrayList<String> extractsAllTriplets(JsonArray hashtags){
		ArrayList<String> result = new ArrayList<String>();
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
                    result.add(tuples.get(0)+","+tuples.get(1)+","+tuples.get(2));
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

	public static void createHBaseTable(List<Tuple2<Tuple2<String,ArrayList<Tuple2<String,Integer>> >,Integer>> sortedRdd ,JavaSparkContext context) {
		Configuration hbConf = HBaseConfiguration.create(context.hadoopConfiguration());
		// Information about the declaration table
		Table table = null;
		String tableName = "testSmelezan"; 
		byte[] familyName = Bytes.toBytes("hashtags");
		Connection connection = null;
		try {
			// Obtain the HBase connection.
			connection = ConnectionFactory.createConnection(hbConf);
			// Obtain the table object.
			table = connection.getTable(TableName.valueOf(tableName));
			
			Integer i = 0;
			for (Tuple2<Tuple2<String,ArrayList<Tuple2<String,Integer>> >,Integer> line : sortedRdd) {
				Gson g = new Gson();
				Put put = new Put(Bytes.toBytes("row"+i));
				put.addColumn(familyName, Bytes.toBytes("times"), Bytes.toBytes(String.format("%s",line._2)));
				put.addColumn(familyName, Bytes.toBytes("hashtags"), Bytes.toBytes(line._1()._1()));
				
				
				put.addColumn(familyName, Bytes.toBytes("users"), Bytes.toBytes(g.toJson(line._1()._2())));

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