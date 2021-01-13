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
import org.apache.spark.util.StatCounter;
import java.util.regex.*;
import bigdata.tweetTest.*;
import scala.Tuple2;
import scala.Tuple3;
import bigdata.comparators.*;

public class HashtagsTriplets {

    public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("Usage: TPSpark <directory path> <day selected> ");
			System.exit(1);
		}
		
		SparkConf conf = new SparkConf().setAppName("TP Spark");
		JavaSparkContext context = new JavaSparkContext(conf);
        String tweetFile = getTweetFile(args[0], args[1]);

        Pattern p = Pattern.compile("(.*tweet_)(.+)(.nljson)");

        Matcher m = p.matcher(tweetFile);
        String date ;
        if(m.find()){
            date = m.group(2);
        }
        else{
            date = "row";
        }
        JavaRDD<String> lines = context.textFile(tweetFile, 8);
        JavaRDD<JsonObject> tweets = convertLinesToTweets(lines);
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

		JavaPairRDD<Tuple3<String,String,String>,String> newTriplets= triplets.mapToPair(triplet->triplet);
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
		
		JavaPairRDD<Tuple3<String,String,String>,Tuple2<ArrayList<String>, Integer>> hashtagsByUsers = newTriplets.aggregateByKey(zeroValue,seqOp,combOp);
		// List<Tuple2<Tuple3<String, String, String>, Tuple2<ArrayList<String>, Integer>>> orderedRdd = hashtagsByUsers.takeOrdered((int)hashtagsByUsers.count(), new NbHashtagComparatorTriplets());
		// System.out.println(orderedRdd.get(0));
		// createHBaseTable(topK,context);
		
		context.stop();
	}

    // public static JavaPairRDD<Tuple3<String,String,String>,Tuple2<Integer,JsonObject>> getTriplets(JavaRDD<JsonObject> tweets){
    //     JavaRDD<Tuple2<Tuple3<String,String,String>,JsonObject>> triplets = tweets.flatMap(tweet ->{
    //         ArrayList<Tuple2<Tuple3<String,String,String>,JsonObject>> res = new ArrayList<Tuple2<Tuple3<String,String,String>,JsonObject>>();
    //         try {
    //             JsonArray hashtags = tweet.getAsJsonObject("entities").getAsJsonArray("hashtags");
    //             if(hashtags.size()< 3){
    //                 return res.iterator();
    //             }
    //             //ArrayList<Tuple3<String,String,String>> tripletsHashtags = extractsAllTriplets(hashtags);
    //             ArrayList<Tuple3<String,String,String>> tripletsHashtags = new ArrayList<Tuple3<String,String,String>>(new Tuple3("test","test","test"));
                
    //             // JsonObject user = tweet.getAsJsonObject("user");
    //             // for(Tuple3<String,String,String> hashtag : result){
    //             //     res.add(new Tuple2<Tuple3<String,String,String>,JsonObject>(hashtag,user));
	// 			// }
	// 			res.add(tripletsHashtags);
    //             return res.iterator();

    //         } catch (Exception e) {
    //             return res.iterator();
    //         }
    //     });
        
    //     return triplets.mapToPair(triplet->triplet);
    // }

    public static ArrayList<Tuple3<String,String,String>> extractsAllTriplets(JsonArray hashtags){
		ArrayList<Tuple3<String,String,String>> result = new ArrayList<Tuple3<String,String,String>>();
        for (int i = 0; i <= hashtags.size()-3;i++){
            for(int j = i+1 ; j<= hashtags.size()-2;j++){
                for(int k = j+1 ; k<= hashtags.size()-1;k++){
					ArrayList<String> tuples = new ArrayList<String>();
					tuples.add(hashtags.get(i).getAsJsonObject().get("text").getAsString());
                    tuples.add(hashtags.get(j).getAsJsonObject().get("text").getAsString());
                    tuples.add(hashtags.get(k).getAsJsonObject().get("text").getAsString());
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
	public static List<Tuple2<Tuple2<String,String>,Integer>> getTopK(JavaPairRDD<Tuple2<String,String>,Integer> hashtags, int k){
		List<Tuple2<Tuple2<String,String>,Integer>> data = hashtags.takeOrdered(100, new NbHashtagComparator());
		return data;
	}

	public static void createHBaseTable(List<JavaPairRDD<Tuple2<String,String>, Integer>> listOfRdd,JavaSparkContext context) {
		Configuration hbConf = HBaseConfiguration.create(context.hadoopConfiguration());
		// Information about the declaration table
		Table table = null;
		String tableName = "seb-mat-hashtags"; 
		byte[] familyName = Bytes.toBytes("hashtags");
		Connection connection = null;
		try {
			// Obtain the HBase connection.
			connection = ConnectionFactory.createConnection(hbConf);
			// Obtain the table object.
			table = connection.getTable(TableName.valueOf(tableName));
			JavaPairRDD<Tuple2<String,String>, Integer> rdd = listOfRdd.get(0);
			for(int i = 1; i < listOfRdd.size() ; i++){
				rdd = rdd.union(listOfRdd.get(i));
			}
			JavaPairRDD<Tuple2<String,String>, Integer> allTopK = rdd.mapToPair(x -> x.swap()).sortByKey(false).mapToPair(x -> x.swap());

			List<Tuple2<Tuple2<String,String>,Integer>> topKMap = allTopK.collect();
			Integer i = 0;
			for (Tuple2<Tuple2<String,String>,Integer> line : topKMap) {
				Put put = new Put(Bytes.toBytes(line._1._2+"-"+i));
				put.addColumn(familyName, Bytes.toBytes("times"), Bytes.toBytes(String.format("%s",line._2)));
				put.addColumn(familyName, Bytes.toBytes("hashtag"), Bytes.toBytes(line._1._1));
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
