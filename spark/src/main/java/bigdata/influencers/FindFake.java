package bigdata.influencers;

import java.util.ArrayList;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class FindFake {
	public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("Usage: TPSpark <directory path> <day selected> ");
			System.exit(1);
		}

		SparkConf conf = new SparkConf().setAppName("TP Spark");
		JavaSparkContext context = new JavaSparkContext(conf);

		JavaRDD<String> lines = context.textFile("/raw_data/tweet_01_03_2020.nljson", 8);
		JavaRDD<Tuple2<String, InfluencerClass>> influencers = convertLinesToTweets(lines);

		JavaPairRDD<String, InfluencerClass> reducedInfluencers = reduceInfluencers(influencers);

		reducedInfluencers.saveAsNewAPIHadoopFile("/users/smelezan/influencers", Text.class, Text.class,
				TextOutputFormat.class);
		context.stop();
	}

	private static JavaPairRDD<String, InfluencerClass> reduceInfluencers(
			JavaRDD<Tuple2<String, InfluencerClass>> influencers) {
		JavaPairRDD<String, Tuple2<InfluencerClass, Integer>> res = influencers
				.mapToPair(influencer -> new Tuple2<String, Tuple2<InfluencerClass, Integer>>(influencer._1(),
						new Tuple2<InfluencerClass, Integer>(influencer._2(), 1)));
		res = res.reduceByKey((a, b) -> new Tuple2<InfluencerClass, Integer>(a._1().add(b._1()), a._2() + b._2()));
		JavaPairRDD<String, InfluencerClass> finalRes = res.mapToPair(user -> {
			return new Tuple2<String, InfluencerClass>(user._1(), user._2()._1());
		});
		return finalRes;
	}

	static JavaRDD<Tuple2<String, InfluencerClass>> convertLinesToTweets(JavaRDD<String> lines) {
		JavaRDD<Tuple2<String, InfluencerClass>> result = lines.flatMap(line -> {
			ArrayList<Tuple2<String, InfluencerClass>> res = new ArrayList<Tuple2<String, InfluencerClass>>();
			Gson gson = new Gson();
			JsonObject tweetJson = gson.fromJson(line, JsonElement.class).getAsJsonObject();
			try {
				JsonObject user = tweetJson.getAsJsonObject("user");

				String screen_name = tweetJson.getAsJsonObject("user").get("screen_name").getAsString();

				int nbRetweets = tweetJson.get("retweet_count").getAsInt();

				int nbFollowers = tweetJson.getAsJsonObject("user").get("followers_count").getAsInt();
				if (nbFollowers > 5000) {
					res.add(new Tuple2<String, InfluencerClass>(screen_name,
							new InfluencerClass(nbFollowers, nbRetweets)));
				}
				return res.iterator();
			} catch (Exception e) {
				// System.out.println(tweetJson.toString());
				return res.iterator();
			}
		});
		return result;
	}

}
