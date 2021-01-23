package bigdata.influencers;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class AverageEngagement {
	public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("Usage: TPSpark <directory path> <day selected> ");
			System.exit(1);
		}

		SparkConf conf = new SparkConf().setAppName("TP Spark");
		JavaSparkContext context = new JavaSparkContext(conf);

		JavaRDD<String> lines = context.textFile("/raw_data/tweet_01_03_2020.nljson", 8);
		JavaRDD<Tuple2<String, EngagementClass>> tweets = convertLinesToTweets(lines);
		JavaPairRDD<String, EngagementClass> reducedUsers = reduceTweets(tweets);
		EngagementClass sumOfEngagement = reducedUsers.map(user -> user._2())
				.reduce((a, b) -> new EngagementClass(a.nbRetweets + b.nbRetweets, a.nbFavs + b.nbFavs,
						a.nbUrls + b.nbUrls, a.mentionned + b.mentionned));
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		System.out.println(sumOfEngagement.average((int) reducedUsers.count()).toString());
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		// System.out.println(String.format("%s : %s", reducedUsers.first()._1,
		// reducedUsers.first()._2.toString()));
		// System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		// hashtags.saveAsNewAPIHadoopFile("/users/smelezan/testSpark",Text.class,
		// Text.class,TextOutputFormat.class);
		// JavaRDD<String> example =
		// context.textFile("/users/smelezan/testSpark/part-r-00000", 4);
		// List<Tuple2<String,EngagementClass>> tweetsToList = reducedUsers.take(30);
		// System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		// for(Tuple2<String,EngagementClass> tweet : tweetsToList){
		// System.out.println(String.format("%s : %s", tweet._1, tweet._2.toString()));
		// }
		// System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		// createHBaseTable(topK,context);

		context.stop();
	}

	private static JavaPairRDD<String, EngagementClass> reduceTweets(JavaRDD<Tuple2<String, EngagementClass>> tweets) {
		JavaPairRDD<String, EngagementClass> res = tweets.mapToPair(tweet -> tweet);
		return res.reduceByKey((a, b) -> a.add(b));

	}

	static JavaRDD<Tuple2<String, EngagementClass>> convertLinesToTweets(JavaRDD<String> lines) {
		JavaRDD<Tuple2<String, EngagementClass>> result = lines.flatMap(line -> {
			ArrayList<Tuple2<String, EngagementClass>> res = new ArrayList<Tuple2<String, EngagementClass>>();
			Gson gson = new Gson();
			try {
				JsonObject tweetJson = gson.fromJson(line, JsonElement.class).getAsJsonObject();
				String screen_name = tweetJson.getAsJsonObject("user").get("screen_name").getAsString();
				int mentionned = tweetJson.getAsJsonObject("entities").getAsJsonArray("user_mentions").size();
				int nbUrls = tweetJson.getAsJsonObject("entities").getAsJsonArray("urls").size();
				int nbFavs = tweetJson.getAsJsonObject("user").get("favourites_count").getAsInt();
				int nbRetweets = tweetJson.get("retweet_count").getAsInt();
				// System.out.println("OOOOOOOOOOOOOOOOOOOOOOO");
				res.add(new Tuple2<String, EngagementClass>(screen_name,
						new EngagementClass(nbRetweets, nbFavs, nbUrls, mentionned)));
				return res.iterator();
			} catch (Exception e) {
				// System.out.println(e);
				return res.iterator();
			}
		});
		return result;
	}

}
