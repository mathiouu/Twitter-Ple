package bigdata;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.StatCounter;

import scala.Tuple2;

public class TPSpark {

	public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("Usage: TPSpark <file>");
			System.exit(1);
		}
		SparkConf conf = new SparkConf().setAppName("TP Spark");
		JavaSparkContext context = new JavaSparkContext(conf);

		JavaRDD<String> lines = context.textFile(args[0], 4);

		JavaRDD<Tuple2<String, Integer>> valid_city_pop = exo2(lines);
		JavaDoubleRDD pops = exo3(valid_city_pop);

		// JavaPairRDD<Double, Iterable<String>> histogram = exo4(valid_city_pop);

	}

	public static JavaRDD<Tuple2<String, Integer>> exo2(JavaRDD<String> lines) {
		JavaRDD<Tuple2<String, Integer>> city_pop = lines.map((line) -> {
			String[] tokens = line.split(",");
			String city = tokens[1];
			Integer pop = 0;
			try {
				pop = Integer.parseInt(tokens[4]);
			} catch (Exception e) {
				pop = -1;
			}
			return new Tuple2<String, Integer>(city, pop);
		});

		JavaRDD<Tuple2<String, Integer>> valid_city_pop = city_pop.filter((line) -> {
			return line._2 != -1;
		});

		// System.out.println(valid_city_pop.count())
		return valid_city_pop;
	}

	public static JavaDoubleRDD exo3(JavaRDD<Tuple2<String, Integer>> valid_city_pop) {
		JavaDoubleRDD pops = valid_city_pop.mapToDouble((line) -> {
			return (double) line._2;
		});
		// System.out.println(pops.stats().toString());
		return pops;
	}

	public static JavaPairRDD<Double, Iterable<String>> exo4(JavaRDD<Tuple2<String, Integer>> valid_city_pop) {
		JavaPairRDD<Double, Iterable<String>> histogram = valid_city_pop.groupBy(city -> {
			return log((double) valid_city_pop._2);
		});

		for (double key : histogram.countByKey().keySet()) {
			System.out.println(key + "=" + histogram.countByKey.get(key));
		}
		return histogram;
	}

}
// Country,City,AccentCity,RegionCode,Population,Latitude,Longitude

// Nombre de villes valides : 47980
// Nombre de villes :3173959
