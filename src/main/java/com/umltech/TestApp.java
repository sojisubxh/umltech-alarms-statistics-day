package com.umltech;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class TestApp {
	public static void main(String[] args) {
		Tuple2<String, String> tuple1 = new Tuple2<>("BYN1010170420896", "500000D");

		Tuple2<String, String> tuple2 = new Tuple2<>("BYN1010170420896", "{\"lastGpsTime\":\"20190306100937\",\"totalMileage\":4159.87,\"did\":\"homer11400001221\"}");
		// static Tuple2<String, String> tuple2 = new Tuple2<>("BYN1010170420896",
		// "4159");

		SparkConf conf = new SparkConf();
		if (args.length == 0) {
			conf.setMaster("local[*]").setAppName("day_status");
		}
		List<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();
		list.add(tuple1);
		List<Tuple2<String, String>> list2 = new ArrayList<Tuple2<String, String>>();
		list2.add(tuple2);
		final JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaPairRDD<String, String> rdd1 = jsc.parallelizePairs(list);
		JavaPairRDD<String, String> rdd2 = jsc.parallelizePairs(list2);

		// System.out.println("****************"+rdd1.collect());
		// System.out.println("%%%%%%%%%%%%%%%%%"+rdd2.collect());
		// rdd1.fullOuterJoin(rdd2).collect();
	}
}
