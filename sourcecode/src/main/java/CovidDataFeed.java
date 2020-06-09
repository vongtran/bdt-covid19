import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import config.CovidKafkaConfig;
import entity.covid.CovidCounty;
import hbase.CovidHbaseTable;
import kafka.serializer.StringDecoder;

public class CovidDataFeed {
	
	public static void main(String[] args) {
		
		SparkConf sparkConf = new SparkConf().setAppName("TwitterData").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		readStreamingFromKafka(sc);
		
	}
	
	public static void readStreamingFromKafka(JavaSparkContext sc) {
		Map<String, String> kafkaParams = new HashedMap();
		kafkaParams.put("bootstrap.servers", CovidKafkaConfig.KAFKA_BROKERS);
		kafkaParams.put("fetch.message.max.bytes", String.valueOf(CovidKafkaConfig.MESSAGE_SIZE));
		kafkaParams.put(CovidKafkaConfig.KEY_SERIALIZER_KEY, CovidKafkaConfig.KEY_SERIALIZER_VALUE);
		kafkaParams.put(CovidKafkaConfig.VALUE_SERIALIZER_KEY, CovidKafkaConfig.VALUE_SERIALIZER_VALUE);
		Set<String> topicName = Collections.singleton(CovidKafkaConfig.TOPIC_NAME);
		Configuration hConf = sc.hadoopConfiguration();
		CovidHbaseTable dataStore = CovidHbaseTable.getInstance();
		try (JavaStreamingContext jsc = new JavaStreamingContext(sc, new Duration(30000))) {
			
			JavaPairInputDStream<String, String> kafkaStream = KafkaUtils
					.createDirectStream(jsc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicName);
			JavaDStream<CovidCounty> covidCounties = kafkaStream.map(CovidCounty::parse);
			covidCounties.foreachRDD(rdd -> {
				if (!rdd.isEmpty()) {
					dataStore.insertData(hConf, rdd);
				}
			});
			jsc.start();
			jsc.awaitTermination();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	
}
