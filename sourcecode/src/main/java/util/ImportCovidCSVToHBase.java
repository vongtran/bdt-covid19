package util;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import entity.covid.CovidCounty;
import hbase.CovidHbaseTable;

public class ImportCovidCSVToHBase {
	private static final String APP_NAME = "Covid19 Query";
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		importCSVToHBase(sc);
		
	}
	
	public static void importCSVToHBase(JavaSparkContext sc) {
		Configuration hConf = sc.hadoopConfiguration();
		CovidHbaseTable dataStore = CovidHbaseTable.getInstance();
		JavaRDD<String> rows = sc.textFile("/user/cloudera/covid/csv/us-counties.csv");
		JavaRDD<CovidCounty> covidCounties = rows.map(CovidCounty::parse);
		covidCounties.foreach(rdd -> {
			dataStore.insertData(rdd);
		});
		
	}
	
}
