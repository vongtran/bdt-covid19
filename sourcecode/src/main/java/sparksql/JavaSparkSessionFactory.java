package sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
public class JavaSparkSessionFactory {
	private static transient SparkSession instance;
	
	public static SparkSession getInstance(SparkConf conf) {
		if (instance == null) {
			instance = SparkSession.builder().config(conf).getOrCreate();
		}
		return instance;
	}
}
