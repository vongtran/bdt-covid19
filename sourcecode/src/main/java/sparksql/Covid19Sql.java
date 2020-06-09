package sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import entity.covid.CovidCounty;
import hbase.CovidHbaseTable;

public class Covid19Sql {
	private static Covid19Sql instance = new Covid19Sql();
	public static final String TABLE_NAME = CovidHbaseTable.TABLE_NAME;
	static final String APP_NAME = "Covid19 Query";
	private SparkSession sparkSession;
	private Covid19Sql() {
		SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster("local[*]");
		sparkSession = JavaSparkSessionFactory.getInstance(conf);
		sparkSession.createDataFrame(CovidHbaseTable.getInstance().getAll(), CovidCounty.class)
			.createOrReplaceTempView(TABLE_NAME);
	}
	
	public static Covid19Sql getInstance() {
		return instance;
	}
	
	public void query(String sqlStatement) {
		Dataset<Row> result = sparkSession.sql(sqlStatement);
		result.show();
	}
	
}
