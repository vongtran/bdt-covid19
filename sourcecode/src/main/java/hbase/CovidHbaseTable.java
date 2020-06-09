package hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import entity.covid.CovidCounty;


public class CovidHbaseTable
{

	private static CovidHbaseTable instance = new CovidHbaseTable();
	private Connection connection;
	public static final String TABLE_NAME = "covid_us_counties";
	private static final String FAMILY_NAME1 = "covid_data";
	private static final byte[] CF_1 = "covid_data".getBytes();
	private static final byte[] CF1_CL1 = "date".getBytes();
	private static final byte[] CF1_CL2 = "county".getBytes();
	private static final byte[] CF1_CL3 = "state".getBytes();
	private static final byte[] CF1_CL4 = "fips".getBytes();
	private static final byte[] CF1_CL5 = "cases".getBytes();
	private static final byte[] CF1_CL6 = "deaths".getBytes();
	
	
	private CovidHbaseTable() {
		Configuration config = HBaseConfiguration.create();

		try 
		{
			connection = ConnectionFactory.createConnection(config);
			Admin admin = connection.getAdmin();
			HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
			table.addFamily(new HColumnDescriptor(FAMILY_NAME1).setCompressionType(Algorithm.NONE));
			
			System.out.print("Creating table.... ");

			if (!admin.tableExists(table.getTableName()))
			{
				admin.createTable(table);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static CovidHbaseTable getInstance() {
		return instance;
	}
	
	public List<CovidCounty> getAll() {
		List<CovidCounty> list = new ArrayList<CovidCounty>();
		try {
			Table ht = connection.getTable(TableName.valueOf(TABLE_NAME));
			Scan scan = new Scan();
			ResultScanner rs = ht.getScanner(scan);
			for (Result r : rs) {
				String date = Bytes.toString(r.getValue(CF_1, CF1_CL1));
				String county = Bytes.toString(r.getValue(CF_1, CF1_CL2));
				String state = Bytes.toString(r.getValue(CF_1, CF1_CL3));
				Integer fips = Integer.valueOf(Bytes.toString(r.getValue(CF_1, CF1_CL4)));
				Integer cases = Integer.valueOf(Bytes.toString(r.getValue(CF_1, CF1_CL5)));
				Integer deaths = Integer.valueOf(Bytes.toString(r.getValue(CF_1, CF1_CL6)));
				list.add(new CovidCounty(date, county, state, fips, cases, deaths));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}
 
	public void insertData(CovidCounty cc) {
		insertData(cc.getKey(), cc.getDate(), cc.getCounty(), cc.getState(), cc.getFips().toString(), cc.getCases().toString(), cc.getDeaths().toString());
	}
	
	private void insertData(String key, String date, String county, String state, String fips, String cases, String deaths)
	{
		try {
			//Adding data
			Put row = new Put(Bytes.toBytes(key.toString()));
			puttingData(row, date, county, state, fips, cases, deaths);
			
			Table ht = connection.getTable(TableName.valueOf(TABLE_NAME));
			ht.put(row);
		} catch (Exception e) {
			e.printStackTrace();
		}	
	}
	
	private static void puttingData(Put row, String date, String county, String state, String fips, String cases, String deaths) {
		row.addColumn(CF_1, CF1_CL1, Bytes.toBytes(date));
		row.addColumn(CF_1, CF1_CL2, Bytes.toBytes(county));
		row.addColumn(CF_1, CF1_CL3, Bytes.toBytes(state));
		row.addColumn(CF_1, CF1_CL4, Bytes.toBytes(fips));
		row.addColumn(CF_1, CF1_CL5, Bytes.toBytes(cases));
		row.addColumn(CF_1, CF1_CL6, Bytes.toBytes(deaths));
	}
	
	public static Put createPutObject(CovidCounty cc) {
		Put row = new Put(Bytes.toBytes(cc.getKey()));
		puttingData(row, cc.getDate(), cc.getCounty(), cc.getState(), cc.getFips().toString(), cc.getCases().toString(), cc.getDeaths().toString());
		return row;
	}
	
	public void insertData(Configuration conf, JavaRDD<CovidCounty> row) throws IOException {
		Job job = Job.getInstance(conf);
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, TABLE_NAME);
		job.setOutputFormatClass(TableOutputFormat.class);
		JavaPairRDD<ImmutableBytesWritable, Put> hbasePut = row.mapToPair(new CovidPairFunction());
		hbasePut.saveAsNewAPIHadoopDataset(job.getConfiguration());
	}
}