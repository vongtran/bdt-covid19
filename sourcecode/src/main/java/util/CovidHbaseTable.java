package util;

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
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;

import entity.covid.CovidCounty;


public class CovidHbaseTable
{

	private static CovidHbaseTable instance = new CovidHbaseTable();
	private Connection connection;
	private static final String TABLE_NAME = "covid19_us_counties";
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
				Integer fips = Bytes.toInt(r.getValue(CF_1, CF1_CL4));
				Integer cases = Bytes.toInt(r.getValue(CF_1, CF1_CL5));
				Integer deaths = Bytes.toInt(r.getValue(CF_1, CF1_CL6));
				list.add(new CovidCounty(date, county, state, fips, cases, deaths));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}
 
	public void insertData(CovidCounty cc) {
		insertData(cc.getDate(), cc.getCounty(), cc.getState(), cc.getFips().toString(), cc.getCases().toString(), cc.getDeaths().toString());
	}
	
	private void insertData(String date, String county, String state, String fips, String cases, String deaths)
	{
		try {
			StringBuilder key = new StringBuilder();
			key.append(date).append("|").append(county).append("|").append(state)
				.append("|").append(fips);
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
	
	public static void main(String[] args) {
		CovidHbaseTable cht = CovidHbaseTable.getInstance();
		cht.insertData("2020-01-21","Snohomish","Washington","53061","1","0");
		cht.insertData("2020-01-22","Snohomish","Washington","53061","1","0");
		cht.insertData("2020-01-23","Snohomish","Washington","53061","1","0");
		cht.insertData("2020-01-24","Cook","Illinois","17031","1","0");
		cht.insertData("2020-01-24","Snohomish","Washington","53061","1","0");
		cht.insertData("2020-01-25","Orange","California","06059","1","0");
		cht.insertData("2020-01-25","Cook","Illinois","17031","1","0");
		cht.insertData("2020-01-25","Snohomish","Washington","53061","1","0");
		cht.insertData("2020-01-26","Maricopa","Arizona","04013","1","0");
		cht.insertData("2020-01-26","Los Angeles","California","06037","1","0");
	}
}