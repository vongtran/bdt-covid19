package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;


public class TweetHbaseTable
{

	private static TweetHbaseTable instance = new TweetHbaseTable();
	private Connection connection;
	private static final String TABLE_NAME = "tweets";
	private static final String CF_1 = "person_data";
	private static final String CF_2 = "tweet_data";
	private static final String CF1_CL1 = "userName";
	private static final String CF1_CL2 = "displayName";
	private static final String CF2_CL1 = "text";
	private static final String CF2_CL2 = "createAt";
	
	private TweetHbaseTable() {
		Configuration config = HBaseConfiguration.create();

		try 
		{
			connection = ConnectionFactory.createConnection(config);
			Admin admin = connection.getAdmin();
			HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
			table.addFamily(new HColumnDescriptor(CF_1).setCompressionType(Algorithm.NONE));
			table.addFamily(new HColumnDescriptor(CF_2));
			System.out.print("Creating table.... ");

			if (!admin.tableExists(table.getTableName()))
			{
				admin.createTable(table);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static TweetHbaseTable getInstance() {
		return instance;
	}
 
	public void insertData(String userName, String displayName, String text, String createAt)
	{
		try {
			StringBuilder key = new StringBuilder();
			key.append(userName).append("|").append(displayName).append("|").append(text).append("|").append(createAt);
			//Adding data
			Put row = new Put(Bytes.toBytes(key.toString()));
			puttingData(row, userName, displayName, text, createAt);
			
			Table ht = connection.getTable(TableName.valueOf(TABLE_NAME));
			ht.put(row);
		} catch (Exception e) {
			e.printStackTrace();
		}	
	}
	
	private static void puttingData(Put row, String name, String displayName, String text, String createAt) {
		row.addColumn(Bytes.toBytes(CF_1), Bytes.toBytes(CF1_CL1), Bytes.toBytes(name));
		row.addColumn(Bytes.toBytes(CF_1), Bytes.toBytes(CF1_CL2), Bytes.toBytes(displayName));
		row.addColumn(Bytes.toBytes(CF_2), Bytes.toBytes(CF2_CL1), Bytes.toBytes(text));
		row.addColumn(Bytes.toBytes(CF_2), Bytes.toBytes(CF2_CL2), Bytes.toBytes(createAt));
	}
}