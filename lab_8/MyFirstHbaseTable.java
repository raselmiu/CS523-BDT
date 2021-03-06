import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;

public class MyFirstHbaseTable {

	private static final String TABLE_NAME = "user";
	private static final String CF_DEFAULT = "personal_details";

	public static void main(String... args) throws IOException {

		Configuration config = HBaseConfiguration.create();

		try (Connection connection = ConnectionFactory.createConnection(config); Admin admin = connection.getAdmin()) {
			HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
			table.addFamily(new HColumnDescriptor(CF_DEFAULT).setCompressionType(Algorithm.NONE));
			table.addFamily(new HColumnDescriptor("prof_details"));

			System.out.print("Creating table.... ");

			if (admin.tableExists(table.getTableName())) {
				admin.disableTable(table.getTableName());
				admin.deleteTable(table.getTableName());
			}
			admin.createTable(table);

			System.out.println(" Done!");

			@SuppressWarnings({ "deprecation", "resource" })
			HTable userTable = new HTable(config, TABLE_NAME);

			Put person1 = new Put(Bytes.toBytes("1"));

			person1.add(Bytes.toBytes("personal_details"), Bytes.toBytes("Name"), Bytes.toBytes("John"));
			person1.add(Bytes.toBytes("personal_details"), Bytes.toBytes("City"), Bytes.toBytes("Boston"));
			person1.add(Bytes.toBytes("prof_details"), Bytes.toBytes("Designation"), Bytes.toBytes("Manager"));
			person1.add(Bytes.toBytes("prof_details"), Bytes.toBytes("Salary"), Bytes.toBytes("150,000"));
			userTable.put(person1);

			Put person2 = new Put(Bytes.toBytes("2"));

			person2.add(Bytes.toBytes("personal_details"), Bytes.toBytes("Name"), Bytes.toBytes("Mary"));
			person2.add(Bytes.toBytes("personal_details"), Bytes.toBytes("City"), Bytes.toBytes("New York"));
			person2.add(Bytes.toBytes("prof_details"), Bytes.toBytes("Designation"), Bytes.toBytes("Sr. Engineer"));
			person2.add(Bytes.toBytes("prof_details"), Bytes.toBytes("Salary"), Bytes.toBytes("130,000"));

			userTable.put(person2);

			Put person3 = new Put(Bytes.toBytes("3"));

			person3.add(Bytes.toBytes("personal_details"), Bytes.toBytes("Name"), Bytes.toBytes("Bob"));
			person3.add(Bytes.toBytes("personal_details"), Bytes.toBytes("City"), Bytes.toBytes("Fremont"));
			person3.add(Bytes.toBytes("prof_details"), Bytes.toBytes("Designation"), Bytes.toBytes("Jr. Engineer"));
			person3.add(Bytes.toBytes("prof_details"), Bytes.toBytes("Salary"), Bytes.toBytes("90,000"));

			userTable.put(person3);

		}
	}
}