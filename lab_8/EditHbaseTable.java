import java.io.IOException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class EditHbaseTable {

	private static final String TABLE_NAME = "user";
	private static final String CF_DEFAULT = "personal_details";

	public static void main(String... args) throws IOException {

		Configuration config = HBaseConfiguration.create();

		try (Connection connection = ConnectionFactory.createConnection(config); Admin admin = connection.getAdmin()) {
			HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
			table.addFamily(new HColumnDescriptor(CF_DEFAULT).setCompressionType(Algorithm.NONE));
			table.addFamily(new HColumnDescriptor("prof_details"));

			System.out.println(" Updating Table!");

			HTable userTable = new HTable(config, TABLE_NAME);

			// Getting Old Value

			Get bobRow = new Get(Bytes.toBytes("3"));
			Result result = userTable.get(bobRow);

			byte[] value = result.getValue(Bytes.toBytes("prof_details"), Bytes.toBytes("salary"));

			String bobOfSalary = Bytes.toString(value);

			double increasedSalary = Double.valueOf(bobOfSalary) * 1.05;

			String newSalary = String.valueOf(increasedSalary);

			Put person3 = new Put(Bytes.toBytes("3"));
			person3.add(Bytes.toBytes("prof_details"), Bytes.toBytes("Designation"), Bytes.toBytes("Sr. Engineer"));
			person3.add(Bytes.toBytes("prof_details"), Bytes.toBytes("salary"), Bytes.toBytes(newSalary));

			userTable.put(person3);
		}
	}
}