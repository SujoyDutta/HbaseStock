import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

public class Main{


	public static void main(String[] args){
		
		Configuration conf = HBaseConfiguration.create();
		long start = new Date().getTime();
		System.out.println("\n**********Starting my hbase job *********\n");	
		try {
			
			HBaseAdmin admin = new HBaseAdmin(conf);
			HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("raw"));
			tableDescriptor.addFamily(new HColumnDescriptor("stock"));
			tableDescriptor.addFamily(new HColumnDescriptor("time"));
			tableDescriptor.addFamily(new HColumnDescriptor("price"));
			if (admin.isTableAvailable("raw")){
				admin.disableTable("raw");
				admin.deleteTable("raw");
			}
			HTableDescriptor tableDescriptor1 = new HTableDescriptor(TableName.valueOf("raw1"));
			tableDescriptor1.addFamily(new HColumnDescriptor("stock-month-year"));
			tableDescriptor1.addFamily(new HColumnDescriptor("companyxiValues"));
			if (admin.isTableAvailable("raw1")){
				admin.disableTable("raw1");
				admin.deleteTable("raw1");
			}
			HTableDescriptor tableDescriptor2 = new HTableDescriptor(TableName.valueOf("raw2"));
			tableDescriptor2.addFamily(new HColumnDescriptor("stockname"));
			tableDescriptor2.addFamily(new HColumnDescriptor("Volatility"));
			if (admin.isTableAvailable("raw2")){
				admin.disableTable("raw2");
				admin.deleteTable("raw2");
			}
			HTableDescriptor tableDescriptor3 = new HTableDescriptor(TableName.valueOf("raw3"));
			tableDescriptor3.addFamily(new HColumnDescriptor("finalstockname"));
			tableDescriptor3.addFamily(new HColumnDescriptor("SortedVolatility"));
			if (admin.isTableAvailable("raw3")){
				admin.disableTable("raw3");
				admin.deleteTable("raw3");
			}
			admin.createTable(tableDescriptor);
			admin.createTable(tableDescriptor1);
			admin.createTable(tableDescriptor2);
			admin.createTable(tableDescriptor3);
			
			//creating job1 
			Job job1 = Job.getInstance(conf, "Job to load raw");
			job1.setJarByClass(Main.class);
			FileInputFormat.addInputPath(job1, new Path(args[0]));
			job1.setInputFormatClass(TextInputFormat.class);
			job1.setMapperClass(Job1.Map.class);
			TableMapReduceUtil.initTableReducerJob("raw", null, job1);
			job1.setNumReduceTasks(0);
			job1.waitForCompletion(true);
			
			
			Job job2 = Job.getInstance(conf, "Job 2, to load raw1");
			job2.setJarByClass(Main.class);
			job2.setInputFormatClass(TextInputFormat.class);
			Scan scan = new Scan();
			scan.setCaching(500);        
			scan.setCacheBlocks(false);
			TableMapReduceUtil.initTableMapperJob("raw",scan,MR1.Map1.class,Text.class,Text.class,job2);
			TableMapReduceUtil.initTableReducerJob("raw1", MR1.Reducer1.class, job2);
			job2.waitForCompletion(true);
			
			
			Job job3 = Job.getInstance(conf, "Job 3, to load raw2");
			job3.setJarByClass(Main.class);
			job3.setInputFormatClass(TextInputFormat.class);
			Scan scan1 = new Scan();
			scan1.setCaching(500);        
			scan1.setCacheBlocks(false);
			TableMapReduceUtil.initTableMapperJob("raw1",scan1,MR2.Map2.class,Text.class,Text.class,job3);
			TableMapReduceUtil.initTableReducerJob("raw2", MR2.Reducer2.class, job3);
			job3.waitForCompletion(true);
			
			 
			 	Job job4 = Job.getInstance(conf, "Job 4, to load raw3");
			 	job4.setJarByClass(Main.class);
				job4.setInputFormatClass(TextInputFormat.class);
				Scan scan2 = new Scan();
				scan2.setCaching(500);        
				scan2.setCacheBlocks(false);
				TableMapReduceUtil.initTableMapperJob("raw2",scan2,MR3.Map3.class,DoubleWritable.class,Text.class,job4);
				TableMapReduceUtil.initTableReducerJob("raw3", MR3.Reducer3.class, job4);
				boolean status=job4.waitForCompletion(true);
								
				HTable table = new HTable(conf, "raw3");
				List<StringDouble> output = new ArrayList<StringDouble>();
				Scan scan4 = new Scan();
				ResultScanner scanner = table.getScanner(scan4);
				for(Result scannerResult:scanner)
				{
				//output.add(scannerResult.getValue(Bytes.toBytes("finalstockname"), Bytes.toBytes("finalstockname")).toString());
					String stockName = Bytes.toString(scannerResult.getValue(Bytes.toBytes("finalstockname"), Bytes.toBytes("finalstockname")));
					double vol = Double.parseDouble(Bytes.toString(scannerResult.getValue(Bytes.toBytes("SortedVolatility"), Bytes.toBytes("SortedVolatility"))));
					output.add(new StringDouble(stockName, vol));
				}
				Collections.sort(output);
				int count = 0;
				System.out.println("------Min volatility stocks----------");
				for(StringDouble sd : output){
					if(count == 10 || count == output.size()/2){
						System.out.println("------Max volatility stocks----------");
					}
					count++;
					System.out.println(sd);
				}
				if (status == true) {
					long end = new Date().getTime();
					System.out.println("\nJob took " + (end-start)/1000 + "seconds\n");
				}		
				admin.close();
			System.out.println("Hbase Volatility completed");
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}