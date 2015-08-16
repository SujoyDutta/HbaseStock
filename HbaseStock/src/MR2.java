
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MR2 {
	
	//public static class Map2 extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	public static class Map2 extends TableMapper<Text,Text> {
		
		//public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
		public void map(ImmutableBytesWritable key, Result value, Context context)throws IOException, InterruptedException{
			
			String stockname = new String(value.getValue(Bytes.toBytes("stock-month-year"), Bytes.toBytes("stock-month-year")));
			stockname = stockname.split(",")[0];
			String xiValues=new String(value.getValue(Bytes.toBytes("companyxiValues"), Bytes.toBytes("xiValues")));
			
			context.write(new Text(stockname),new Text(xiValues));
			//String values[]=value.toString().split("\t");
			// context.write(new Text(values[0]), new DoubleWritable(Double.parseDouble(values[1])));
		}
	
	}	
	
	//public static class Reduce2 extends Reducer<Text, DoubleWritable, Text, FloatWritable> {

		public static class Reducer2 extends TableReducer<Text, Text,ImmutableBytesWritable>{	
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {			

			float sum=0;
			float x=0;
			int count=0;
			float mean;
			float summation=0;
			float productvalue;
			float volatility;
			float valu;
			List<Float> list = new ArrayList();
			
			while (values.iterator().hasNext()) {
				//x=(float)values.iterator().next().get();
				x = Float.parseFloat(values.iterator().next().toString());
				sum =sum+x;
				count++;
				list.add(x);
			    }
			if(count < 2) return;
			mean=sum/count;			
						
			for (Float float1 : list) {
				valu=float1-mean;
				summation=summation+(valu *valu);
			}
			
			productvalue=summation*((float)1/(count-1)); 
									
			volatility=(float) Math.sqrt(productvalue);
			if(volatility == 0) return;
			byte[] Volatility=Bytes.toBytes(String.valueOf(volatility));
			byte[] rowid = Bytes.toBytes(key.toString());
			Put p = new Put(rowid);
			p.add(Bytes.toBytes("stockname"), Bytes.toBytes("stockname"), rowid);
			p.add(Bytes.toBytes("Volatility"), Bytes.toBytes("Volatility"), Volatility);
			context.write(new ImmutableBytesWritable(rowid), p);
						
			//context.write(key,new FloatWritable(volatility));
		}
	}
}
