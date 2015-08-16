import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;


public class MR3 {
	
	//public static class Map3 extends Mapper<LongWritable, Text, DoubleWritable, Text> {
	
		public static class Map3 extends TableMapper<DoubleWritable,Text>	{
			public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

				String Volatility=new String(value.getValue(Bytes.toBytes("Volatility"), Bytes.toBytes("Volatility")));
				String stockname=new String(value.getValue(Bytes.toBytes("stockname"), Bytes.toBytes("stockname")));
			
				context.write(new DoubleWritable(Double.parseDouble(Volatility)), new Text(stockname));	
			//String values[]=value.toString().split("\t");
			//context.write(new DoubleWritable(Double.parseDouble(values[1])), new Text(values[0]));
		}
	}

	
	//public static class Reduce3 extends Reducer<DoubleWritable, Text, Text, NullWritable> {
	public static class Reducer3 extends TableReducer<DoubleWritable,Text,ImmutableBytesWritable>{
		
		String filename="";
		ArrayList<StringDouble> al = new ArrayList<StringDouble>();	
		
		public void reduce(DoubleWritable key, Iterable<Text> values,Context context)throws IOException, InterruptedException {
			
			while (values.iterator().hasNext()) {
				filename=values.iterator().next().toString();
			//context.write(new Text(filename), key);	
				if(!(key.toString().equals("NaN"))||!(key.get() > 0)){
					al.add(new StringDouble(filename, key.get()));
				}
			}
			
		}
	  public void cleanup(Context context)throws IOException, InterruptedException {
		  
		  int size =al.size();
		 
		 // context.write(new Text("############10 least volatile stocks###########"), NullWritable.get());
		  for(int i=0;i<10;i++){
			  StringDouble sd = al.get(i);
			  byte[] rowid = Bytes.toBytes(sd.getStr());
			  Put p = new Put(rowid);
			  p.add(Bytes.toBytes("finalstockname"), Bytes.toBytes("finalstockname"), rowid);
			  p.add(Bytes.toBytes("SortedVolatility"), Bytes.toBytes("SortedVolatility"), Bytes.toBytes(Double.toString(sd.getD())));
			  context.write(new ImmutableBytesWritable(rowid), p);		
		  }
		 // context.write(new Text("#############10 most volatile stocks###########"), NullWritable.get());
		  for(int i=size;i>(size-10);i--){
			  
			  StringDouble sd = al.get(i - 1);
			  byte[] rowid = Bytes.toBytes(sd.getStr());
			  Put p = new Put(rowid);
			  p.add(Bytes.toBytes("finalstockname"), Bytes.toBytes("finalstockname"), rowid);
			  p.add(Bytes.toBytes("SortedVolatility"), Bytes.toBytes("SortedVolatility"), Bytes.toBytes(Double.toString(sd.getD())));
			  context.write(new ImmutableBytesWritable(rowid), p);
		  }
	  }
      
			
	
		
	}
}
