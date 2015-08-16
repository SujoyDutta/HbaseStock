

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class MR1 {
	
	public static class Map1 extends TableMapper<Text,Text> {
		String filename;
	 	private Text txtMapOutputKey = new Text("");
	    private Text txtMapOutputValue = new Text("");
	    
	   // SimpleDateFormat sdf_input_format=new SimpleDateFormat("M/d/yyyy");
	    SimpleDateFormat sdf_input_format=new SimpleDateFormat("yyyy-MM-dd");
	    SimpleDateFormat date_output_format=new SimpleDateFormat("MM/yyyy");
		
	    
		@Override
		public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException{
//			if(Integer.parseInt(key.toString()) == 0){
//		    	return;
//		    }
						
			//extracted column families
//	    	NavigableMap<byte[], byte[]> stockhbase = value.getNoVersionMap().get(Bytes.toBytes("stock"));
//	    	NavigableMap<byte[], byte[]> timehbase = value.getNoVersionMap().get(Bytes.toBytes("time"));
//	    	NavigableMap<byte[], byte[]> pricehbase = value.getNoVersionMap().get(Bytes.toBytes("price"));
	    	
	    	

//	    	String stockname = Bytes.toString(stockhbase.get("name"));
//	    	String year = Bytes.toString(timehbase.get("yr"));
//	    	String month = Bytes.toString(timehbase.get("mm"));
//	    	String day = Bytes.toString(timehbase.get("dd"));
//	    	String closeval = Bytes.toString(pricehbase.get("price"));
			
	    	String stockname = Bytes.toString(value.getValue(Bytes.toBytes("stock"), Bytes.toBytes("name")));
	    	String year = Bytes.toString(value.getValue(Bytes.toBytes("time"), Bytes.toBytes("yr")));
	    	String month = Bytes.toString(value.getValue(Bytes.toBytes("time"), Bytes.toBytes("mm")));
	    	String day = Bytes.toString(value.getValue(Bytes.toBytes("time"), Bytes.toBytes("dd")));
	    	String closeval = Bytes.toString(value.getValue(Bytes.toBytes("price"), Bytes.toBytes("price")));
			
	    	
	    	
			//filename=((FileSplit)context.getInputSplit()).getPath().getName().toString();
			 filename = stockname;           
			
			//String line = value.toString();
			//String data_fields[] = line.split(",");
			
			 try {
			//	Date input_dateformat=sdf_input_format.parse(data_fields[0]);
			//	String output_format = date_output_format.format(input_dateformat);
			//	txtMapOutputKey.set(filename+","+output_format);
				 txtMapOutputKey.set(filename+","+month+"/"+year);
			//	txtMapOutputValue.set(data_fields[0]+","+data_fields[6]);
				 txtMapOutputValue.set(year+"-"+month+"-"+day+","+closeval);
				//output.collect(txtMapOutputKey, txtMapOutputValue);
				context.write(txtMapOutputKey, txtMapOutputValue);
			} catch (Exception e) {
				e.printStackTrace();
			} 
		}
		
	}	
				
	
	
	public static class Reducer1 extends TableReducer<Text, Text,ImmutableBytesWritable>{
		//SimpleDateFormat date_input_format=new SimpleDateFormat("M/d/yyyy");
		SimpleDateFormat date_input_format=new SimpleDateFormat("yyyy-MM-dd");
		private Text txtOutputKey = new Text("");
		
		@Override
		public void reduce(Text key, Iterable<Text> values,Context context)throws IOException, InterruptedException	{

			HashMap<Date, Double> hashmap=new HashMap<Date, Double>();
			Double max,min,outputValue;
			
		while (values.iterator().hasNext()) {
				String dates_values[]=values.iterator().next().toString().split(",");
				try {
					hashmap.put(date_input_format.parse(dates_values[0]),Double.parseDouble(dates_values[1]) );
					
				} catch (NumberFormatException e) {
					e.printStackTrace();
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
			
			try {
				max=hashmap.get(date_input_format.parseObject( date_input_format.format( Collections.max(hashmap.keySet()) ) ));
				min=hashmap.get(date_input_format.parseObject( date_input_format.format( Collections.min(hashmap.keySet()) ) ));
			
				outputValue=(max-min)/min;		
				
				String xi=outputValue.toString();
				byte[] xivalues=Bytes.toBytes(xi);
//				byte[] rowid = Bytes.toBytes(key.toString().split(",")[0]);
				byte[] rowid = Bytes.toBytes(key.toString());
				Put p = new Put(rowid);
				p.add(Bytes.toBytes("stock-month-year"), Bytes.toBytes("stock-month-year"), rowid);
				p.add(Bytes.toBytes("companyxiValues"), Bytes.toBytes("xiValues"), xivalues);
				context.write(new ImmutableBytesWritable(rowid), p);
								
				//txtOutputKey.set(key.toString().split(",")[0]);
				//context.write(txtOutputKey,  new DoubleWritable(outputValue));
				
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		
	 }	
		
	}
		
		
	
	
	
	