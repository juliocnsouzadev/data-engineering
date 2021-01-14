package IGTI;

import java.io.*;
import java.util.*;
import java.util.Random;
import java.text.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


public class Exemplo extends Configured implements Tool 
{          
    public static void main (final String[] args) throws Exception {   
      int res = ToolRunner.run(new Configuration(), new ExemploIGTI(), args);        
      System.exit(res);           
    }   

    public int run (final String[] args) throws Exception {
      try{ 	             	       	
          	JobConf conf = new JobConf(getConf(), ExemploIGTI.class);
		String jobName = "Dados Games";
		String pathName = "DADOS_GAME";		
		conf.setJobName(jobName);
		FileSystem fs = FileSystem.get(conf);
		Path inDir = new Path(pathName), outDir	= new Path("outputFolder");

		FileInputFormat.setInputPaths(conf, inDir);
		FileOutputFormat.setOutputPath(conf, outDir);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(MapIGTI.class);
		conf.setReducerClass(ReduceIGTI.class);
		
		JobClient.runJob(conf);
        }
        catch ( Exception e ) {
            throw e;
        }
        return 0;
     }
 
    public static class MapIGTI extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
            
      public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException{                  
		//2 nome do jogo, 4 ano, 7 venda america ,8 venda europa
		Text valueTxt = new Text();
		Text keyTxt = new Text();
		String[] data = value.toString().split(",");
		String valueFinal = data[1]   "|"    sumStrings(data[6], data[7]);
		keyTxt.set(data[3]);
		valueTxt.set(valueFinal);
		output.collect(keyTxt, valueTxt);//passa os dados para mapping
      }

      private static String sumStrings(String a, String b) {
		return String.valueOf(Double.parseDouble(a.trim())   Double.parseDouble(b.trim()));
	}        
    }
 
    
    public static class ReduceIGTI extends MapReduceBase implements Reducer<Text, Text, Text, Text> {       
       public void reduce (Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException { 
		double biggerSales = 0;
		String gameName = "";
		String[] salesData = new String[2];		
		Text value = new Text();
		while(values.hasNext()){
			value = values.next();
			salesData = value.toString().split("\\|");
			Double salesValue = Double.parseDouble(salesData[1]);
			boolean isThisSalesBigger = salesValue > biggerSales;
			biggerSales = isThisSalesBigger ? salesValue: biggerSales;
			gameName = isThisSalesBigger ? salesData[0] : gameName;
		}
		String valueFinal = gameName   "|"   String.valueOf(biggerSales);
		value.set(valueFinal);
		output.collect(key, value);
    	}
}

   public static class MapIGTIMaior extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

      public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)  throws IOException {
        

            
     }
}

    public static class ReduceIGTIMaior extends MapReduceBase implements Reducer<Text, Text, Text, Text> {   
       public void reduce (Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {     
               
  }

}
}