import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class Imdb {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
FileSplit fileSplit = (FileSplit)context.getInputSplit();
String filename = fileSplit.getPath().getName();

      StringTokenizer itr = new StringTokenizer(value.toString(),";");
	  String firstWord="";
	  for(int i=0;itr.hasMoreTokens();i++)
	  {
  //System.out.println("\n\n\n\n\nThis is iteration"+i);
      if(i==1)
      {
          String temp = itr.nextToken();
        if(!temp.equals("movie"))
        {
         // System.out.println("\n\n\n\n\nthis is not a movie\n\n\n\n\n");
          break;
        }
      }


		 else if(i==3)
		  {


			firstWord+=(itr.nextToken());
	try{
	Integer.parseInt(firstWord);

}

catch(Exception e)
{
return;
}
     // System.out.println("The year value is "+firstWord);
		 if(Integer.parseInt(firstWord)<2000)
      {
        break;
      }
    	firstWord+="_";
     // System.out.println("The variable firstword is given the value "+firstWord);


      }// if==3  condition
			
		
		else if(i==4)
		{
      String lastword = itr.nextToken();
//System.out.println("The value of last word is "+lastword);

		if(lastword.contains("\\N"))
		{break;}
	
		if(lastword.contains(","))
		{
		StringTokenizer st = new StringTokenizer(lastword,",");
		while(st.hasMoreTokens())
		{
			word.set(firstWord+st.nextToken());
			context.write(word,one);
		}//while close
		
		
		}//inner if of loop i==4 
		else
		{
		word.set(firstWord+lastword);
		  context.write(word,one);
//System.out.println("Wrote context from mapper");
		}//inner else of loop i==4
		
		
		}//outer else if i==4 close
		
		else
		{
		
//Only have one of these lines uncommented
    //  System.out.println(" and the next token value is "+itr.nextToken()+"\n\n\n\n\n\n\n");
    itr.nextToken();  

			}//final else close
	  }//for close
	  
	  
    }//map function close
  }//class close

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "imdb");
    job.setJarByClass(Imdb.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
