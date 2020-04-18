import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

public class Count_Genre 
{
    private final static IntWritable one = new IntWritable(1);
    public static Text mapperKey = new Text();
	//Mapper Class
	public static class Count_Genre_Mapper extends Mapper<LongWritable,Text,Text,Text>{
	@Override
	//Mapper Method
	public void map(LongWritable key, Text value, Context c) throws IOException, InterruptedException
	{
	//Take each line as a string
	String line = value.toString();
	//Split them into tokens 
	String [] tokens  = line.split("\\;");
	//Store the year of the movie title
	String year = tokens[3];
	//Check if the data is present or not(\N)
	//If it is present then check if it is a movie after 2000
	if(year.startsWith("2"))
	{
	//Take the various genres of the movie 
		String genre=tokens[4];
		//Split and store each genre seperately
		String [] tokens_genre=genre.split("\\,");
		//For each genre in the genre list of the movie run the loop
		for(int i=0;i<tokens_genre.length;i++)
		{	
		//Check if the data is provided in the genre list(not \N)
	if(!tokens_genre[i].startsWith("\\N"))
	{
	//For each genre set the mapper key
	//Here each key is the year of the movie and its genre connected with a '_'
		mapperKey.set(new Text(year) + "_" + new Text(tokens_genre[i]));
		//Create the key value pair
		c.write(mapperKey, new Text("1"));
	}
		}
	
	}
	
	}
}
//Reducer Classs
public static class Count_Genre_Reducer extends Reducer<Text,Text,Text,Text>{
		@Override
		//Reducer method
		public void reduce(Text key, Iterable<Text>values, Context c) throws IOException,InterruptedException{			
		//for each genre assign a count variable
			int count = 0;
			//for each time the particular key value pair occurs 
			//Increase the count by 1
			for(Text val:values){
				count += 1;
			}
			String info = key.toString();
                        String [] info_part = info.split("\\_");
						//Create a list key value pairs 
			c.write(new Text(info_part[0]+","+info_part[1]+","), new Text(""+count));	

		}
	}
	public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
			Job j3 = Job.getInstance(conf);
			j3.setJobName("Count_Genre job");
			j3.setJarByClass(Count_Genre.class);
			//Setting the output of the Mapper class
			j3.setMapOutputKeyClass(Text.class);
			j3.setMapOutputValueClass(Text.class);
			//Setting the output of the Reducer class
			j3.setOutputKeyClass(Text.class);
			j3.setOutputValueClass(Text.class);
			//Setting the input and output of the program
			j3.setInputFormatClass(TextInputFormat.class);
			j3.setOutputFormatClass(TextOutputFormat.class);
			//Set the mapper class
			j3.setMapperClass(Count_Genre_Mapper.class);
			//Set the reducer class
			j3.setReducerClass(Count_Genre_Reducer.class);
			FileOutputFormat.setOutputPath(j3, new Path(args[1]));
			FileInputFormat.addInputPath(j3, new Path(args[0]));
			int code = j3.waitForCompletion(true) ? 0 : 1;
System.exit(code);
  }

}
