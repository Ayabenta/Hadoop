import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
public class PopularTrack {
	//why object here as type of keyin 
	public static class Mappercode extends Mapper<Object, Text, IntWritable, IntWritable>{
        		
		IntWritable trackId = new IntWritable();
		IntWritable userId = new IntWritable(); 
		
		public void map( Object key, Text value, Context context) throws IOException, InterruptedException {
			//Splitter code 
			String [] parts = value.toString().split("[|]"); // reading line by line 
			trackId.set(Integer.parseInt(parts[0]));
			userId.set(Integer.parseInt(parts[1]));
			context.write(trackId, userId);
			// (key(the input), value)
			
		}
	}
	public static class reducercode extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
		public void reduce(IntWritable trackId, Iterable <IntWritable> userId , Context context) throws IOException, InterruptedException{
			Set <Integer> useridset = new HashSet<Integer>();
			for(IntWritable userid : userId) {
				useridset.add(userid.get());
			}
			IntWritable size = new IntWritable(useridset.size());
			context.write(trackId, size);
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration(); 
		Job job =Job.getInstance(conf,"Trackanalysis" );
		job.setJarByClass(PopularTrack.class);
		job.setMapperClass(Mappercode.class);
		job.setCombinerClass(reducercode.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true)? 0 : 1);
		
		
	}
	   
}
