
import java.io.IOException;
import java.util.StringTokenizer;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;      

public class WikipediaPopular extends Configured implements Tool{
 public static class TokenizerMapper
	extends Mapper<LongWritable, Text, Text, LongWritable>{

		
		private Text word = new Text();
		

		@Override
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
		
		 
		 String line = value.toString().toLowerCase();
		 LongWritable one = new LongWritable();
		 
		  if(line.contains("en") && !line.contains("main_page") && !line.contains("special:")){
		  		String [] container = line.split(" ");
		  		one.set(Long.parseLong(container[3]));
		 		word.set(container[0]);
				context.write(word, one);	 
		 }
		 
			 	 
		}
	}

	

	public static class HitReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
	 
	private LongWritable result = new LongWritable();

	@Override
	public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
	
	long max = Long.MIN_VALUE;
	for(LongWritable val: values){
		if(val.get() > max)
		  max = val.get();
	}
	result.set(max);
	context.write(key,result);

	}
		
	}
	

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WikipediaPopular(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "WikipediaPopular");
		job.setJarByClass(WikipediaPopular.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(TokenizerMapper.class);
		//job.setCombinerClass(LongSumReducer.class);
		job.setReducerClass(HitReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}


