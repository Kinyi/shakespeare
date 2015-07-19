package hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/*
 * 如果定义一个全局arraylist，在main函数里读取文件内容作为元素，结果在map里面ls却为空，
 * 因为运行到map的时候，这个Step1是反射出来的，不会执行main方法，所以ls没对象
 */
public class Step1 {
	public static final String INPUT_PATH = "hdfs://crxy1:9000/shakespear/sample";
	public static final String OUT_PATH = "hdfs://crxy1:9000/shakespear/step1";
	public static final String STOP_PATH = "hdfs://crxy1:9000/shakespear/stopword.txt";
	

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		
		if(fileSystem.exists(new Path(OUT_PATH))){
			fileSystem.delete(new Path(OUT_PATH), true);
		}
		Job job = new Job(conf, Step1.class.getSimpleName());
		job.setJarByClass(Step1.class);
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.waitForCompletion(true);
	}
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
		Text k2 = new Text();
		static List<String> ls = new ArrayList<String>();
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			Configuration conf = new Configuration();
			FileSystem fileSystem;
			try {
				fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
				FSDataInputStream in = fileSystem.open(new Path(STOP_PATH));
				InputStreamReader isr = new InputStreamReader(in);
				BufferedReader bufferedReader = new BufferedReader(isr);
				String input;
				while ((input = bufferedReader.readLine())!=null) {
					ls.add(input);
				}
			} catch (URISyntaxException e) {
				e.printStackTrace();
			}
			
			String line = value.toString().replaceAll("[(,.:;'|?!)]", " ");
			StringTokenizer st = new StringTokenizer(line);
			while (st.hasMoreTokens()) {
				String word = st.nextToken();
				k2.set(word);
				if(! ls.contains(word)){
					context.write(k2, new LongWritable(1L));
				}
			}
		}
	}
	
	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
		@Override
		protected void reduce(Text k2, Iterable<LongWritable> v2s,
				Reducer<Text, LongWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			long count= 0L;
			for (LongWritable time : v2s) {
				count+=time.get();
			}
			context.write(k2, new LongWritable(count));
		}
	}
	
}
