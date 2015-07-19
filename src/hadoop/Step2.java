package hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Step2 {
	public static final String INPUT_PATH = "hdfs://crxy1:9000/shakespear/step1/part*";
	public static final String OUT_PATH = "hdfs://crxy1:9000/shakespear/step2";

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
		if (fileSystem.exists(new Path(OUT_PATH))) {
			fileSystem.delete(new Path(OUT_PATH), true);
		}

		Job job = new Job(conf, Step2.class.getSimpleName());
		job.setJarByClass(Step2.class);
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(NewK2.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		job.waitForCompletion(true);
	}

	public static class MyMapper extends Mapper<Text, LongWritable, NewK2, Text>{
		@Override
		protected void map(Text key, LongWritable value,
				Mapper<Text, LongWritable, NewK2, Text>.Context context)
				throws IOException, InterruptedException {
			context.write(new NewK2(value.get()), key);
		}
	}
	
	public static class MyReducer extends Reducer<NewK2, Text, Text, NullWritable>{
		List<String> ls = new ArrayList<String>();
		
		@Override
		protected void reduce(NewK2 k2, Iterable<Text> v2s,
				Reducer<NewK2, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			for (Text word : v2s) {
				ls.add(word.toString());
				if (ls.size()==100) {
					for (String top : ls) {
						context.write(new Text(top), NullWritable.get());
					}
				}
			}
		}
	}
	
	public static class NewK2 implements WritableComparable<NewK2> {
		long first;

		public NewK2() {
		}

		public NewK2(long first) {
			this.first = first;
		}

		@Override
		public String toString() {
			return first+"";
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.first = in.readLong();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeLong(first);
		}

		@Override
		public int compareTo(NewK2 o) {
			int minus = (int) (o.first - this.first);
			return minus;
		}

	}

}
