package hadoop;
 
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Demo {
	static int k;
	static List ls = new ArrayList();

	/*
	 * 定义了2个job，第一个job是对数据进行词频统计，并且把结果输入到临时文件当中，然后利用mapreduce的管道功能，
	 * 再递交第二个job，将第一个job的输出作为第二个排序任务的输入，注意要文件读入的格式一定要符合数据类型。
	 * 第二个job通过调用系统给出的jobsort.setMapperClass(InverseMapper.class);
	 * 作为map函数，reduce利用SortReduce最后map输出的结果输出到输出文件当中。
	 */
	public static void main(String[] args) throws Exception {
		Scanner scannerin = new Scanner(System.in);
		System.out.println("请输入最低词频数：");
		k = scannerin.nextInt();
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		// 从hdfs读取停词，将停词存放在list中
		String uri = "hdfs://localhost:8000/user/tzj/stop_words";
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		FSDataInputStream in = fs.open(new Path(uri));
		InputStreamReader lsr = new InputStreamReader(in);
		BufferedReader buf = new BufferedReader(lsr);
		String input;
		while ((input = buf.readLine()) != null) {
			ls.add(input);
		}
		System.out.println("The stop_words are:");
		Iterator it = ls.iterator();
		while (it.hasNext()) {
			System.out.print(it.next() + " ");
		}
		System.out.println();
		Job job = new Job(conf, "word count"); // 进行第一个job
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// 创建临时存储文件存放第一个作业的reduce输出
		Path tempath = new Path("/home/tzj/tmp");
		tempath.getFileSystem(conf).deleteOnExit(tempath);
		FileOutputFormat.setOutputPath(job, tempath); // 设置第一个reduce输出路径
		job.setJarByClass(Demo.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class); // 设置第一个作业的OutputFormat类，
		job.waitForCompletion(false);

		// 进行第二个作业
		Job jobsort = new Job();
		FileInputFormat.addInputPath(jobsort, tempath);

		jobsort.setOutputKeyClass(IntWritable.class);
		jobsort.setOutputValueClass(Text.class);
		jobsort.setInputFormatClass(SequenceFileInputFormat.class);
		jobsort.setMapperClass(InverseMapper.class); //InverseMapper类实现了key和value的转换，这是类包中自带的Mapper类
		jobsort.setReducerClass(SortReduce.class);
		jobsort.setNumReduceTasks(1);
		jobsort.setSortComparatorClass(IntWritableCompare.class);
		FileOutputFormat.setOutputPath(jobsort, new Path(otherArgs[1]));
		jobsort.waitForCompletion(false);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	/*
	 * 这个类实现 Mapper 接口中的 map 方法，输入参数中的 value 是文本文件中的一行，
	 * 利用正则表达式对数据进行处理，使文本中的非字母和数字符号转换成空格，
	 * 然后利用StringTokenizer 将这个字符串拆成单词，最后将输出结果。
	 */
	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// System.out.println(key+".........."+value);
			String line = value.toString();
			String s;
			// 将文本中的非字母和数字符号转换成空格
			Pattern p = Pattern.compile("[(,.:;'|?!)]");
			Matcher m = p.matcher(line);
			String line2 = m.replaceAll(" ");
			// System.out.println(line2);
			StringTokenizer itr = new StringTokenizer(line2); // 将字符串拆成单词
			while (itr.hasMoreTokens()) {
				s = itr.nextToken();
				word.set(s);
				if (!ls.contains(s))
					context.write(word, one);
			}
		}
	}

	/*
	 * 这个类是第一个作业的Reduce类，实现Reducer 接口中的 reduce 方法, 
	 * 输入参数中的 key, values 是由 Map任务输出的中间结果，values 是一个 Iterator, 
	 * 遍历这个 Iterator, 就可以得到属于同一个 key 的所有 value.此处，key 是一个单词，value 是词频。
	 * 只需要将所有的 value 相加，就可以得到这个单词的总的出现次数。
	 */
	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	/*
	 * 这个类实现了对IntWritabl类型数据的降序排列。
	 */
	public static class IntWritableCompare extends IntWritable.Comparator {
		public int compare(WritableComparable a, WritableComparable b) {
			return -super.compare(a, b);

		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			// TODO Auto-generated method stub
			return -super.compare(b1, s1, l1, b2, s2, l2);
		}
	}

	/*
	 * 这个类实现了按照用户输入的最低词频数由高到底输出结果。
	 */
	public static class SortReduce extends
			Reducer<IntWritable, Text, IntWritable, Text> {
		// private static IntWritable num=new IntWritable();

		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			for (Text value : values) {
				if (key.get() >= k)
					context.write(key, value);
			}
		}
	}
}
