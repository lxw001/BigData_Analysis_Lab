import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Sample {
	
	static String INPUT_PATH = "hdfs://localhost:9000/Lab1/input/data.txt";
	static String OUTPUT_PATH = "hdfs://localhost:9000/Lab1/output/D_Sample";
	
	public static final class SampleMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {
			
			String str = value.toString();//将Hadoop中Text类型转为String类型
	        String[] values = str.split("\\|");//将一行字符串按照字符'|'分隔
	        Text career = new Text();
	        career.set(values[10]);//将career属性作为map输出的key
	        context.write(career, value);
		}
	}

	public static final class SampleReducer extends Reducer<Text, Text, Text, Text> {
	    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			ArrayList<String> list=new ArrayList<>();//存储相同职业的所有行
			int count=0;
			//得到该职业人数总数，经过测试在60万左右
	        for (Text value : values) {
	            list.add(value.toString());
				count=count+1;
	        }
			//打乱列表
			Collections.shuffle(list);
			//选取分层抽样比为1/100
			int samplesNum=count/100;
			for(int i=0;i<samplesNum;i++){
				context.write(new Text(String.valueOf(list.get(i))),new Text(""));
			}
	    }
	}
	
	public static void main(String[] arg) throws Exception{

		Path outputpath=new Path(OUTPUT_PATH);
		Path inputpath=new Path(INPUT_PATH);
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf, "sample");	
		FileInputFormat.setInputPaths(job, inputpath);
		FileOutputFormat.setOutputPath(job, outputpath);
		job.setJarByClass(Sample.class);
		job.setMapperClass(SampleMapper.class);
		job.setReducerClass(SampleReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
	
}
