package classificationAnalysis;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NaiveBayesTraining {

	static String DATA_PATH = "hdfs://localhost:9000/Lab2/input/训练数据.txt";
	static String Classified_Statistics = "hdfs://localhost:9000/Lab2/output/Classified_Statistics";
	
	public static int dim = 20;			// 数据维度为20
	public static int typeIndex = 20;	// 数据类别下标为20（第21维数据为0或1，代表类别）
	
	
	public static final class NaiveBayesTrainingMapper extends Mapper<Object, Text, Text, IntWritable> {
		
		// 生成<属性,属性类别,分类结果>, <1>这样的组合，其中1表示该属性大于0, 0表示该属性小于等于0
		// 类别统计则为<type,分类结果>, <1>这样的形式进行统计
		public void map(Object key, Text value, Context context) throws IOException,InterruptedException {
			
			String[] items = value.toString().split(",");
			String type = items[typeIndex];
			for(int i = 0; i < dim; i++) {
				double itemNum = Double.valueOf(items[i]);
				if(itemNum > 0) {
					context.write(new Text(i + "," + "1" + "," + type), new IntWritable(1));
				} else {
					context.write(new Text(i + "," + "0" + "," + type), new IntWritable(1));
				}
			}
			context.write(new Text("type" + "," + type), new IntWritable(1));
	        
		}
		
	}
	
	public static final class NaiveBayesTrainingReducer extends Reducer<Text, IntWritable, Text, Text> {
		//统计每一维数据为正的总数和为负的总数，以及分别属于类别0和1的数据总数
		// <属性,属性类别,分类结果>, <总数>
		// <type,分类结果>, <总数>
	    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	        
	    	int total = 0;
	    	
	    	for(IntWritable value : values) {
	    		total += value.get();
	        }
	    	
	    	String result = key.toString() + "," + total;
	    	
	    	context.write(new Text(""), new Text(result));
	    }
	    
	}
	
	
	public static void main(String[] arg) throws Exception{
		Path inputpath=new Path(DATA_PATH);
		Path outputpath=new Path(Classified_Statistics);
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf, "NaiveBayesTraining");	
		FileInputFormat.setInputPaths(job, inputpath);
		FileOutputFormat.setOutputPath(job, outputpath);
		job.setJarByClass(NaiveBayesTraining.class);
		job.setMapperClass(NaiveBayesTrainingMapper.class);
		job.setReducerClass(NaiveBayesTrainingReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
