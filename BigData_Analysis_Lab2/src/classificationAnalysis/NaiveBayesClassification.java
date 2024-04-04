package classificationAnalysis;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NaiveBayesClassification {

//	static String DATA_PATH = "hdfs://localhost:9000/Lab2/input/测试数据.txt";
	static String DATA_PATH = "hdfs://localhost:9000/Lab2/input/验证数据.txt";
	static String Classified_Statistics = "hdfs://localhost:9000/Lab2/output/Classified_Statistics/part-r-00000";
//	static String Classified_Result = "hdfs://localhost:9000/Lab2/output/Classified_Result";
	static String Classified_Result = "hdfs://localhost:9000/Lab2/output/Classified_Result2";
	
	public static int dim = 20;			// 数据维度为20
	public static int typeIndex = 20;	// 数据类别下标为20
	
	
	public static final class NaiveBayesClassificationMapper extends Mapper<Object, Text, Text, Text> {
		
		// 通过连续属性离散化的转换规则，转化出新的属性序列
		//输出key为该行20个属性是否为正构成的用","分隔的01串+该行数据所属类别，value为输入的一行value
		public void map(Object key, Text value, Context context) throws IOException,InterruptedException {
			
//			String[] items = value.toString().split(",");
//			String newSequence = "";
//			for(int i = 0; i < dim-1; i++) {
//				double itemNum = Double.valueOf(items[i]);
//				if(itemNum > 0) {
//					newSequence = newSequence + 1 + ",";
//				} else {
//					newSequence = newSequence + 0 + ",";
//				}
//			}
//			double itemNum = Double.valueOf(items[dim-1]);
//			if(itemNum > 0) {
//				newSequence = newSequence + 1;
//			} else {
//				newSequence = newSequence + 0;
//			}
//			context.write(new Text(newSequence), value);
			
			String[] items = value.toString().split(",");
			String newSequence = "";
			for(int i = 0; i < dim; i++) {
				double itemNum = Double.valueOf(items[i]);
				if(itemNum > 0) {
					newSequence = newSequence + 1 + ",";
				} else {
					newSequence = newSequence + 0 + ",";
				}
			}
			newSequence = newSequence + Integer.valueOf(items[dim]);
			context.write(new Text(newSequence), value);
		}
		
	}
	
	public static final class NaiveBayesClassificationReducer extends Reducer<Text, Text, Text, Text> {
		
		public static ProbabilityTable probabilityTable;
		
		public static int count = 0;//总数
		public static int right = 0;//正确数
		
		protected void setup(Context context) throws IOException {
			probabilityTable = new ProbabilityTable(Classified_Statistics);
		}
		//输出key为每一行+预测的类别，value为""
	    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	        
	    	int selectedClass = 0;
	    	double maxProbability = 0.0;
	    	double[] classProbability = {1.0, 1.0};
	    	int preType = 0;
	    	
	    	String[] items = key.toString().split(",");
	    	
	    	int[] itemsNum = new int[20];//存储0或1共20个
	    	
	    	preType = Integer.valueOf(items[20]);//所属类别
	    	
	    	for(int i = 0; i < 20; i++) {
	    		itemsNum[i] = Integer.valueOf(items[i]);
	    	}
	    	
	    	for(int i = 0; i < 2; i++) {
	    		for(int j = 0; j < 20; j++) {
					//贝叶斯概率计算
	    			classProbability[i] = classProbability[i] * 
	    								  probabilityTable.getConditionalProbability(j, itemsNum[j], i) / 
	    								  probabilityTable.getTypeProbability(j, itemsNum[j]);
	    		}
	    		classProbability[i] *= probabilityTable.getClassProbability(i);
	    	}
	    	//属于哪个类的概率大，就将该行数据归于哪一个类
	    	for(int i = 0; i < 2; i++) {
	    		if(classProbability[i] > maxProbability) {
	    			maxProbability = classProbability[i];
	    			selectedClass = i;
	    		}
	    	}
	    	
	    	for(Text value : values) {
	    		count++;
	    		if(preType == selectedClass) {
	    			right++;
	    		}
	    		String result = value.toString() + "," + selectedClass;
	    		context.write(new Text(result), new Text(""));
	    	}
	    	
	    }
	    
	    protected void cleanup(Context context)  throws IOException {
	    	double accuracy = (double)right / (double)count;
			System.out.println("准确率为：" + accuracy);
		}
	    
	}
	
	public static void main(String[] arg) throws Exception{
		Path inputpath=new Path(DATA_PATH);
		Path outputpath=new Path(Classified_Result);
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf, "NaiveBayesClassification");	
		FileInputFormat.setInputPaths(job, inputpath);
		FileOutputFormat.setOutputPath(job, outputpath);
		job.setJarByClass(NaiveBayesClassification.class);
		job.setMapperClass(NaiveBayesClassificationMapper.class);
		job.setReducerClass(NaiveBayesClassificationReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
