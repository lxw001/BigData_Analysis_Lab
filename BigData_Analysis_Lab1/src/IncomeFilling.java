import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IncomeFilling {

    static String INPUT_PATH = "hdfs://localhost:9000/Lab1/output/D_Filter2/part-r-00000";
    static String OUTPUT_PATH = "hdfs://localhost:9000/Lab1/output/D_incomeFilled";

    //填充策略：用同国家同职业的平均income填充该行缺失的income
	public static final class IncomeFillingMapper extends Mapper<LongWritable, Text, Text, Text> {
		//用来存储国家职业，以及相应的除缺失值之外的income总数和总人数，方便计算平均值
		public static Map<String, List<Double>> map1 = new HashMap<>();
		//用来存储该国家平均工资
		public static Map<String, List<Double>> map2 = new HashMap<>();
		public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {

			String str = value.toString();
	        String[] values = str.split("\\|");
	        String nationality = values[9];
	        String career = values[10];
	        String income = values[11];
	        String nationality_career = nationality + career;

	        if(!income.contains("?")) {
	        	if(map1.containsKey(nationality_career)) {
	        		List<Double> list = map1.get(nationality_career);
	        		double sum = (double)list.get(0) + Double.valueOf(income);
	        		double num = list.get(1) + 1.0;
	        		list.set(0, sum);
	        		list.set(1, num);
	        	} else {
	        		List<Double> list = new ArrayList<>();
	        		list.add(Double.valueOf(income));
	        		list.add(1.0);
	        		map1.put(nationality_career, list);
	        	}

				if(map2.containsKey(nationality)) {
					List<Double> list = map2.get(nationality);
					double sum = (double)list.get(0) + Double.valueOf(income);
					double num = list.get(1) + 1.0;
					list.set(0, sum);
					list.set(1, num);
				} else {
					List<Double> list = new ArrayList<>();
					list.add(Double.valueOf(income));
					list.add(1.0);
					map2.put(nationality, list);
				}
	        }
	        context.write(value,new Text(" "));
		}

	}

	public static final class IncomeFillingReducer extends Reducer<Text, Text, Text, Text> {
		public static Map<String, List<Double>> map = IncomeFillingMapper.map1;
		public static Map<String, List<Double>> map2 = IncomeFillingMapper.map2;
	    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	        for(Text value : values) {
	        	String str = key.toString();
		        String[] temp = str.split("\\|");
				String nationality = temp[9];
				String career = temp[10];
				String nationality_career = nationality + career;
		        String income = temp[11];
		        if(income.contains("?")) {
		        	if(map.containsKey(nationality_career)) {
		        		List<Double> list = map.get(nationality_career);
		        		double incomeToFill = list.get(0)/list.get(1);
		        		str = str.replace(income, String.valueOf(incomeToFill));//用该国家该职业的平均工资填充 ？
		        	} else {
						List<Double> list = map2.get(nationality);
						double incomeToFill = list.get(0)/list.get(1);
		        		str = str.replace(income, String.valueOf(incomeToFill));//用该国家平均工资填充 ？
		        	}
		        	context.write(new Text(str), value);
		        } else {
		        	context.write(key,new Text(""));
		        }

	        }
	    }

    }

    public static void main(String[] arg) throws Exception {

        Path outputpath = new Path(OUTPUT_PATH);
        Path inputpath = new Path(INPUT_PATH);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "income_filling");
        FileInputFormat.setInputPaths(job, inputpath);
        FileOutputFormat.setOutputPath(job, outputpath);
        job.setJarByClass(IncomeFilling.class);
        job.setMapperClass(IncomeFillingMapper.class);
        job.setReducerClass(IncomeFillingReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}