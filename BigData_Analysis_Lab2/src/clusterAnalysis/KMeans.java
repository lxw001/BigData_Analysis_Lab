package clusterAnalysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.LineReader;

public class KMeans {

	static String DATA_PATH = "hdfs://localhost:9000/Lab2/input/聚类数据.txt";//输入路径
	static String CENTER_PATH = "hdfs://localhost:9000/Lab2/input/centers.txt";//保存每轮计算得到的k个中心点的路径
	static String NEW_CENTER_PATH = "hdfs://localhost:9000/Lab2/output/KMeans_new_center_path";//输出路径
	public static int k = 3;		// 分为k个中心
	public static int dim = 20;		// 数据维度为20

	public static final class KMeansMapper extends Mapper<Object, Text, Text, Text> {

		// 初始化中心点，这里分为k类，维度为dim
		public static double[][] centers = new double[k][dim];
		protected void setup(Context context) throws IOException {
			centers = getCenters(CENTER_PATH, false);
//			System.out.println("--------------------------------");
//						for(int i = 0; i < k; i++) {
//				for(int j = 0; j < dim; j++) {
//					System.out.print(centers[i][j] + ",");
//				}
//			}
		}

		//输出key为中心点下标（即类编号），value为一行数据
		public void map(Object key, Text value, Context context) throws IOException,InterruptedException {

			double[] valueList = textToList(value);
			double minDistance = 9999999;//最小距离初始化为无穷大
			int centerIndex = 0;//中心点对应的类标号
			//根据到k个中心点的平方和距离来判断属于哪一类（距离最小的即为所属类）
			for(int i = 0; i < k; i++) {
				double tempDistance = 0;
				for(int j = 0; j < dim; j++) {
					tempDistance += Math.pow(Math.abs(centers[i][j] - valueList[j]), 2);
				}

				if(tempDistance < minDistance) {
					minDistance = tempDistance;
					centerIndex = i;
				}
			}

			context.write(new Text(String.valueOf(centerIndex)), value);
		}

	}
	//输出key为空，value为类编号
	public static final class KMeansMapper2 extends Mapper<Object, Text, Text, Text> {

		// 初始化中心点，这里分为k类，维度为dim
		public static double[][] centers = new double[k][dim];
		protected void setup(Context context) throws IOException {
			centers = getCenters(CENTER_PATH, false);
		}
		public void map(Object key, Text value, Context context) throws IOException,InterruptedException {

			double[] item = textToList(value);
			double minDistance = 9999999;
			int centerIndex = 0;
			for(int i = 0; i < k; i++) {
				double tempDistance = 0;
				for(int j = 0; j < dim; j++) {
					tempDistance += Math.pow(Math.abs(centers[i][j] - item[j]), 2);
				}

				if(tempDistance < minDistance) {
					minDistance = tempDistance;
					centerIndex = i;
				}
			}

			context.write(new Text(""), new Text(String.valueOf(centerIndex)));
		}

	}

	public static final class KMeansReducer extends Reducer<Text, Text, Text, Text> {

		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			List<double[]> fieldList = new ArrayList<double[]>();
			for(Text value : values) {
				double[] list = textToList(value);
				fieldList.add(list);
			}
			int number = fieldList.size();
			//计算k个中心点（每一维数据取平均值）
			double[] avg = new double[dim];
			for(int i = 0; i < dim; i++) {
				double sum = 0;
				for(double[] temp : fieldList) {
					sum += temp[i];
				}
				avg[i] = sum / number;
			}
			String resultAvg = String.valueOf(avg[0]);
			for(int i = 1; i < dim; i++) {
				resultAvg = resultAvg + "," + String.valueOf(avg[i]);
			}
			System.out.println("中心点" + String.valueOf(key) + "    " + resultAvg);
			context.write(new Text(""), new Text(resultAvg));
		}

	}

	// 得到文件中的中心点
	public static double[][] getCenters(String centerPath, boolean inDirectory) throws IOException {

		// 初始化中心点
		double[][] centers = new double[k][dim];

		Path centerpath = new Path(centerPath);
		Configuration conf = new Configuration();
		FileSystem fileSystem = centerpath.getFileSystem(conf);

		if(inDirectory) {
			FileStatus[] listStatus = fileSystem.listStatus(centerpath);
			for(int i = 0; i < listStatus.length; i++) {
				if(listStatus[i].getPath().toString().contains("part"))
					centers = getCenters(listStatus[i].getPath().toString(), false);
			}
		} else {
			int i = 0;
			FSDataInputStream fsis = fileSystem.open(centerpath);
			LineReader lineReader = new LineReader(fsis, conf);
			Text line = new Text();

			while(lineReader.readLine(line) > 0) {
				double[] list = textToList(line);
				centers[i] = list;
				i++;
			}
			lineReader.close();
		}

		return centers;
	}

	// 将Text转换为double数组
	public static double[] textToList(Text text) {
		double[] list = new double[dim];
		String[] words = text.toString().split(",");
		for(int i = 0; i < words.length; i++) {
			list[i] = Double.parseDouble(words[i]);
		}
		return list;
	}

	// 判断临近两次的中心点是否满足循环退出条件, 满足则返回true, 否则返回false并进行新老文件的替换
	public static boolean compareCenters(String centerPath, String newCenterPath) throws IOException {
		double[][] oldCenters = getCenters(centerPath, false);
		double[][] newCenters = getCenters(newCenterPath, true);

		for(int i = 0; i < k; i++) {
			for(int j = 0; j < dim; j++) {
				if(Math.abs(oldCenters[i][j]-newCenters[i][j]) > 1e-2) {
					replaceCenterFile(centerPath, newCenterPath);
					return false;
				}
			}
		}
		deleteFile(newCenterPath);
		return true;
	}

	// 删除文件
	public static void deleteFile(String pathstr) throws IOException {
		Configuration conf = new Configuration();
		Path path = new Path(pathstr);
		FileSystem hdfs = path.getFileSystem(conf);
		hdfs.delete(path, true);
	}
	//新老中心点文件替换
	public static void replaceCenterFile(String centerPath, String newCenterPath) throws IOException {
		Configuration conf = new Configuration();
		Path centerpath = new Path(centerPath);
		FileSystem fileSystem = centerpath.getFileSystem(conf);

		FSDataOutputStream overWrite = fileSystem.create(centerpath, true);
		overWrite.writeChars("");
		overWrite.close();

		Path newcenterpath = new Path(newCenterPath);
		FileStatus[] listFiles = fileSystem.listStatus(newcenterpath);
		for(int i = 0; i < listFiles.length; i++) {
			if(listFiles[i].getPath().toString().contains("part")) {
				FSDataOutputStream out = fileSystem.create(centerpath);
				FSDataInputStream in = fileSystem.open(listFiles[i].getPath());
				IOUtils.copyBytes(in, out, 4096, true);
			}
		}
		deleteFile(newCenterPath);
	}

	public static void run(boolean runReduce) throws IOException, ClassNotFoundException, InterruptedException {

		Path outputpath = new Path(NEW_CENTER_PATH);
		Path inputpath = new Path(DATA_PATH);

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "KMeans");
		job.setJarByClass(KMeans.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		if(runReduce) {
			job.setMapperClass(KMeansMapper.class);
			job.setReducerClass(KMeansReducer.class);
		} else {
			job.setMapperClass(KMeansMapper2.class);
		}

		FileInputFormat.setInputPaths(job, inputpath);
		FileOutputFormat.setOutputPath(job, outputpath);

		System.out.println(job.waitForCompletion(true));
	}

	public static void main(String[] arg) throws Exception{
		int count = 0;

		while(true) {
			run(true);
			System.out.println("第 " + ++count + " 轮计算");
			//邻近两次的中心点一致时，退出循环
			if(compareCenters(CENTER_PATH, NEW_CENTER_PATH)) {
				//最后一轮只进行map，不进行reduce
				run(false);
				break;
			}
		}

	}
}