package classificationAnalysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.LineReader;

public class knnClassify {

    static String Train_PATH = "hdfs://localhost:9000/Lab2/input/训练数据.txt";
    static String Classify_PATH = "hdfs://localhost:9000/Lab2/input/验证数据.txt";
    static String knn_Result = "hdfs://localhost:9000/Lab2/output/knn_Result";

    public static int dim = 20;			// 数据维度为20
    public static int typeIndex = 20;	// 数据类别下标为20
    private static List<double[]> TrainData = new ArrayList<>();
    public static int k = 3; //指定k近邻

    // accuracy
    private static double right = 0.0;
    private static double sum = 0.0;

    public static long no = 0;

    public static final class knnClassifyMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        protected void setup(Mapper<Object, Text, Text, Text>.Context context)
                throws IOException {
            //打开文件
            Path centerpath = new Path(Train_PATH);
            Configuration conf = new Configuration();
            FileSystem fileSystem = centerpath.getFileSystem(conf);
            FSDataInputStream fsis = fileSystem.open(centerpath);
            LineReader lineReader = new LineReader(fsis, conf);
            Text line = new Text();

            // 将训练数据读入文件
            while(lineReader.readLine(line) > 0) {

                String[] tuple = line.toString().split(",");
                if(tuple.length == 21) {

                    double[] data = new double[dim + 1];
                    for(int i = 0; i < dim + 1; i++) {
                        data[i] = Double.valueOf(tuple[i]);
                    }
                    TrainData.add(data);
                }
            }
            lineReader.close();
        }

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
//            跑的太慢了，只跑一部分大概计算一下准确率
//            if(count==202) return;
            String[] tuple = value.toString().split(",");

            double[] k_index = {0,0,0};//最近的k个点对应的下标
            double[] distance = {10000.0, 10000.0, 10000.0};//最短的k个距离

            // 开始计算距离，并更新k-距离表
            for(double[] t_data : TrainData) {
                double sum = 0.0;
                for (int i = 0; i < dim; i++) {
                    double attribute = Double.valueOf(tuple[i]);
                    sum += Math.pow(attribute - t_data[i], 2.0);
                }
                double d = Math.sqrt(sum);

                for(int i=0;i<k;i++){
                    if (d < distance[i]) {
                        distance[i] = d;
                        k_index[i] = t_data[typeIndex];
                        break;
                    }
                }
//                if (d < distance[0]) {
//                    distance[0] = d;
//                    k_type[0] = t_data[typeIndex];
//                } else if (d < distance[1]) {
//                    distance[1] = d;
//                    k_type[1] = t_data[typeIndex];
//                } else if (d < distance[2]) {
//                    distance[2] = d;
//                    k_type[2] = t_data[typeIndex];
//                }
            }

            // count the number of 1 and 0
            int num1 = 0;
            int num0 = 0;
            for (double type : k_index) {
                if(type == 1.0) {
                    num1 += 1;
                } else {
                    num0 += 1;
                }
            }
            // 比较两种类型的数量计算结果
            double result = 1.0;
            if (num1 < num0) {
                result = 0.0;
            }

            sum += 1.0;
            if(result == Double.valueOf(tuple[typeIndex])) {
                right += 1.0;
            }
            no += 1;
            System.out.println(no);
            context.write(new Text(""), new Text(String.valueOf(result)));
        }

        @Override
        protected void cleanup(Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            double accuracy = right / sum;
            System.out.println("准确率为：" + accuracy);
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Path outputpath = new Path(knn_Result);
        Path inputpath = new Path(Classify_PATH);

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "knnClassify");
        FileInputFormat.setInputPaths(job, inputpath);
        FileOutputFormat.setOutputPath(job, outputpath);

        job.setJarByClass(knnClassify.class);
        job.setMapperClass(knnClassifyMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
