import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RatingFilling {

    static String INPUT_PATH = "hdfs://localhost:9000/Lab1/output/D_incomeFilled/part-r-00000";
    static String OUTPUT_PATH = "hdfs://localhost:9000//Lab1/output/D_Done";

    public static final class RatingFillingMapper extends Mapper<LongWritable, Text, Text, Text> {


        //rating近似依赖于user_income、longitude、latitude和altitude
        private final List<Double[]> xList = new ArrayList<>();//存储所有行的user_income、longitude、latitude和altitude字段
        private final List<Double> yList = new ArrayList<>();//存储所有行的rating字段
        public static double[] coefficient;//存储多元线性回归分析结果的系数

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String str = value.toString();
            String[] values = str.split("\\|");

            String rating = values[6];
            //将所有不缺失的值放入列表，方便寻找函数依赖关系
            if (!rating.contains("?")) {
                //将user_income、longitude、latitude和altitude字段放先放入一个数组，再放入xList
                Double longitude = Double.valueOf(values[1]);
                Double latitude = Double.valueOf(values[2]);
                Double altitude = Double.valueOf(values[3]);
                Double income = Double.valueOf(values[11]);
                Double[] xValues = new Double[4];
                xValues[0] = longitude;
                xValues[1] = latitude;
                xValues[2] = altitude;
                xValues[3] = income;
                xList.add(xValues);
                //将rating字段放入yList当中
                yList.add(Double.valueOf(rating));
            }
            context.write(value, new Text(""));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            //由于回归分析参数限制，需要先将arrayList转化为数组
            double[] y = new double[yList.size()];
            int lineIndex = 0;
            for (double value : yList) {
                y[lineIndex++] = value;
            }
            double[][] x = new double[xList.size()][4];
            lineIndex = 0;
            for (Double[] values : xList) {
                for (int i = 0; i < 4; i++) {
                    x[lineIndex][i] = values[i];
                }
                lineIndex++;
            }

            //进行多元线性回归分析
            OLSMultipleLinearRegression regression = new OLSMultipleLinearRegression();
            regression.newSampleData(y, x);
            coefficient = regression.estimateRegressionParameters();
        }

    }

    public static final class RatingFillingReducer extends Reducer<Text, Text, Text, Text> {

        public static double[] coefficient = RatingFillingMapper.coefficient;

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                String str = key.toString();
                String[] temp = str.split("\\|");
                String rating = temp[6];
                if (rating.contains("?")) {
                    double longitude = Double.valueOf(temp[1]);
                    double latitude = Double.valueOf(temp[2]);
                    double altitude = Double.valueOf(temp[3]);
                    double income = Double.valueOf(temp[11]);
                    double ratingToFill = coefficient[0] + longitude * coefficient[1] + latitude * coefficient[2] +
                            altitude * coefficient[3] + income * coefficient[4];
                    str = str.replace(rating, String.valueOf(ratingToFill));
                    context.write(new Text(str), value);
                } else {
                    context.write(key, value);
                }

            }
        }

    }

    public static void main(String[] arg) throws Exception {

        Path outputpath = new Path(OUTPUT_PATH);
        Path inputpath = new Path(INPUT_PATH);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "rating_filling");
        FileInputFormat.setInputPaths(job, inputpath);
        FileOutputFormat.setOutputPath(job, outputpath);
        job.setJarByClass(RatingFilling.class);
        job.setMapperClass(RatingFillingMapper.class);
        job.setReducerClass(RatingFillingReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}