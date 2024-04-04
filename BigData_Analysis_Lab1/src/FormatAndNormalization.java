import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.regex.Pattern;

public class FormatAndNormalization {
    static String INPUT_PATH = "hdfs://localhost:9000/Lab1/output/D_Filter/part-r-00000";
    static String OUTPUT_PATH = "hdfs://localhost:9000/Lab1/output/D_Filter2";
    static double max = 0.0;
    static double min = 0.0;

    public static final class FormatAndNormalizationMapper extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //一共3种格式的时间
            SimpleDateFormat format1 = new SimpleDateFormat("yyyy/MM/dd", Locale.ENGLISH);
            SimpleDateFormat format2 = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
            SimpleDateFormat format3 = new SimpleDateFormat("MMMM d,yyyy", Locale.ENGLISH);//MMMM: 月，显示为英文月份全称，如 January。d: 日，1-2位显示，如2或02
            //对应3类正则表达式
            String regex1 = "[0-9]{4}/[0-9]{2}/[0-9]{2}";
            Pattern pattern1 = Pattern.compile(regex1);
            String regex2 = "[0-9]{4}-[0-9]{2}-[0-9]{2}";
            Pattern pattern2 = Pattern.compile(regex2);
            String regex3 = "[a-zA-Z]+ [0-9]+,[0-9]{4}";
            Pattern pattern3 = Pattern.compile(regex3);

            String str = value.toString();
            String[] values = str.split("\\|");
            String review_Date = values[4];
            String temperature = values[5];
            String birthday = values[8];

            //将日期全部转化为第一种格式，即yyyy/MM/dd
            try {
                if (pattern1.matcher(review_Date).matches()) {

                } else if (pattern2.matcher(review_Date).matches()) {
                    review_Date = format1.format(format2.parse(review_Date));
                } else if (pattern3.matcher(review_Date).matches()) {
                    review_Date = format1.format(format3.parse(review_Date));
                }

                if (pattern1.matcher(birthday).matches()) {

                } else if (pattern2.matcher(birthday).matches()) {
                    birthday = format1.format(format2.parse(birthday));
                } else if (pattern3.matcher(birthday).matches()) {
                    birthday = format1.format(format3.parse(birthday));
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }

            //将温度格式化为摄氏温度
            if (temperature.endsWith("℉")) {
                double tem = Double.valueOf(temperature.substring(0, temperature.length() - 2));
                tem = (tem - 32) / 1.8;
                temperature = tem + "℃";
            }
            //找到rating属性中的最大值和最小值，方便进行归一化
            if (!values[6].contains("?")) {
                Double rating = Double.valueOf(values[6]);
                if (rating > max) max = rating;
                if (rating < min) min = rating;
            }

            str = str.replace(values[4], review_Date);
            str = str.replace(values[5], temperature);
            str = str.replace(values[8], birthday);
            context.write(new Text(str), new Text(""));
        }

    }

    public static final class FormatAndNormalizationReducer extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String str = key.toString();
            String[] strings = str.split("\\|");
            //数据值可能存在缺失，故需要进行判断
            if (!strings[6].contains("?")) {
                double rating = Double.valueOf(strings[6]);
                rating = (rating - min) / (max - min);
                str = str.replace(strings[6], String.valueOf(rating));
            }
            context.write(new Text(str), new Text(""));
        }
    }

    public static void main(String[] arg) throws Exception {

        Path outputpath = new Path(OUTPUT_PATH);
        Path inputpath = new Path(INPUT_PATH);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Format And Normalization");
        FileInputFormat.setInputPaths(job, inputpath);
        FileOutputFormat.setOutputPath(job, outputpath);
        job.setJarByClass(FormatAndNormalization.class);
        job.setMapperClass(FormatAndNormalizationMapper.class);
        job.setReducerClass(FormatAndNormalizationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
