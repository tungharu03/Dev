import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

public class KMeansClustering {

    public static class KMeansMapper extends Mapper<Object, Text, Text, Text> {

        // Các centroid cố định
        private static final double[][] centroids = {
            {45.2, 26.3, 20.9},
            {40.3, 87.4, 18.2},
            {32.7, 86.5, 82.1},
            {43.1, 54.8, 49.8},
            {25.3, 25.7, 79.4}
        };

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Đọc dữ liệu từ file khách hàng
            String[] tokens = value.toString().split(",");
            int customerId = -1;
            String gender = null;
            double age = 0.0, annualIncome = 0.0, spendingScore = 0.0;

            try {
                customerId = Integer.parseInt(tokens[0]);
                gender = tokens[1];
                age = tryParseDouble(tokens[2]);
                annualIncome = tryParseDouble(tokens[3]);
                spendingScore = tryParseDouble(tokens[4]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid data format for customer ID: " + customerId);
                return;
            }

            // Tính toán khoảng cách đến các centroid
            double minDistance = Double.MAX_VALUE;
            int closestCentroid = -1;
            for (int i = 0; i < centroids.length; i++) {
                double[] centroid = centroids[i];
                double distance = calculateEuclideanDistance(new double[] {age, annualIncome, spendingScore}, centroid);
                if (distance < minDistance) {
                    minDistance = distance;
                    closestCentroid = i;
                }
            }

            // Emit kết quả, nhóm theo centroid
            context.write(new Text("Centroid" + closestCentroid), value);
        }

        private double tryParseDouble(String value) {
            try {
                return Double.parseDouble(value);
            } catch (NumberFormatException e) {
                return 0.0;
            }
        }

        private double calculateEuclideanDistance(double[] point, double[] centroid) {
            double sum = 0.0;
            for (int i = 0; i < point.length; i++) {
                sum += Math.pow(point[i] - centroid[i], 2);
            }
            return Math.sqrt(sum);
        }
    }

    public static class KMeansReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder result = new StringBuilder();
            result.append("\n");
            for (Text value : values) {
                result.append(value.toString()).append("\n");
            }
            context.write(key, new Text(result.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "KMeans Clustering");
        job.setJarByClass(KMeansClustering.class);
        job.setMapperClass(KMeansMapper.class);
        job.setReducerClass(KMeansReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}



45.2,26.3,20.9
40.3,87.4,18.2
32.7,86.5,82.1
43.1,54.8,49.8
25.3,25.7,79.4

hadoop jar /home/cloudera/KMeansMapReduce.jar KMeansMapReduce \
/user/cloudera/inputkmeans/mall_customers.csv \
/user/cloudera/outputkmeans \
/user/cloudera/inputkmeans/centroids.txt

hdfs dfs -ls /user/cloudera/inputkmeans

hdfs dfs -mkdir -p /user/cloudera/inputkmeans/
hdfs dfs -put /home/cloudera/mall_customers.csv /user/cloudera/inputkmeans/
hdfs dfs -put /home/cloudera/centroids.txt /user/cloudera/inputkmeans/

hdfs dfs -ls /user/cloudera/inputkmeans/

hdfs dfs -rm -r /user/cloudera/outputkmeans/


