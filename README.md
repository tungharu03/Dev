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
import java.util.List;

public class KMeansClustering {

    public static class KMeansMapper extends Mapper<Object, Text, Text, Text> {
        private List<double[]> centroids = new ArrayList<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String[] centroidStrings = context.getConfiguration().get("centroids").split(";\s*");
            for (String centroid : centroidStrings) {
                String[] values = centroid.split(",");
                centroids.add(new double[]{
                        Double.parseDouble(values[0]),
                        Double.parseDouble(values[1]),
                        Double.parseDouble(values[2])
                });
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (value.toString().startsWith("CustomerID")) return; // Skip header

            String[] fields = value.toString().split(",");
            double age = Double.parseDouble(fields[2]);
            double income = Double.parseDouble(fields[3]);
            double score = Double.parseDouble(fields[4]);

            double[] point = {age, income, score};
            int closestCentroid = 0;
            double minDistance = Double.MAX_VALUE;

            for (int i = 0; i < centroids.size(); i++) {
                double distance = euclideanDistance(point, centroids.get(i));
                if (distance < minDistance) {
                    minDistance = distance;
                    closestCentroid = i;
                }
            }

            context.write(new Text(String.valueOf(closestCentroid)), value);
        }

        private double euclideanDistance(double[] point, double[] centroid) {
            double sum = 0.0;
            for (int i = 0; i < point.length; i++) {
                sum += Math.pow(point[i] - centroid[i], 2);
            }
            return Math.sqrt(sum);
        }
    }

    public static class KMeansReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<double[]> points = new ArrayList<>();

            for (Text value : values) {
                String[] fields = value.toString().split(",");
                points.add(new double[]{
                        Double.parseDouble(fields[2]),
                        Double.parseDouble(fields[3]),
                        Double.parseDouble(fields[4])
                });
            }

            double[] newCentroid = calculateCentroid(points);
            context.write(new Text(String.format("%.2f,%.2f,%.2f", newCentroid[0], newCentroid[1], newCentroid[2])), key);
        }

        private double[] calculateCentroid(List<double[]> points) {
            double[] centroid = new double[3];
            for (double[] point : points) {
                for (int i = 0; i < point.length; i++) {
                    centroid[i] += point[i];
                }
            }
            for (int i = 0; i < centroid.length; i++) {
                centroid[i] /= points.size();
            }
            return centroid;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("centroids", "45.2,26.3,20.9;40.3,87.4,18.2;32.7,86.5,82.1;43.1,54.8,49.8;25.3,25.7,79.4");

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

CREATE TABLE kmeans_output (
    centroid STRING,
    cluster_id INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','; 

LOAD DATA INPATH '/outputcluster/part-r-00000' INTO TABLE kmeans_output;
SELECT * FROM kmeans_output;


hdfs dfs -ls /user/cloudera/inputkmeans/

hdfs dfs -chown hive:hive /outputcluster/part-r-00000
hdfs dfs -chmod 755 /outputcluster/part-r-00000
hdfs dfs -chmod -R 777 /user/hive/warehouse

hdfs dfs -rm -r /user/cloudera/outputkmeans/


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KMeansClustering {
    public static class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private static final int K = 5;  // Số lượng cụm

        // Tính toán khoảng cách Euclidean giữa 2 điểm (x1, y1) và (x2, y2)
        private double euclideanDistance(double x1, double y1, double x2, double y2) {
            return Math.sqrt(Math.pow(x2 - x1, 2) + Math.pow(y2 - y1, 2));
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split("\t");

            // Đọc dữ liệu
            int customerID = Integer.parseInt(parts[0]);
            String gender = parts[1];
            double age = Double.parseDouble(parts[2]);
            double income = Double.parseDouble(parts[3]);
            double spendingScore = Double.parseDouble(parts[4]);

            // Tính toán khoảng cách từ điểm khách hàng đến các trung tâm cụm
            double[] customerData = {age, income, spendingScore};
            double[] centroid = { /* Trung tâm các cụm */ };

            // Tìm cụm gần nhất
            int closestCluster = -1;
            double minDistance = Double.MAX_VALUE;

            for (int i = 0; i < K; i++) {
                double distance = euclideanDistance(customerData[0], customerData[1], centroid[0], centroid[1]);
                if (distance < minDistance) {
                    minDistance = distance;
                    closestCluster = i;
                }
            }

            // Gửi thông tin CustomerID, Gender, Age, Income, SpendingScore và Cluster
            context.write(new IntWritable(closestCluster), new Text(customerID + "\t" + gender + "\t" + age + "\t" + income + "\t" + spendingScore));
        }
    }

    public static class KMeansReducer extends Reducer<IntWritable, Text, Text, NullWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Các bước cập nhật trung tâm cụm và phân loại lại các điểm
            List<String> customerData = new ArrayList<>();
            double sumAge = 0, sumIncome = 0, sumSpendingScore = 0;
            int count = 0;

            // Cập nhật trung tâm cụm và phân loại lại các điểm
            for (Text value : values) {
                String[] parts = value.toString().split("\t");
                int customerID = Integer.parseInt(parts[0]);
                String gender = parts[1];
                double age = Double.parseDouble(parts[2]);
                double income = Double.parseDouble(parts[3]);
                double spendingScore = Double.parseDouble(parts[4]);

                customerData.add(value.toString());

                sumAge += age;
                sumIncome += income;
                sumSpendingScore += spendingScore;
                count++;
            }

            // Tính toán trung tâm mới của cụm
            double newCentroidAge = sumAge / count;
            double newCentroidIncome = sumIncome / count;
            double newCentroidSpendingScore = sumSpendingScore / count;

            // Ghi các kết quả phân cụm vào output
            for (String customer : customerData) {
                String[] parts = customer.split("\t");
                context.write(new Text(customer + "\t" + key.get()), NullWritable.get());
            }

            // Cập nhật trung tâm mới (Có thể lưu trung tâm vào một file riêng hoặc tính toán lại trong các bước tiếp theo)
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "KMeans Clustering");
        job.setJarByClass(KMeansClustering.class);

        // Định nghĩa Mapper và Reducer
        job.setMapperClass(KMeansMapper.class);
        job.setReducerClass(KMeansReducer.class);

        // Định nghĩa Output Key và Value
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        // Đọc dữ liệu vào và xuất kết quả ra
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Chạy job MapReduce
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
