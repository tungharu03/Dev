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

    // Lớp Mapper, xử lý các dòng dữ liệu và xác định centroid gần nhất
    public static class KMeansMapper extends Mapper<Object, Text, Text, Text> {
        private List<double[]> centroids = new ArrayList<>();

        // Phương thức setup, được gọi một lần trước khi xử lý các dòng dữ liệu, để thiết lập các centroid
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String[] centroidStrings = context.getConfiguration().get("centroids").trim().split(";\s*");
            for (String centroid : centroidStrings) {
                String[] values = centroid.split(",");
                centroids.add(new double[]{
                        Double.parseDouble(values[0].trim()), 
                        Double.parseDouble(values[1].trim()), 
                        Double.parseDouble(values[2].trim())
                });
            }
        }

        // Phương thức map, xử lý từng dòng dữ liệu và xác định centroid gần nhất
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return; // Bỏ qua dòng tiêu đề

            String[] fields = line.split(",");
            if (fields.length < 5) return; // Kiểm tra định dạng dữ liệu

            int customerID;
            double age;
            double income;
            double score;
            try {
                // Chuyển đổi các giá trị số
                customerID = Integer.parseInt(fields[0].trim());
                age = Double.parseDouble(fields[2].trim());
                income = Double.parseDouble(fields[3].trim());
                score = Double.parseDouble(fields[4].trim());
            } catch (NumberFormatException e) {
                return; 
            }

            // Xác định điểm dữ liệu gần với centroid nào nhất
            double[] point = {age, income, score};
            int centroid = 0;
            double minDistance = Double.MAX_VALUE;

            // Duyệt qua tất cả các centroid và tính khoảng cách Euclidean
            for (int i = 0; i < centroids.size(); i++) {
                double distance = euclideanDistance(point, centroids.get(i)); // Tính khoảng cách Euclidean
                if (distance < minDistance) {
                    minDistance = distance;
                    centroid = i;  // Đổi tên từ 'closestCentroid' thành 'centroid'
                }
            }

            // Gửi kết quả: chỉ thông tin khách hàng và ID của cluster
            context.write(new Text(String.valueOf(customerID)), 
                          new Text(customerID + "," + age + "," + income + "," + score + "," + centroid));
        }

        // Hàm tính khoảng cách Euclidean giữa điểm dữ liệu và centroid
        private double euclideanDistance(double[] point, double[] centroid) {
            double sum = 0.0;
            for (int i = 0; i < point.length; i++) {
                sum += Math.pow(point[i] - centroid[i], 2);
            }
            return Math.sqrt(sum); // Trả về khoảng cách Euclidean
        }
    }

    // Lớp Reducer, giúp gom nhóm kết quả theo Cluster ID
    public static class KMeansReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Ghi thông tin khách hàng cùng với centroid vào output, không ghi thêm Cluster ID
            for (Text value : values) {
                context.write(value, null); // Ghi thông tin khách hàng và centroid vào output
            }
        }
    }

    // Phương thức main, thiết lập cấu hình và thực thi Job MapReduce
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: KMeansClustering <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        // Cấu hình các centroid (giá trị ban đầu cho các cluster)
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

    // Lớp Mapper, xử lý các dòng dữ liệu và xác định centroid gần nhất
    public static class KMeansMapper extends Mapper<Object, Text, Text, Text> {
        private List<int[]> centroids = new ArrayList<>();

        // Phương thức setup, được gọi một lần trước khi xử lý các dòng dữ liệu, để thiết lập các centroid
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String[] centroidStrings = context.getConfiguration().get("centroids").trim().split(";\s*");
            for (String centroid : centroidStrings) {
                String[] values = centroid.split(",");
                centroids.add(new int[]{
                        Integer.parseInt(values[0].trim()), 
                        Integer.parseInt(values[1].trim()), 
                        Integer.parseInt(values[2].trim())
                });
            }
        }

        // Phương thức map, xử lý từng dòng dữ liệu và xác định centroid gần nhất
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return; // Bỏ qua dòng tiêu đề

            String[] fields = line.split(",");
            if (fields.length < 5) return; // Kiểm tra định dạng dữ liệu

            int customerID;
            int age;
            int income;
            int score;
            try {
                // Chuyển đổi các giá trị số
                customerID = Integer.parseInt(fields[0].trim());
                age = Integer.parseInt(fields[2].trim()); // Sử dụng int thay cho double
                income = Integer.parseInt(fields[3].trim()); // Sử dụng int thay cho double
                score = Integer.parseInt(fields[4].trim()); // Sử dụng int thay cho double
            } catch (NumberFormatException e) {
                return; 
            }

            // Xác định điểm dữ liệu gần với centroid nào nhất
            int[] point = {age, income, score};
            int centroid = 0;  // Đổi tên từ 'closestCentroid' thành 'centroid'
            double minDistance = Double.MAX_VALUE;

            // Duyệt qua tất cả các centroid và tính khoảng cách Euclidean
            for (int i = 0; i < centroids.size(); i++) {
                double distance = euclideanDistance(point, centroids.get(i)); // Tính khoảng cách Euclidean
                if (distance < minDistance) {
                    minDistance = distance;
                    centroid = i;  // Đổi tên từ 'closestCentroid' thành 'centroid'
                }
            }

            // Gửi kết quả: chỉ thông tin khách hàng và ID của cluster
            context.write(new Text(String.valueOf(customerID)), 
                          new Text(customerID + "," + age + "," + income + "," + score + "," + centroid));
        }

        // Hàm tính khoảng cách Euclidean giữa điểm dữ liệu và centroid
        private double euclideanDistance(int[] point, int[] centroid) {
            double sum = 0.0;
            for (int i = 0; i < point.length; i++) {
                sum += Math.pow(point[i] - centroid[i], 2);
            }
            return Math.sqrt(sum); // Trả về khoảng cách Euclidean
        }
    }

    // Lớp Reducer, giúp gom nhóm kết quả theo Cluster ID
    public static class KMeansReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Ghi thông tin khách hàng cùng với centroid vào output
            for (Text value : values) {
                context.write(value, null); // Ghi thông tin khách hàng và centroid
            }
        }
    }

    // Phương thức main, thiết lập cấu hình và thực thi Job MapReduce
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: KMeansClustering <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        // Cấu hình các centroid (giá trị ban đầu cho các cluster)
        conf.set("centroids", "45,26,20;40,87,18;32,86,82;43,54,49;25,25,79");

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
            String customerID = fields[0];
            String gender = fields[1];
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

            // Output format: Cluster ID, Customer Information
            context.write(new Text(String.valueOf(closestCentroid)),
                    new Text(customerID + "," + gender + "," + age + "," + income + "," + score));
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
            for (Text value : values) {
                // Append cluster ID to customer info
                context.write(value, new Text(key.toString()));
            }
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


CREATE TABLE phankhuckhachhang (
    customerID STRING,
    age DOUBLE,
    income DOUBLE,
    score DOUBLE,
    centroid INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' ;

LOAD DATA INPATH 'outputcluster/part-00000' INTO TABLE phankhuckhachhang;

CREATE TABLE phankhuckhachhang (
    customerID INT,
    age DOUBLE,
    income DOUBLE,
    score DOUBLE,
    centroid INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
LOCATION '/outputcluster'; 

SELECT 
    AVG(age) AS mean_age,
    MIN(age) AS min_age,
    MAX(age) AS max_age,
    AVG(income) AS mean_income,
    MIN(income) AS min_income,
    MAX(income) AS max_income,
    AVG(score) AS mean_score,
    MIN(score) AS min_score,
    MAX(score) AS max_score
FROM phankhuckhachhang
WHERE centroid = 0;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KMeansClustering {

    public static class Point {
        public double age, income, score;

        public Point(double age, double income, double score) {
            this.age = age;
            this.income = income;
            this.score = score;
        }

        public static double calculateDistance(Point p1, Point p2) {
            return Math.sqrt(Math.pow(p1.age - p2.age, 2) + Math.pow(p1.income - p2.income, 2) + Math.pow(p1.score - p2.score, 2));
        }

        @Override
        public String toString() {
            return age + "," + income + "," + score;
        }
    }

    public static class KMeansMapper extends Mapper<Object, Text, IntWritable, Text> {
        private List<Point> centroids = new ArrayList<>();

        @Override
        protected void setup(Context context) throws IOException {
            // Load initial centroids from configuration
            Configuration conf = context.getConfiguration();
            String[] centroidStrings = conf.get("initial.centroids").split(";");
            for (String s : centroidStrings) {
                String[] parts = s.split(",");
                centroids.add(new Point(Double.parseDouble(parts[0]), Double.parseDouble(parts[1]), Double.parseDouble(parts[2])));
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (value.toString().contains("CustomerID")) {
                return; // Skip header row
            }

            String[] parts = value.toString().split(",");
            double age = Double.parseDouble(parts[2]);
            double income = Double.parseDouble(parts[3]);
            double score = Double.parseDouble(parts[4]);

            Point point = new Point(age, income, score);

            int nearestCluster = 0;
            double minDistance = Double.MAX_VALUE;
            for (int i = 0; i < centroids.size(); i++) {
                double distance = Point.calculateDistance(point, centroids.get(i));
                if (distance < minDistance) {
                    minDistance = distance;
                    nearestCluster = i;
                }
            }

            context.write(new IntWritable(nearestCluster), value);
        }
    }

    public static class KMeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sumAge = 0, sumIncome = 0, sumScore = 0;
            int count = 0;

            List<String> points = new ArrayList<>();
            for (Text value : values) {
                points.add(value.toString());
                String[] parts = value.toString().split(",");
                sumAge += Double.parseDouble(parts[2]);
                sumIncome += Double.parseDouble(parts[3]);
                sumScore += Double.parseDouble(parts[4]);
                count++;
            }

            Point newCentroid = new Point(sumAge / count, sumIncome / count, sumScore / count);

            for (String point : points) {
                context.write(key, new Text(point));
            }

            context.getCounter("Centroids", key.toString()).setValue(Double.doubleToLongBits(newCentroid.age));
            context.getCounter("Centroids", key.toString() + "_income").setValue(Double.doubleToLongBits(newCentroid.income));
            context.getCounter("Centroids", key.toString() + "_score").setValue(Double.doubleToLongBits(newCentroid.score));
        }
    }

    public static void main(String[] args) throws Exception {
        int k = 5;
        Configuration conf = new Configuration();

        // Generate random initial centroids
        Random random = new Random();
        StringBuilder initialCentroids = new StringBuilder();
        for (int i = 0; i < k; i++) {
            double age = 18 + random.nextInt(40);
            double income = 15 + random.nextInt(100);
            double score = 1 + random.nextInt(100);
            initialCentroids.append(age).append(",").append(income).append(",").append(score);
            if (i < k - 1) {
                initialCentroids.append(";");
            }
        }
        conf.set("initial.centroids", initialCentroids.toString());

        boolean converged = false;
        int iteration = 0;

        while (!converged) {
            Job job = Job.getInstance(conf, "K-Means Clustering - Iteration " + iteration);
            job.setJarByClass(KMeansClustering.class);
            job.setMapperClass(KMeansMapper.class);
            job.setReducerClass(KMeansReducer.class);

            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1] + "_iteration_" + iteration));

            boolean success = job.waitForCompletion(true);

            if (!success) {
                System.exit(1);
            }

            // Check for convergence
            converged = true;
            for (int i = 0; i < k; i++) {
                long age = job.getCounters().findCounter("Centroids", Integer.toString(i)).getValue();
                long income = job.getCounters().findCounter("Centroids", Integer.toString(i) + "_income").getValue();
                long score = job.getCounters().findCounter("Centroids", Integer.toString(i) + "_score").getValue();

                String newCentroid = Double.longBitsToDouble(age) + "," + Double.longBitsToDouble(income) + "," + Double.longBitsToDouble(score);
                if (!newCentroid.equals(initialCentroids.toString().split(";")[i])) {
                    converged = false;
                }
            }

            iteration++;
        }

        System.out.println("K-Means Clustering completed in " + iteration + " iterations.");
    }
}

from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler

# 1. Tạo SparkSession
spark = SparkSession.builder \
    .appName("KMeansClustering") \
    .master("yarn") \
    .getOrCreate()

# 2. Đọc dữ liệu từ mall_customers.csv
data = spark.read.csv("mall_customers.csv", header=True, inferSchema=True)

# Hiển thị dữ liệu
data.show()

# 3. Chuyển đổi các cột thành vector
# Chỉ sử dụng các cột số liệu "Age", "Annual Income (k$)", "Spending Score (1-100)" để phân cụm
assembler = VectorAssembler(inputCols=["Age", "Annual Income (k$)", "Spending Score (1-100)"], outputCol="features")
vector_data = assembler.transform(data)

# Chỉ chọn cột "features"
final_data = vector_data.select("features")

# 4. Áp dụng mô hình K-Means với 5 cụm
kmeans = KMeans(k=5, seed=8)
model = kmeans.fit(final_data)

# In ra các tâm cụm
centers = model.clusterCenters()
print("Centers:")
for center in centers:
    print(center)

# Dự đoán cụm cho từng điểm dữ liệu
predictions = model.transform(final_data)
predictions.show()

# 5. Lưu kết quả phân cụm (Tùy chọn)
# Thêm ID và lưu kết quả phân cụm
results = data.join(predictions, data.index == predictions.index) \
    .select("CustomerID", "Age", "Annual Income (k$)", "Spending Score (1-100)", "prediction")

# Lưu kết quả phân cụm vào file CSV
results.write.csv("kmeans_results.csv", header=True)

# Tắt SparkSession
spark.stop()



