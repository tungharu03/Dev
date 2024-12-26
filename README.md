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

            // Gửi kết quả: ID của cluster, CustomerID và thông tin khách hàng
            context.write(new Text(String.valueOf(centroid)),
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
            for (Text value : values) {
                // Gửi thông tin khách hàng cùng với ID của cluster
                context.write(value, new Text(key.toString()));
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

    public static class KMeansMapper extends Mapper<Object, Text, Text, Text> {
        private List<double[]> centroids = new ArrayList<>();

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

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty() || line.startsWith("CustomerID")) return; // Skip header or empty lines

            String[] fields = line.split(",");
            if (fields.length < 5) return; // Ensure correct format

            String customerID = fields[0].trim();
            double age;
            double income;
            double score;
            try {
                age = Double.parseDouble(fields[2].trim());
                income = Double.parseDouble(fields[3].trim());
                score = Double.parseDouble(fields[4].trim());
            } catch (NumberFormatException e) {
                return; // Skip invalid numeric entries
            }

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

            // Output format: Cluster ID, Customer ID, Customer Information
            context.write(new Text(String.valueOf(closestCentroid)),
                    new Text(customerID + "," + age + "," + income + "," + score + "," + closestCentroid));
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
                // Output customer info along with cluster ID
                context.write(value, new Text(key.toString()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: KMeansClustering <input path> <output path>");
            System.exit(-1);
        }

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

