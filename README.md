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

        // Các centroid cố định, tương ứng với dữ liệu trong centroids.txt
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
            int customerId = Integer.parseInt(tokens[0]);
            String gender = tokens[1];
            double age = Double.parseDouble(tokens[2]);
            double annualIncome = Double.parseDouble(tokens[3]);
            double spendingScore = Double.parseDouble(tokens[4]);

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
            context.write(new Text("Centroid" + closestCentroid), new Text(value));
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

        private ArrayList<double[]> newCentroids = new ArrayList<>();
        private int k = 5; // Số lượng centroid

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Khởi tạo các centroid mới bằng các giá trị mặc định
            for (int i = 0; i < k; i++) {
                newCentroids.add(new double[] {0.0, 0.0, 0.0});
            }
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            double[] sum = new double[3]; // Để tính tổng các giá trị của centroid

            // Tính tổng các điểm thuộc về centroid này
            for (Text val : values) {
                String[] tokens = val.toString().split(",");
                double age = Double.parseDouble(tokens[2]);
                double annualIncome = Double.parseDouble(tokens[3]);
                double spendingScore = Double.parseDouble(tokens[4]);

                sum[0] += age;
                sum[1] += annualIncome;
                sum[2] += spendingScore;
                count++;
            }

            // Cập nhật centroid mới
            if (count > 0) {
                newCentroids.add(new double[] {sum[0] / count, sum[1] / count, sum[2] / count});
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Ghi centroid mới
            for (int i = 0; i < newCentroids.size(); i++) {
                double[] centroid = newCentroids.get(i);
                String centroidString = centroid[0] + "," + centroid[1] + "," + centroid[2];
                context.write(new Text("NewCentroid" + i), new Text(centroidString));
            }
        }
    }

    public static class KMeansDriver {

        public static void main(String[] args) throws Exception {
            if (args.length != 2) {
                System.err.println("Usage: KMeansDriver <input path> <output path>");
                System.exit(-1);
            }

            // Cấu hình và tạo job Hadoop
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "KMeans Clustering");
            job.setJarByClass(KMeansClustering.class);

            // Cấu hình Mapper và Reducer
            job.setMapperClass(KMeansMapper.class);
            job.setReducerClass(KMeansReducer.class);

            // Cấu hình định dạng dữ liệu đầu ra
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            // Thiết lập đường dẫn input và output
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            // Chạy job
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
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


