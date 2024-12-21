import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.*;

public class KMeansMapReduce {

    // Class for storing a cluster center
    public static class ClusterCenter {
        double age, income, spending;

        public ClusterCenter(double age, double income, double spending) {
            this.age = age;
            this.income = income;
            this.spending = spending;
        }

        public static ClusterCenter fromString(String str) {
            String[] parts = str.split(",");
            return new ClusterCenter(
                Double.parseDouble(parts[0]),
                Double.parseDouble(parts[1]),
                Double.parseDouble(parts[2])
            );
        }

        public String toString() {
            return age + "," + income + "," + spending;
        }

        public double distanceTo(ClusterCenter other) {
            return Math.sqrt(
                Math.pow(this.age - other.age, 2) +
                Math.pow(this.income - other.income, 2) +
                Math.pow(this.spending - other.spending, 2)
            );
        }
    }

    // Mapper Class
    public static class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private List<ClusterCenter> centers = new ArrayList<>();

        @Override
        protected void setup(Context context) throws IOException {
            Path centroidsPath = new Path(context.getConfiguration().get("centroids.path"));
            try (BufferedReader reader = new BufferedReader(new FileReader(centroidsPath.toString()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    centers.add(ClusterCenter.fromString(line));
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get() == 0 && value.toString().contains("CustomerID")) return; // Skip header

            String[] fields = value.toString().split(",");
            double age = Double.parseDouble(fields[2]);
            double income = Double.parseDouble(fields[3]);
            double spending = Double.parseDouble(fields[4]);

            ClusterCenter customer = new ClusterCenter(age, income, spending);
            int nearestCenterId = -1;
            double nearestDistance = Double.MAX_VALUE;

            for (int i = 0; i < centers.size(); i++) {
                double distance = customer.distanceTo(centers.get(i));
                if (distance < nearestDistance) {
                    nearestDistance = distance;
                    nearestCenterId = i;
                }
            }

            context.write(new IntWritable(nearestCenterId), new Text(customer.toString()));
        }
    }

    // Reducer Class
    public static class KMeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sumAge = 0, sumIncome = 0, sumSpending = 0;
            int count = 0;

            for (Text value : values) {
                ClusterCenter customer = ClusterCenter.fromString(value.toString());
                sumAge += customer.age;
                sumIncome += customer.income;
                sumSpending += customer.spending;
                count++;
            }

            ClusterCenter newCenter = new ClusterCenter(sumAge / count, sumIncome / count, sumSpending / count);
            context.write(key, new Text(newCenter.toString()));
        }
    }

    // Driver Class
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("centroids.path", args[2]);

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        Job job = Job.getInstance(conf, "KMeans Clustering");
        job.setJarByClass(KMeansMapReduce.class);

        job.setMapperClass(KMeansMapper.class);
        job.setReducerClass(KMeansReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
