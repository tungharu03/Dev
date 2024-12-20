import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

    public static class KMeansMapper extends Mapper<Object, Text, IntWritable, Text> {
        private List<double[]> centroids = new ArrayList<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Load centroids from configuration
            String[] centroidStrings = context.getConfiguration().get("centroids").split(";");
            for (String centroid : centroidStrings) {
                String[] parts = centroid.split(",");
                double[] coords = {Double.parseDouble(parts[0]), Double.parseDouble(parts[1]), Double.parseDouble(parts[2])};
                centroids.add(coords);
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length != 5 || parts[0].equals("CustomerID")) return; // Skip header or malformed lines

            double age = Double.parseDouble(parts[2]);
            double income = Double.parseDouble(parts[3]);
            double score = Double.parseDouble(parts[4]);

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

            context.write(new IntWritable(closestCentroid), new Text(age + "," + income + "," + score));
        }

        private double euclideanDistance(double[] point, double[] centroid) {
            return Math.sqrt(Math.pow(point[0] - centroid[0], 2) + Math.pow(point[1] - centroid[1], 2) + Math.pow(point[2] - centroid[2], 2));
        }
    }

    public static class KMeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sumAge = 0, sumIncome = 0, sumScore = 0;
            int count = 0;

            for (Text value : values) {
                String[] parts = value.toString().split(",");
                double age = Double.parseDouble(parts[0]);
                double income = Double.parseDouble(parts[1]);
                double score = Double.parseDouble(parts[2]);

                sumAge += age;
                sumIncome += income;
                sumScore += score;
                count++;
            }

            if (count > 0) {
                double newAge = sumAge / count;
                double newIncome = sumIncome / count;
                double newScore = sumScore / count;
                context.write(key, new Text(newAge + "," + newIncome + "," + newScore));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Initial centroids (manually set for simplicity)
        conf.set("centroids", "19,15,39;21,15,81;20,16,6;23,16,77;31,17,40");

        Job job = Job.getInstance(conf, "KMeans Clustering");
        job.setJarByClass(KMeansClustering.class);
        job.setMapperClass(KMeansMapper.class);
        job.setReducerClass(KMeansReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
