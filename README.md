import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KMeansClustering {

    public static class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private List<double[]> centroids = new ArrayList<>();

        @Override
        protected void setup(Context context) throws IOException {
            // Read centroids from file
            Path[] cacheFiles = context.getLocalCacheFiles();
            try (BufferedReader reader = new BufferedReader(new FileReader(cacheFiles[0].toString()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] tokens = line.split(",");
                    double[] centroid = new double[tokens.length];
                    for (int i = 0; i < tokens.length; i++) {
                        centroid[i] = Double.parseDouble(tokens[i]);
                    }
                    centroids.add(centroid);
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Parse input data
            String[] tokens = value.toString().split(",");
            double[] point = new double[3];
            point[0] = Double.parseDouble(tokens[2]); // Age
            point[1] = Double.parseDouble(tokens[3]); // Annual Income
            point[2] = Double.parseDouble(tokens[4]); // Spending Score

            // Find closest centroid
            int closestCentroid = 0;
            double minDistance = Double.MAX_VALUE;
            for (int i = 0; i < centroids.size(); i++) {
                double distance = 0;
                for (int j = 0; j < point.length; j++) {
                    distance += Math.pow(point[j] - centroids.get(i)[j], 2);
                }
                if (distance < minDistance) {
                    minDistance = distance;
                    closestCentroid = i;
                }
            }

            // Emit centroid ID and point
            context.write(new IntWritable(closestCentroid), new Text(value.toString()));
        }
    }

    public static class KMeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            List<double[]> points = new ArrayList<>();
            for (Text value : values) {
                String[] tokens = value.toString().split(",");
                double[] point = new double[3];
                point[0] = Double.parseDouble(tokens[2]); // Age
                point[1] = Double.parseDouble(tokens[3]); // Annual Income
                point[2] = Double.parseDouble(tokens[4]); // Spending Score
                points.add(point);
            }

            // Compute new centroid
            double[] newCentroid = new double[3];
            for (double[] point : points) {
                for (int i = 0; i < point.length; i++) {
                    newCentroid[i] += point[i];
                }
            }
            for (int i = 0; i < newCentroid.length; i++) {
                newCentroid[i] /= points.size();
            }

            // Emit new centroid
            StringBuilder centroidBuilder = new StringBuilder();
            for (int i = 0; i < newCentroid.length; i++) {
                centroidBuilder.append(newCentroid[i]);
                if (i < newCentroid.length - 1) {
                    centroidBuilder.append(",");
                }
            }
            context.write(key, new Text(centroidBuilder.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "KMeans Clustering");
        job.setJarByClass(KMeansClustering.class);

        // Set Mapper and Reducer classes
        job.setMapperClass(KMeansMapper.class);
        job.setReducerClass(KMeansReducer.class);

        // Set output key and value types
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Add centroids file to cache
        job.addCacheFile(new Path(args[2]).toUri());

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


