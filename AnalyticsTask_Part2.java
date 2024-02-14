import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// DS503 Project 1, Question 3.3: Query 3 (Part 2)

public class AnalyticsTask_Part2 {

    // Take in joined values, set age range, and prep for reduce phase
    public static class JoinedMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Takes in a joined customer transaction with [0] id, [1] age, [2] gender, and
            // [3] TransTotal
            // and turns it into a string
            String record = value.toString();
            // Divides record string into pieces based on commas
            String[] parts = record.split(",");
            // Arranges ages based on range
            int age = Integer.parseInt(parts[1]);
            String ageRange = "";
            if (age >= 10 && age < 20) {
                ageRange = "[10 to 20)";
            } else if (age >= 20 && age < 30) {
                ageRange = "[20 to 30)";
            } else if (age >= 30 && age < 40) {
                ageRange = "[30 to 40)";
            } else if (age >= 40 && age < 50) {
                ageRange = "[40 to 50)";
            } else if (age >= 50 && age < 60) {
                ageRange = "[50 to 60)";
            } else if (age >= 60 && age <= 70) {
                ageRange = "[60 to 70]";
            }
            // Sends needed info through key-value pair
            // key = ageRange, gender
            // value = ageRange, gender, TransTotal
            context.write(new Text(ageRange + "," + parts[2]), new Text(ageRange + "," + parts[2] + "," + parts[3]));
        }
    }

    // Do TransTotal calculations and print results
    public static class AnalyticsReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            // Initialize variables
            String ageRange = "";
            String gender = "";
            float minTransTotal = Float.MAX_VALUE;
            float maxTransTotal = Float.MIN_VALUE;
            float totalTransTotal = 0;
            int count = 0;

            // For each input...
            for (Text t : values) {
                // Separate fields by commas
                String parts[] = t.toString().split(",");

                // Set ageRange, gender, and transTotal
                ageRange = parts[0];
                gender = parts[1];
                float transTotal = Float.parseFloat(parts[2]);

                // If current transTotal is less than minTransTotal...
                if (transTotal < minTransTotal) {
                    // Set minTransTotal to that value
                    minTransTotal = transTotal;
                }

                // If current transTotal is more than maxTransTotal...
                if (transTotal > maxTransTotal) {
                    // Set maxTransTotal to that value
                    maxTransTotal = transTotal;
                }

                // Update totalTransTotal
                totalTransTotal += transTotal;

                // Update count
                count++;
            }

            // Calculate average TransTotal using totalTransTotal and count
            float avgTransTotal = totalTransTotal / count;

            // Print output: ageRange, gender, minTransTotal, maxTransTotal, avgTransTotal
            String result = String.format("%s,%s,%f,%f,%f", ageRange, gender, minTransTotal, maxTransTotal,
                    avgTransTotal);
            context.write(null, new Text(result));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "analytics task part 2");
        job.setJarByClass(AnalyticsTask_Part2.class);
        job.setMapperClass(JoinedMapper.class);
        job.setReducerClass(AnalyticsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}