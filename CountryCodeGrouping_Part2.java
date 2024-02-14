import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// DS503 Project 1, Question 3.2: Query 2 (Part 2)

public class CountryCodeGrouping_Part2 {

    // Take in joined values and prep for reduce phase
    public static class JoinedMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Takes in a joined customer transaction with [0] id, [1] countryCode, and [2]
            // TransTotal and turns it into a string
            String record = value.toString();
            // Divides record string into pieces based on commas
            String[] parts = record.split(",");
            // Sends needed info through key-value pair
            // key = [1] countryCode
            // value = [1] countryCode, [2] TransTotal
            context.write(new Text(parts[1]), new Text(parts[1] + "," + parts[2]));
        }
    }

    // Group by countryCode, do TransTotal and NumberOfCustomers calculations, and
    // print results
    public static class CountryCodeGroupingReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            // Initialize variables
            int countryCode = 0;
            float minTransTotal = Float.MAX_VALUE;
            float maxTransTotal = Float.MIN_VALUE;
            int numberOfCustomers = 0;

            // For each input...
            for (Text t : values) {
                // Separate fields by commas
                String parts[] = t.toString().split(",");

                // Set countryCode and TransTotal
                countryCode = Integer.parseInt(parts[0]);
                float transTotal = Float.parseFloat(parts[1]);

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

                // Update numberOfCustomers
                numberOfCustomers++;
            }

            // Print output: countryCode, numberOfCustomers, minTransTotal, maxTransTotal
            String result = String.format("%d,%d,%f,%f", countryCode, numberOfCustomers, minTransTotal,
                    maxTransTotal);
            context.write(null, new Text(result));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "country code grouping part 2");
        job.setJarByClass(CountryCodeGrouping_Part2.class);
        job.setMapperClass(JoinedMapper.class);
        job.setReducerClass(CountryCodeGroupingReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}