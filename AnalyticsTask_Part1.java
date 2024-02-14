import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// DS503 Project 1, Question 3.3: Query 3 (Part 1)

public class AnalyticsTask_Part1 {

    // Take in customer dataset and prep for join
    public static class CustomerMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Takes in a single customer record and turns it into a string
            String record = value.toString();
            // Divides record string into pieces based on commas
            // Customer record: [0] ID, [1] Name, [2] Age, [3] Gender, [4] CountryCode, [5]
            // Salary
            String[] parts = record.split(",");
            // Sends needed info through key-value pair:
            // key = [0] ID
            // value = "customer", [0] ID, [2] Age, [3] Gender
            context.write(new Text(parts[0]), new Text("customer" + "," + parts[0] + "," + parts[2] + "," + parts[3]));
        }
    }

    // Take in transaction dataset and prep for join
    public static class TransactionMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Takes in a single transaction record and turns it into a string
            String record = value.toString();
            // Divides record string into pieces based on commas
            // Transaction record: [0] TransID, [1] CustID, [2] TransTotal, [3]
            // TransNumItems, [4] TransDesc
            String[] parts = record.split(",");
            // Sends needed info through key-value pair:
            // key = [1] CustID
            // value = "transaction", [2] TransTotal
            context.write(new Text(parts[1]), new Text("transaction" + "," + parts[2]));
        }
    }

    // Join based on ID
    public static class AnalyticsReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            // Initialize variables
            String id = "";
            int age = 0;
            String gender = "";
            float TransTotal = 0;

            // For each input...
            for (Text t : values) {
                // Separate fields by commas
                String parts[] = t.toString().split(",");

                // If transaction...
                if (parts[0].equals("transaction")) {
                    // Set TransTotal
                    TransTotal = Float.parseFloat(parts[1]);
                }
                // If customer...
                else if (parts[0].equals("customer")) {
                    // Set id, age, and gender values
                    id = parts[1];
                    age = Integer.parseInt(parts[2]);
                    gender = parts[3];
                }
            }

            // Print output: CustomerID, Age, Gender, TransTotal
            String result = String.format("%s,%d,%s,%f", id, age, gender, TransTotal);
            context.write(null, new Text(result));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "analytics task part 1");
        job.setJarByClass(AnalyticsTask_Part1.class);
        job.setReducerClass(AnalyticsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CustomerMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TransactionMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}