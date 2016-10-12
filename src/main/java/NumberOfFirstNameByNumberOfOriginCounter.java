import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NumberOfFirstNameByNumberOfOriginCounter {

    public static class SemiColonMapper
            extends Mapper<Object, Text, IntWritable, IntWritable>{

        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            // Separate each information to get the one we want
            String[] data = value.toString().split(";");

            // The origins are in the third place of the data set
            // Each origin is separated with a comma
            String[] origins = data[2].split(", ");

            // The number of origins is the key, and 1 is the value
            context.write(new IntWritable(origins.length), one);
        }
    }

    public static class IntSumReducer
            extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(IntWritable key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            // Initialize the final sum with 0
            int sum = 0;

            // Let's browse all the given values
            for (IntWritable val : values) {
                // Each value is added to the final sum
                sum += val.get();
            }

            result.set(sum);

            // The key is the word given in argument, and the value is the final sum
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // A new job is created, with a given configuration
        Job job = Job.getInstance(conf, "count number of first name by number of origin");
        job.setJarByClass(NumberOfFirstNameByNumberOfOriginCounter.class);

        // Specify the mapper and reducer class
        job.setMapperClass(SemiColonMapper.class);
        job.setReducerClass(IntSumReducer.class);

        // Set the output key and value as integers
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        // Set the input and output files the same as the ones given in argument
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Start the job and wait for it to finish
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}