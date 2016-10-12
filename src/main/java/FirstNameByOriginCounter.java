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

public class FirstNameByOriginCounter {

    public static class SemiColonMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            // Separate each information to get the one we want
            String[] data = value.toString().split(";");

            // The origins are in the third part of the data set
            // There can be more than one origin, each separated by a comma
            String[] origins = data[2].split(", ");

            // Let's browse the origins
            for (int i = 0; i < origins.length; ++i) {
                word.set(origins[i]);

                // The origin is the key, and 1 is the value
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
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
        Job job = Job.getInstance(conf, "count first name by origin");
        job.setJarByClass(FirstNameByOriginCounter.class);

        // Specify the mapper and reducer class
        job.setMapperClass(SemiColonMapper.class);
        job.setReducerClass(IntSumReducer.class);

        // Set the output key as text and output value as integer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Set the input and output files the same as the ones given in argument
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Start the job and wait for it to finish
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}