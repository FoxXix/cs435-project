import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Top10Driver {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "top10high");
        job.setJarByClass(Top10Driver.class);
        job.setMapperClass(Top10Mapper.class);
        job.setReducerClass(Top10Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        //job.setNumReduceTasks(6);
        //job.setPartitionerClass(CategoryPartitioner.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        if (job.waitForCompletion(true)){
        
            Configuration conf2 = new Configuration();
            Job job2 = Job.getInstance(conf2, "top10low");
            job2.setJarByClass(Top10Driver.class);
            job2.setMapperClass(Top10LowMapper.class);
            job2.setReducerClass(Top10LowReducer.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(DoubleWritable.class);
            //job2.setNumReduceTasks(6);
            //job2.setPartitionerClass(CategoryPartitioner.class);
            FileInputFormat.addInputPath(job2, new Path(args[0]));
            FileOutputFormat.setOutputPath(job2, new Path(args[2]));
            
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
        
    }
}
