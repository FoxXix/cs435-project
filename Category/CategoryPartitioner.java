import org.apache.hadoop.mapreduce.Partitioner;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

public class CategoryPartitioner extends Partitioner<Text, IntWritable> {
    @Override
    public int getPartition(Text key, IntWritable value, int numReduceTasks){ 
    
        if (numReduceTasks == 6){ // change logic for top 5 cities and everything else
            String partitionKey = key.toString().split(",")[0].toLowerCase();
            if (partitionKey.equals("las vegas"))
                return 0;
            if (partitionKey.equals("phoenix"))
                return 1;
            if (partitionKey.equals("toronto"))
                return 2;
            if (partitionKey.equals("charlotte"))
                return 3;
            if (partitionKey.equals("scottsdale"))
                return 4;
        }
        return 5;
    }
}
