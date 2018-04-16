import org.apache.hadoop.mapreduce.Partitioner;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public static class profile1Partitioner extends Partitioner<Text, NullWritable> {
    @Override
    public int getPartition(Text key, NullWritable value, int numReduceTasks){ 
        if (key.toString().isEmpty()){
            return 0;
        }
        if (numReduceTasks == 6){ // change logic for top 5 cities and everything else
            Character partitionKey = key.toString().toLowerCase().charAt(0);
            if (partitionKOpey >= 'a' && partitionKey <= 'd')
                return 0;
            if (partitionKey >= 'e' && partitionKey <= 'k')
                return 1;
            if (partitionKey >= 'l' && partitionKey <= 'p')
                return 2;
            if (partitionKey >= 'q' && partitionKey <= 't')
                return 3;
            if (partitionKey >= 'u' && partitionKey <= 'z')
                return 4;
        }
        return 0;
    }
}
