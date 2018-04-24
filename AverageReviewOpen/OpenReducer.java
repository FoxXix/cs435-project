import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;


public class OpenReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        DoubleWritable totalAverage = new DoubleWritable();
        double numerator = 0, denominator = 0;
            
            for (Text val : values) {
                String[] cityInfo = val.toString().split(",");
                
                double stars = Double.parseDouble(cityInfo[0]);
                int numReviews = Integer.parseInt(cityInfo[1]);
                int isOpen = Integer.parseInt(cityInfo[2]);
                
                numerator += (stars * numReviews);
                denominator += numReviews;
            }
            
            totalAverage.set(numerator / denominator);
            
            context.write(key, totalAverage);
        }
    }
