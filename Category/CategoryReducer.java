import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;


public class CategoryReducer extends Reducer<Text, IntWritable, Text, Text> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        
            double isClosed = 0;
            double isOpen = 0;
            int totalBusinesses = 0;
            
            for (IntWritable value : values) {
                if(value.get() == 1) {
                    isOpen++;
                }
                else {
                    isClosed++;
                }
                totalBusinesses++;
            }
            
            double closedToOpenRatio = isClosed / isOpen;
            if (totalBusinesses >= 100) {
                context.write(new Text("CITY: " + key.toString().split(",")[0] + "\t" +
                                       "CATEGORY: " + key.toString().split(",")[1]), 
                                        new Text("RATIO: " + Double.toString(closedToOpenRatio)
                                        + "\t" + "NUMBER OF BUSINESSES: " + Integer.toString(totalBusinesses)));
            }
        }
    }
