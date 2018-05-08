import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;


public class CategoryReducer extends Reducer<Text, IntWritable, Text, Text> {
        
        // INPUT K-V: <{city,category}, [0, 1, 0, ...]>
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
            
            // Write out ratio if the number of businesses is greater than threshold
            double closedToOpenRatio = isClosed / isOpen;
            if (totalBusinesses >= 100) {
                context.write(new Text("CITY: " + key.toString().split(",")[0] + "\t" +
                                       "CATEGORY: " + key.toString().split(",")[1]), 
                                        new Text("RATIO: " + Double.toString(closedToOpenRatio)
                                        + "\t" + "NUMBER OF BUSINESSES: " + Integer.toString(totalBusinesses)));
            }
        }
        
    }
