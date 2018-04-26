import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;


public class Top10Reducer extends Reducer<Text, DoubleWritable, Text, Text> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            for(DoubleWritable value : values) {
                context.write(new Text("CATEGORY: " + key.toString()), 
                              new Text("RATIO: " + Double.toString(value.get())));
            }
        }
    }
