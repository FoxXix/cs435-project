import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;


public class OpenReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double totalClosedAverage = 0;
        double totalOpenAverage = 0;
        double O_numerator = 0, O_denominator = 0, C_numerator = 0, C_denominator = 0, total_reviews = 0;
            
            for (Text val : values) {
                String[] cityInfo = val.toString().split(",");
                
                Double stars = Double.parseDouble(cityInfo[0]);
                Double numReviews = Double.parseDouble(cityInfo[1]);
                Double isOpen = Double.parseDouble(cityInfo[2]);
                
                if (isOpen == 1) {
                    O_numerator += (stars * numReviews);
                    O_denominator += numReviews;
                } else {
                    C_numerator += (stars * numReviews);
                    C_denominator += numReviews;
                }
                
              total_reviews += numReviews;
            }
            
            totalOpenAverage = (O_numerator / O_denominator);
            totalClosedAverage = (C_numerator / C_denominator);
            
            context.write(key, new Text("AVERAGE OPEN: " + Double.toString(totalOpenAverage)
                                        + "\t AVERAGE CLOSED: " + Double.toString(totalClosedAverage)
                                        + "\t TOTAL REVIEWS: " + Double.toString(total_reviews)));
        }
    }
