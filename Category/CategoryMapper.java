import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


public class CategoryMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //preprocess
        if (!value.toString().isEmpty()) {
            // StringTokenizer itr = new StringTokenizer(value.toString().split(",")[2]);
            String[] line = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
            if(line[0].equals("business_id")) {
                return;
            }
            
            String city = line[4];
            String cat = line[12];
            int isOpen = Integer.parseInt(line[11]);
            
            String[] categories = cat.split(";");
            
            for(String category : categories) {
                context.write(new Text(city + "," + category), new IntWritable(isOpen));
            }
            
           /* String cityInfo = numStars + "," + numReviews + "," + isOpen;
            if (!city.equals("city") && !city.isEmpty())
                context.write(new Text(city.toLowerCase()), new Text(cityInfo)); */
            

        }
    }
}
