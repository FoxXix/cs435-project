import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


public class OpenMapper extends Mapper<LongWritable, Text, Text, Text>{
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //preprocess
        if (!value.toString().isEmpty()) {
            // StringTokenizer itr = new StringTokenizer(value.toString().split(",")[2]);
            String[] line = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
            String city = line[4];
            String numStars = line[9];
            String numReviews = line[10];
            String isOpen = line[11];
            
            String cityInfo = numStars + "," + numReviews + "," + isOpen;
            if (!city.equals("city"))
                context.write(new Text(city.toLowerCase()), new Text(cityInfo));
            
            /*
            while (itr.hasMoreTokens()) {
                String out = itr.nextToken().replaceAll("[^A-Za-z0-9]", "").toLowerCase();                    
                word.set(out);                    
                context.write(word, one);
            } */
        }
    }
}
