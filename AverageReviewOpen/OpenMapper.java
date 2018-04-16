import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public static class Mapper extends Mapper<Object, Text, Text, Text>{
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        //preprocess
        if (!value.toString().isEmpty()) {
            StringTokenizer itr = new StringTokenizer(value.toString().split("<====>")[2]);
            
            while (itr.hasMoreTokens()) {
                String out = itr.nextToken().replaceAll("[^A-Za-z0-9]", "").toLowerCase();                    
                word.set(out);                    
                context.write(word, one);
            }
        }
    }
}
