import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import java.util.TreeMap;
import java.util.Map;

public class Top10Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
    /*private final static IntWritable one = new IntWritable(1);
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
            
            String cityInfo = numStars + "," + numReviews + "," + isOpen;
            if (!city.equals("city") && !city.isEmpty())
                context.write(new Text(city.toLowerCase()), new Text(cityInfo)); 
            

        }
    } */
    
    private static TreeMap<Double, String> categoryToHighRatioMap = new TreeMap<>();
    // private static TreeMap<String, Double> categoryToLowRatioMap = new TreeMap<>();
    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\\t");
        
        String city = fields[0];
        String category = fields[1].split(":")[1].trim();
        String ratio = fields[2].split(":")[1].trim();
        String numBusinesses = fields[3];
        
        categoryToHighRatioMap.put(Double.parseDouble(ratio), category);
        
        if(categoryToHighRatioMap.size() > 10) {
            categoryToHighRatioMap.remove(categoryToHighRatioMap.firstKey());
        }
    }
    
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for(Map.Entry<Double, String> entry : categoryToHighRatioMap.entrySet()) {
            context.write(new Text(entry.getValue()), new DoubleWritable(entry.getKey()));
        }
    }
}
