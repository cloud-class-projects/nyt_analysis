import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BasicBSONObject;

import com.mongodb.hadoop.io.BSONWritable;

/*
 * This is the reducer class.
 * Input Key format: Text
 * Input value format: Iterable<IntWritable>
 * Output Key format: Text
 * Output value format: BSONWritable
 */

public class SentimentAnalyzerReducer extends
		Reducer<Text, IntWritable, Text, BSONWritable> {

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int sum = 0;
		
		// get the sentiment values for each type of sentiment
		// and aggregate them by month
		for (IntWritable num : values) {
			sum += num.get();
		}
		
		// Instantiate a BSONObject(MongoDB format)
		BasicBSONObject count = new BasicBSONObject();
		count.put("count", sum);
		
		// Write the output to MongoDB
		context.write(key, new BSONWritable(count));
	}

}
