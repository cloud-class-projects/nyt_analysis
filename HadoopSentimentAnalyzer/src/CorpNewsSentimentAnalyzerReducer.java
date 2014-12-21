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

public class CorpNewsSentimentAnalyzerReducer extends
		Reducer<Text, IntWritable, Text, BSONWritable> {

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int result = 0;
		
		// get the sentiment values for each day.
		for (IntWritable num : values) {
			result = num.get();
		}
		
		// Instantiate a BSONObject(MongoDB format)
		BasicBSONObject sentiment = new BasicBSONObject();
		sentiment.put("sentiment", result);
		
		// Write the output to MongoDB
		context.write(key, new BSONWritable(sentiment));
	}

}
