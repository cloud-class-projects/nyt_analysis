import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BasicBSONObject;

import com.mongodb.hadoop.io.BSONWritable;

public class CorpNewsSentimentAnalyzerReducer extends
		Reducer<Text, IntWritable, Text, BSONWritable> {

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int result = 0;

		for (IntWritable num : values) {
			result = num.get();
		}

		BasicBSONObject sentiment = new BasicBSONObject();
		sentiment.put("sentiment", result);

		context.write(key, new BSONWritable(sentiment));
	}

}
