import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BasicBSONObject;

import com.mongodb.hadoop.io.BSONWritable;

public class SentimentAnalyzerReducer extends
		Reducer<Text, IntWritable, Text, BSONWritable> {

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int sum = 0;
		for (IntWritable num : values) {
			sum += num.get();
		}

		BasicBSONObject count = new BasicBSONObject();
		count.put("count", sum);

		context.write(key, new BSONWritable(count));
	}

}
