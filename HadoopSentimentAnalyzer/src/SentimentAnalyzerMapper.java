import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;

public class SentimentAnalyzerMapper extends
		Mapper<Object, BSONObject, Text, IntWritable> {

	CoreNLPSentimentAnalyzer sentiAnalyzer = new CoreNLPSentimentAnalyzer();

	public void map(Object key, BSONObject value, Context context)
			throws IOException, InterruptedException {

		if (value.containsField("year") && value.containsField("month")
				&& value.containsField("main_content")) {
			String main_content = value.get("main_content").toString();
			int result = sentiAnalyzer.getSentiment(main_content);

			if (result == 0) {
				result = 1;
			} else if (result == 4) {
				result = 3;
			}

			String year = value.get("year").toString();
			String month = value.get("month").toString();
			String mapperKey = year + "-" + month + "-" + result;

			context.write(new Text(mapperKey), new IntWritable(1));
		}
	}
}
