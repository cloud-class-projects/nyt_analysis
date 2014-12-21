import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;

/*
 * This is the mapper class.
 * Input Key format: Object
 * Input value format: BSONObject
 * Output Key format: Text
 * Output value format: IntWritable
 */

public class CorpNewsSentimentAnalyzerMapper extends
		Mapper<Object, BSONObject, Text, IntWritable> {

	// Instantiate CoreNLP object
	CoreNLPSentimentAnalyzer sentiAnalyzer = new CoreNLPSentimentAnalyzer();

	public void map(Object key, BSONObject value, Context context)
			throws IOException, InterruptedException {

		// validate the input and pass the input to the CoreNLP object
		// to generate sentiments
		if (value.containsField("year") && value.containsField("month")
				&& value.containsField("day")
				&& value.containsField("main_content")) {
			String main_content = value.get("main_content").toString();
			int result = sentiAnalyzer.getSentiment(main_content);

			if (result == 0) {
				result = 1;
			} else if (result == 4) {
				result = 3;
			}
			
			// create a custom key to write to the output
			String year = value.get("year").toString();
			String month = value.get("month").toString();
			String date = value.get("day").toString();
			String mapperKey = year + "-" + month + "-" + date;
			
			// write the output to context
			context.write(new Text(mapperKey), new IntWritable(result));
		}
	}
}
