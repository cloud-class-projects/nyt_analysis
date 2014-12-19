import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;

import com.mongodb.BasicDBObject;
import com.mongodb.hadoop.MongoConfig;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoTool;

public class HadoopSentimentAnalyzer extends MongoTool {

	public HadoopSentimentAnalyzer(String[] args) {

		Configuration config = new Configuration();
		MongoConfig mongoConfig = new MongoConfig(config);
		setConf(config);

		mongoConfig.setInputFormat(MongoInputFormat.class);
		mongoConfig.setInputURI("mongodb://" + args[0]
				+ ":27017/nytimesdb.parsed_articles");
		mongoConfig.setQuery(new BasicDBObject("country", args[1]));

		mongoConfig.setMapper(SentimentAnalyzerMapper.class);
		mongoConfig.setReducer(SentimentAnalyzerReducer.class);
		mongoConfig.setMapperOutputKey(Text.class);
		mongoConfig.setMapperOutputValue(IntWritable.class);
		mongoConfig.setOutputKey(Text.class);
		mongoConfig.setOutputValue(BSONWritable.class);

		mongoConfig.setOutputURI("mongodb://" + args[0] + ":27017/nytimesdb."
				+ args[1] + "_sentiment");
		mongoConfig.setOutputFormat(MongoOutputFormat.class);

	}

	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.err
					.println("Usage: NYTimesSentimentAnalysis.sh <IP Adress of the server running MongoDB> <Country>");
			System.exit(-1);
		}

		long startTime = System.currentTimeMillis();
		int exitStatus = ToolRunner
				.run(new HadoopSentimentAnalyzer(args), args);
		long endTime = System.currentTimeMillis();
		System.out.println("Hadoop job completed in "
				+ ((endTime - startTime) / 1000) + " seconds");
		System.exit(exitStatus);
	}

}
