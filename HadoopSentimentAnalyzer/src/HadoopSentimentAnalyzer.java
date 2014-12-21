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

	/*
	 * This constructor takes in the name of the country as input.
	 * Builds the MongoDB query and sets all the classes required for the job.
	 * Starts Hadoop job
	 */
	
	public HadoopSentimentAnalyzer(String[] args) {

		// Get the Hadoop configuration
		Configuration config = new Configuration();
		
		// Pass the Hadoop configuration to MongoDB
		MongoConfig mongoConfig = new MongoConfig(config);
		setConf(config);

		// Set input data format class
		mongoConfig.setInputFormat(MongoInputFormat.class);
		
		// Set input URI of the running MongoDB instance
		mongoConfig.setInputURI("mongodb://" + args[0]
				+ ":27017/nytimesdb.parsed_articles");
		
		// Build the MongoDB query/filter criteria
		mongoConfig.setQuery(new BasicDBObject("country", args[1]));

		
		/*
		 * Set all the following classes required for the job to run
		 *  - Mapper
		 *  - Reducer
		 *  - MapperOutputKey
		 *  - MapperOutputValue
		 *  - ReducerOutputKey
		 *  - ReducerOutputValue
		 */
		mongoConfig.setMapper(SentimentAnalyzerMapper.class);
		mongoConfig.setReducer(SentimentAnalyzerReducer.class);
		mongoConfig.setMapperOutputKey(Text.class);
		mongoConfig.setMapperOutputValue(IntWritable.class);
		mongoConfig.setOutputKey(Text.class);
		mongoConfig.setOutputValue(BSONWritable.class);

		// Set output URI of the running MongoDB instance
		mongoConfig.setOutputURI("mongodb://" + args[0] + ":27017/nytimesdb."
				+ args[1] + "_sentiment");
		
		// Set output data format class
		mongoConfig.setOutputFormat(MongoOutputFormat.class);

	}

	/*
	 * This method takes in 2 parameters - IP address of the server running MongoDB
	 * and the name of the country for which sentiments have to generated.
	 * Instantiates a ToolRunner to start the Hadoop Job.
	 */
	
	public static void main(String[] args) throws Exception {

		// Validate input
		if (args.length != 2) {
			System.err
					.println("Usage: NYTimesSentimentAnalysis.sh <IP Adress of the server running MongoDB> <Country>");
			System.exit(-1);
		}
		
		// Start Hadoop job and print the time taken when it completes
		long startTime = System.currentTimeMillis();
		int exitStatus = ToolRunner
				.run(new HadoopSentimentAnalyzer(args), args);
		long endTime = System.currentTimeMillis();
		System.out.println("Hadoop job completed in "
				+ ((endTime - startTime) / 1000) + " seconds");
		System.exit(exitStatus);
	}

}
