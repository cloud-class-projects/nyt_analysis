import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Map.Entry;
import java.util.TreeMap;

import com.google.gson.Gson;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;

public class MongoSentimentDataRetriever {
	
	/*
	 * This method takes the name of a country or company as input and 
	 * retrieves its sentiment values from MongoDB.
	 */
	
	public static void main(String[] args) {
		try {

			if (args.length != 2) {
				System.out
						.println("Usage: MongoSentimentDataRetriever country/company <CountryName/CompanyName>");
				System.exit(-1);
			}
			
			// Set the MongoDB drivers.
			DBCollection coll = setDrivers(args[1]);
			BasicDBObject returnField = new BasicDBObject();
			
			// based on the given input (country/company), set the 
			// corresponding fields to retrieve from MongoDB
			if (args[0].equals("country")) {
				returnField.put("_id", 1);
				returnField.put("count", 1);
			} else if (args[0].equals("company")) {
				returnField.put("_id", 1);
				returnField.put("sentiment", 1);
			}

			// Instantiate a file writer object
			BufferedWriter writer = new BufferedWriter(new FileWriter(
					"/home/ubuntu/" + args[1] + "_sentiment"));
			
			// Build and Execute the query
			DBCursor cursor = coll.find(null, returnField);
			
			// Instantiate a Gson object to convert the retrieved 
			// JSON values to Java objects.
			Gson gson = new Gson();
			
			// Instantiate a TreeMap to store the retrieved values.
			TreeMap<String, Integer> map = new TreeMap<String, Integer>();

			// Convert and Store the values in a TreeMap.
			while (cursor.hasNext()) {

				if (args[0].equals("country")) {
					CountryDataObject obj = gson.fromJson(cursor.next()
							.toString(), CountryDataObject.class);
					String date = obj._id;
					int count = obj.count;
					map.put(date, count);
				} else if (args[0].equals("company")) {
					CompanyDataObject obj = gson.fromJson(cursor.next()
							.toString(), CompanyDataObject.class);
					String date = obj._id;
					int sentiment = obj.sentiment;
					map.put(date, sentiment);
				}
			}
			
			// Write the map to a file
			for (Entry<String, Integer> entry : map.entrySet()) {
				writer.write(entry.getKey() + "\t" + entry.getValue());
				writer.write("\n");
			}
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/*
	 * This method sets the drivers so that Java can communicate with MongoDB.
	 * Returns a MongoDB collection object.
	 */
	public static DBCollection setDrivers(String args1) {
		DBCollection coll = null;
		try {
			MongoClient client = new MongoClient("127.0.0.1", 27017);
			DB db = client.getDB("nytimesdb");
			coll = db.getCollection(args1 + "_sentiment");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return coll;
	}
}
