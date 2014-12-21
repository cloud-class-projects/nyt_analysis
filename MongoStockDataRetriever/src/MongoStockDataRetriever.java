import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.TreeMap;

import com.google.gson.Gson;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;

public class MongoStockDataRetriever {

	/*
	 * This method takes the name of a company as input and retrieves its stock
	 * values from MongoDB.
	 */

	public static void main(String[] args) throws IOException {

		if (args.length != 1) {
			System.out.println("Usage: MongoStockDataRetriever <CompanyName>");
			System.exit(-1);
		}

		// Set the MongoDB drivers.
		DBCollection coll = setDrivers();

		// Parse the name of the company
		String company = null;
		if (args[0].equals("Microsoft")) {
			company = "MSFT";
		} else if (args[0].equals("Amazon")) {
			company = "AMZN";
		} else if (args[0].equals("Google")) {
			company = "GOOGL";
		}

		// Instantiate a file writer object
		BufferedWriter writer = new BufferedWriter(new FileWriter(
				"/home/ubuntu/" + args[0] + "_stock"));

		// Create MongoDB objects required to query the ddatabase
		BasicDBObject searchCriteria = new BasicDBObject("company", company);
		BasicDBObject returnFields = new BasicDBObject("_id", 0).append("date",
				1).append("close", 1);

		// Build and Execute the query
		DBCursor cursor = coll.find(searchCriteria, returnFields);
		Gson gson = new Gson();
		int x = 0;
		float calculatedClosing = 0;

		// Instantiate a TreeMap to store the retrieved values.
		TreeMap<String, Integer> map = new TreeMap<String, Integer>();

		/*
		 * Parse the retrieved values. If the stock price is less than previous
		 * day's price, assign code 1 If the stock price is equal to previous
		 * day's price, assign code 2 If the stock price is greater than
		 * previous day's price, assign code 3 Write the values to the map.
		 */
		while (cursor.hasNext()) {

			StockDataObject obj = gson.fromJson(cursor.next().toString(),
					StockDataObject.class);

			String date = obj.date;
			float closingPrice = obj.close;

			String[] dateArray = null;
			dateArray = date.split("-");

			String month = dateArray[1].replaceFirst("^0+(?!$)", "");
			String day = dateArray[2].replaceFirst("^0+(?!$)", "");

			String newDate = dateArray[0] + "-" + month + "-" + day;

			calculatedClosing = closingPrice - calculatedClosing;
			if (calculatedClosing > 0) {
				x = 3;
			} else if (calculatedClosing < 0) {
				x = 1;
			} else if (calculatedClosing == 0) {
				x = 2;
			}
			calculatedClosing = closingPrice;

			map.put(newDate, x);
		}

		// Write the map to a file
		for (Entry<String, Integer> entry : map.entrySet()) {
			writer.write(entry.getKey() + "\t" + entry.getValue());
			writer.write("\n");
		}

		writer.close();
	}

	/*
	 * This method sets the drivers so that Java can communicate with MongoDB.
	 * Returns a MongoDB collection object.
	 */
	public static DBCollection setDrivers() {
		DBCollection coll = null;
		try {
			MongoClient client = new MongoClient("127.0.0.1", 27017);
			DB db = client.getDB("nytimesdb");
			coll = db.getCollection("stock");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return coll;
	}

}