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

	public static void main(String[] args) {
		try {

			if (args.length != 2) {
				System.out
						.println("Usage: MongoSentimentDataRetriever country/company <CountryName/CompanyName>");
				System.exit(-1);
			}

			DBCollection coll = setDrivers(args[1]);
			BasicDBObject returnField = new BasicDBObject();

			if (args[0].equals("country")) {
				returnField.put("_id", 1);
				returnField.put("count", 1);
			} else if (args[0].equals("company")) {
				returnField.put("_id", 1);
				returnField.put("sentiment", 1);
			}

			BufferedWriter writer = new BufferedWriter(new FileWriter(
					"/home/ubuntu/" + args[1] + "_sentiment"));

			DBCursor cursor = coll.find(null, returnField);
			Gson gson = new Gson();
			TreeMap<String, Integer> map = new TreeMap<String, Integer>();

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

			for (Entry<String, Integer> entry : map.entrySet()) {
				writer.write(entry.getKey() + "\t" + entry.getValue());
				writer.write("\n");
			}
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

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
