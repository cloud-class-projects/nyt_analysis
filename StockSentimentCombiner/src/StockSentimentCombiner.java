import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

public class StockSentimentCombiner {

	/**
	 * @param args
	 *            Company Name
	 * @throws IOException
	 * @author Sriram Pulipaka
	 */

	/**
	 * This method takes in 2 files as input: 1.Stock Data of a company
	 * 2.Sentiment Data of a company and parses the files and generates a new
	 * file which can be used as input to Rscript
	 */

	public static void main(String[] args) throws IOException {

		/*
		 * Instantiate 2 readers to read from 2 files and a writer to write data
		 * to a file
		 */
		BufferedReader reader1 = new BufferedReader(new FileReader(
				"/home/ubuntu/" + args[0] + "_stock"));

		BufferedReader reader2 = new BufferedReader(new FileReader(
				"/home/ubuntu/" + args[0] + "_sentiment"));

		BufferedWriter writer = new BufferedWriter(new FileWriter(
				"/home/ubuntu/" + args[0] + "_combined"));

		/*
		 * Instantiate a TreeMap with an ArrayList. Key of the map is the Date
		 * and the 2 values in the ArrayList are stock and sentiment. All the
		 * values are tab separated
		 */

		TreeMap<String, ArrayList<Integer>> map = new TreeMap<String, ArrayList<Integer>>();

		String line = null;
		String[] split;
		while ((line = reader1.readLine()) != null) {
			split = line.split("\t");
			if (map.get(split[0]) == null) {
				String[] dateArray = split[0].split("-");
				int year = Integer.parseInt(dateArray[0]);
				// Insert stock values into the ArrayList for each date
				// where year > 2007
				if (year > 2007) {
					map.put(split[0], new ArrayList<Integer>());
					map.get(split[0]).add(Integer.parseInt(split[1]));
				}
			}
		}
		while ((line = reader2.readLine()) != null) {
			split = line.split("\t");
			// Insert sentiment values into the ArrayList for each date
			// where stock is available
			if (map.get(split[0]) != null)
				map.get(split[0]).add(Integer.parseInt(split[1]));
		}
		reader1.close();
		reader2.close();

		// Write the contents of the TreeMap to a file

		for (Map.Entry<String, ArrayList<Integer>> entry : map.entrySet()) {
			writer.write(entry.getKey() + "\t");
			for (Integer e : entry.getValue()) {
				writer.write(e + "\t");
			}
			writer.write("\n");
		}
		writer.close();
	}
}
