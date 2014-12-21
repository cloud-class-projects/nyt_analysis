import java.util.List;
import java.util.Properties;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

public class CoreNLPSentimentAnalyzer {

	/*
	 * This method takes in a sentence or a group of sentences.
	 * Uses StanfordCoreNLP annotations to parse and calculate the
	 * sentiments of the given input. Returns the sentiment generated.
	 */
	
	public int getSentiment(String inputSentences) {
		
		// Initialize a sentiment variable.
		int sentiment = 0;
		
		// Instantiate a properties object and specify the annotators
		// to use to pasrse and generate sentiments.
		Properties props = new Properties();
		props.setProperty("annotators",
				"tokenize, ssplit, pos, lemma, ner, parse, sentiment");
		
		// If input is multiple sentences, put each sentence in a list
		// and pass the list to the 'sentiment annotator'
		StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
		Annotation annotation = new Annotation(inputSentences);
		pipeline.annotate(annotation);
		List<CoreMap> sentences = annotation
				.get(CoreAnnotations.SentencesAnnotation.class);
		
		// Generate the sentiment for the given input and return it
		for (int i = 0; i < sentences.size(); i++) {
			if (sentences != null && sentences.size() > 0) {
				CoreMap sentence = sentences.get(i);
				Tree tree = sentence
						.get(SentimentCoreAnnotations.AnnotatedTree.class);
				sentiment = RNNCoreAnnotations.getPredictedClass(tree);
			}
		}
		return sentiment;
	}
}
