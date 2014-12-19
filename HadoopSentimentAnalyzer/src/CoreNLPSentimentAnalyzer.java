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

	public int getSentiment(String inputSentences) {
		int sentiment = 0;
		Properties props = new Properties();
		props.setProperty("annotators",
				"tokenize, ssplit, pos, lemma, ner, parse, sentiment");
		StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
		Annotation annotation = new Annotation(inputSentences);
		pipeline.annotate(annotation);
		List<CoreMap> sentences = annotation
				.get(CoreAnnotations.SentencesAnnotation.class);
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
