package de.text;

import java.io.IOException;
import java.io.StringReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.de.GermanLightStemFilter;
import org.apache.lucene.analysis.de.GermanMinimalStemFilter;
import org.apache.lucene.analysis.de.GermanStemFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.tartarus.snowball.ext.German2Stemmer;

import de.hs.osnabrueck.tenbeitel.mahout.analyzer.GermanStemAnalyzer;
import de.text.tokenizator.GermanAnalyzer;
import de.text.tokenizator.StemmingType;

public class TestLucence {

	private static final String TEXT = "Dies ist ein Text der Flüchtlinge und den Flüchtling miteinander in Verbindung setzt.... Bücher und das Buch";

	public static void main(String[] args) throws IOException {
		GermanStemAnalyzer analyzer = new GermanStemAnalyzer(Version.LUCENE_5_3_1);
		TokenStream s = analyzer.tokenStream(null, "Flüchtlinge raus aus Deutschland");
		CharTermAttribute term = s.addAttribute(CharTermAttribute.class);

		s.reset();
		StringBuilder sb = new StringBuilder();
		while (s.incrementToken()) {
			sb.append("[");
			sb.append(term.toString());
			sb.append("] ");
		}
		
		System.out.println(sb.toString());
	
		s = analyzer.tokenStream(null, "Flüchtlinge raus aus Deutschland");
		term = s.addAttribute(CharTermAttribute.class);

		s.reset();
		sb = new StringBuilder();
		while (s.incrementToken()) {
			sb.append("[");
			sb.append(term.toString());
			sb.append("] ");
		}
		
		System.out.println(sb.toString());
	}

	public String test(String text) throws IOException {
		try (GermanAnalyzer analyzer = new GermanAnalyzer()) {
			TokenStream s = analyzer.tokenStream(null, text);
			CharTermAttribute term = s.addAttribute(CharTermAttribute.class);

			s.reset();
			StringBuilder sb = new StringBuilder();
			while (s.incrementToken()) {
				sb.append("[");
				sb.append(term.toString());
				sb.append("] ");
			}

			return sb.toString();

		}
	}

	public String standard(String text) throws IOException {
		try (Analyzer analyzer = new StandardAnalyzer()) {
			TokenStream stream = analyzer.tokenStream(null, text);
			CharTermAttribute term = stream.addAttribute(CharTermAttribute.class);

			stream.reset();
			StringBuilder sb = new StringBuilder();
			while (stream.incrementToken()) {
				sb.append("[");
				sb.append(term.toString());
				sb.append("] ");
			}

			return sb.toString();
		}
	}

	public String stemText(String text) throws IOException {
		StringBuilder stemWords;
		try (GermanAnalyzer analyzer = new GermanAnalyzer()) {

			analyzer.enableNormalization();
			analyzer.enableStopWords();
			analyzer.enableStemming(StemmingType.GermanStemFilter);

			TokenStream tStream = analyzer.tokenStream(null, text);
			CharTermAttribute term = tStream.addAttribute(CharTermAttribute.class);
			GermanStemFilter stemFilter = new GermanStemFilter(tStream);
			stemWords = new StringBuilder();
			tStream.reset();
			while (tStream.incrementToken()) {
				stemWords.append(term.toString());
				stemWords.append(" ");
			}
			// stemFilter.close();
		}
		return stemWords.toString();
	}

	public String stemTextLight(String text) throws IOException {
		StringBuilder stemWords;
		try (Analyzer analyzer = new GermanAnalyzer()) {
			TokenStream tStream = analyzer.tokenStream(null, text);
			CharTermAttribute term = tStream.addAttribute(CharTermAttribute.class);
			GermanLightStemFilter stemFilter = new GermanLightStemFilter(tStream);
			stemWords = new StringBuilder();
			tStream.reset();
			while (stemFilter.incrementToken()) {
				stemWords.append(term.toString());
				stemWords.append(" ");
			}
			stemFilter.close();
		}
		return stemWords.toString();
	}

	public String stemTextMinimal(String text) throws IOException {
		StringBuilder stemWords;
		try (Analyzer analyzer = new GermanAnalyzer()) {
			TokenStream tStream = analyzer.tokenStream(null, text);
			CharTermAttribute term = tStream.addAttribute(CharTermAttribute.class);
			GermanMinimalStemFilter stemFilter = new GermanMinimalStemFilter(tStream);
			stemWords = new StringBuilder();
			tStream.reset();
			while (stemFilter.incrementToken()) {
				stemWords.append(term.toString());
				stemWords.append(" ");
			}
			stemFilter.close();
		}
		return stemWords.toString();
	}

	public void stem2(String text) {
		German2Stemmer stemmer = new German2Stemmer();

		stemmer.setCurrent(text);
		stemmer.stem();
		System.out.println(stemmer.getCurrent());

	}

	// public String stopWord(String text) throws IOException {
	// Analyzer analyzer = new CustomTokenizer();
	// TokenStream tStream = analyzer.tokenStream(null, text);
	// CharTermAttribute term = tStream.addAttribute(CharTermAttribute.class);
	// StringBuilder builder = new StringBuilder();
	// tStream.reset();
	// while (tStream.incrementToken()) {
	// builder.append(term.toString());
	// builder.append("\n");
	// }
	// analyzer.close();
	// return builder.toString();
	// }

	public String ownTokenStream(String text) {
		return null;
	}

}
