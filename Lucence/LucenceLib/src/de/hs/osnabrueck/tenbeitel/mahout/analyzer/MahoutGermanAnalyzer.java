package de.hs.osnabrueck.tenbeitel.mahout.analyzer;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.de.GermanLightStemFilter;
import org.apache.lucene.analysis.de.GermanNormalizationFilter;
import org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.StopwordAnalyzerBase;
import org.apache.lucene.analysis.util.WordlistLoader;
import org.apache.lucene.util.IOUtils;

/**
 * Custom Implemenation of the GermanAnalyzer of Lucence v5. It is necessary to
 * get controll of the stemming and tokenization process
 * 
 * @author H. Tenbeitel
 *
 */
public class MahoutGermanAnalyzer extends StopwordAnalyzerBase {
	/** File containing default German stopwords. */
	public final static String DEFAULT_STOPWORD_FILE = "german_stop.txt";

	/**
	 * Returns a set of default German-stopwords
	 * 
	 * @return a set of default German-stopwords
	 */
	public static final CharArraySet getDefaultStopSet() {
		return DefaultSetHolder.DEFAULT_SET;
	}

	private static class DefaultSetHolder {
		private static final CharArraySet DEFAULT_SET;

		static {
			try {
				DEFAULT_SET = WordlistLoader.getSnowballWordSet(
						IOUtils.getDecodingReader(SnowballFilter.class, DEFAULT_STOPWORD_FILE, StandardCharsets.UTF_8));
			} catch (IOException ex) {
				// default set should always be present as it is part of the
				// distribution (JAR)
				throw new RuntimeException("Unable to load default stopword set");
			}
		}
	}

	/**
	 * Contains the stopwords used with the {@link StopFilter}.
	 */

	/**
	 * Contains words that should be indexed but not stemmed.
	 */
	private final CharArraySet exclusionSet;

	/**
	 * Builds an analyzer with the default stop words:
	 * {@link #getDefaultStopSet()}.
	 */
	public MahoutGermanAnalyzer() {
		this(DefaultSetHolder.DEFAULT_SET);
	}

	/**
	 * Builds an analyzer with the given stop words
	 * 
	 * @param stopwords
	 *            a stopword set
	 */
	public MahoutGermanAnalyzer(CharArraySet stopwords) {
		this(stopwords, CharArraySet.EMPTY_SET);
	}

	/**
	 * Builds an analyzer with the given stop words
	 * 
	 * @param stopwords
	 *            a stopword set
	 * @param stemExclusionSet
	 *            a stemming exclusion set
	 */
	public MahoutGermanAnalyzer(CharArraySet stopwords, CharArraySet stemExclusionSet) {
		super(stopwords);
		exclusionSet = CharArraySet.unmodifiableSet(CharArraySet.copy(stemExclusionSet));
	}

	/**
	 * Creates {@link org.apache.lucene.analysis.Analyzer.TokenStreamComponents}
	 * used to tokenize all the text in the provided {@link Reader}.
	 * 
	 * @return {@link org.apache.lucene.analysis.Analyzer.TokenStreamComponents}
	 *         built from a {@link StandardTokenizer} filtered with
	 *         {@link StandardFilter}, {@link LowerCaseFilter},
	 *         {@link StopFilter} , {@link SetKeywordMarkerFilter} if a stem
	 *         exclusion set is provided, {@link GermanNormalizationFilter} and
	 *         {@link GermanLightStemFilter}
	 */
	@Override
	protected TokenStreamComponents createComponents(String fieldName) {
		final Tokenizer source;
		// source = new StandardTokenizer();
		source = new WhitespaceTokenizer();

		TokenStream result = new StandardFilter(source);
		result = new LowerCaseFilter(result);
		result = new StopFilter(result, stopwords);
		result = new GermanNormalizationFilter(result);
		return new TokenStreamComponents(source, result);
	}
}