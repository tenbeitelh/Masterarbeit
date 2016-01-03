package de.hs.osnabrueck.tenbeitel.mahout.analyzer;

import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.de.GermanLightStemFilter;
import org.apache.lucene.analysis.de.GermanNormalizationFilter;
import org.apache.lucene.analysis.de.GermanStemFilter;
import org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.StopwordAnalyzerBase;
import org.apache.lucene.analysis.util.WordlistLoader;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.Version;
import org.tartarus.snowball.ext.German2Stemmer;

public class GermanStemAnalyzer extends StopwordAnalyzerBase {
	/** @deprecated in 3.1, remove in Lucene 5.0 (index bw compat) */
	@Deprecated
	private final static String[] GERMAN_STOP_WORDS = { "einer", "eine", "eines", "einem", "einen", "der", "die", "das",
			"dass", "daß", "du", "er", "sie", "es", "was", "wer", "wie", "wir", "und", "oder", "ohne", "mit", "am",
			"im", "in", "aus", "auf", "ist", "sein", "war", "wird", "ihr", "ihre", "ihres", "als", "für", "von", "mit",
			"dich", "dir", "mich", "mir", "mein", "sein", "kein", "durch", "wegen", "wird" };

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
		/** @deprecated in 3.1, remove in Lucene 5.0 (index bw compat) */
		@Deprecated
		private static final CharArraySet DEFAULT_SET_30 = CharArraySet
				.unmodifiableSet(new CharArraySet(Version.LUCENE_CURRENT, Arrays.asList(GERMAN_STOP_WORDS), false));
		private static final CharArraySet DEFAULT_SET;

		static {
			try {
				DEFAULT_SET = WordlistLoader.getSnowballWordSet(
						IOUtils.getDecodingReader(SnowballFilter.class, DEFAULT_STOPWORD_FILE, IOUtils.CHARSET_UTF_8),
						Version.LUCENE_CURRENT);
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
	public GermanStemAnalyzer(Version matchVersion) {
		this(matchVersion, matchVersion.onOrAfter(Version.LUCENE_31) ? DefaultSetHolder.DEFAULT_SET
				: DefaultSetHolder.DEFAULT_SET_30);
	}

	/**
	 * Builds an analyzer with the given stop words
	 * 
	 * @param matchVersion
	 *            lucene compatibility version
	 * @param stopwords
	 *            a stopword set
	 */
	public GermanStemAnalyzer(Version matchVersion, CharArraySet stopwords) {
		this(matchVersion, stopwords, CharArraySet.EMPTY_SET);
	}

	/**
	 * Builds an analyzer with the given stop words
	 * 
	 * @param matchVersion
	 *            lucene compatibility version
	 * @param stopwords
	 *            a stopword set
	 * @param stemExclusionSet
	 *            a stemming exclusion set
	 */
	public GermanStemAnalyzer(Version matchVersion, CharArraySet stopwords, CharArraySet stemExclusionSet) {
		super(matchVersion, stopwords);
		exclusionSet = CharArraySet.unmodifiableSet(CharArraySet.copy(matchVersion, stemExclusionSet));
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
	protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
		final Tokenizer source = new StandardTokenizer(matchVersion, reader);
		TokenStream result = new StandardFilter(matchVersion, source);
		result = new LowerCaseFilter(matchVersion, result);
		result = new StopFilter(matchVersion, result, stopwords);
		result = new SetKeywordMarkerFilter(result, exclusionSet);
		if (matchVersion.onOrAfter(Version.LUCENE_36)) {
			result = new GermanNormalizationFilter(result);
			result = new GermanLightStemFilter(result);
		} else if (matchVersion.onOrAfter(Version.LUCENE_31)) {
			result = new SnowballFilter(result, new German2Stemmer());
		} else {
			result = new GermanStemFilter(result);
		}
		return new TokenStreamComponents(source, result);
	}
}
