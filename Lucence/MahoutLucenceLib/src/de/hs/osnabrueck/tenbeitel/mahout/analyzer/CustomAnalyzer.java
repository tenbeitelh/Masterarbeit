package de.hs.osnabrueck.tenbeitel.mahout.analyzer;

import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;

import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.StopwordAnalyzerBase;
import org.apache.lucene.analysis.util.WordlistLoader;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.Version;

abstract class CustomAnalyzer extends StopwordAnalyzerBase {
	
	@Deprecated
	private final static String[] GERMAN_STOP_WORDS = { "einer", "eine", "eines", "einem", "einen", "der", "die", "das",
			"dass", "daß", "du", "er", "sie", "es", "was", "wer", "wie", "wir", "und", "oder", "ohne", "mit", "am",
			"im", "in", "aus", "auf", "ist", "sein", "war", "wird", "ihr", "ihre", "ihres", "als", "für", "von", "mit",
			"dich", "dir", "mich", "mir", "mein", "sein", "kein", "durch", "wegen", "wird" };

	/** File containing default German stopwords. */
	public final static String DEFAULT_STOPWORD_FILE = "german_stop.txt";

	
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

	
	protected final CharArraySet exclusionSet;

	
	public CustomAnalyzer(Version matchVersion) {
		this(matchVersion, matchVersion.onOrAfter(Version.LUCENE_31) ? DefaultSetHolder.DEFAULT_SET
				: DefaultSetHolder.DEFAULT_SET_30);
	}

	
	public CustomAnalyzer(Version matchVersion, CharArraySet stopwords) {
		this(matchVersion, stopwords, CharArraySet.EMPTY_SET);
	}

	
	public CustomAnalyzer(Version matchVersion, CharArraySet stopwords, CharArraySet stemExclusionSet) {
		super(matchVersion, stopwords);
		exclusionSet = CharArraySet.unmodifiableSet(CharArraySet.copy(matchVersion, stemExclusionSet));
	}


	@Override
	abstract protected TokenStreamComponents createComponents(String arg0, Reader arg1);

	
	
}
