package de.hs.osnabrueck.tenbeitel.mahout.analyzer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

public class Test {
	public static void main(String[] args) throws IOException {
		GermanStemAnalyzer analyzer = new GermanStemAnalyzer(Version.LUCENE_46);

		

		List<String> strings = new ArrayList<String>();

		strings.add(
				"Darauf wartet #Merkel, weil dann hat man das Totschlagargument um #Flüchtlinge zu uns in die Wohnungen zu stecken.");
		strings.add(
				"Juncker: \"Zäune haben keinen Platz in Europa\" Klar, sonst würde ja der Weg nach Deutschland versperrt werden #Fluechtlinge #refugeeswelcome");
		strings.add(
				"Nie mehr bekommt ihr meine Stimme ! Helft endlich den #Fluechtlingen auf der #balkanroute #Wegscheid");
		strings.add(
				"So ist es. Das Chaos auf dem #Balkan bis Passau wird einzig durch #Merkel verschuldet. WIE LANGE NOCH?");
		strings.add("Selten eine so gute Stimmung erlebt. Willkommenskultur in Meschede läuft. #refugeeswelcome");
		strings.add("#CDU + #SPD kommen mit #Abschiebungen der vielen illegalen #Asyl-Forderer nicht in die Puschen.");
		strings.add(
				"Offensichtlicher Rechtsmissbrauch, der auch geahndet werden sollte! Absurde Hochverratsanzeigen gegen #Merkel.");
		strings.add(
				"Denken Sie an dieses Bild aus #Erfurt, wenn Medien die Teilnehmerzahlen der #AfD-Demo heute mal wieder kleinreden!");
		
		for(String s : strings){
			TokenStream stream = analyzer.tokenStream(null, s);

			CharTermAttribute term = stream.addAttribute(CharTermAttribute.class);
			stream.reset();
			StringBuilder sb = new StringBuilder();
			while (stream.incrementToken()) {
				sb.append("[");
				sb.append(term.toString());
				sb.append("] ");
			}
			stream.close();
			System.out.println(sb.toString());
		}

	}
}
