package de.hs.osnabrueck.tenbeitel.mr.cmd;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import de.hs.osnabrueck.tenbeitel.mr.cmd.comp.ComponentName;

public class CommandLineBuilder {

	private static final String K_MEANS_INPUT_DESC = "";
	private static final String APRIORI_INPUT_DESC = "";
	private static final String NAIVE_INPUT_DESC = "";
	private static final String LDA_INPUT_DESC = "";
	private static final String TRANSFORMATION_INPUT_DESC = "";

	public CommandLineBuilder() {

	}

	public void getCommandLineForName(ComponentName name) {
		Options options = new Options();
		options.addOption(buildInputDirOption(name));
		
		switch (name) {
		case TRANSFORMATION:
			Option minDFOption = new Option("df", "Minimal DF Frequency of a term(default :1)");
			
			break;

		default:
			break;
		}

	}

	public Option buildInputDirOption(ComponentName name) {
		Option fileOption = new Option("f", "inpudDir", true, K_MEANS_INPUT_DESC);
		fileOption.setRequired(true);
		fileOption.setArgName("inputDir");

		switch (name) {
		case TRANSFORMATION:
			fileOption.setDescription(TRANSFORMATION_INPUT_DESC);
			break;
		case KMEANS:
			break;
		case LDA:
			fileOption.setDescription(LDA_INPUT_DESC);
			break;
		case NAIVE_BAYES:
			fileOption.setDescription(NAIVE_INPUT_DESC);
			break;
		case APRIORI:
			fileOption.setDescription(APRIORI_INPUT_DESC);
			break;
		default:
			break;
		}
		return fileOption;
	}

}
