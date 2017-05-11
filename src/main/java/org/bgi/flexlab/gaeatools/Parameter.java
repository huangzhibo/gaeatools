package org.bgi.flexlab.gaeatools;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

public class Parameter {

    private final static  String SOFTWARE_NAME= "gaeatools";
	private final static String SOFTWARE_VERSION_NUMBER = "0.1";
	private final static String LAST_UPDATE = "2017-5-11";
    private final static String FOOTER = "\nPlease report issues at https://github.com/huangzhibo/gaeatools/issues";

	protected Options options = new Options();
	protected CommandLine cmdLine;
	private CommandLineParser parser = new DefaultParser();
	private HelpFormatter helpFormatter = new HelpFormatter();

	Parameter() {}

	public Parameter(String[] args) {
		helpFormatter.setWidth(2 * HelpFormatter.DEFAULT_WIDTH);
		parse(args);
	}


	protected String helpHeader() {
		StringBuilder sb = new StringBuilder();
		sb.append("\nProgram    : ");
		sb.append(SOFTWARE_NAME);
		sb.append("\nVersion    : ");
		sb.append(SOFTWARE_VERSION_NUMBER);
		sb.append("\nAuthor     : huangzhibo@genomics.cn");
		sb.append("\nLast update: ");
		sb.append(LAST_UPDATE);
		sb.append("\nNote       : Read data from plain text file and write it into Excel\n");
		sb.append("\nOptions:\n");
		return sb.toString();
	}

    void usage(){
        String header = helpHeader();
        helpFormatter.printHelp("java -jar "+SOFTWARE_NAME +".jar", header, options, FOOTER, true);
    }

	public void parse(String[] args) {
	}

	/**
	 * 测试
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		String[] arg = {  "-i", "test.txt" , "-i", "test2.txt"};
		Parameter parameter = new Parameter();
		parameter.parse(arg);
	}
}
