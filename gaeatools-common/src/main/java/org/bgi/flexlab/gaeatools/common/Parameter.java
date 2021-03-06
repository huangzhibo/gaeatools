package org.bgi.flexlab.gaeatools.common;

import org.apache.commons.cli.*;

public class Parameter {
	private static final String SOFTWARE_NAME = "gaeatools";
	private static final String SOFTWARE_VERSION_NUMBER = "1.3";
	private static final String FULL_SOFTWARE_NAME= SOFTWARE_NAME+"-"+SOFTWARE_VERSION_NUMBER;
	private static final String LAST_UPDATE = "2018-9-4";
    public static final String FOOTER = "\nPlease report issues at https://github.com/huangzhibo/gaeatools/issues\n";

	protected Options options = new Options();
	protected CommandLine cmdLine;
	protected CommandLineParser parser = new BasicParser();
    private HelpFormatter helpFormatter = new HelpFormatter();
	private String header;
	private String cmdLineSyntax;

	public Parameter() {
        setHeader();
        setCmdLineSyntax("<command>");
    }

	public Parameter(String command, String[] args) {
	    setCmdLineSyntax(command);
		parse(args);
	}

    protected void printHelp(){
		helpFormatter.setWidth(2 * HelpFormatter.DEFAULT_WIDTH);
        helpFormatter.printHelp(cmdLineSyntax, header, options, FOOTER, true);
        System.exit(1);
    }

//    public void usage(){
//        helpFormatter.setWidth(2 * HelpFormatter.DEFAULT_WIDTH);
//        helpFormatter.printHelp(cmdLineSyntax, header, options, null, true);
//        System.out.println("    SortVcf      sort vcf");
//        System.out.println(FOOTER);
//    }

	public void parse(String[] args) {
	}

    private void setHeader() {
        header ="\nProgram    : " + SOFTWARE_NAME +
                "\nVersion    : " + SOFTWARE_VERSION_NUMBER +
                "\nAuthor     : huangzhibo@genomics.cn" +
                "\nLast update: " + LAST_UPDATE;
    }

    private void setCmdLineSyntax(String command) {
        cmdLineSyntax = "hadoop jar "+ FULL_SOFTWARE_NAME + ".jar "+ command + " [options]";
    }

    public String getCmdLineSyntax() {
        return cmdLineSyntax;
    }

    public String getHeader() {
        return header;
    }
}
