package org.bgi.flexlab.gaeatools;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;
import org.bgi.flexlab.gaeatools.common.Parameter;

/**
 * Created by huangzhibo on 2017/5/11.
 */
public class GzUploaderOptions extends Parameter {

    private String input;
    private String output;
    private String outputName;
    private int threadNum;
    private boolean isList;

    GzUploaderOptions(String[] args) {
        super("GzUploader",args);
    }

    @Override
    public void parse(String[] args) {
        Option option;
        option = new Option("i", "input", true, "Input file  [required]");
        option.setArgName("FILE");
        option.setRequired(true);
        options.addOption(option);

        option = new Option("l", "isList", false, "Input file is list: (inputfile <output> <outputName>) per line. [false]");
        options.addOption(option);

        option = new Option("o", "output", true, "HDFS path (suppressed by -l) [null]");
        option.setArgName("DIR");
        options.addOption(option);

        option = new Option("n", "outputName", true, "output file name (suppressed by -l) [use original filename]");
        option.setArgName("STRING");
        options.addOption(option);

        option = new Option("t", "thread", true, "thread num (for -l) [32]");
        option.setArgName("INT");
        options.addOption(option);

        option = new Option("h", "help", false, "Print this help.");
        options.addOption(option);

        try {
            cmdLine = parser.parse(options, args);
            if(cmdLine.hasOption("h")) printHelp();
        } catch (ParseException e) {
            printHelp();
        }

        input = cmdLine.getOptionValue("input");

        if(cmdLine.hasOption("output")){
            output = cmdLine.getOptionValue("output");
        }else if(!cmdLine.hasOption("isList")){
            System.err.println("No output option!");
            System.exit(1);
        }

        outputName = cmdLine.getOptionValue("outputName");
        isList = cmdLine.hasOption("isList");
        threadNum = Integer.valueOf(cmdLine.getOptionValue("thread", "32"));
    }

    String getInput() {
        return input;
    }

    String getOutput() {
        return output;
    }

    public String getOutputName() {
        return outputName;
    }

    public boolean inputIsList() {
        return isList;
    }

    public int getThreadNum() {
        return threadNum;
    }
}
