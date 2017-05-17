package org.bgi.flexlab.gaeatools.sortvcf;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;
import org.bgi.flexlab.gaeatools.common.Parameter;

/**
 * Created by huangzhibo on 2017/5/11.
 */
public class SortVcfOptions extends Parameter {

    private String input;
    private String output;
    private String outputFormat;
    private int reducerNum;

    SortVcfOptions(String[] args) {
        super("SortVcf",args);
    }

    @Override
    public void parse(String[] args) {
        Option option;
        option = new Option("i", "input", true, "Input vcf file  [required]");
        option.setArgName("FILE");
        option.setRequired(true);
        options.addOption(option);

        option = new Option("o", "output", true, "Output vcf/bcf file  [required]");
        option.setArgName("FILE");
        option.setRequired(true);
        options.addOption(option);

        option = new Option("f", "outputFormat", true, "The format of output file (VCF/BCF)  [VCF]");
        option.setArgName("STRING");
        options.addOption(option);

        option = new Option("R", "reducerNum", true, "hadoop reducer task num [30]");
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
        }

        outputFormat = cmdLine.getOptionValue("outputFormat", "VCF");
        reducerNum = Integer.parseInt(cmdLine.getOptionValue("reducerNum","30"));
    }

    String getInput() {

        return input;
    }

    String getOutput() {
        return output;
    }

    String getOutputFormat() {
        return outputFormat;
    }

    int getReducerNum() {
        return reducerNum;
    }
}
