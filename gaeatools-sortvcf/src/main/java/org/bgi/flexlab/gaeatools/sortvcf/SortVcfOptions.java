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

        options.addOption(Option.builder("i")
                .longOpt("input").required(true)
                .hasArg()
                .argName("FILE")
                .desc("Input vcf file  [required]")
                .build());
        options.addOption(Option.builder("o")
                .longOpt("output").required(true)
                .hasArg()
                .argName("FILE")
                .desc("Output vcf/bcf file  [required]")
                .build());
        options.addOption(Option.builder("f")
                .longOpt("outputFormat")
                .hasArg()
                .desc("The format of output file  [VCF]")
                .build());
        options.addOption(Option.builder("R")
                .longOpt("reducerNum")
                .hasArg()
                .desc("hadoop reducer task num [30]")
                .build());
        options.addOption(Option.builder("h")
                .longOpt("help")
                .desc("Print this help.")
                .build());

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

    public int getReducerNum() {
        return reducerNum;
    }
}
