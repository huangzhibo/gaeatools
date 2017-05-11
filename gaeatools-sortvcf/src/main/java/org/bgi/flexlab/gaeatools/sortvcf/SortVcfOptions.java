package org.bgi.flexlab.gaeatools.sortvcf;

import org.apache.commons.cli.Option;

/**
 * Created by huangzhibo on 2017/5/11.
 */
public class SortVcfOptions extends Parameter{

    private String input;
    private String output;
    private String outputFormat;

    public SortVcfOptions(String[] args) {
        super(args);
    }

    @Override
    public void parse(String[] args) {
        String header = helpHeader();
        String footer = "\nPlease report issues at https://github.com/huangzhibo/Data2Excel/issues";

        options.addOption(Option.builder("i")
                .longOpt("input").required(true)
                .hasArg()
                .argName("FILE")
                .desc("Input plain text files. Support multiple files input(example：\"-i file1 -i file2\")")
                .build());
        options.addOption(Option.builder("o")
                .longOpt("output").required(true)
                .hasArg()
                .argName("FILE")
                .desc("Output Excel file, multi input will be writed into different sheets in the same workbook. [file1.xlsx]")
                .build());
        options.addOption(Option.builder("f")
                .longOpt("outputFormat")
                .hasArg()
                .desc("The format file to set sheet column style.")
                .build());
        options.addOption(Option.builder("h")
                .longOpt("help")
                .desc("Print this help.")
                .build());

        input = cmdLine.getOptionValue("input");

        if(cmdLine.hasOption("output")){
            output = cmdLine.getOptionValue("output");
        }

        outputFormat = cmdLine.getOptionValue("outputFormat", "VCF");
    }

}
