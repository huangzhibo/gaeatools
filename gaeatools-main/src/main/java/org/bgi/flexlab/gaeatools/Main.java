package org.bgi.flexlab.gaeatools;

import org.bgi.flexlab.gaeatools.common.Parameter;
import org.bgi.flexlab.gaeatools.sortvcf.SortVcf;

import java.util.Arrays;

/**
 * Created by huangzhibo on 2017/5/11.
 */
public class Main {
    /**
     * Provides ANSI colors for the terminal output *
     */
    private static final String KNRM = "\u001B[0m"; // reset
    private static final String RED = "\u001B[31m";
    private static final String GREEN = "\u001B[32m";
    private static final String BOLDCYAN = "\u001B[1m\u001B[36m";
    private static final String WHITE = "\u001B[37m";
    private static final String BOLDRED = "\u001B[1m\u001B[31m";
    private static final String BOLD = "\u001B[1m";

    public static void main(String[] args) throws Exception {
        if (args.length < 2) usage();

        if (args[1].equals("SortVcf")){
            final String[] mainArgs = Arrays.copyOfRange(args, 1, args.length);
            System.exit(SortVcf.instanceMain(mainArgs));
        }else {
            System.err.println(RED+"[main] unrecognized command " + args[1] + KNRM);
            usage();
        }
    }

    private static void usage(){
        Parameter parameter = new Parameter();
        String cmdLineSyntax = parameter.getCmdLineSyntax();
        System.out.println(BOLDRED+cmdLineSyntax+KNRM);
        System.out.println(parameter.getHeader());
        System.out.println(BOLDRED+"\nCommands:"+KNRM);
        System.out.println(BOLDCYAN+"        SortVcf    sort vcf"+KNRM);
        System.out.println(BOLD+Parameter.FOOTER+KNRM);
        System.exit(1);
    }

}
