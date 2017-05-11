package org.bgi.flexlab.gaeatools;

import java.util.Arrays;

/**
 * Created by huangzhibo on 2017/5/11.
 */
public class Main {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            Parameter parameter = new Parameter();
            parameter.usage();
            System.exit(0);
        }

        if (args[1].equals("SortVcf")){
            final String[] mainArgs = Arrays.copyOfRange(args, 1, args.length);
            System.exit(SortVcf.instanceMain(mainArgs));
        }else {
            System.err.println("[main] unrecognized command " + args[1]);
        }
    }

}
