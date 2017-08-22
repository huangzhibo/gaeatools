package org.bgi.flexlab.gaeatools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class GzUploader {
    /**
     * 数据源地址
     */
    private static Map<String, String> dataSources = new HashMap<String, String>();

    /**
     * 验证文件是否存在
     * @param s
     * @return
     */
    public static boolean isexists(String s){
        File f  = new File(s);
        if(f.exists())
            return true;
        else
            return false;
    }

    private static String getOutputFileName(String input, String optionOutputName){
        String filename = "";
        String outputname = "";
        File f = new File(input);
        filename = f.getName();

        if(optionOutputName != null){
            outputname = optionOutputName;
        }else if(filename.endsWith(".gz")) {
            outputname = filename.substring(0, filename.length() - 3);
        }
        else
            outputname = filename;
        return outputname;
    }

    /**
     * 读取数据源地址信息
     * @throws IOException
     */
    private static void readDataList(String dataList) throws IOException{
        BufferedReader dataListReader = new BufferedReader(new FileReader(new File(dataList))); // 读取数据地址文件

        String line;
        while((line = dataListReader.readLine()) != null) {
            String[] fields = line.split("\\s+");
            if(!dataSources.containsKey(line)) {
                String outputfile = "";
                if(fields.length == 3 && !fields[2].equalsIgnoreCase("null"))
                    outputfile = fields[1] + File.separator + fields[2];
                else if (fields.length == 2 && !fields[1].equalsIgnoreCase("null"))
                    outputfile = fields[1] + File.separator + getOutputFileName(line, null);
                else
                    System.err.println("data.list format is error:" + line + "!")
                            ;
                if(!isexists(fields[0])){
                    System.err.println("file:" + fields[0] + " do not exists!");
                    continue;
                }
                dataSources.put(fields[0], outputfile);
            }
        }
        dataListReader.close();
    }

    /**
     * 主方法
     * @param args
     * @throws Exception
     */
    public static void main(String args[]) throws Exception {
        // 解析参数
        GzUploaderOptions options = new GzUploaderOptions(args);

        if(options.getInput() != null)
            if(isexists(options.getInput()))
                if(options.inputIsList())
                    readDataList(options.getInput());
                else {
                    String outputname = getOutputFileName(options.getInput(), options.getOutputName());
                    dataSources.put(options.getInput(), options.getOutput() + File.separator + outputname);
                }
            else {
                System.err.println("Data list or input file do not exists, please have a check!");
                System.exit(2);
            }
        if(dataSources.size() == 0) {
            System.err.println("No right Data Sources found!");
            System.exit(3);
        }

        int nThreads = dataSources.size() > 16 ? 16 : dataSources.size();
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(nThreads);
        for(String local : dataSources.keySet()) {
            System.out.println("upload:" + local + " to " + dataSources.get(local) + " ......");
            GzUploaderTask myTask = new GzUploaderTask(local, dataSources.get(local));
            executor.execute(myTask);
        }
        executor.shutdown();
    }
}