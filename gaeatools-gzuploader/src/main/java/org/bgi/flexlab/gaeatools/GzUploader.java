package org.bgi.flexlab.gaeatools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

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

    /**
     * 读取数据源地址信息
     * @throws IOException
     */
    private static void readDataList(String dataList) throws IOException{
        BufferedReader dataListReader = new BufferedReader(new FileReader(new File(dataList))); // 读取数据地址文件

        String line;
        while((line = dataListReader.readLine()) != null) {


            if(!dataSources.containsKey(line)) {
                String filename = "";
                File f = new File(line);
                if(f.exists()) {
                    filename = f.getName();
                } else {
                    System.err.println("file:" + line + " do not exists!");
                    continue;
                }

                String outputname = "";
                if(filename.endsWith(".gz")) {
                    outputname = filename.substring(0, filename.length() - 3);
                }
                else
                    outputname = filename;

                dataSources.put(line, outputname);
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
        if (args.length < 2){
            System.err.println("> java -jar gzupload.jar data.list(one gz file per line, list file must end with \".list\")|xx.gz  hdfs_path");
            System.exit(1);
        }
        if(isexists(args[0]))
            if(args[0].endsWith(".list"))
                readDataList(args[0]);
            else {
                String filename = "";
                String outputname = "";

                File f = new File(args[0]);
                filename = f.getName();

                if(filename.endsWith(".gz")) {
                    outputname = filename.substring(0, filename.length() - 3);
                }
                else
                    outputname = filename;

                dataSources.put(args[0], outputname);
            }
        else {
            System.err.println("Data list or input file do not exists, please have a check!");
            System.exit(2);
        }


        if(dataSources.size() != 0) {
            for(String local : dataSources.keySet()) {
                System.out.println("upload:" + local + " to " + args[1] + "......");
                //open gz file reader
                //BufferedReader localreader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(loacl))));
                InputStream loaclin;
                if(local.endsWith(".gz")) {
                    loaclin = new BufferedInputStream(new GZIPInputStream(new FileInputStream(local)));
                } else {
                    loaclin = new BufferedInputStream(new FileInputStream(local));
                }

                //open hdfs file writer
                StringBuffer hdfsPath = new StringBuffer();
                hdfsPath.append(args[1]);
                hdfsPath.append(File.separator);
                hdfsPath.append(dataSources.get(local));
                Path hdfsout = new Path(hdfsPath.toString());

                Configuration conf = new Configuration();
                FileSystem fileSystem = FileSystem.get(URI.create(hdfsPath.toString()), conf);
                //fileSystem.mkdirs(hdfsout);
                FSDataOutputStream fs = fileSystem.create(hdfsout);

                //读取gz写入hdfs
                IOUtils.copyBytes(loaclin, fs, conf, true);
            }
        } else {
            System.err.println("No right Data Sources found!");
            System.exit(3);
        }
    }
}