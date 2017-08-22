package org.bgi.flexlab.gaeatools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.zip.GZIPInputStream;

public class GzUploaderTask implements Runnable {
    /**
     * 数据源地址
     */
    private String inputfile;
    private String outputfile;

    public GzUploaderTask(String inputfile, String outputfile) {
        this.inputfile = inputfile;
        this.outputfile = outputfile;
    }

    @Override
    public void run() {
        //open gz file reader
        //BufferedReader localreader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(loacl))));
        InputStream loaclin;
        try {
            if (inputfile.endsWith(".gz")) {
                loaclin = new BufferedInputStream(new GZIPInputStream(new FileInputStream(inputfile)));
            } else {
                loaclin = new BufferedInputStream(new FileInputStream(inputfile));
            }

            //open hdfs file writer
            Path hdfsout = new Path(outputfile);

            Configuration conf = new Configuration();
            FileSystem fileSystem = FileSystem.get(URI.create(outputfile), conf);
            //fileSystem.mkdirs(hdfsout);
            FSDataOutputStream fs = fileSystem.create(hdfsout);

            //读取gz写入hdfs
            IOUtils.copyBytes(loaclin, fs, conf, true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}