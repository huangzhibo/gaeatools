// Copyright (c) 2013 Aalto University
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to
// deal in the Software without restriction, including without limitation the
// rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
// sell copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
// IN THE SOFTWARE.
package org.bgi.flexlab.gaeatools.sortvcf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.bgi.flexlab.gaeatools.common.Parameter;
import org.seqdoop.hadoop_bam.KeyIgnoringVCFOutputFormat;
import org.seqdoop.hadoop_bam.VCFInputFormat;
import org.seqdoop.hadoop_bam.VCFOutputFormat;
import org.seqdoop.hadoop_bam.VariantContextWritable;

import java.io.IOException;

/**
 * Simple example that reads a VCF file, groups variants by their ID and writes the
 * output again as VCF file.
 *
 * Usage: hadoop jar target/*-jar-with-dependencies.jar org.seqdoop.hadoop_bam.examples.TestVCF \
 *     <input.vcf> <output_directory>
 */
public class SortVcf extends Configured implements Tool {

    static class MyVCFOutputFormat
            extends FileOutputFormat<Text, VariantContextWritable> {
        static final String INPUT_PATH_PROP = "vcf.input_path";

        private KeyIgnoringVCFOutputFormat<Text> baseOF;

        private void initBaseOF(Configuration conf) {
            if (baseOF == null)
                baseOF = new KeyIgnoringVCFOutputFormat<>(conf);
        }

        @Override
        public RecordWriter<Text, VariantContextWritable> getRecordWriter(
                TaskAttemptContext context)
                throws IOException {
            final Configuration conf = context.getConfiguration();
            initBaseOF(conf);

            if (baseOF.getHeader() == null) {
                final Path p = new Path(conf.get(INPUT_PATH_PROP));
                baseOF.readHeaderFrom(p, p.getFileSystem(conf));
            }

            return baseOF.getRecordWriter(context, getDefaultWorkFile(context, ""));
        }
    }

    public int run(String[] args) throws Exception {
        final Configuration conf = getConf();
        SortVcfOptions options = new SortVcfOptions(args);

        conf.set(VCFOutputFormat.OUTPUT_VCF_FORMAT_PROPERTY, options.getOutputFormat());
        conf.set(MyVCFOutputFormat.INPUT_PATH_PROP, options.getInput());

        Job job = Job.getInstance(conf, "SortVcf");

        job.setJarByClass(SortVcf.class);

        job.setMapperClass (Mapper.class);
        job.setReducerClass(SortVcfReducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setOutputKeyClass   (NullWritable.class);
        job.setOutputValueClass (VariantContextWritable.class);

        job.setInputFormatClass (VCFInputFormat.class);
        job.setOutputFormatClass(MyVCFOutputFormat.class);

        FileInputFormat.addInputPath  (job, new Path(options.getInput()));
        FileOutputFormat.setOutputPath(job, new Path(options.getOutput()));

        job.setPartitionerClass(TotalOrderPartitioner.class);

        System.out.println("vcf-sort :: Sampling...");

        InputSampler.writePartitionFile(
                job,
                new InputSampler.RandomSampler<LongWritable,VariantContextWritable>
                        (0.01, 10000, Math.max(100, options.getReducerNum())));


        job.submit();

        if (!job.waitForCompletion(true)) {
            System.err.println("sort :: Job failed.");
            return 1;
        }

        return 0;
    }

    public static int instanceMain(String[] args) throws Exception {
        return ToolRunner.run(new Configuration(), new SortVcf(), args);
    }


    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.printf("Usage: hadoop jar <name.jar> %s <input.vcf> <output_directory>\n", SortVcf.class.getCanonicalName());
            System.exit(0);
        }
        int res = ToolRunner.run(new Configuration(), new SortVcf(), args);
        System.exit(res);
    }
}

final class SortVcfReducer
        extends Reducer<LongWritable,VariantContextWritable,
        NullWritable,VariantContextWritable>
{
    @Override protected void reduce(
            LongWritable ignored, Iterable<VariantContextWritable> records,
            Reducer<LongWritable,VariantContextWritable,
                    NullWritable,VariantContextWritable>.Context
                    ctx)
            throws IOException, InterruptedException
    {
        for (VariantContextWritable rec : records)
            ctx.write(NullWritable.get(), rec);
    }
}