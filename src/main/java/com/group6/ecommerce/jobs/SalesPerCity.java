package com.group6.ecommerce.jobs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.group6.ecommerce.common.DataCoRecord;
import com.group6.ecommerce.common.RecordMapper;

/**
 * Q1: Total sales per city.
 *   map    : Order City -> Order Item Product Price * Order Item Quantity
 *   reduce : sum
 */
public class SalesPerCity extends Configured implements Tool {

    public static class M extends RecordMapper<Text, DoubleWritable> {
        private final Text k = new Text();
        private final DoubleWritable v = new DoubleWritable();

        @Override protected String[] requiredColumns() {
            return new String[] { "Order City", "Order Item Product Price", "Order Item Quantity" };
        }

        @Override protected void handle(DataCoRecord r, Context ctx) throws IOException, InterruptedException {
            String city = r.get("Order City");
            if (blank(city)) { ctx.getCounter(Quality.MISSING_FIELD).increment(1); return; }
            k.set(city);
            v.set(r.getDouble("Order Item Product Price") * r.getLong("Order Item Quantity"));
            ctx.write(k, v);
        }
    }

    /** Combiner: partial sums on the map side. */
    public static class C extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private final DoubleWritable out = new DoubleWritable();
        @Override protected void reduce(Text key, Iterable<DoubleWritable> vals, Context ctx)
                throws IOException, InterruptedException {
            double s = 0;
            for (DoubleWritable d : vals) s += d.get();
            out.set(s);
            ctx.write(key, out);
        }
    }

    public static class R extends Reducer<Text, DoubleWritable, Text, Text> {
        private final Text out = new Text();
        @Override protected void reduce(Text key, Iterable<DoubleWritable> vals, Context ctx)
                throws IOException, InterruptedException {
            double s = 0;
            for (DoubleWritable d : vals) s += d.get();
            out.set(String.format("%.2f", s));
            ctx.write(key, out);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) { System.err.println("Usage: salesPerCity <input> <output>"); return 2; }
        Job job = Job.getInstance(getConf(), "Total sales per city");
        job.setJarByClass(SalesPerCity.class);
        job.setMapperClass(M.class);
        job.setCombinerClass(C.class);
        job.setReducerClass(R.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new SalesPerCity(), args));
    }
}
