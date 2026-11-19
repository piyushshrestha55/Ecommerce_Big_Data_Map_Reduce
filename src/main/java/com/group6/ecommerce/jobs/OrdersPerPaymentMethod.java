package com.group6.ecommerce.jobs;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
 * Q3: Number of DISTINCT orders per payment method.
 * An Order Id repeats once per order line, so we cannot just count rows.
 *   map     : Type -> Order Id
 *   combine : de-duplicate Order Ids per Type on the map side (less shuffle)
 *   reduce  : count distinct Order Ids
 */
public class OrdersPerPaymentMethod extends Configured implements Tool {

    public static class M extends RecordMapper<Text, Text> {
        private final Text k = new Text();
        private final Text v = new Text();

        @Override protected String[] requiredColumns() {
            return new String[] { "Type", "Order Id" };
        }

        @Override protected void handle(DataCoRecord r, Context ctx) throws IOException, InterruptedException {
            String type = r.get("Type"), id = r.get("Order Id");
            if (blank(type) || blank(id)) { ctx.getCounter(Quality.MISSING_FIELD).increment(1); return; }
            k.set(type);
            v.set(id);
            ctx.write(k, v);
        }
    }

    public static class C extends Reducer<Text, Text, Text, Text> {
        @Override protected void reduce(Text key, Iterable<Text> vals, Context ctx)
                throws IOException, InterruptedException {
            Set<String> seen = new HashSet<>();
            for (Text t : vals) {
                if (seen.add(t.toString())) ctx.write(key, t);
            }
        }
    }

    public static class R extends Reducer<Text, Text, Text, Text> {
        private final Text out = new Text();
        @Override protected void reduce(Text key, Iterable<Text> vals, Context ctx)
                throws IOException, InterruptedException {
            Set<String> distinct = new HashSet<>();
            for (Text t : vals) distinct.add(t.toString());
            out.set(Integer.toString(distinct.size()));
            ctx.write(key, out);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) { System.err.println("Usage: ordersPerPayment <input> <output>"); return 2; }
        Job job = Job.getInstance(getConf(), "Distinct orders per payment method");
        job.setJarByClass(OrdersPerPaymentMethod.class);
        job.setMapperClass(M.class);
        job.setCombinerClass(C.class);
        job.setReducerClass(R.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new OrdersPerPaymentMethod(), args));
    }
}
