package com.group6.ecommerce.jobs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.group6.ecommerce.common.DataCoRecord;
import com.group6.ecommerce.common.PairSum;
import com.group6.ecommerce.common.PairSumCombiner;
import com.group6.ecommerce.common.RecordMapper;

/**
 * Q6: Average total transaction value of the orders placed in each city.
 * An order has several lines, so the average must be over ORDERS, not rows. Two chained jobs:
 *   Job 1: Order Id -> "city \t qty*price"   reduce: sum of lines = order total
 *          output: orderId \t city \t orderTotal
 *   Job 2: city -> "orderTotal,1"            combine/reduce: sum, then total / count
 *          output: city \t averageOrderValue \t numberOfOrders
 */
public class AvgOrderValuePerCity extends Configured implements Tool {

    // ---------------- Job 1: order totals ----------------
    public static class OrderM extends RecordMapper<Text, Text> {
        private final Text k = new Text();
        private final Text v = new Text();

        @Override protected String[] requiredColumns() {
            return new String[] { "Order Id", "Order City", "Order Item Quantity", "Order Item Product Price" };
        }

        @Override protected void handle(DataCoRecord r, Context ctx) throws IOException, InterruptedException {
            String id = r.get("Order Id"), city = r.get("Order City");
            if (blank(id) || blank(city)) { ctx.getCounter(Quality.MISSING_FIELD).increment(1); return; }
            double line = r.getLong("Order Item Quantity") * r.getDouble("Order Item Product Price");
            k.set(id);
            v.set(city + "\t" + line);
            ctx.write(k, v);
        }
    }

    public static class OrderR extends Reducer<Text, Text, Text, Text> {
        private final Text out = new Text();
        @Override protected void reduce(Text orderId, Iterable<Text> vals, Context ctx)
                throws IOException, InterruptedException {
            String city = null;
            double total = 0;
            for (Text t : vals) {
                String[] p = t.toString().split("\t");
                if (city == null) city = p[0];
                total += Double.parseDouble(p[1]);
            }
            out.set(city + "\t" + total);
            ctx.write(orderId, out);
        }
    }

    // ---------------- Job 2: average per city ----------------
    /** Input lines: orderId \t city \t orderTotal */
    public static class CityM extends Mapper<LongWritable, Text, Text, Text> {
        private final Text k = new Text();
        private final Text v = new Text();

        @Override protected void map(LongWritable off, Text line, Context ctx)
                throws IOException, InterruptedException {
            String[] p = line.toString().split("\t");
            if (p.length != 3) return;
            k.set(p[1]);
            v.set(PairSum.encode(Double.parseDouble(p[2]), 1));
            ctx.write(k, v);
        }
    }

    public static class CityR extends Reducer<Text, Text, Text, Text> {
        private final Text out = new Text();
        @Override protected void reduce(Text city, Iterable<Text> vals, Context ctx)
                throws IOException, InterruptedException {
            double[] t = PairSum.sum(vals);
            out.set(String.format("%.2f\t%d", t[0] / t[1], Math.round(t[1])));
            ctx.write(city, out);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) { System.err.println("Usage: avgOrderValuePerCity <input> <output>"); return 2; }
        Path tmp = new Path(args[1] + "_stage1");

        Job j1 = Job.getInstance(getConf(), "Order totals");
        j1.setJarByClass(AvgOrderValuePerCity.class);
        j1.setMapperClass(OrderM.class);
        j1.setReducerClass(OrderR.class);
        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(j1, new Path(args[0]));
        FileOutputFormat.setOutputPath(j1, tmp);
        if (!j1.waitForCompletion(true)) return 1;

        Job j2 = Job.getInstance(getConf(), "Average order value per city");
        j2.setJarByClass(AvgOrderValuePerCity.class);
        j2.setMapperClass(CityM.class);
        j2.setCombinerClass(PairSumCombiner.class);
        j2.setReducerClass(CityR.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(j2, tmp);
        FileOutputFormat.setOutputPath(j2, new Path(args[1]));
        return j2.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new AvgOrderValuePerCity(), args));
    }
}
