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
import com.group6.ecommerce.common.TopN;

/**
 * Q5: Top 10 products by total sales revenue and by total units sold.
 * Two chained jobs:
 *   Job 1 (aggregate): Product Name -> "quantity,sales"  => product <TAB> units <TAB> revenue
 *   Job 2 (top-N)    : every mapper keeps its local top 10 for each metric, one reducer
 *                      merges them into the global top 10 (tiny shuffle).
 * Revenue uses the "Sales" column if present, otherwise Order Item Quantity * Order Item Product Price
 * (the cleaned 8-column file has no Sales column).
 */
public class Top10Products extends Configured implements Tool {

    private static final int N = 10;

    // ---------------- Job 1 ----------------
    public static class AggM extends RecordMapper<Text, Text> {
        private final Text k = new Text();
        private final Text v = new Text();

        @Override protected String[] requiredColumns() {
            return new String[] { "Product Name", "Order Item Quantity", "Order Item Product Price" };
        }

        @Override protected void handle(DataCoRecord r, Context ctx) throws IOException, InterruptedException {
            String p = r.get("Product Name");
            if (blank(p)) { ctx.getCounter(Quality.MISSING_FIELD).increment(1); return; }
            k.set(p);
            long qty = r.getLong("Order Item Quantity");
            // use the "Sales" column when the file has it, otherwise quantity * price
            double sales = r.has("Sales") ? r.getDouble("Sales")
                    : qty * r.getDouble("Order Item Product Price");
            v.set(PairSum.encode(qty, sales));
            ctx.write(k, v);
        }
    }

    public static class AggR extends Reducer<Text, Text, Text, Text> {
        private final Text out = new Text();
        @Override protected void reduce(Text key, Iterable<Text> vals, Context ctx)
                throws IOException, InterruptedException {
            double[] t = PairSum.sum(vals);
            out.set(String.format("%d\t%.2f", Math.round(t[0]), t[1]));
            ctx.write(key, out);
        }
    }

    // ---------------- Job 2 ----------------
    /** Input lines: product \t units \t revenue */
    public static class TopM extends Mapper<LongWritable, Text, Text, Text> {
        private final TopN byRevenue = new TopN(N);
        private final TopN byUnits = new TopN(N);

        @Override protected void map(LongWritable off, Text line, Context ctx) {
            String[] p = line.toString().split("\t");
            if (p.length != 3) return;
            byUnits.add(p[0], Double.parseDouble(p[1]));
            byRevenue.add(p[0], Double.parseDouble(p[2]));
        }

        @Override protected void cleanup(Context ctx) throws IOException, InterruptedException {
            for (TopN.Entry e : byRevenue.sortedDesc())
                ctx.write(new Text("REVENUE"), new Text(e.name + "\t" + e.value));
            for (TopN.Entry e : byUnits.sortedDesc())
                ctx.write(new Text("UNITS"), new Text(e.name + "\t" + e.value));
        }
    }

    public static class TopR extends Reducer<Text, Text, Text, Text> {
        @Override protected void reduce(Text metric, Iterable<Text> vals, Context ctx)
                throws IOException, InterruptedException {
            TopN top = new TopN(N);
            for (Text t : vals) {
                String[] p = t.toString().split("\t");
                top.add(p[0], Double.parseDouble(p[1]));
            }
            int rank = 1;
            boolean money = metric.toString().equals("REVENUE");
            for (TopN.Entry e : top.sortedDesc()) {
                String value = money ? String.format("%.2f", e.value) : Long.toString(Math.round(e.value));
                ctx.write(new Text("TOP_BY_" + metric + " #" + rank++), new Text(e.name + "\t" + value));
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) { System.err.println("Usage: top10Products <input> <output>"); return 2; }
        Path tmp = new Path(args[1] + "_stage1");

        Job j1 = Job.getInstance(getConf(), "Product totals");
        j1.setJarByClass(Top10Products.class);
        j1.setMapperClass(AggM.class);
        j1.setCombinerClass(PairSumCombiner.class);
        j1.setReducerClass(AggR.class);
        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(j1, new Path(args[0]));
        FileOutputFormat.setOutputPath(j1, tmp);
        if (!j1.waitForCompletion(true)) return 1;

        Job j2 = Job.getInstance(getConf(), "Top 10 products");
        j2.setJarByClass(Top10Products.class);
        j2.setMapperClass(TopM.class);
        j2.setReducerClass(TopR.class);
        j2.setNumReduceTasks(1);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(j2, tmp);
        FileOutputFormat.setOutputPath(j2, new Path(args[1]));
        return j2.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Top10Products(), args));
    }
}
