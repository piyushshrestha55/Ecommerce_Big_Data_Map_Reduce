package com.group6.ecommerce.jobs;

import java.io.IOException;

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
import com.group6.ecommerce.common.PairSum;
import com.group6.ecommerce.common.PairSumCombiner;
import com.group6.ecommerce.common.RecordMapper;

/**
 * Q4: Total revenue and quantity sold per category.
 *   map    : Category Name -> "quantity,quantity*price"
 *   reduce : element-wise sum  -> category <TAB> units <TAB> revenue
 */
public class CategoryRevenue extends Configured implements Tool {

    public static class M extends RecordMapper<Text, Text> {
        private final Text k = new Text();
        private final Text v = new Text();

        @Override protected String[] requiredColumns() {
            return new String[] { "Category Name", "Order Item Quantity", "Order Item Product Price" };
        }

        @Override protected void handle(DataCoRecord r, Context ctx) throws IOException, InterruptedException {
            String cat = r.get("Category Name");
            if (blank(cat)) { ctx.getCounter(Quality.MISSING_FIELD).increment(1); return; }
            long qty = r.getLong("Order Item Quantity");
            double price = r.getDouble("Order Item Product Price");
            k.set(cat);
            v.set(PairSum.encode(qty, qty * price));
            ctx.write(k, v);
        }
    }

    public static class R extends Reducer<Text, Text, Text, Text> {
        private final Text out = new Text();
        @Override protected void reduce(Text key, Iterable<Text> vals, Context ctx)
                throws IOException, InterruptedException {
            double[] t = PairSum.sum(vals);
            out.set(String.format("%d\t%.2f", Math.round(t[0]), t[1]));
            ctx.write(key, out);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) { System.err.println("Usage: categoryRevenue <input> <output>"); return 2; }
        Job job = Job.getInstance(getConf(), "Revenue and units per category");
        job.setJarByClass(CategoryRevenue.class);
        job.setMapperClass(M.class);
        job.setCombinerClass(PairSumCombiner.class);
        job.setReducerClass(R.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new CategoryRevenue(), args));
    }
}
