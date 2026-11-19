package com.group6.ecommerce.jobs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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
 * Q2: Most sold category.
 *   map    : Category Name -> Order Item Quantity
 *   reduce : sum; one reducer collects all (few) categories and writes them
 *            ranked by units sold, so the FIRST output line is the most sold category.
 */
public class MostSoldCategory extends Configured implements Tool {

    public static class M extends RecordMapper<Text, LongWritable> {
        private final Text k = new Text();
        private final LongWritable v = new LongWritable();

        @Override protected String[] requiredColumns() {
            return new String[] { "Category Name", "Order Item Quantity" };
        }

        @Override protected void handle(DataCoRecord r, Context ctx) throws IOException, InterruptedException {
            String cat = r.get("Category Name");
            if (blank(cat)) { ctx.getCounter(Quality.MISSING_FIELD).increment(1); return; }
            k.set(cat);
            v.set(r.getLong("Order Item Quantity"));
            ctx.write(k, v);
        }
    }

    public static class C extends Reducer<Text, LongWritable, Text, LongWritable> {
        private final LongWritable out = new LongWritable();
        @Override protected void reduce(Text key, Iterable<LongWritable> vals, Context ctx)
                throws IOException, InterruptedException {
            long s = 0;
            for (LongWritable l : vals) s += l.get();
            out.set(s);
            ctx.write(key, out);
        }
    }

    public static class R extends Reducer<Text, LongWritable, Text, LongWritable> {
        private final Map<String, Long> totals = new HashMap<>();

        @Override protected void reduce(Text key, Iterable<LongWritable> vals, Context ctx) {
            long s = 0;
            for (LongWritable l : vals) s += l.get();
            totals.put(key.toString(), s);
        }

        @Override protected void cleanup(Context ctx) throws IOException, InterruptedException {
            List<Map.Entry<String, Long>> list = new ArrayList<>(totals.entrySet());
            list.sort((a, b) -> b.getValue() != a.getValue().longValue()
                    ? Long.compare(b.getValue(), a.getValue()) : a.getKey().compareTo(b.getKey()));
            for (Map.Entry<String, Long> e : list) {
                ctx.write(new Text(e.getKey()), new LongWritable(e.getValue()));
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) { System.err.println("Usage: mostSoldCategory <input> <output>"); return 2; }
        Job job = Job.getInstance(getConf(), "Most sold category");
        job.setJarByClass(MostSoldCategory.class);
        job.setMapperClass(M.class);
        job.setCombinerClass(C.class);
        job.setReducerClass(R.class);
        job.setNumReduceTasks(1); // global ranking needs a single reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new MostSoldCategory(), args));
    }
}
