package com.group6.ecommerce.common;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Combiner for values encoded as "x,y" (two numbers). Emits the element-wise sum.
 * Used to cut shuffle volume for jobs that aggregate two measures at once.
 */
public class PairSumCombiner extends Reducer<Text, Text, Text, Text> {
    private final Text out = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context ctx)
            throws IOException, InterruptedException {
        double[] t = PairSum.sum(values);
        out.set(t[0] + "," + t[1]);
        ctx.write(key, out);
    }
}
