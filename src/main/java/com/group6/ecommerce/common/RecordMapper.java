package com.group6.ecommerce.common;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Base mapper for jobs that read the raw DataCo CSV.
 * Handles: header skipping, latin-1 decoding, CSV parsing, bad-row counters.
 * Subclasses only implement {@link #handle}.
 */
public abstract class RecordMapper<K, V> extends Mapper<LongWritable, Text, K, V> {

    public enum Quality { INPUT_ROWS, MALFORMED_ROWS, MISSING_FIELD, BAD_NUMBER }

    protected DataCoRecord rec;

    /** Column names this mapper needs (validated against the header). */
    protected abstract String[] requiredColumns();

    protected abstract void handle(DataCoRecord r, Context ctx)
            throws IOException, InterruptedException;

    @Override
    protected void setup(Context ctx) throws IOException {
        rec = DataCoRecord.forSplit(ctx.getConfiguration(), ctx.getInputSplit());
        rec.require(requiredColumns());
    }

    @Override
    protected void map(LongWritable offset, Text line, Context ctx)
            throws IOException, InterruptedException {
        if (offset.get() == 0) {
            return; // header row (byte offset 0 of the file)
        }
        ctx.getCounter(Quality.INPUT_ROWS).increment(1);
        // The DataCo file is latin-1; decode bytes explicitly instead of Text.toString() (UTF-8).
        String s = new String(line.getBytes(), 0, line.getLength(), StandardCharsets.ISO_8859_1);
        if (!rec.parse(s)) {
            ctx.getCounter(Quality.MALFORMED_ROWS).increment(1);
            return;
        }
        try {
            handle(rec, ctx);
        } catch (NumberFormatException e) {
            ctx.getCounter(Quality.BAD_NUMBER).increment(1);
        }
    }

    protected static boolean blank(String s) {
        return s == null || s.isEmpty();
    }
}
