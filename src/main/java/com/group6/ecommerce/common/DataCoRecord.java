package com.group6.ecommerce.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * One parsed row of the DataCo CSV.
 *
 * Columns are looked up by NAME (from the file's header line), not by position,
 * so the same code works on the original 53-column file and on a cleaned copy
 * where personal columns were removed.
 */
public class DataCoRecord {

    private final Map<String, Integer> columnIndex;
    private final int columnCount;
    private String[] fields;

    private DataCoRecord(Map<String, Integer> columnIndex, int columnCount) {
        this.columnIndex = columnIndex;
        this.columnCount = columnCount;
    }

    /** Reads the header line of the file this split belongs to. */
    public static DataCoRecord forSplit(Configuration conf, InputSplit split) throws IOException {
        Path path = ((FileSplit) split).getPath();
        FileSystem fs = path.getFileSystem(conf);
        String header;
        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(fs.open(path), StandardCharsets.ISO_8859_1))) {
            header = br.readLine();
        }
        if (header == null) {
            throw new IOException("Empty input file: " + path);
        }
        header = stripBom(header);
        String[] names = splitCsv(header);
        Map<String, Integer> idx = new HashMap<>();
        for (int i = 0; i < names.length; i++) {
            idx.put(names[i].trim().toLowerCase(), i);
        }
        return new DataCoRecord(idx, names.length);
    }

    /** Fails fast (with a readable message) if a needed column is not in the header. */
    public void require(String... columns) throws IOException {
        for (String c : columns) {
            if (!columnIndex.containsKey(c.toLowerCase())) {
                throw new IOException("Column '" + c + "' not found in CSV header. Found: "
                        + columnIndex.keySet());
            }
        }
    }

    /** Parses one data line. Returns false if the row does not match the header width. */
    public boolean parse(String line) {
        String[] f = splitCsv(line);
        if (f.length != columnCount) {
            return false;
        }
        this.fields = f;
        return true;
    }

    public boolean has(String column) {
        return columnIndex.containsKey(column.toLowerCase());
    }

    public String get(String column) {
        return fields[columnIndex.get(column.toLowerCase())].trim();
    }

    public double getDouble(String column) {
        return Double.parseDouble(get(column));
    }

    public long getLong(String column) {
        // some exports write integers as "3.0"
        return (long) Double.parseDouble(get(column));
    }

    private static String stripBom(String s) {
        if (s.startsWith("﻿")) {
            return s.substring(1);
        }
        if (s.startsWith("ï»¿")) { // UTF-8 BOM read as latin-1
            return s.substring(3);
        }
        return s;
    }

    /** RFC-4180 style splitter: handles "quoted, commas" and "" escaped quotes. */
    public static String[] splitCsv(String line) {
        List<String> out = new ArrayList<>(64);
        StringBuilder sb = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (inQuotes) {
                if (c == '"') {
                    if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
                        sb.append('"');
                        i++;
                    } else {
                        inQuotes = false;
                    }
                } else {
                    sb.append(c);
                }
            } else if (c == '"') {
                inQuotes = true;
            } else if (c == ',') {
                out.add(sb.toString());
                sb.setLength(0);
            } else {
                sb.append(c);
            }
        }
        out.add(sb.toString());
        return out.toArray(new String[0]);
    }
}
