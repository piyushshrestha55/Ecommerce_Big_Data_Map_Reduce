package com.group6.ecommerce.common;

import org.apache.hadoop.io.Text;

/** Helper for "x,y" encoded values. */
public final class PairSum {
    private PairSum() { }

    public static String encode(double x, double y) {
        return x + "," + y;
    }

    public static double[] sum(Iterable<Text> values) {
        double a = 0, b = 0;
        for (Text v : values) {
            String s = v.toString();
            int comma = s.indexOf(',');
            a += Double.parseDouble(s.substring(0, comma));
            b += Double.parseDouble(s.substring(comma + 1));
        }
        return new double[] { a, b };
    }
}
