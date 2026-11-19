package com.group6.ecommerce.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;

/** Keeps the N largest (name, value) entries seen. Ties broken by name for stable output. */
public class TopN {
    public static final class Entry {
        public final String name;
        public final double value;
        Entry(String name, double value) { this.name = name; this.value = value; }
    }

    private final int n;
    private final PriorityQueue<Entry> heap = new PriorityQueue<>(
            (a, b) -> a.value != b.value ? Double.compare(a.value, b.value) : b.name.compareTo(a.name));

    public TopN(int n) { this.n = n; }

    public void add(String name, double value) {
        heap.add(new Entry(name, value));
        if (heap.size() > n) {
            heap.poll(); // drop the smallest
        }
    }

    /** Entries sorted from largest to smallest. */
    public List<Entry> sortedDesc() {
        List<Entry> l = new ArrayList<>(heap);
        l.sort((a, b) -> a.value != b.value ? Double.compare(b.value, a.value) : a.name.compareTo(b.name));
        return Collections.unmodifiableList(l);
    }
}
