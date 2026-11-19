package com.group6.ecommerce;

import org.apache.hadoop.util.ProgramDriver;

import com.group6.ecommerce.jobs.*;

/** Entry point: hadoop jar ecommerce-mapreduce-1.0.jar <jobName> <input> <output> */
public class EcommerceJobs {
    public static void main(String[] args) throws Throwable {
        ProgramDriver d = new ProgramDriver();
        d.addClass("salesPerCity", SalesPerCity.class, "Q1 Total sales per city");
        d.addClass("mostSoldCategory", MostSoldCategory.class, "Q2 Most sold category (ranked)");
        d.addClass("ordersPerPayment", OrdersPerPaymentMethod.class, "Q3 Distinct orders per payment method");
        d.addClass("categoryRevenue", CategoryRevenue.class, "Q4 Revenue and units per category");
        d.addClass("top10Products", Top10Products.class, "Q5 Top 10 products by revenue and units");
        d.addClass("avgOrderValuePerCity", AvgOrderValuePerCity.class, "Q6 Average order value per city");
        System.exit(d.run(args));
    }
}
