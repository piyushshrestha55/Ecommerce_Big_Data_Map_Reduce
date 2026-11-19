# DataCo E-commerce – MapReduce jobs (Group 6)

Java MapReduce implementation of the six questions in Section 6 of the group sheet.

| Command name | Question | Map key -> value | Output |
|---|---|---|---|
| `salesPerCity` | Total sales per city | Order City -> Price × Quantity | city, total |
| `mostSoldCategory` | Most sold category | Category Name -> Quantity | categories ranked by units (first line = answer) |
| `ordersPerPayment` | Orders per payment method | Type -> Order Id (distinct) | type, distinct orders |
| `categoryRevenue` | Revenue + units per category | Category -> "qty,qty×price" | category, units, revenue |
| `top10Products` | Top 10 products by revenue and by units | 2 chained jobs | `TOP_BY_REVENUE #n`, `TOP_BY_UNITS #n` |
| `avgOrderValuePerCity` | Avg order value per city | 2 chained jobs: Order Id -> order total, then city -> avg | city, avg, #orders |

## Build and run
    mvn clean package                      # needs Maven + JDK 8+; Hadoop is "provided"
    hdfs dfs -mkdir -p /user/group6/dataco
    hdfs dfs -put DataCoSupplyChainDataset.csv /user/group6/dataco/
    hadoop jar target/ecommerce-mapreduce-1.0.jar salesPerCity /user/group6/dataco /user/group6/out/salesPerCity
    # or all six:  ./run_all.sh /user/group6/dataco /user/group6/out
    hdfs dfs -cat /user/group6/out/salesPerCity/part-r-*
Output directories must not exist yet. The two-stage jobs also write `<output>_stage1`.

## Design notes
* **Columns are found by header name**, so it works on the original file and on your cleaned copy
  (personal columns removed). Missing required column -> job fails with a clear message.
* **Encoding:** input is decoded as ISO-8859-1 (the DataCo file is latin-1); output is UTF-8.
* **CSV quoting:** quoted fields containing commas and `""` escapes are handled.
* **Bad rows are skipped and counted** (`MALFORMED_ROWS`, `MISSING_FIELD`, `BAD_NUMBER` counters).
* **Distinct orders:** Order Id repeats per order line, so Q3 counts distinct ids and Q6 first sums lines
  into order totals, then averages over orders.
* **Combiners** on every aggregation to cut shuffle volume.
* Limitation: records with line breaks *inside* quoted fields are not supported (the DataCo file has none that matter for these columns).
* Top-10 revenue uses the `Sales` column (as in the group sheet); the other revenue figures use
  `Order Item Product Price × Order Item Quantity`.

`tools/make_synthetic_sample.py` creates a small **synthetic** 53-column file for smoke tests only.
