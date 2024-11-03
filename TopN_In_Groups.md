## Traditional Top N by Group

In most databases, the way to filter to the top N within a group is to use a window function and a common table expression (CTE).
This approach also works in DuckDB. For example, this query returns the 3 most recent shipments for each supplier:
```SQL
WITH ranked_lineitem AS (
    FROM lineitem 
    SELECT 
        *,
        row_number() OVER
            (PARTITION BY l_suppkey ORDER BY l_shipdate DESC)
            AS my_ranking
)
FROM ranked_lineitem
WHERE 
    my_ranking <= 3;
```

| l_orderkey | l_partkey | l_suppkey | … | l_shipmode | l_comment                                 | my_ranking |
| ---------- | --------- | --------- | - | ---------- | ----------------------------------------- | ---------- |
| 1310688    | 169532    | 7081      | … | RAIL       | ully final exc                            | 1          |
| 910561     | 194561    | 7081      | … | SHIP       | ly bold excuses caj                       | 2          |
| 4406883    | 179529    | 7081      | … | RAIL       | tions. furious                            | 3          |
| 4792742    | 52095     | 7106      | … | RAIL       | onic, ironic courts. final deposits sleep | 1          |
| 4010212    | 122081    | 7106      | … | MAIL       | accounts cajole finally ironic instruc    | 2          |
| 1220871    | 94596     | 7106      | … | TRUCK      | regular requests above t                  | 3          |
| …          | …         | …         | … | …          | …                                         | …          |



In DuckDB, this can be simplified using the QUALIFY clause. QUALIFY acts like a WHERE clause, but specifically operates on the results of window functions. 
By making this adjustment, the CTE can be avoided while returning the same results.

```SQL
FROM lineitem 
SELECT
    *,
    row_number() OVER
        (PARTITION BY l_suppkey ORDER BY l_shipdate DESC)
        AS my_ranking
QUALIFY
    my_ranking <= 3;
```

This is certainly a viable approach! However, what are its weaknesses? Even though the query is interested in only the 3 most recent shipments, 
it must sort every shipment just to retrieve those top 3. Sorting in DuckDB has a complexity of O(kn) due to DuckDB's innovative Radix sort implementation,
but this is still higher than the O(n) of DuckDB's hash aggregate, 
for example. Sorting is also a memory intensive operation when compared with aggregation.

 ## Top N in DuckDB

DuckDB 1.1 added a new capability to dramatically simplify and improve performance of top N calculations.
Namely, the functions min, max, min_by, and max_by all now accept an optional parameter N. If N is greater than 1 (the default), they will return an array of the top values.
As a simple example, let's query the most recent (top 3) shipment dates:

```SQL
FROM lineitem
SELECT
    max(l_shipdate, 3) AS top_3_shipdates;
```

As a result

```
top_3_shipdates
[1998-12-01, 1998-12-01, 1998-12-01]
```

 ## Top N by Group in DuckDB

Armed with the new N parameter, how can we speed up a top N by group analysis?

Want to cut to the chase and see the final output? Feel free to skip ahead!

We will take advantage of three other DuckDB SQL features to make this possible:

    The max_by function (also known as arg_max)
    The unnest function
    Automatically packing an entire row into a STRUCT column

The max function will return the max (or now the max N!) of a specific column. 
In contrast, the max_by function will find the maximum value in a column, and then retrieve a value from the same row, but a different column. 
For example, this query will return the ids of the 3 most recently shipped orders for each supplier:

```SQL
FROM lineitem
SELECT 
    l_suppkey,
    max_by(l_orderkey, l_shipdate, 3) AS recent_orders
GROUP BY
    l_suppkey;
```

| l_suppkey | recent_orders               |
| --------- | --------------------------- |
| 2992      | [233573, 3597639, 3060227]  |
| 8516      | [4675968, 5431174, 4626530] |
| 3205      | [3844610, 4396966, 3405255] |
| 2152      | [1672000, 4209601, 3831138] |
| 1880      | [4852999, 2863747, 1650084] |
| …         | …                           |

The max_by function is an aggregate function, so it takes advantage of DuckDB's fast hash aggregation rather than sorting. 
Instead of sorting by l_shipdate, the max_by function scans through the dataset just once and keeps track of the N highest l_shipdate values. 
It then returns the order id that corresponds with each of the most recent shipment dates. 
The radix sort in DuckDB must scan through the dataset once per byte, so scanning only once provides a significant speedup. 
For example, if sorting by a 64-bit integer, the sort algorithm must loop through the dataset 8 times vs. 1 with this approach! 

However, this SQL query has a few gaps. The query returns results as a LIST rather than as separate rows. Thankfully the unnest function can split a LIST into separate rows:
```SQL
FROM lineitem
SELECT 
    l_suppkey,
    unnest(
        max_by(l_orderkey, l_shipdate, 3)
    ) AS recent_orders
GROUP BY
    l_suppkey;
```

| l_suppkey | recent_orders |
| --------- | ------------- |
| 2576      | 930468        |
| 2576      | 2248354       |
| 2576      | 3640711       |
| 5559      | 4022148       |
| 5559      | 1675680       |
| 5559      | 4976259       |
| …         | …             |

The next gap is that there is no way to easily see the l_shipdate associated with the returned l_orderkey values. 
This query only returns a single column, while typically a top N by group analysis will require the entire row.

Fortunately, DuckDB allows us to refer to the entire contents of a row as if it were just a single column! 
By referring to the name of the table itself (here, lineitem) instead of the name of a column, the max_by function can retrieve all columns.

 ## The Final Top N by Group Query

Passing in one more argument to UNNEST will split this out into separate columns by running recursively.
In this case, that means that UNNEST will run twice: once to convert each LIST into separate rows, and then again to convert each STRUCT into separate columns. 
The l_suppkey column can also be excluded, since it will automatically be included already.
```SQL
FROM lineitem
SELECT 
    unnest(
        max_by(lineitem, l_shipdate, 3),
        recursive := 1
    ) AS recent_orders
GROUP BY
    l_suppkey;
```

| l_orderkey | l_partkey | l_suppkey | … | l_shipinstruct    | l_shipmode | l_comment                            |
| ---------- | --------- | --------- | - | ----------------- | ---------- | ------------------------------------ |
| 1234726    | 6875      | 6876      | … | COLLECT COD       | FOB        | cajole carefully slyly fin           |
| 2584193    | 51865     | 6876      | … | TAKE BACK RETURN  | TRUCK      | fully regular deposits at the q      |
| 2375524    | 26875     | 6876      | … | DELIVER IN PERSON | AIR        | nusual ideas. busily bold deposi     |
| 5751559    | 95626     | 8136      | … | NONE              | SHIP       | ers nag fluffily against the spe     |
| 3103457    | 103115    | 8136      | … | TAKE BACK RETURN  | FOB        | y slyly express warthogs– unusual, e |
| 5759105    | 178135    | 8136      | … | COLLECT COD       | TRUCK      | es. regular pinto beans haggle.      |
| …          | …         | …         | … | …                 | …          | …                                    |

 
## Performance Comparisons

We will compare the QUALIFY approach with the max_by approach for solving the top N by group problem. We have discussed both queries, but for reference they are repeated below.

* QUALIFY query
* max_by query:

While the main query is running, we will also kick off a background thread to periodically measure DuckDB's memory use. 
This uses the built in table function duckdb_memory() and includes information about Memory usage as well as temporary disk usage. 
| SF | Threads | Metric       | `QUALIFY` | `max_by` | Improvement |
| -- | ------- | ------------ | --------- | -------- | ----------- |
| 10 | 10      | Total time   | 36.8 s    | 25.4 s   | 1.4×        |
| 10 | 4       | Total time   | 49.0 s    | 21.0 s   | 2.3×        |
| 10 | 1       | Total time   | 115.7 s   | 12.7 s   | 9.1×        |
| 10 | 10      | Memory usage | 15.7 GB   | 17.1 GB  | 0.9×        |
| 10 | 4       | Memory usage | 15.9 GB   | 17.3 GB  | 0.9×        |
| 10 | 1       | Memory usage | 14.5 GB   | 1.8 GB   | 8.1×        |
