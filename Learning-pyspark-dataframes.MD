Here's a structured breakdown of what to learn about PySpark DataFrames:

**1. Creating DataFrames**
- From Python lists, dicts, and Pandas DataFrames
- Reading from files: CSV, JSON, Parquet, Delta
- Reading from databases via JDBC

**2. Basic Operations**
- `show()`, `printSchema()`, `describe()`
- `select()`, `filter()` / `where()`, `limit()`
- `withColumn()`, `drop()`, `rename()`

**3. Aggregations & Grouping**
- `groupBy()` + `agg()` with functions like `sum()`, `count()`, `avg()`, `max()`, `min()`
- `pivot()` for reshaping data
- Window functions (`ROW_NUMBER`, `RANK`, `LAG`, `LEAD`)

**4. Joins**
- Inner, left, right, full outer, semi, anti joins
- Handling duplicate column names after joins
- Broadcast joins for performance with small tables

**5. Working with Columns**
- `pyspark.sql.functions` — `col()`, `lit()`, `when()`, `coalesce()`, `cast()`
- String functions: `upper()`, `trim()`, `split()`, `regexp_replace()`
- Date functions: `to_date()`, `datediff()`, `date_format()`

**6. Handling Nulls**
- `fillna()`, `dropna()`, `isNull()`, `isNotNull()`

**7. Performance & Optimisation**
- Partitioning and `repartition()` vs `coalesce()`
- `cache()` and `persist()` — when and why
- Understanding lazy evaluation and the DAG
- Reading the query plan with `explain()`

**8. Writing Data**
- Writing to CSV, Parquet, Delta
- Save modes: `overwrite`, `append`, `ignore`, `errorIfExists`
- Partitioning output with `partitionBy()`

**9. Interoperability**
- Converting to/from Pandas with `toPandas()` and `createDataFrame()`
- Using SQL alongside DataFrames with `createOrReplaceTempView()`

---

Since you're already working with **Delta Lake**, I'd prioritise learning items 2, 3, 4, and 7 first — they'll give you the most leverage for the kind of data pipeline work you're doing.

Want me to dive deep into any of these areas with code examples?