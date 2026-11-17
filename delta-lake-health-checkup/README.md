# Delta Lake for Late-Arriving Health Checkup Results

A technology adoption proposal and hands-on validation of **Delta Lake on Apache Spark**, written as a data engineer for a (fictional) multi-site health checkup center.

> Course project · Big Data Platform · Hanshin University · Nov 2026
> Full report (Korean, 18 pages): \[`report/proposal\_ko.pdf`](report/proposal\_ko.pdf)

## The problem

Health checkup results are not final on the day of the exam. Biopsies and outsourced lab tests come back days later, and radiologists revise findings. The existing pipeline stores results as daily Parquet files written by Spark, which causes four problems:

|#|Problem|Why it happens with plain Parquet|
|-|-|-|
|1|Late corrections force a nightly rewrite of the last 30 days|Parquet cannot update individual rows|
|2|No change history in the analytics layer|Overwriting a file loses the previous value|
|3|A failed job can leave half-written data|Multi-file writes are not atomic|
|4|Readers can see partially updated data|No snapshot isolation between readers and writers|

Context checked against real sources: Korean medical law requires both the corrected record and the original to be kept (Medical Service Act, Art. 22(2)), and one hospital checkup center reported biopsies in 19.8% of gastroscopy and 36.97% of colonoscopy patients, so late updates are routine.

## Proposed architecture

!\[Pipeline architecture](images/00\_architecture.png)

* **Spark batch 1** cleans the nightly CSV extract and applies it to the results table with `MERGE` (only changed rows are rewritten).
* **Change Data Feed (CDF)** exposes each change as `update\_preimage` / `update\_postimage` rows with commit version and timestamp.
* **Spark batch 2** reads CDF incrementally and appends it to a separate **history table** protected with `delta.appendOnly = true`.

**Key design decision:** Time Travel is *not* used for long-term history. By default Delta keeps removed files for 7 days and log entries for 30 days, while the legal retention period for test results is 5 years. Time Travel is kept only for short-term recovery; the append-only history table is the long-term audit trail.

## Hands-on validation (Google Colab)

Environment: `pyspark==3.5.3`, `delta-spark==3.2.1`, local mode, synthetic data only (no real patient data).

|#|Experiment|Result|Screenshot|
|-|-|-|-|
|1|Spark + Delta session|Spark 3.5.3 / Delta 3.2.1 running|[01](images/01_setup.png)|
|2|Create table|`\_delta\_log` holds JSON commits for versions 0 and 1|[02](images/02_create.png)|
|3|Inspect a commit file|`commitInfo` + two `add` actions|[03](images/03_commit_log.png)|
|4|`MERGE` a late biopsy result|1 row updated, 1 inserted, other rows untouched|[04](images/04_merge.png)|
|5|Read Change Data Feed|insert, update\_preimage, update\_postimage at version 2|[05](images/05_cdf.png)|
|6|Append-only history table|`DELETE` blocked with `DELTA\_CANNOT\_MODIFY\_APPEND\_ONLY`|[06](images/06_append_only.png)|
|7|Time Travel|`versionAsOf 1` shows the pre-MERGE state|[07](images/07_time_travel.png)|
|8|Checkpoint interval|checkpoint written at version 10|[08](images/08_checkpoint.png)|

Not tested (and stated as such in the report): atomicity under a mid-write failure, concurrent reader/writer isolation, and performance at production scale (\~450k rows/day).

## Core code

```python
from delta.tables import DeltaTable

# 1) Apply late-arriving results: update matched rows, insert new ones
(DeltaTable.forPath(spark, RESULTS).alias("t")
    .merge(updates.alias("s"),
           "t.exam\_id = s.exam\_id AND t.item\_code = s.item\_code")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute())

# 2) Read only what changed since the last run and append it to history
(spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", last\_version + 1)
    .load(RESULTS)
    .withColumnRenamed("\_change\_type", "change\_type")
    .withColumnRenamed("\_commit\_version", "commit\_version")
    .withColumnRenamed("\_commit\_timestamp", "commit\_timestamp")
    .write.format("delta").mode("append").save(HISTORY))
```

## Operational findings

* **Dependency conflicts:** Colab's preinstalled `dataproc-spark-connect` expects PySpark 4.0, so Spark, Delta and related libraries must be pinned together.
* **Maven dependency at startup:** `configure\_spark\_with\_delta\_pip` downloads the Delta JAR when Spark starts. In a sandbox without Maven access Spark failed to start, so restricted networks (such as hospitals) need the JAR mirrored internally.
* **Reserved CDF column names:** rename `\_change\_type` and related columns before persisting them.
* **CDF retention:** if the history batch stops for more than the retention window (7 days by default), changes are lost, so the batch needs failure alerts.
* **Small files:** frequent small writes create many files; schedule `OPTIMIZE` and `VACUUM`.

## Recommendation

**Adopt Delta Lake, with two conditions:** run a proof of concept on one center's month of data to measure MERGE cost, and keep long-term change history in the append-only table rather than relying on Time Travel. It solves the core problem without new servers, which matters for a three-person data team. Iceberg and Hudi were compared and are better fits only if multiple query engines or real-time ingestion are added later.

## Repository layout

```
.
├── README.md
├── report/proposal\_ko.pdf          # full proposal (Korean)
├── notebooks/delta\_lake\_lab.ipynb  # Colab notebook with outputs
├── images/                         # architecture diagram and screenshots
└── requirements.txt
```

## Run it

Open `notebooks/delta\_lake\_lab.ipynb` in Google Colab and run the cells in order, or locally (Java 8/11/17 required):

```bash
pip install -r requirements.txt
```

## References

* [Delta Transaction Log Protocol (PROTOCOL.md)](https://github.com/delta-io/delta/blob/master/PROTOCOL.md)
* [Delta Lake documentation](https://docs.delta.io) · [Release compatibility](https://docs.delta.io/releases/) · [Change data feed](https://docs.delta.io/3.0.0/delta-change-data-feed.html)
* M. Armbrust et al., [Delta Lake: High-Performance ACID Table Storage over Cloud Object Stores](https://vldb.org/pvldb/vol13/p3411-armbrust.pdf), PVLDB 13(12), 2020
* [Apache Iceberg spec](https://iceberg.apache.org/spec) · [Apache Hudi concepts](https://hudi.apache.org/docs/concepts)

The full reference list (14 sources, including Korean statutes) is in the report.



## Use of generative AI

Drafted the report and generated diagrams with help from generative AI (Claude); I selected the topic, conducted the research, verified technical claims against official documentation, executed all hands-on tests, and made the final adoption decision.

