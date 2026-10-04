Yes. Your attached project is a good beginner Databricks workflow demo. It shows:

```text
SFTP or hospital files
        ↓
Cloud landing folder / Unity Catalog Volume
        ↓
Task 1: Auto Loader → Bronze Delta table
        ↓
Task 2: Clean and MERGE → Silver Delta table
        ↓
Gold daily hospital summary
        ↓
Task 3: Validation
```

Databricks now calls this an **Lakeflow Job**. A job coordinates multiple tasks, dependencies, retries, schedules, and repairs. [Databricks on AWS](https://docs.databricks.com/aws/en/jobs/?utm_source=chatgpt.com)

## 1. Understand the attached files

Your ZIP contains four notebooks:

| Notebook | Purpose |
|---|---|
| `00_setup_and_generate.py` | Creates the Unity Catalog objects and generates sample hospital CSV files |
| `01_pipeline1_sftp_to_bronze.py` | Uses Auto Loader to ingest CSV files into Bronze |
| `02_pipeline2_bronze_to_gold.py` | Transforms Bronze into Silver and Gold |
| `03_validation.py` | Checks row counts and duplicate records |

Important: the demo does not connect to a real SFTP server. It simulates SFTP by generating files inside:

```text
/Volumes/main/sftp_demo/demo_files/inbound
```

In production, an SFTP tool such as Azure Data Factory, AWS Transfer Family, or another managed file-transfer service would copy files into cloud storage. Databricks would then read those files with Auto Loader.

Unity Catalog Volumes are designed for governed file storage, including landing areas and staging locations for ingestion. [Databricks on AWS](https://docs.databricks.com/aws/en/volumes/?utm_source=chatgpt.com)

---

# Part 1: Prepare Databricks

## 2. Confirm your workspace requirements

You need:

- A Databricks workspace
- Unity Catalog enabled
- Permission to create:
  - A schema
  - A volume
  - Delta tables
- Permission to use compute
- Databricks Runtime 13.3 LTS or newer when using `/Volumes` paths

The project defaults to:

```text
Catalog: main
Schema: sftp_demo
Volume: demo_files
```

The full volume path is:

```text
/Volumes/main/sftp_demo/demo_files
```

The attached setup notebook creates the schema and volume automatically:

```python
CREATE SCHEMA IF NOT EXISTS main.sftp_demo
CREATE VOLUME IF NOT EXISTS main.sftp_demo.demo_files
```

If you do not have permission to use the `main` catalog, choose another catalog, such as:

```text
Catalog: alan_catalog
Schema: sftp_demo
Volume: demo_files
```

Then use that catalog consistently in every notebook.

---

## 3. Import the notebooks

In Databricks:

1. Open **Workspace**.
2. Create a folder, for example:

```text
/Workspace/Users/<your-email>/sftp-demo
```

3. Select the folder.
4. Click **Import**.
5. Upload the ZIP file:

```text
databricks-sftp-resumable-pipeline-demo-main.zip
```

Databricks can import ZIP archives and recreate their folder structure. Files containing the comment `# Databricks notebook source` are recognized as Databricks source-format notebooks. [docs.databricks.com](https://docs.databricks.com/aws/en/notebooks/notebook-export-import?utm_source=chatgpt.com)

After importing, you should see:

```text
sftp-demo/
└── databricks-sftp-resumable-pipeline-demo-main/
    ├── 00_setup_and_generate
    ├── 01_pipeline1_sftp_to_bronze
    ├── 02_pipeline2_bronze_to_gold
    └── 03_validation
```

---

# Part 2: Run the setup notebook

## 4. Open `00_setup_and_generate`

Attach the notebook to compute.

Use a small cluster or serverless compute for this learning exercise.

Run it with these parameters:

| Parameter | Value |
|---|---|
| `catalog` | `main` |
| `schema` | `sftp_demo` |
| `batch_hour` | `2026-10-04-01` |
| `num_rows` | `1000` |

The notebook creates this file:

```text
/Volumes/main/sftp_demo/demo_files/inbound/hospital_a/2026-10-04-01/encounters_2026-10-04-01.csv
```

It contains 1,000 simulated healthcare encounter records.

After the notebook completes, you can verify the volume with:

```sql
LIST '/Volumes/main/sftp_demo/demo_files/inbound';
```

You can also verify the schema and volume in **Catalog Explorer**.

---

# Part 3: Create the Databricks Workflow

## 5. Open Jobs & Pipelines

1. In the Databricks left navigation menu, select **Jobs & Pipelines**.
2. Click **Create**.
3. Select **Job**.
4. Name the job:

```text
hospital_sftp_lakehouse_demo
```

Databricks supports creating and managing jobs through the Jobs UI, CLI, or REST API. For your first workflow, the UI is easiest. [Databricks on AWS](https://docs.databricks.com/aws/en/jobs/configure-job?utm_source=chatgpt.com)

---

## 6. Configure the compute

For learning, use:

- Serverless compute, if available; or
- A small job cluster

Recommended settings:

```text
Runtime: Databricks Runtime 13.3 LTS or newer
Access mode: Standard or Shared, depending on workspace policy
```

For production, use a job cluster instead of an all-purpose interactive cluster. A job cluster starts for the workflow and terminates after the workflow completes.

---

## 7. Create Task 1: Ingest files into Bronze

Click **Add task**.

Configure it as follows:

| Setting | Value |
|---|---|
| Task type | Notebook |
| Task key | `pipeline1_ingest` |
| Notebook path | `01_pipeline1_sftp_to_bronze` |
| Compute | Your selected job compute |

The task uses this code:

```python
spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .load(inbound_path)
```

Auto Loader uses the `cloudFiles` format to incrementally discover new files. The attached notebook also uses:

```python
.trigger(availableNow=True)
```

That means the task processes the files currently available and then stops. This is appropriate for hourly or daily scheduled ingestion. [Databricks on AWS](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/unity-catalog?utm_source=chatgpt.com)

### Add task parameters

Under **Parameters**, add:

```text
catalog = main
schema = sftp_demo
```

### Add retries

Configure:

```text
Maximum retries: 2
Retry interval: 30 seconds
```

Retries are useful for temporary compute, networking, or storage errors. Databricks allows retry policies at the task level. [Databricks on AWS](https://docs.databricks.com/aws/en/jobs/configure-task?utm_source=chatgpt.com)

### What this task creates

The notebook writes to:

```text
main.sftp_demo.encounters_bronze
```

It also creates a durable checkpoint:

```text
/Volumes/main/sftp_demo/demo_files/checkpoints/pipeline1
```

The checkpoint is important because it remembers which files Auto Loader has already processed. Databricks recommends keeping the checkpoint in durable output-side storage rather than inside the source directory. [Databricks on AWS](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/best-practices?utm_source=chatgpt.com)

---

## 8. Create Task 2: Bronze to Silver to Gold

Click **Add task** again.

Configure:

| Setting | Value |
|---|---|
| Task type | Notebook |
| Task key | `pipeline2_transform` |
| Notebook path | `02_pipeline2_bronze_to_gold` |
| Depends on | `pipeline1_ingest` |
| Compute | Same job compute |

The dependency should look like:

```text
pipeline1_ingest
        ↓
pipeline2_transform
```

### Add parameters

```text
catalog = main
schema = sftp_demo
fail_after_silver = false
```

### Add retries

Configure:

```text
Maximum retries: 2
Retry interval: 30 seconds
```

This task performs two writes:

```text
Bronze → Silver
Silver → Gold
```

The Silver table is:

```text
main.sftp_demo.encounters_silver
```

The Gold table is:

```text
main.sftp_demo.daily_hospital_summary
```

The Silver write uses a `MERGE` based on `event_id`:

```sql
MERGE INTO encounters_silver
ON t.event_id = s.event_id
```

This makes the task idempotent. If the task runs again, existing event IDs are updated instead of duplicated.

---

## 9. Create Task 3: Validation

Click **Add task**.

Configure:

| Setting | Value |
|---|---|
| Task type | Notebook |
| Task key | `validation` |
| Notebook path | `03_validation` |
| Depends on | `pipeline2_transform` |
| Compute | Same job compute |

Add parameters:

```text
catalog = main
schema = sftp_demo
```

Your completed workflow should look like this:

```text
pipeline1_ingest
        ↓
pipeline2_transform
        ↓
validation
```

Notebook tasks can be configured with dependencies, parameters, compute, and retry settings inside Lakeflow Jobs. [Databricks on AWS](https://docs.databricks.com/aws/en/jobs/tasks/notebook?utm_source=chatgpt.com)

---

# Part 4: Run the workflow

## 10. Save and run

Click **Save** and then **Run now**.

Expected task results:

```text
pipeline1_ingest       SUCCESS
pipeline2_transform    SUCCESS
validation             SUCCESS
```

The first run should produce:

| Layer | Expected rows |
|---|---:|
| Bronze | 1,000 |
| Silver | 1,000 |
| Gold | Aggregated results |

The validation notebook should show:

```text
bronze rows = 1000
silver rows = 1000
duplicate Silver event IDs = 0
```

You can manually verify the results with SQL:

```sql
SELECT COUNT(*) AS bronze_count
FROM main.sftp_demo.encounters_bronze;
```

```sql
SELECT COUNT(*) AS silver_count,
       COUNT(DISTINCT event_id) AS unique_event_ids
FROM main.sftp_demo.encounters_silver;
```

```sql
SELECT *
FROM main.sftp_demo.daily_hospital_summary
ORDER BY event_date, hospital_id;
```

---

# Part 5: Test that the workflow is incremental

## 11. Run the setup notebook again

Open `00_setup_and_generate` and use:

```text
batch_hour = 2026-10-04-02
num_rows = 500
```

Run the workflow again.

Expected results:

| Layer | Expected rows |
|---|---:|
| Bronze | 1,500 |
| Silver | 1,500 |
| Duplicate event IDs | 0 |

Why?

- The first file contained 1,000 rows.
- The second file contains 500 new rows.
- Auto Loader sees only the new file.
- The checkpoint prevents the old file from being loaded again.

Now run the workflow a third time without generating a new file.

The totals should remain:

```text
Bronze: 1,500
Silver: 1,500
```

This demonstrates checkpoint-based incremental ingestion.

---

# Part 6: Test automatic retries

## 12. Generate another batch

Run the setup notebook with:

```text
batch_hour = 2026-10-04-03
num_rows = 250
```

Then change the Task 2 parameter:

```text
fail_after_silver = true
```

Run the workflow.

The sequence will be:

```text
pipeline1_ingest
        ↓
pipeline2_transform
        ├── Silver succeeds
        ├── Gold intentionally fails
        ├── Retry 1
        ├── Silver MERGE runs again
        └── Retry 2 fails again
```

The workflow should eventually show as failed.

Silver should not contain duplicate records because the task uses `MERGE`.

Expected total:

```text
Bronze: 1,750
Silver: 1,750
```

The 1,750 rows come from:

```text
1,000 + 500 + 250 = 1,750
```

---

# Part 7: Perform a Repair Run

## 13. Fix the intentional failure

Edit Task 2 and change:

```text
fail_after_silver = false
```

Save the job.

Open the failed job run. Select **Repair run** or **Repair**.

Choose the option to rerun the failed task and its dependent tasks.

Databricks repair runs rerun failed or skipped tasks while preserving the successful tasks from the original run. [Databricks on AWS](https://docs.databricks.com/aws/en/jobs/repair-job-failures?utm_source=chatgpt.com)

Expected behavior:

```text
pipeline1_ingest       not rerun
pipeline2_transform    rerun
validation             rerun
```

The Silver `MERGE` runs again safely, and Gold is created successfully.

Important distinction:

> A Repair run does not continue from the exact Python line where the task failed. It reruns the failed task from the beginning.

That is why idempotent logic matters.

---

# Part 8: Add a schedule

## 14. Schedule the workflow

After testing manually:

1. Open the job.
2. Click **Add schedule**.
3. Choose a schedule, such as:
   - Every hour
   - Every day at 2:00 AM
4. Set the correct timezone.
5. Enable the schedule.

For this demo, an hourly schedule would make sense because the sample files are organized by hourly batch:

```text
2026-10-04-01
2026-10-04-02
2026-10-04-03
```

Use a schedule that starts after the SFTP transfer process has finished copying the files into cloud storage.

---

# Production version of this design

The demo currently generates local files. A real production architecture would be:

```text
External SFTP server
        ↓
Managed file transfer service
        ↓
Cloud storage external location
        ↓
Unity Catalog external volume
        ↓
Auto Loader
        ↓
Bronze Delta table
        ↓
Silver Delta table
        ↓
Gold reporting table
```

For production, use an **external volume** when files are written by systems outside Databricks. Databricks documentation recommends external volumes for existing cloud storage used by external systems. [Databricks on AWS](https://docs.databricks.com/aws/en/volumes/?utm_source=chatgpt.com)

You would replace this demo path:

```python
base_path = f"/Volumes/{catalog}/{schema_name}/demo_files"
```

with a path corresponding to your governed production volume, for example:

```python
base_path = "/Volumes/prod_healthcare/landing/sftp_volume"
```

You would also replace the setup notebook with the actual SFTP-to-cloud-storage process.

---

# Recommended workflow configuration

| Area | Recommendation |
|---|---|
| Job name | `hospital_sftp_lakehouse_demo` |
| Task 1 | `pipeline1_ingest` |
| Task 2 | `pipeline2_transform` |
| Task 3 | `validation` |
| Task 1 dependency | None |
| Task 2 dependency | Task 1 |
| Task 3 dependency | Task 2 |
| Task retries | 2 |
| Retry interval | 30 seconds |
| Auto Loader trigger | `availableNow=True` |
| Bronze format | Delta |
| Silver write | Idempotent `MERGE` |
| Gold write | `CREATE OR REPLACE TABLE` |
| Checkpoint | Durable Unity Catalog Volume path |
| Schedule | After the expected SFTP arrival time |

The most important concepts to remember are:

- **Workflow/job:** controls task order.
- **Checkpoint:** remembers which files Auto Loader already processed.
- **Retry:** reruns a failed task automatically.
- **Repair run:** reruns failed tasks after the problem is fixed.
- **Idempotency:** allows reruns without creating duplicates.
