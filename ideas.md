This is where the **Azure Data Migration Tool (DMT)** really shines — you can automate a ton of repetitive or complex data transfer operations between sources and sinks. Since you’ve got the Cosmos DMT pack installed, here are some **cool ops you can automate**:

---

## 🔄 Common Migration Ops
- **CSV → Cosmos DB (NoSQL)**  
  Automate ingestion of large CSV files into Cosmos DB containers with partition keys defined.
- **JSON → Cosmos DB**  
  Perfect for bulk loading JSON datasets into Cosmos DB, especially when working with IoT or log data.
- **SQL Server → Cosmos DB**  
  Migrate relational tables into Cosmos DB documents, useful for modernizing legacy apps.
- **Cosmos DB → Parquet (Azure Blob/S3)**  
  Export Cosmos DB data into Parquet format for analytics pipelines (Synapse, Databricks, Spark).
- **MongoDB → Cosmos DB**  
  Lift-and-shift MongoDB collections into Cosmos DB with minimal schema changes.

---

## ⚙️ Automation Scenarios
- **Scheduled Data Loads**  
  Wrap `dmt run` in a scheduled task (Windows Task Scheduler or cron) to refresh Cosmos DB nightly.
- **ETL Pipelines**  
  Chain multiple runs: e.g., CSV → Cosmos DB, then Cosmos DB → Parquet for analytics.
- **Environment Bootstrap**  
  Use `dmt init` to generate template settings files for dev/test/prod environments, then automate applying them.
- **Extension Discovery**  
  Automate `dmt list` to inventory available source/sink extensions — useful when managing multiple environments.
- **Dynamic Settings Injection**  
  Pass `--SourceSettings:FilePath` or `--SinkSettings:Database` dynamically from your GUI, so you can run migrations without editing JSON manually.

---

## 🖥️ Example Automations
- **Bulk Upload JSON Files**
  ```bash
  dmt run -from JSON -to Cosmos-nosql --SourceSettings:FilePath data.json --SinkSettings:Database MyDB --SinkSettings:Container MyContainer
  ```
- **Export Cosmos DB to Parquet in Azure Blob**
  ```bash
  dmt run -from Cosmos-nosql -to Parquet-AzureBlob --SinkSettings:BlobContainer myblob --SinkSettings:FilePath export.parquet
  ```
- **List Extensions (for automation scripts)**
  ```bash
  dmt list
  ```
- **Generate Settings Template**
  ```bash
  dmt init --settings migrationsettings.json
  ```

---

## 🚀 Cool Ideas for Your GUI Tabs
- **Config Tab**: Save/load JSON configs for different pipelines (e.g., nightly CSV → Cosmos DB).  
- **CLI Tab**: Run ad-hoc commands like `dmt list` or `dmt settings Cosmos-nosql`.  
- **Logs Tab**: Capture stdout/stderr from DMT runs for auditing.  
- **Automation Tab (future)**: Queue multiple jobs (e.g., batch load + export) and run them sequentially.

---

👉 In short: you can automate **data ingestion, export, transformation, and environment setup** across Cosmos DB, SQL Server, MongoDB, Parquet, CSV, and JSON.  

Would you like me to sketch out a **few ready-to-use automation workflows** (like “Nightly CSV ingestion into Cosmos DB” or “Cosmos DB backup to Parquet in Blob Storage”) that you can wire directly into your GUI?


Nice, you’ve got `dmt.exe` up and listing extensions — that’s the heavy lifting done. Let’s wrap it in a *proper* pipeline so you’re not manually typing commands forever.

I’ll break it into:

1. **Pipeline patterns** you can use with these extensions
2. **Config & directory layout** so it doesn’t turn into a mess
3. **A reusable command pattern** for `dmt.exe`
4. **Automation on Windows** (PowerShell + Task Scheduler)
5. **Optional Python orchestrator** for more complex workflows
6. **Logging, retries, and safety nets**

---

## 1. Pick your pipeline patterns

Given the extensions you have:

* **Sources/Sinks supported**

  * `SqlServer`, `PostgreSQL`, `MongoDB`
  * `CSV`, `JSON`, `Parquet` (local, AzureBlob, AWSS3)
  * `Cosmos-nosql`
  * `CognitiveSearch`, `AzureTableAPI`

Common practical patterns:

1. **SqlServer → Cosmos-nosql (main migration / sync)**

   * `--source SqlServer --sink Cosmos-nosql`
   * Use this for incremental loads, nightly syncs, or cut-over migrations.

2. **SqlServer → CSV/Parquet (staging/backup)**

   * `--source SqlServer --sink CSV` or `Parquet`
   * Useful as a “land first, transform later” pipeline or backup/export.

3. **Cosmos-nosql → CSV/JSON (backup/verification)**

   * `--source Cosmos-nosql --sink CSV`
   * For snapshots, point-in-time backups, or data validation jobs.

4. **CSV/JSON → Cosmos-nosql (bulk imports)**

   * `--source CSV --sink Cosmos-nosql`
   * For batch ingestion from external systems.

5. **Cosmos-nosql → CognitiveSearch (indexing pipeline)**

   * `--source Cosmos-nosql --sink CognitiveSearch`
   * Sync Cosmos data into a search index on a schedule.

You don’t need all of them; pick 1–3 core pipelines and build the automation around those.

---

## 2. Structure your project & configs

Inside `C:\Users\person\Projects\azure_data_migration_tool` I’d standardize it like this:

```text
azure_data_migration_tool/
├── win-x64-package/
│   ├── dmt.exe
│   └── Extensions/
├── configs/
│   ├── sql_to_cosmos/
│   │   ├── dev.migrationsettings.json
│   │   ├── prod.migrationsettings.json
│   ├── cosmos_backup/
│   │   ├── daily_backup.json
│   └── csv_to_cosmos/
│       ├── import_template.json
├── logs/
│   ├── sql_to_cosmos/
│   ├── cosmos_backup/
├── scripts/
│   ├── invoke_dmt.ps1
│   └── orchestrator.py   (optional)
└── README.md
```

**Config rules:**

* One **settings file per pipeline** and environment (e.g. `sql_to_cosmos/dev.migrationsettings.json`).
* Use `dmt.exe init` or `dmt.exe settings` to see/refresh the schema of settings.
* Store *connection strings/secrets* outside the JSON:

  * Use env vars (e.g. `COSMOS_CONN_STRING`) and have your wrapper script inject them via `--SourceSettings:*` arguments.
  * Or pull from Key Vault and only pass via CLI, not stored on disk.

---

## 3. A clean, reusable `dmt` command pattern

From your help output, a canonical call looks like:

```powershell
win-x64-package\dmt.exe `
  --source SqlServer `
  --destination Cosmos-nosql `
  --settings "configs\sql_to_cosmos\prod.migrationsettings.json" `
  --SourceSettings:ConnectionString "$env:SQLSERVER_CONN" `
  --SinkSettings:ConnectionString "$env:COSMOS_CONN" `
  --SinkSettings:DatabaseName "MyDb" `
  --SinkSettings:ContainerName "MyContainer"
```

You can use **Additional Arguments** to override JSON settings at runtime while keeping the JSON mostly static.

---

## 4. PowerShell wrapper + Task Scheduler

### 4.1 PowerShell wrapper function

Create `scripts\invoke_dmt.ps1`:

```powershell
param(
    [Parameter(Mandatory=$true)]
    [ValidateSet("sql_to_cosmos", "cosmos_backup", "csv_to_cosmos")]
    [string]$Pipeline,

    [ValidateSet("dev", "prod")]
    [string]$Environment = "dev"
)

$ErrorActionPreference = "Stop"

$BASE_DIR = Split-Path -Parent $MyInvocation.MyCommand.Path | Split-Path -Parent
$DMT_EXE  = Join-Path $BASE_DIR "win-x64-package\dmt.exe"

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$logDir    = Join-Path $BASE_DIR "logs\$Pipeline"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$logFile   = Join-Path $logDir "$Pipeline-$Environment-$timestamp.log"

function Write-Log {
    param([string]$Message)
    $line = "$(Get-Date -Format o) [$Pipeline/$Environment] $Message"
    $line | Tee-Object -FilePath $logFile -Append
}

Write-Log "Starting pipeline..."

switch ($Pipeline) {
    "sql_to_cosmos" {
        $settingsPath = Join-Path $BASE_DIR "configs\sql_to_cosmos\$Environment.migrationsettings.json"
        $srcConn = $env:SQLSERVER_CONN
        $sinkConn = $env:COSMOS_CONN

        if (-not $srcConn -or -not $sinkConn) {
            throw "Missing required env vars SQLSERVER_CONN or COSMOS_CONN."
        }

        $args = @(
            "--source", "SqlServer",
            "--destination", "Cosmos-nosql",
            "--settings", $settingsPath,
            "--SourceSettings:ConnectionString", $srcConn,
            "--SinkSettings:ConnectionString", $sinkConn
        )
    }
    "cosmos_backup" {
        $settingsPath = Join-Path $BASE_DIR "configs\cosmos_backup\daily_backup.json"
        $sinkPath = Join-Path $BASE_DIR "backups\cosmos"
        New-Item -ItemType Directory -Force -Path $sinkPath | Out-Null

        $args = @(
            "--source", "Cosmos-nosql",
            "--destination", "CSV",
            "--settings", $settingsPath,
            "--SinkSettings:FilePath", (Join-Path $sinkPath "backup-$timestamp.csv")
        )
    }
    "csv_to_cosmos" {
        $settingsPath = Join-Path $BASE_DIR "configs\csv_to_cosmos\import_template.json"
        $srcFile = Join-Path $BASE_DIR "incoming\data.csv"
        $sinkConn = $env:COSMOS_CONN

        $args = @(
            "--source", "CSV",
            "--destination", "Cosmos-nosql",
            "--settings", $settingsPath,
            "--SourceSettings:FilePath", $srcFile,
            "--SinkSettings:ConnectionString", $sinkConn
        )
    }
}

Write-Log "Running: $DMT_EXE $($args -join ' ')"

try {
    & $DMT_EXE @args 2>&1 | Tee-Object -FilePath $logFile -Append
    if ($LASTEXITCODE -ne 0) {
        throw "dmt.exe exited with code $LASTEXITCODE"
    }
    Write-Log "Pipeline completed successfully."
}
catch {
    Write-Log "Pipeline failed: $_"
    exit 1
}
```

**What this gives you:**

* Single entrypoint: `.\invoke_dmt.ps1 -Pipeline sql_to_cosmos -Environment prod`
* Logs per run under `logs/<pipeline>/...`
* Minimal secrets in files (comes from env vars)
* Non-zero exit codes on failure → usable in schedulers / CI

### 4.2 Schedule it (Windows Task Scheduler)

For a **nightly sync**:

1. Open Task Scheduler → *Create Basic Task*
2. Trigger: Daily at, say, 01:00
3. Action: *Start a Program*

   * Program: `powershell.exe`
   * Arguments:

     ```text
     -NoProfile -ExecutionPolicy Bypass -File "C:\Users\person\Projects\azure_data_migration_tool\scripts\invoke_dmt.ps1" -Pipeline sql_to_cosmos -Environment prod
     ```
4. Configure the task to run whether user is logged in or not.

You get a fully automated, logged pipeline without touching the GUI.

---

## 5. Optional: Python orchestrator for multi-step flows

If you want more advanced logic (dependency chains, retries, notifications), use Python (`orchestrator.py`) and call `dmt.exe` via `subprocess`.

```python
import subprocess
import sys
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
DMT_EXE = BASE_DIR / "win-x64-package" / "dmt.exe"
LOG_DIR = BASE_DIR / "logs" / "sql_to_cosmos"
LOG_DIR.mkdir(parents=True, exist_ok=True)

def run_dmt(args: list[str], log_name: str) -> int:
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    log_file = LOG_DIR / f"{log_name}_{ts}.log"
    cmd = [str(DMT_EXE)] + args
    with log_file.open("w", encoding="utf-8") as f:
        f.write(f"{datetime.utcnow().isoformat()} Running: {' '.join(cmd)}\n")
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        for line in proc.stdout:
            sys.stdout.write(line)
            f.write(line)
        proc.wait()
        f.write(f"\nExit code: {proc.returncode}\n")
    return proc.returncode

def nightly_sql_to_cosmos():
    settings = BASE_DIR / "configs" / "sql_to_cosmos" / "prod.migrationsettings.json"
    args = [
        "--source", "SqlServer",
        "--destination", "Cosmos-nosql",
        "--settings", str(settings),
        "--SourceSettings:ConnectionString",  os.environ["SQLSERVER_CONN"],
        "--SinkSettings:ConnectionString",    os.environ["COSMOS_CONN"],
    ]
    code = run_dmt(args, "nightly_sql_to_cosmos")
    if code != 0:
        # TODO: send email/Teams notification, etc.
        raise SystemExit(code)

if __name__ == "__main__":
    nightly_sql_to_cosmos()
```

You can then:

* Call this Python script from Task Scheduler instead of the PowerShell script.
* Add pre-/post-steps: row counts, validation queries, Slack/Teams notifications, etc.

---

## 6. Logging, validation, and retries

A few more suggestions so this doesn’t bite you later:

1. **Row-count checks**

   * Before running `dmt`, run a `SELECT COUNT(*)` against SqlServer and store the expected row count.
   * After import, run equivalent counts against Cosmos (or query `/_count`) and log the delta.

2. **Retry rules**

   * For transient failures (network, throttling), wrap `run_dmt` in a simple retry loop (e.g. up to 3 attempts, exponential backoff).

3. **Health dashboard (later)**

   * You can log each run into a tiny SQLite table (`runs` with `pipeline`, `start_time`, `end_time`, `status`, `rows_migrated`, `log_path`) and build a small Tk/ttkbootstrap or web UI to view status over time.

4. **Dry-run / test mode**

   * Have a “dev” pipeline that writes to a *dev* Cosmos container or a CSV sink, so you can validate mappings/settings before flipping to prod.

---

If you tell me **your primary scenario** (e.g. “SqlServer → Cosmos-nosql nightly from these tables”) I can:

* Draft the corresponding `migrationsettings.json` skeleton, and
* Wire a concrete `invoke_dmt.ps1` section specifically for that job, including example `SourceSettings`/`SinkSettings` names that match the extension schema.
Got it — you want **rock-solid CSV/JSON → Cosmos** using `dmt.exe`, not some toy demo.

Let’s design this in a way you can actually trust in prod.

---

## 1. Overall pattern (what “robust” means here)

For CSV/JSON → Cosmos-NoSQL, “robust” basically means:

* Handles **many files** without manual intervention
* Clear **success/failed** paths per file (no mystery)
* **Logging** per run, per file
* **Id + partitionKey** consistently populated
* **Schema drift / junk data** doesn’t kill the pipeline — it’s isolated
* Easy to run: **single script/command** + schedulable

We’ll build around `dmt.exe` as the engine, and use **PowerShell/Python to orchestrate**:

1. **Drop files** into `incoming/`
2. Orchestrator picks them up one-by-one
3. For each file:

   * Call `dmt.exe` (source = CSV/JSON, sink = Cosmos-nosql)
   * Log output
   * On success → move to `archive/success/`
   * On failure → move to `archive/failed/` + keep logs

Optionally, you can add a **pre-normalization step** (Python) for heavy validation, but let’s first get the direct path right.

---

## 2. Directory & config layout

Inside `azure_data_migration_tool`:

```text
azure_data_migration_tool/
├── win-x64-package/
│   ├── dmt.exe
│   └── Extensions/
├── data/
│   ├── incoming/
│   │   ├── csv/
│   │   └── json/
│   ├── archive/
│   │   ├── csv/
│   │   │   ├── success/
│   │   │   └── failed/
│   │   └── json/
│   │       ├── success/
│   │       └── failed/
├── configs/
│   ├── csv_to_cosmos.migrationsettings.json
│   └── json_to_cosmos.migrationsettings.json
├── logs/
│   ├── csv_to_cosmos/
│   └── json_to_cosmos/
└── scripts/
    ├── invoke_dmt.ps1
    └── run_csv_json_pipeline.py  (optional)
```

You already saw from `dmt.exe --help`:

* `--source` = `CSV` or `JSON`
* `--destination` = `Cosmos-nosql`
* `--settings` = migration settings file
* Extra extension settings via `--SourceSettings:*` / `--SinkSettings:*`

👉 **Important honesty note:**
I don’t know the *exact* internal setting names the `Cosmos-nosql` and `CSV/JSON` extensions expect (e.g. `ConnectionString`, `DatabaseName`, etc.) — those come from the tool itself.
Use:

```powershell
win-x64-package\dmt.exe settings
```

(plus whatever flags it requires) to inspect the extension’s schema, then plug the real property names into the JSON and CLI overrides. I’ll use **placeholders** below; you’ll replace them with the actual names from `settings`.

---

## 3. Core design: ID & partition key strategy

Before touching any script, decide:

* **Which column/field → `id`**

  * E.g. `OrderId`, `Sku`, `CustomerId`, or a composite string like `"{StoreId}-{OrderId}"`.
* **Which column/field → Partition key** (e.g. `/storeId`, `/tenantId`, `/country`)

For **CSV**:

* If file has columns `id` and `partitionKey`, great — map them directly in settings.
* If not, you have two options:

  1. Have `dmt` extension support mapping columns → `id` / partition key (if supported in settings)
  2. Pre-process the CSV with a small Python script that **adds `id` & `partitionKey` columns**, then feed that into `dmt`.

For **JSON**:

* Prefer **JSON Lines** / NDJSON format (1 JSON object per line).
* Ensure each object has an `id` and partition key property before sending to `dmt`.

---

## 4. PowerShell orchestrator (direct CSV/JSON → Cosmos)

Here’s a **single PowerShell script** that:

* Loops through CSV or JSON files in `data/incoming/...`
* Runs `dmt.exe` per file
* Logs output
* Moves files to `archive/success` or `archive/failed`

`scripts\invoke_dmt.ps1`:

```powershell
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("csv", "json")]
    [string]$Mode,       # 'csv' or 'json'

    [ValidateSet("dev", "prod")]
    [string]$Environment = "dev"
)

$ErrorActionPreference = "Stop"

# Resolve paths relative to this script
$SCRIPT_DIR = Split-Path -Parent $MyInvocation.MyCommand.Path
$BASE_DIR   = Split-Path -Parent $SCRIPT_DIR

$DMT_EXE    = Join-Path $BASE_DIR "win-x64-package\dmt.exe"

$DATA_IN    = Join-Path $BASE_DIR "data\incoming\$Mode"
$ARCHIVE_OK = Join-Path $BASE_DIR "data\archive\$Mode\success"
$ARCHIVE_BAD= Join-Path $BASE_DIR "data\archive\$Mode\failed"

$LOG_DIR    = Join-Path $BASE_DIR "logs\${Mode}_to_cosmos"
New-Item -ItemType Directory -Force -Path $DATA_IN, $ARCHIVE_OK, $ARCHIVE_BAD, $LOG_DIR | Out-Null

# Environment variables for secrets (set these in system/user env)
$cosmosConn = $env:COSMOS_CONN
if (-not $cosmosConn) { throw "Env var COSMOS_CONN is not set." }

# Pick settings file
switch ($Mode) {
    "csv"  { $settingsPath = Join-Path $BASE_DIR "configs\csv_to_cosmos.migrationsettings.json" }
    "json" { $settingsPath = Join-Path $BASE_DIR "configs\json_to_cosmos.migrationsettings.json" }
}

function Write-Log {
    param([string]$Message)
    $timestamp = Get-Date -Format o
    $line = "$timestamp [$Mode/$Environment] $Message"
    Write-Host $line
    $line | Add-Content -Path (Join-Path $LOG_DIR "pipeline-$((Get-Date -Format yyyyMMdd)).log")
}

Write-Log "Starting $Mode to Cosmos pipeline..."

$files = Get-ChildItem -Path $DATA_IN -File -Include *.csv, *.json
if (-not $files) {
    Write-Log "No files found in $DATA_IN. Nothing to do."
    exit 0
}

foreach ($file in $files) {
    $runId     = Get-Date -Format "yyyyMMdd_HHmmss_fff"
    $logFile   = Join-Path $LOG_DIR "$Mode-$runId-$($file.Name).log"

    Write-Log "Processing file: $($file.FullName)"

    # Build dmt args – REPLACE setting names with actual ones from 'dmt.exe settings'
    $commonArgs = @(
        "--destination", "Cosmos-nosql",
        "--settings", $settingsPath,
        "--SinkSettings:ConnectionString", $cosmosConn
        # Add more SinkSettings:* here as needed:
        # "--SinkSettings:DatabaseName", "MyDatabase",
        # "--SinkSettings:ContainerName", "MyContainer"
    )

    if ($Mode -eq "csv") {
        $args = @(
            "--source", "CSV",
            "--SourceSettings:FilePath", $file.FullName
            # e.g. delimiter, header settings, etc – based on CSV extension schema
        ) + $commonArgs
    }
    else {
        $args = @(
            "--source", "JSON",
            "--SourceSettings:FilePath", $file.FullName
            # e.g. JsonMode=Lines, encoding, etc – based on JSON extension schema
        ) + $commonArgs
    }

    Write-Log "Running: $DMT_EXE $($args -join ' ')"

    # Run dmt.exe and pipe output to per-file log
    & $DMT_EXE @args 2>&1 | Tee-Object -FilePath $logFile

    if ($LASTEXITCODE -eq 0) {
        Write-Log "SUCCESS: $($file.Name)"
        Move-Item -Path $file.FullName -Destination $ARCHIVE_OK -Force
    }
    else {
        Write-Log "FAILED (code=$LASTEXITCODE): $($file.Name) – see $logFile"
        Move-Item -Path $file.FullName -Destination $ARCHIVE_BAD -Force
    }
}

Write-Log "Pipeline run complete."
```

Usage:

```powershell
# CSV -> Cosmos (dev)
.\scripts\invoke_dmt.ps1 -Mode csv -Environment dev

# JSON -> Cosmos (prod)
.\scripts\invoke_dmt.ps1 -Mode json -Environment prod
```

Now you have:

* **Batch processing of all files** in the drop folder
* **Per-run & per-file logs**
* Clear **success/failed archive** directories
* Ready to hook into **Task Scheduler**

---

## 5. Scheduling (CSV and JSON) in Task Scheduler

Create two scheduled tasks, e.g.:

### Task 1 – “CSV to Cosmos (every 15 minutes)”

* Program: `powershell.exe`
* Arguments:

```text
-NoProfile -ExecutionPolicy Bypass -File "C:\Users\person\Projects\azure_data_migration_tool\scripts\invoke_dmt.ps1" -Mode csv -Environment prod
```

Trigger: every 15 minutes.

### Task 2 – “JSON to Cosmos (nightly)”

* Same program
* Arguments:

```text
-NoProfile -ExecutionPolicy Bypass -File "C:\Users\person\Projects\azure_data_migration_tool\scripts\invoke_dmt.ps1" -Mode json -Environment prod
```

Trigger: daily at, say, 01:00.

---

## 6. Optional: pre-normalization step (for really dirty data)

If your CSV/JSON is **messy** or doesn’t expose `id` / partition key cleanly, a robust pattern is:

1. **Incoming dir** → Python normalizer → **normalized dir** (strict schema, id, pk added)
2. Normalized JSON Lines → `dmt.exe` (JSON → Cosmos)

Pseudo-flow:

```text
incoming\csv\*.csv
    ↓ normalize_csv.py
normalized\orders\*.ndjson  (each line is a JSON doc with id + partitionKey)
    ↓ dmt.exe (source=JSON, sink=Cosmos-nosql)
Cosmos DB
```

The normalizer:

* Validates required columns
* Fills in missing IDs
* Drops clearly invalid rows to `bad_rows.csv`
* Emits clean NDJSON that `dmt` can ingest reliably

If you want, next step we can write a **Python normalizer script** that:

* Uses `Path(__file__).resolve().parent` (PyInstaller-friendly)
* Processes CSV + “loose” JSON into strict NDJSON
* Integrates with the same `incoming/normalized/archive` structure

---

If you tell me:

* **Example CSV headers** (or JSON shape)
* Your **desired `id` and partition key** logic

I can give you a concrete **`csv/json → normalized NDJSON → Cosmos`** pipeline:
settings JSON skeleton + a real Python normalizer + the updated PowerShell wrapper wired to JSON source.
Perfect, that’s exactly the kind of shape we want for a **robust validator + stats logger + Cosmos uploader**.

You already have the right idea:

* `ingest_runs` = per-run stats
* `ingest_rows` (or similar) = per-row tracking with a uniqueness constraint
* Cosmos config with `partition_key_path: "/id"` and `allow_partial_upload: true`

Below is how I’d wire this into a **solid CSV/JSON → Cosmos pipeline** with validation + stats + dedupe.

---

## 1. Cosmos document shape (aligned with your uniqueness)

In Cosmos you want each document to reflect your unique constraint:

```text
UniqueConstraint("vendor_id", "sku", "postCode")
```

So use a **composite id**:

```python
cosmos_id = f"{vendor_id}|{sku}|{postCode}"
```

Then your Cosmos doc can look like:

```json
{
  "id": "V123|SKU001|2000",
  "vendor_id": "V123",
  "sku": "SKU001",
  "postCode": "0200",
  "price": 19.99,
  "run_id": 42
}
```

Partition key is `/id` (as per your config), which is fine for moderate volumes. If you later need scaling, you’ll probably want `/vendor_id` or similar, but don’t over-optimize now.

---

## 2. DB models for stats + dedupe (SQLite/Postgres, doesn’t matter)

These are basically what you pasted, with a couple of useful extras:

```python
from __future__ import annotations
from datetime import datetime
from typing import Optional

from sqlalchemy import (
    String,
    Integer,
    Float,
    DateTime,
    ForeignKey,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class IngestRun(Base):
    __tablename__ = "ingest_runs"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    profile_name: Mapped[str] = mapped_column(String(64))
    vendor_id: Mapped[str] = mapped_column(String(128))
    source_name: Mapped[str] = mapped_column(String(256))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    rows_total: Mapped[int] = mapped_column(Integer, default=0)
    rows_valid: Mapped[int] = mapped_column(Integer, default=0)
    rows_invalid: Mapped[int] = mapped_column(Integer, default=0)
    duplicates: Mapped[int] = mapped_column(Integer, default=0)
    unique_skus: Mapped[int] = mapped_column(Integer, default=0)
    rows_uploaded: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(String(32), default="pending")  # pending/success/failed
    error_message: Mapped[Optional[str]] = mapped_column(String(1024), nullable=True)

    rows: Mapped[list[IngestRow]] = relationship(
        "IngestRow", back_populates="run", cascade="all, delete-orphan"
    )


class IngestRow(Base):
    __tablename__ = "ingest_rows"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    vendor_id: Mapped[str] = mapped_column(String(128), index=True)
    sku: Mapped[str] = mapped_column(String(128), index=True)
    postCode: Mapped[str] = mapped_column(String(16), index=True)
    price: Mapped[float] = mapped_column(Float)

    run_id: Mapped[int | None] = mapped_column(
        ForeignKey("ingest_runs.id", ondelete="SET NULL"), nullable=True
    )

    run: Mapped[IngestRun | None] = relationship("IngestRun", back_populates="rows")

    __table_args__ = (
        UniqueConstraint("vendor_id", "sku", "postCode", name="uq_vendor_sku_postcode"),
    )
```

This gives you:

* Per-run state + metrics
* Per-row traceability
* Enforced uniqueness on `(vendor_id, sku, postCode)`

---

## 3. Pipeline flow: CSV/JSON → validate → stats → NDJSON → dmt.exe → Cosmos

### Step-by-step

1. User drops CSV/JSON into `data/incoming/`.
2. Python script:

   * Creates an `IngestRun`.
   * Validates each row.
   * Writes **valid, deduped** docs to an NDJSON file.
   * Logs the metrics into `ingest_runs` + `ingest_rows`.
3. Script calls `dmt.exe` with:

   * `--source JSON`
   * `--destination Cosmos-nosql`
   * `--settings json_to_cosmos.migrationsettings.json`
   * `--SourceSettings:FilePath <path to ndjson>`
4. On success: mark run as `success` and archive file; on failure: mark run `failed` and archive to `failed`.

---

## 4. Example Python validator + NDJSON writer + dmt call

This is the **core** you care about.

```python
import csv
import json
import os
import subprocess
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models import Base, IngestRun, IngestRow  # from section above


BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "ingest_stats.db"
DMT_EXE = BASE_DIR / "win-x64-package" / "dmt.exe"
CONFIG_JSON_TO_COSMOS = BASE_DIR / "configs" / "json_to_cosmos.migrationsettings.json"

DATA_INCOMING = BASE_DIR / "data" / "incoming" / "csv"
DATA_ARCHIVE_OK = BASE_DIR / "data" / "archive" / "csv" / "success"
DATA_ARCHIVE_BAD = BASE_DIR / "data" / "archive" / "csv" / "failed"
DATA_SANITIZED = BASE_DIR / "data" / "sanitized"

for p in (DATA_INCOMING, DATA_ARCHIVE_OK, DATA_ARCHIVE_BAD, DATA_SANITIZED):
    p.mkdir(parents=True, exist_ok=True)

engine = create_engine(f"sqlite:///{DB_PATH}", future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
Base.metadata.create_all(engine)


def normalize_postcode(raw: str) -> str:
    if raw is None:
        return ""
    s = str(raw).strip()
    # Example: ensure 4 digits, left-padded with zeros if needed
    if s.isdigit() and len(s) <= 4:
        return s.zfill(4)
    return s  # fallback; you can enforce stricter validation


def validate_and_prepare_docs(
    vendor_id: str,
    rows: Iterable[dict],
) -> tuple[list[dict], dict]:
    """
    Returns:
      docs: list of valid Cosmos docs
      stats: dict with rows_total, rows_valid, rows_invalid, duplicates, unique_skus
    """
    seen_keys: set[tuple[str, str, str]] = set()
    skus: set[str] = set()

    docs: list[dict] = []
    rows_total = rows_valid = rows_invalid = duplicates = 0

    for row in rows:
        rows_total += 1

        sku = (row.get("sku") or "").strip()
        post_code_raw = row.get("postCode") or row.get("postcode") or row.get("PostCode")
        price_raw = row.get("price")

        if not sku:
            rows_invalid += 1
            continue

        post_code = normalize_postcode(post_code_raw)
        try:
            price = float(price_raw)
        except (TypeError, ValueError):
            rows_invalid += 1
            continue

        key = (vendor_id, sku, post_code)
        if key in seen_keys:
            duplicates += 1
            rows_invalid += 1
            continue
        seen_keys.add(key)
        skus.add(sku)

        cosmos_id = f"{vendor_id}|{sku}|{post_code}"

        doc = {
            "id": cosmos_id,
            "vendor_id": vendor_id,
            "sku": sku,
            "postCode": post_code,
            "price": price,
        }
        docs.append(doc)
        rows_valid += 1

    stats = {
        "rows_total": rows_total,
        "rows_valid": rows_valid,
        "rows_invalid": rows_invalid,
        "duplicates": duplicates,
        "unique_skus": len(skus),
    }
    return docs, stats


def read_csv_rows(path: Path) -> Iterable[dict]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row


def write_ndjson(path: Path, docs: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for doc in docs:
            f.write(json.dumps(doc, ensure_ascii=False) + "\n")


def run_dmt_json_to_cosmos(ndjson_path: Path) -> int:
    cosmos_conn = os.environ.get("COSMOS_CONN")
    if not cosmos_conn:
        raise RuntimeError("COSMOS_CONN env var is not set.")

    args = [
        str(DMT_EXE),
        "--source", "JSON",
        "--destination", "Cosmos-nosql",
        "--settings", str(CONFIG_JSON_TO_COSMOS),
        "--SourceSettings:FilePath", str(ndjson_path),
        "--SinkSettings:ConnectionString", cosmos_conn,
        # add these if your extension uses them:
        # "--SinkSettings:DatabaseName", "soh",
        # "--SinkSettings:ContainerName", "dropshipPricingTest",
    ]

    proc = subprocess.run(args, capture_output=True, text=True)
    # You should log proc.stdout / proc.stderr somewhere
    return proc.returncode


def process_csv_file(path: Path, profile_name: str, vendor_id: str, source_name: str) -> None:
    session = SessionLocal()
    run = IngestRun(
        profile_name=profile_name,
        vendor_id=vendor_id,
        source_name=source_name,
        created_at=datetime.now(timezone.utc),
    )
    session.add(run)
    session.flush()  # assign run.id

    try:
        rows = list(read_csv_rows(path))
        docs, stats = validate_and_prepare_docs(vendor_id, rows)

        run.rows_total = stats["rows_total"]
        run.rows_valid = stats["rows_valid"]
        run.rows_invalid = stats["rows_invalid"]
        run.duplicates = stats["duplicates"]
        run.unique_skus = stats["unique_skus"]

        # Persist valid rows in DB (optional, but useful for audit)
        for doc in docs:
            row = IngestRow(
                vendor_id=doc["vendor_id"],
                sku=doc["sku"],
                postCode=doc["postCode"],
                price=doc["price"],
                run_id=run.id,
            )
            session.add(row)

        ndjson_path = DATA_SANITIZED / f"run_{run.id}.ndjson"
        write_ndjson(ndjson_path, docs)

        session.commit()

        # Call dmt.exe
        code = run_dmt_json_to_cosmos(ndjson_path)
        if code == 0:
            run.status = "success"
            run.rows_uploaded = run.rows_valid
            session.commit()
            path.rename(DATA_ARCHIVE_OK / path.name)
        else:
            run.status = "failed"
            run.error_message = f"dmt.exe exit code {code}"
            session.commit()
            path.rename(DATA_ARCHIVE_BAD / path.name)

    except Exception as exc:
        session.rollback()
        run.status = "failed"
        run.error_message = str(exc)[:1000]
        session.add(run)
        session.commit()
        path.rename(DATA_ARCHIVE_BAD / path.name)
    finally:
        session.close()


def main():
    for csv_file in DATA_INCOMING.glob("*.csv"):
        process_csv_file(
            csv_file,
            profile_name="dropship_pricing",
            vendor_id="VENDOR123",  # you can infer this from filename if needed
            source_name=str(csv_file),
        )


if __name__ == "__main__":
    main()
```

Key points:

* **Validation & stats** calculated *before* hitting Cosmos.
* `id` and partition key `/id` are consistent.
* Each run recorded in `ingest_runs`, rows in `ingest_rows`.
* Files are archived into success/failed dirs.
* dmt is called with JSON source; you can reuse this for JSON input directly too.

---

## 5. Cosmos sink config (dmt settings)

In your `json_to_cosmos.migrationsettings.json` (structure will depend on the tool’s schema, this is conceptual):

```jsonc
{
  "source": {
    "name": "JSON",
    "settings": {
      "file_path": "DUMMY.json",        // overridden via --SourceSettings:FilePath
      "mode": "Lines"                   // JSON Lines / NDJSON
    }
  },
  "sink": {
    "name": "Cosmos-nosql",
    "settings": {
      "connection_string": "AccountEndpoint=https://*****=",
      "database_name": "soh",
      "container_name": "dropshipPricingTest",
      "partition_key_path": "/id",
      "allow_partial_upload": true,
      "log_level": "WARNING"
    }
  }
}
```

Then at runtime, your CLI overrides the `file_path` and `connection_string`:

```text
--SourceSettings:FilePath <ndjson path>
--SinkSettings:ConnectionString %COSMOS_CONN%
```

Use `dmt.exe settings` to confirm the **real** property names.

---

## 6. Automation

Hook this into the PowerShell wrapper or Task Scheduler:

* **Task Scheduler → Python script** (above)
* Or **Task Scheduler → PowerShell** that just runs `python run_csv_json_pipeline.py`.

Example Task Scheduler action:

```text
Program:  C:\Python\python.exe
Args:     "C:\Users\person\Projects\azure_data_migration_tool\scripts\run_csv_json_pipeline.py"
```

---

If you want, next step I can:

* Add **JSON input support** (same validator, different reader)
* Add a **profile table** so you can store per-vendor configs (partition logic, postcode rules, etc.)
* Or wire this **directly into your existing freight/pricing tool**, reusing the same models and validation logic.
Nice, this is a good place to level it up properly: **JSON input + per-vendor profiles** driving ID, partition logic, and postcode rules.

Below are **two files**:

1. `models.py` – adds a `VendorProfile` table + updates run/row models
2. `pipeline.py` – supports **CSV + JSON**, uses vendor profiles, writes NDJSON, then calls `dmt.exe` (JSON → Cosmos)

You can drop these into your `azure_data_migration_tool` project and wire them into your existing structure.

---

## 1. `models.py` – with `VendorProfile` + updated run/row models

```python
# models.py
from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import (
    String,
    Integer,
    Float,
    DateTime,
    ForeignKey,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class VendorProfile(Base):
    __tablename__ = "vendor_profiles"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Your logical vendor identifier: e.g. "VS", "SUPPLIER123"
    vendor_id: Mapped[str] = mapped_column(String(128), unique=True, index=True)

    # Friendly profile name, e.g. "Virtualstock Dropship Pricing"
    profile_name: Mapped[str] = mapped_column(String(128))

    # Cosmos-related knobs – these drive how we call dmt.exe
    cosmos_database_name: Mapped[str] = mapped_column(String(128), default="soh")
    cosmos_container_name: Mapped[str] = mapped_column(
        String(128), default="dropshipPricingTest"
    )
    cosmos_partition_key_path: Mapped[str] = mapped_column(
        String(256), default="/id"
    )

    # ID logic – simple template using Python format fields:
    # e.g. "{vendor_id}|{sku}|{postCode}" or "{sku}|{postCode}"
    id_pattern: Mapped[str] = mapped_column(
        String(256), default="{vendor_id}|{sku}|{postCode}"
    )

    # Postcode rules
    postcode_length: Mapped[int] = mapped_column(Integer, default=4)
    postcode_zero_pad: Mapped[bool] = mapped_column(default=True)
    postcode_uppercase: Mapped[bool] = mapped_column(default=False)

    # Misc flags
    is_active: Mapped[bool] = mapped_column(default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class IngestRun(Base):
    __tablename__ = "ingest_runs"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    profile_name: Mapped[str] = mapped_column(String(64))
    vendor_id: Mapped[str] = mapped_column(String(128))
    source_name: Mapped[str] = mapped_column(String(256))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    rows_total: Mapped[int] = mapped_column(Integer, default=0)
    rows_valid: Mapped[int] = mapped_column(Integer, default=0)
    rows_invalid: Mapped[int] = mapped_column(Integer, default=0)
    duplicates: Mapped[int] = mapped_column(Integer, default=0)
    unique_skus: Mapped[int] = mapped_column(Integer, default=0)
    rows_uploaded: Mapped[int] = mapped_column(Integer, default=0)

    status: Mapped[str] = mapped_column(String(32), default="pending")  # pending/success/failed
    error_message: Mapped[Optional[str]] = mapped_column(String(1024), nullable=True)

    rows: Mapped[list[IngestRow]] = relationship(
        "IngestRow", back_populates="run", cascade="all, delete-orphan"
    )


class IngestRow(Base):
    __tablename__ = "ingest_rows"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    vendor_id: Mapped[str] = mapped_column(String(128), index=True)
    sku: Mapped[str] = mapped_column(String(128), index=True)
    postCode: Mapped[str] = mapped_column(String(16), index=True)
    price: Mapped[float] = mapped_column(Float)

    run_id: Mapped[int | None] = mapped_column(
        ForeignKey("ingest_runs.id", ondelete="SET NULL"), nullable=True
    )
    run: Mapped[IngestRun | None] = relationship("IngestRun", back_populates="rows")

    __table_args__ = (
        UniqueConstraint("vendor_id", "sku", "postCode", name="uq_vendor_sku_postcode"),
    )
```

That `VendorProfile` table is your control centre for:

* per-vendor **ID pattern**
* per-vendor **postcode rules**
* per-vendor **Cosmos container / DB / partition path** (even if right now you’re fixed to `/id`)

---

## 2. `pipeline.py` – CSV + JSON input, vendor profiles, NDJSON + dmt.exe

This is a consolidated pipeline script:

* Handles **CSV and JSON inputs**
* Infers vendor from file name (you can adjust)
* Loads or auto-creates a `VendorProfile`
* Validates, logs stats, writes NDJSON
* Calls `dmt.exe` (JSON → Cosmos) using vendor’s profile

```python
# pipeline.py
from __future__ import annotations

import csv
import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Tuple

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from models import Base, VendorProfile, IngestRun, IngestRow


# --- Paths / basic setup ------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent  # adjust if needed

DB_PATH = BASE_DIR / "ingest_stats.db"
DMT_EXE = BASE_DIR / "win-x64-package" / "dmt.exe"
CONFIG_JSON_TO_COSMOS = BASE_DIR / "configs" / "json_to_cosmos.migrationsettings.json"

DATA_IN_CSV = BASE_DIR / "data" / "incoming" / "csv"
DATA_IN_JSON = BASE_DIR / "data" / "incoming" / "json"
DATA_ARCHIVE_CSV_OK = BASE_DIR / "data" / "archive" / "csv" / "success"
DATA_ARCHIVE_CSV_BAD = BASE_DIR / "data" / "archive" / "csv" / "failed"
DATA_ARCHIVE_JSON_OK = BASE_DIR / "data" / "archive" / "json" / "success"
DATA_ARCHIVE_JSON_BAD = BASE_DIR / "data" / "archive" / "json" / "failed"
DATA_SANITIZED = BASE_DIR / "data" / "sanitized"

for p in (
    DATA_IN_CSV,
    DATA_IN_JSON,
    DATA_ARCHIVE_CSV_OK,
    DATA_ARCHIVE_CSV_BAD,
    DATA_ARCHIVE_JSON_OK,
    DATA_ARCHIVE_JSON_BAD,
    DATA_SANITIZED,
):
    p.mkdir(parents=True, exist_ok=True)

engine = create_engine(f"sqlite:///{DB_PATH}", future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
Base.metadata.create_all(engine)


# --- Helpers ------------------------------------------------------------------


def infer_vendor_id_from_filename(path: Path) -> str:
    """
    Simple heuristic: 'VENDOR123_pricing_20250101.csv' -> 'VENDOR123'
    Adjust to your naming standards if needed.
    """
    stem = path.stem  # e.g. "VENDOR123_pricing_20250101"
    return stem.split("_")[0]


def get_or_create_profile(session, vendor_id: str) -> VendorProfile:
    stmt = select(VendorProfile).where(VendorProfile.vendor_id == vendor_id)
    profile = session.execute(stmt).scalar_one_or_none()
    if profile:
        return profile

    # Default profile – you can tweak this once and forget about it
    profile = VendorProfile(
        vendor_id=vendor_id,
        profile_name=f"default_profile_{vendor_id}",
        cosmos_database_name="soh",
        cosmos_container_name="dropshipPricingTest",
        cosmos_partition_key_path="/id",
        id_pattern="{vendor_id}|{sku}|{postCode}",
        postcode_length=4,
        postcode_zero_pad=True,
        postcode_uppercase=False,
        is_active=True,
    )
    session.add(profile)
    session.commit()
    session.refresh(profile)
    return profile


def normalize_postcode(raw: str | None, profile: VendorProfile) -> str:
    if raw is None:
        return ""

    s = str(raw).strip()
    if profile.postcode_uppercase:
        s = s.upper()

    # If numeric + length rule → pad with zeros on the left
    if s.isdigit() and profile.postcode_zero_pad and profile.postcode_length > 0:
        if len(s) <= profile.postcode_length:
            s = s.zfill(profile.postcode_length)

    return s


def build_cosmos_id(
    profile: VendorProfile,
    vendor_id: str,
    sku: str,
    post_code: str,
) -> str:
    # Safe formatting; if pattern is broken, fallback to basic
    try:
        return profile.id_pattern.format(
            vendor_id=vendor_id,
            sku=sku,
            postCode=post_code,
            postcode=post_code,
        )
    except Exception:
        return f"{vendor_id}|{sku}|{post_code}"


def validate_and_prepare_docs(
    vendor_id: str,
    profile: VendorProfile,
    rows: Iterable[dict],
) -> Tuple[list[dict], dict]:
    """
    Shared validator for CSV- or JSON-derived rows.

    Returns:
      docs: list of valid Cosmos docs
      stats: dict with rows_total, rows_valid, rows_invalid, duplicates, unique_skus
    """
    seen_keys: set[tuple[str, str, str]] = set()
    skus: set[str] = set()
    docs: list[dict] = []

    rows_total = rows_valid = rows_invalid = duplicates = 0

    for row in rows:
        rows_total += 1

        sku = (row.get("sku") or "").strip()
        post_code_raw = (
            row.get("postCode")
            or row.get("postcode")
            or row.get("PostCode")
            or row.get("POSTCODE")
        )
        price_raw = row.get("price")

        if not sku:
            rows_invalid += 1
            continue

        post_code = normalize_postcode(post_code_raw, profile)

        try:
            price = float(price_raw)
        except (TypeError, ValueError):
            rows_invalid += 1
            continue

        key = (vendor_id, sku, post_code)
        if key in seen_keys:
            duplicates += 1
            rows_invalid += 1
            continue
        seen_keys.add(key)
        skus.add(sku)

        cosmos_id = build_cosmos_id(profile, vendor_id, sku, post_code)

        doc = {
            "id": cosmos_id,
            "vendor_id": vendor_id,
            "sku": sku,
            "postCode": post_code,
            "price": price,
            # partition_key is /id in your config, so we don't need a separate field
        }
        docs.append(doc)
        rows_valid += 1

    stats = {
        "rows_total": rows_total,
        "rows_valid": rows_valid,
        "rows_invalid": rows_invalid,
        "duplicates": duplicates,
        "unique_skus": len(skus),
    }
    return docs, stats


def read_csv_rows(path: Path) -> Iterable[dict]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row


def read_json_rows(path: Path) -> Iterable[dict]:
    """
    Supports:
    - JSON array: [ {..}, {..} ]
    - NDJSON (JSONL): one JSON object per line
    """
    with path.open("r", encoding="utf-8") as f:
        # Peek first non-space char
        content = f.read()
        stripped = content.lstrip()
        if stripped.startswith("["):
            # Array of objects
            data = json.loads(content)
            for obj in data:
                if isinstance(obj, dict):
                    yield obj
        else:
            # Assume NDJSON
            for line in content.splitlines():
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                if isinstance(obj, dict):
                    yield obj


def write_ndjson(path: Path, docs: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for doc in docs:
            f.write(json.dumps(doc, ensure_ascii=False) + "\n")


def run_dmt_json_to_cosmos(ndjson_path: Path, profile: VendorProfile) -> int:
    cosmos_conn = os.environ.get("COSMOS_CONN")
    if not cosmos_conn:
        raise RuntimeError("COSMOS_CONN env var is not set.")

    args = [
        str(DMT_EXE),
        "--source",
        "JSON",
        "--destination",
        "Cosmos-nosql",
        "--settings",
        str(CONFIG_JSON_TO_COSMOS),
        "--SourceSettings:FilePath",
        str(ndjson_path),
        "--SinkSettings:ConnectionString",
        cosmos_conn,
        # NOTE: replace these setting names with the actual ones from `dmt.exe settings`
        # if your Cosmos extension exposes database/container via settings:
        # "--SinkSettings:DatabaseName", profile.cosmos_database_name,
        # "--SinkSettings:ContainerName", profile.cosmos_container_name,
    ]

    proc = subprocess.run(args, capture_output=True, text=True)
    # You can log proc.stdout / proc.stderr into your own log file if needed
    return proc.returncode


# --- Core processing ----------------------------------------------------------


def process_file(
    path: Path,
    input_type: str,  # "csv" or "json"
) -> None:
    """
    One file → one IngestRun → stats + NDJSON → dmt.exe → archive.
    """
    session = SessionLocal()
    vendor_id = infer_vendor_id_from_filename(path)
    profile = get_or_create_profile(session, vendor_id)

    run = IngestRun(
        profile_name=profile.profile_name,
        vendor_id=vendor_id,
        source_name=str(path),
        created_at=datetime.now(timezone.utc),
    )
    session.add(run)
    session.flush()  # assigns run.id

    try:
        if input_type == "csv":
            rows = list(read_csv_rows(path))
        elif input_type == "json":
            rows = list(read_json_rows(path))
        else:
            raise ValueError(f"Unsupported input_type: {input_type}")

        docs, stats = validate_and_prepare_docs(vendor_id, profile, rows)

        run.rows_total = stats["rows_total"]
        run.rows_valid = stats["rows_valid"]
        run.rows_invalid = stats["rows_invalid"]
        run.duplicates = stats["duplicates"]
        run.unique_skus = stats["unique_skus"]

        # Optional: persist valid rows for audit
        for doc in docs:
            row = IngestRow(
                vendor_id=doc["vendor_id"],
                sku=doc["sku"],
                postCode=doc["postCode"],
                price=doc["price"],
                run_id=run.id,
            )
            session.add(row)

        ndjson_path = DATA_SANITIZED / f"{input_type}_run_{run.id}.ndjson"
        write_ndjson(ndjson_path, docs)

        session.commit()

        # Send to Cosmos via dmt.exe
        code = run_dmt_json_to_cosmos(ndjson_path, profile)
        if code == 0:
            run.status = "success"
            run.rows_uploaded = run.rows_valid
            session.commit()
            if input_type == "csv":
                path.rename(DATA_ARCHIVE_CSV_OK / path.name)
            else:
                path.rename(DATA_ARCHIVE_JSON_OK / path.name)
        else:
            run.status = "failed"
            run.error_message = f"dmt.exe exit code {code}"
            session.commit()
            if input_type == "csv":
                path.rename(DATA_ARCHIVE_CSV_BAD / path.name)
            else:
                path.rename(DATA_ARCHIVE_JSON_BAD / path.name)

    except Exception as exc:
        session.rollback()
        run.status = "failed"
        run.error_message = str(exc)[:1000]
        session.add(run)
        session.commit()
        if input_type == "csv":
            path.rename(DATA_ARCHIVE_CSV_BAD / path.name)
        else:
            path.rename(DATA_ARCHIVE_JSON_BAD / path.name)
    finally:
        session.close()


def main() -> None:
    # Process CSV files
    for csv_file in sorted(DATA_IN_CSV.glob("*.csv")):
        process_file(csv_file, "csv")

    # Process JSON files
    for json_file in sorted(DATA_IN_JSON.glob("*.json")):
        process_file(json_file, "json")


if __name__ == "__main__":
    main()
```

---

### How this fits your “robust csv/json → Cosmos” requirement

* **Vendor-aware**: `VendorProfile` drives:

  * `id` composition (`id_pattern`)
  * postcode normalization (length, padding, casing)
  * per-vendor Cosmos container/DB (when you wire the right dmt settings)
* **CSV & JSON**: same validation pipeline, two readers:

  * CSV via `csv.DictReader`
  * JSON: auto-detects `[...]` vs NDJSON
* **Stats & logging**:

  * `ingest_runs` has totals, valids, invalids, duplicates, unique SKUs, status, error message.
  * `ingest_rows` has deduped rows tied to a run.
* **Safe Cosmos IDs**: `id = id_pattern.format(...)`, defaulting to `vendor_id|sku|postCode`.
* **Archive & retry**:

  * Success/failed directories for both CSV and JSON.

---

If you want next iteration, we can add:

* explicit **per-vendor field mapping** (e.g. JSON using `itemSku` vs `sku`) in `VendorProfile`
* a quick **Tk/CLI dashboard** to see `ingest_runs` stats per day/vendor
* or **retry queues** that re-run failed NDJSON batches without re-reading original files.

Alright, let’s bolt a **Tk dashboard + retry queue** on top of what you already have.

### What this gives you

* **Tk dashboard** over `ingest_runs`:

  * Filter by status + vendor
  * See per-run stats (`rows_total`, `rows_valid`, `rows_invalid`, `duplicates`, etc.)
  * Inspect error messages

* **Retry queue**:

  * **Retry selected runs** (in background, sequentially)
  * **Retry all failed runs**
  * Reuses/creates NDJSON and calls `dmt.exe` again.
  * Uses `VendorProfile` for ID + postcode rules, so behaviour matches the original ingest.

Below are:

1. Small **model extension** for retry metadata
2. A full **Tk dashboard + retry worker** script: `ingest_dashboard.py`

You can keep your `pipeline.py` as-is; this just reads the same DB and `data/sanitized` directory.

---

## 1. Update `IngestRun` to track retries

In `models.py`, extend `IngestRun` with `retry_count` and `max_retries` (you can drop/recreate DB while iterating; otherwise migrate):

```python
class IngestRun(Base):
    __tablename__ = "ingest_runs"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    profile_name: Mapped[str] = mapped_column(String(64))
    vendor_id: Mapped[str] = mapped_column(String(128))
    source_name: Mapped[str] = mapped_column(String(256))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    rows_total: Mapped[int] = mapped_column(Integer, default=0)
    rows_valid: Mapped[int] = mapped_column(Integer, default=0)
    rows_invalid: Mapped[int] = mapped_column(Integer, default=0)
    duplicates: Mapped[int] = mapped_column(Integer, default=0)
    unique_skus: Mapped[int] = mapped_column(Integer, default=0)
    rows_uploaded: Mapped[int] = mapped_column(Integer, default=0)

    status: Mapped[str] = mapped_column(String(32), default="pending")  # pending/success/failed/retrying
    error_message: Mapped[Optional[str]] = mapped_column(String(1024), nullable=True)

    # NEW: retry metadata
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    max_retries: Mapped[int] = mapped_column(Integer, default=3)

    rows: Mapped[list["IngestRow"]] = relationship(
        "IngestRow", back_populates="run", cascade="all, delete-orphan"
    )
```

Everything else in your models can stay as we discussed earlier.

---

## 2. Tk dashboard + retry queues – `ingest_dashboard.py`

Create a new file `ingest_dashboard.py` alongside `pipeline.py`:

```python
from __future__ import annotations

import json
import queue
import threading
import tkinter as tk
from datetime import datetime
from pathlib import Path
from tkinter import messagebox, ttk
from typing import List

import os
import subprocess

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from models import Base, VendorProfile, IngestRun, IngestRow


# --- Paths / DB / shared config ----------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent  # adjust if needed

DB_PATH = BASE_DIR / "ingest_stats.db"
DMT_EXE = BASE_DIR / "win-x64-package" / "dmt.exe"
CONFIG_JSON_TO_COSMOS = BASE_DIR / "configs" / "json_to_cosmos.migrationsettings.json"
DATA_SANITIZED = BASE_DIR / "data" / "sanitized"

DATA_SANITIZED.mkdir(parents=True, exist_ok=True)

engine = create_engine(f"sqlite:///{DB_PATH}", future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
Base.metadata.create_all(engine)


# --- Shared helpers (mirrors your pipeline logic) ----------------------------


def get_or_create_profile(session, vendor_id: str) -> VendorProfile:
    stmt = select(VendorProfile).where(VendorProfile.vendor_id == vendor_id)
    profile = session.execute(stmt).scalar_one_or_none()
    if profile:
        return profile

    profile = VendorProfile(
        vendor_id=vendor_id,
        profile_name=f"default_profile_{vendor_id}",
        cosmos_database_name="soh",
        cosmos_container_name="dropshipPricingTest",
        cosmos_partition_key_path="/id",
        id_pattern="{vendor_id}|{sku}|{postCode}",
        postcode_length=4,
        postcode_zero_pad=True,
        postcode_uppercase=False,
        is_active=True,
    )
    session.add(profile)
    session.commit()
    session.refresh(profile)
    return profile


def normalize_postcode(raw: str | None, profile: VendorProfile) -> str:
    if raw is None:
        return ""

    s = str(raw).strip()
    if profile.postcode_uppercase:
        s = s.upper()

    if s.isdigit() and profile.postcode_zero_pad and profile.postcode_length > 0:
        if len(s) <= profile.postcode_length:
            s = s.zfill(profile.postcode_length)

    return s


def build_cosmos_id(
    profile: VendorProfile,
    vendor_id: str,
    sku: str,
    post_code: str,
) -> str:
    try:
        return profile.id_pattern.format(
            vendor_id=vendor_id,
            sku=sku,
            postCode=post_code,
            postcode=post_code,
        )
    except Exception:
        return f"{vendor_id}|{sku}|{post_code}"


def write_ndjson(path: Path, docs: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for doc in docs:
            f.write(json.dumps(doc, ensure_ascii=False) + "\n")


def run_dmt_json_to_cosmos(ndjson_path: Path, profile: VendorProfile) -> int:
    cosmos_conn = os.environ.get("COSMOS_CONN")
    if not cosmos_conn:
        raise RuntimeError("COSMOS_CONN env var is not set.")

    args = [
        str(DMT_EXE),
        "--source",
        "JSON",
        "--destination",
        "Cosmos-nosql",
        "--settings",
        str(CONFIG_JSON_TO_COSMOS),
        "--SourceSettings:FilePath",
        str(ndjson_path),
        "--SinkSettings:ConnectionString",
        cosmos_conn,
        # NOTE: replace these with actual setting names from `dmt.exe settings`
        # "--SinkSettings:DatabaseName", profile.cosmos_database_name,
        # "--SinkSettings:ContainerName", profile.cosmos_container_name,
    ]

    proc = subprocess.run(args, capture_output=True, text=True)
    # For debugging, you can write proc.stdout/proc.stderr to a log file here.
    return proc.returncode


def retry_run_once(run_id: int) -> None:
    """
    Single retry attempt for one run:
      - Respects max_retries
      - Reuses existing NDJSON if present
      - Otherwise rebuilds NDJSON from IngestRow records
      - Calls dmt.exe and updates status / error_message / rows_uploaded
    """
    session = SessionLocal()
    try:
        run = session.get(IngestRun, run_id)
        if not run:
            return

        if run.retry_count >= run.max_retries:
            return

        profile = get_or_create_profile(session, run.vendor_id)

        # Try existing NDJSON under multiple naming conventions
        candidates = [
            DATA_SANITIZED / f"run_{run.id}.ndjson",
            DATA_SANITIZED / f"csv_run_{run.id}.ndjson",
            DATA_SANITIZED / f"json_run_{run.id}.ndjson",
        ]
        ndjson_path = None
        for c in candidates:
            if c.exists():
                ndjson_path = c
                break

        # If none exist, rebuild from IngestRow
        if ndjson_path is None:
            ndjson_path = candidates[0]
            docs: list[dict] = []
            for row in run.rows:
                pc_norm = normalize_postcode(row.postCode, profile)
                cosmos_id = build_cosmos_id(profile, row.vendor_id, row.sku, pc_norm)
                docs.append(
                    {
                        "id": cosmos_id,
                        "vendor_id": row.vendor_id,
                        "sku": row.sku,
                        "postCode": pc_norm,
                        "price": row.price,
                    }
                )
            write_ndjson(ndjson_path, docs)

        # Increment retry counter, mark as retrying
        run.retry_count += 1
        run.status = "retrying"
        session.commit()

        code = run_dmt_json_to_cosmos(ndjson_path, profile)

        # Refresh and update final status
        run = session.get(IngestRun, run_id)
        if code == 0:
            run.status = "success"
            run.rows_uploaded = run.rows_valid
        else:
            run.status = "failed"
            msg = f"dmt.exe exit code {code}"
            if run.error_message:
                merged = f"{run.error_message} | {msg}"
            else:
                merged = msg
            run.error_message = merged[:1024]

        session.commit()
    finally:
        session.close()


# --- Tk dashboard -------------------------------------------------------------


class IngestDashboard(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("Ingest Monitor & Retry Dashboard")
        self.geometry("1100x600")

        self.retry_queue: queue.Queue[int] = queue.Queue()
        self.worker_thread: threading.Thread | None = None

        self._build_ui()
        self.refresh_runs()

    # UI ----------------------------------------------------------------------

    def _build_ui(self) -> None:
        # Filters frame
        filter_frame = ttk.Frame(self)
        filter_frame.pack(side=tk.TOP, fill=tk.X, padx=8, pady=4)

        ttk.Label(filter_frame, text="Status:").pack(side=tk.LEFT, padx=(0, 4))
        self.status_var = tk.StringVar(value="All")
        status_cb = ttk.Combobox(
            filter_frame,
            textvariable=self.status_var,
            state="readonly",
            values=["All", "pending", "success", "failed", "retrying"],
            width=12,
        )
        status_cb.pack(side=tk.LEFT, padx=(0, 8))

        ttk.Label(filter_frame, text="Vendor:").pack(side=tk.LEFT, padx=(0, 4))
        self.vendor_var = tk.StringVar()
        vendor_entry = ttk.Entry(filter_frame, textvariable=self.vendor_var, width=18)
        vendor_entry.pack(side=tk.LEFT, padx=(0, 8))

        ttk.Button(filter_frame, text="Apply", command=self.refresh_runs).pack(
            side=tk.LEFT, padx=(0, 4)
        )
        ttk.Button(filter_frame, text="Clear", command=self.clear_filters).pack(
            side=tk.LEFT
        )

        ttk.Button(filter_frame, text="Refresh", command=self.refresh_runs).pack(
            side=tk.RIGHT
        )

        # Treeview frame
        tree_frame = ttk.Frame(self)
        tree_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=8, pady=(0, 4))

        columns = [
            "id",
            "created_at",
            "vendor_id",
            "profile_name",
            "source_name",
            "status",
            "rows_total",
            "rows_valid",
            "rows_invalid",
            "duplicates",
            "unique_skus",
            "rows_uploaded",
            "retry_count",
        ]

        self.tree = ttk.Treeview(
            tree_frame, columns=columns, show="headings", selectmode="extended"
        )

        headings = {
            "id": "Run ID",
            "created_at": "Created",
            "vendor_id": "Vendor",
            "profile_name": "Profile",
            "source_name": "Source",
            "status": "Status",
            "rows_total": "Total",
            "rows_valid": "Valid",
            "rows_invalid": "Invalid",
            "duplicates": "Dupes",
            "unique_skus": "Unique SKUs",
            "rows_uploaded": "Uploaded",
            "retry_count": "Retries",
        }

        widths = {
            "id": 60,
            "created_at": 140,
            "vendor_id": 100,
            "profile_name": 150,
            "source_name": 220,
            "status": 80,
            "rows_total": 80,
            "rows_valid": 80,
            "rows_invalid": 80,
            "duplicates": 80,
            "unique_skus": 100,
            "rows_uploaded": 90,
            "retry_count": 80,
        }

        for col in columns:
            self.tree.heading(col, text=headings[col])
            self.tree.column(col, width=widths[col], anchor=tk.W)

        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(tree_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        self.tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")

        tree_frame.rowconfigure(0, weight=1)
        tree_frame.columnconfigure(0, weight=1)

        self.tree.tag_configure("success", background="#e1f7e1")
        self.tree.tag_configure("failed", background="#fbdada")
        self.tree.tag_configure("pending", background="#f7f3d6")
        self.tree.tag_configure("retrying", background="#d6e4f7")

        # Bottom controls
        bottom_frame = ttk.Frame(self)
        bottom_frame.pack(side=tk.BOTTOM, fill=tk.X, padx=8, pady=4)

        ttk.Button(
            bottom_frame, text="View Details", command=self.view_details
        ).pack(side=tk.LEFT, padx=(0, 4))

        ttk.Button(
            bottom_frame, text="Retry Selected", command=self.retry_selected
        ).pack(side=tk.LEFT, padx=(0, 4))

        ttk.Button(
            bottom_frame, text="Retry All Failed", command=self.retry_all_failed
        ).pack(side=tk.LEFT, padx=(0, 4))

        ttk.Button(bottom_frame, text="Exit", command=self.destroy).pack(
            side=tk.RIGHT
        )

        self.status_label = ttk.Label(bottom_frame, text="Idle")
        self.status_label.pack(side=tk.RIGHT, padx=(0, 8))

    # Filters ------------------------------------------------------------------

    def clear_filters(self) -> None:
        self.status_var.set("All")
        self.vendor_var.set("")
        self.refresh_runs()

    # Data loading -------------------------------------------------------------

    def load_runs(self) -> List[IngestRun]:
        session = SessionLocal()
        try:
            stmt = select(IngestRun).order_by(IngestRun.id.desc())
            runs = session.execute(stmt).scalars().all()

            status_filter = self.status_var.get()
            vendor_filter = self.vendor_var.get().strip()

            filtered: List[IngestRun] = []
            for r in runs:
                if status_filter != "All" and r.status != status_filter:
                    continue
                if vendor_filter and vendor_filter.lower() not in r.vendor_id.lower():
                    continue
                filtered.append(r)
            return filtered
        finally:
            session.close()

    def refresh_runs(self) -> None:
        self.tree.delete(*self.tree.get_children())
        runs = self.load_runs()
        for run in runs:
            created = (
                run.created_at.astimezone().strftime("%Y-%m-%d %H:%M")
                if run.created_at
                else ""
            )
            values = [
                run.id,
                created,
                run.vendor_id,
                run.profile_name,
                run.source_name,
                run.status,
                run.rows_total,
                run.rows_valid,
                run.rows_invalid,
                run.duplicates,
                run.unique_skus,
                run.rows_uploaded,
                run.retry_count,
            ]
            tag = run.status if run.status in ("success", "failed", "pending", "retrying") else ""
            self.tree.insert("", tk.END, values=values, tags=(tag,))

    # View details -------------------------------------------------------------

    def view_details(self) -> None:
        selection = self.tree.selection()
        if not selection:
            messagebox.showinfo("Details", "Select a run first.")
            return

        item = self.tree.item(selection[0])
        run_id = int(item["values"][0])

        session = SessionLocal()
        try:
            run = session.get(IngestRun, run_id)
            if not run:
                messagebox.showerror("Details", f"Run {run_id} not found.")
                return

            win = tk.Toplevel(self)
            win.title(f"Run {run.id} details")
            win.geometry("700x400")

            info_frame = ttk.Frame(win)
            info_frame.pack(side=tk.TOP, fill=tk.X, padx=8, pady=4)

            def add_row(label, value):
                row = ttk.Frame(info_frame)
                row.pack(side=tk.TOP, anchor="w", pady=2, fill=tk.X)
                ttk.Label(row, text=f"{label}:", width=16, anchor="w").pack(
                    side=tk.LEFT
                )
                ttk.Label(row, text=str(value), anchor="w").pack(
                    side=tk.LEFT, fill=tk.X, expand=True
                )

            add_row("Run ID", run.id)
            add_row("Vendor", run.vendor_id)
            add_row("Profile", run.profile_name)
            add_row("Status", run.status)
            add_row("Source", run.source_name)
            add_row("Created", run.created_at)
            add_row("Rows total", run.rows_total)
            add_row("Valid", run.rows_valid)
            add_row("Invalid", run.rows_invalid)
            add_row("Duplicates", run.duplicates)
            add_row("Unique SKUs", run.unique_skus)
            add_row("Uploaded", run.rows_uploaded)
            add_row("Retries", f"{run.retry_count}/{run.max_retries}")

            # Error message box
            err_frame = ttk.LabelFrame(win, text="Error Message")
            err_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=8, pady=4)

            txt = tk.Text(err_frame, wrap="word", height=6)
            txt.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
            if run.error_message:
                txt.insert("1.0", run.error_message)
            txt.config(state="disabled")

            yscroll = ttk.Scrollbar(err_frame, orient="vertical", command=txt.yview)
            txt.configure(yscrollcommand=yscroll.set)
            yscroll.pack(side=tk.RIGHT, fill=tk.Y)

        finally:
            session.close()

    # Retry handling -----------------------------------------------------------

    def get_selected_run_ids(self) -> List[int]:
        ids: List[int] = []
        for item_id in self.tree.selection():
            item = self.tree.item(item_id)
            try:
                ids.append(int(item["values"][0]))
            except Exception:
                continue
        return ids

    def retry_selected(self) -> None:
        run_ids = self.get_selected_run_ids()
        if not run_ids:
            messagebox.showinfo("Retry", "Select one or more runs to retry.")
            return
        self.enqueue_retries(run_ids)

    def retry_all_failed(self) -> None:
        # Collect all failed runs currently in the view
        ids: List[int] = []
        for item_id in self.tree.get_children():
            item = self.tree.item(item_id)
            run_id = int(item["values"][0])
            status = item["values"][5]  # status column
            if status == "failed":
                ids.append(run_id)

        if not ids:
            messagebox.showinfo("Retry", "No failed runs in the current view.")
            return

        self.enqueue_retries(ids)

    def enqueue_retries(self, run_ids: List[int]) -> None:
        for run_id in run_ids:
            self.retry_queue.put(run_id)
        self.start_worker()

    def start_worker(self) -> None:
        if self.worker_thread and self.worker_thread.is_alive():
            return

        self.worker_thread = threading.Thread(target=self._retry_worker_loop, daemon=True)
        self.worker_thread.start()

    def _retry_worker_loop(self) -> None:
        while not self.retry_queue.empty():
            run_id = self.retry_queue.get()
            self._update_status(f"Retrying run {run_id}...")
            try:
                retry_run_once(run_id)
            except Exception as exc:
                # Report minimal info back to UI thread
                msg = f"Error retrying run {run_id}: {exc}"
                self.after(0, lambda m=msg: messagebox.showerror("Retry error", m))
            finally:
                self.retry_queue.task_done()
                self.after(0, self.refresh_runs)
        self._update_status("Idle")

    def _update_status(self, text: str) -> None:
        def _set():
            self.status_label.config(text=text)

        self.after(0, _set)


if __name__ == "__main__":
    app = IngestDashboard()
    app.mainloop()
```

---

### How to use this setup

1. Keep running your existing `pipeline.py` to ingest CSV/JSON and populate:

   * `VendorProfile`
   * `IngestRun`
   * `IngestRow`
   * NDJSON files in `data/sanitized/`

2. Launch the dashboard:

```bash
python scripts/ingest_dashboard.py
```

3. In the dashboard you can:

   * Filter by status/vendor
   * Inspect runs & errors
   * Hit **Retry Selected** or **Retry All Failed**
   * Retries go through a **queue + background worker**, so the UI stays responsive.

If you want, we can next:

* Add a **summary bar** per vendor (today’s totals, success rate)
* Add a **manual “force rebuild NDJSON”** option for a run
* Or integrate **per-vendor mapping configuration** into the `VendorProfile` model and edit it from the same Tk app.

Here’s a **single Python bootstrap script** that will create a complete, testable project for your CSV/JSON → Cosmos ingest pipeline, including:

* Dev/prod separation via `APP_ENV`
* `models.py`, `pipeline.py`, `ingest_dashboard.py`
* `configs/` with dev/prod config placeholders
* `data/` tree (incoming, archive, sanitized)
* `db/` for per-env SQLite
* `win-x64-package/` placeholder (you drop `dmt.exe` here)
* `requirements.txt`, `.env.example`, `README.md`

You can scale/reshape it later, but this is already robust enough to grow.

> **Usage:**
> Save this as e.g. `bootstrap_cosmos_ingest.py` and run:
>
> ```bash
> python bootstrap_cosmos_ingest.py
> ```
>
> It will create a folder `cosmos_ingest_pipeline` in your current directory.

---

````python
from __future__ import annotations

import textwrap
from pathlib import Path


PROJECT_ROOT = Path.cwd() / "cosmos_ingest_pipeline"


def write_file(relative_path: str, content: str, overwrite: bool = False) -> None:
    path = PROJECT_ROOT / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not overwrite:
        print(f"[SKIP] {path} already exists")
        return
    path.write_text(textwrap.dedent(content).lstrip("\n"), encoding="utf-8")
    print(f"[OK]   Wrote {path}")


def main() -> None:
    PROJECT_ROOT.mkdir(parents=True, exist_ok=True)
    print(f"Project root: {PROJECT_ROOT}")

    # -------------------------------------------------------------------------
    # models.py
    # -------------------------------------------------------------------------
    models_py = r"""
    from __future__ import annotations

    from datetime import datetime
    from typing import Optional

    from sqlalchemy import (
        String,
        Integer,
        Float,
        DateTime,
        ForeignKey,
        UniqueConstraint,
        func,
    )
    from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


    class Base(DeclarativeBase):
        pass


    class VendorProfile(Base):
        __tablename__ = "vendor_profiles"

        id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

        # Logical vendor identifier: e.g. "VS", "SUPPLIER123"
        vendor_id: Mapped[str] = mapped_column(String(128), unique=True, index=True)

        # Friendly name, e.g. "Virtualstock Dropship Pricing"
        profile_name: Mapped[str] = mapped_column(String(128))

        # Cosmos configuration knobs
        cosmos_database_name: Mapped[str] = mapped_column(String(128), default="soh")
        cosmos_container_name: Mapped[str] = mapped_column(
            String(128), default="dropshipPricingTest"
        )
        cosmos_partition_key_path: Mapped[str] = mapped_column(
            String(256), default="/id"
        )

        # ID composition template, python .format style
        # available keys: vendor_id, sku, postCode, postcode
        id_pattern: Mapped[str] = mapped_column(
            String(256), default="{vendor_id}|{sku}|{postCode}"
        )

        # Postcode rules
        postcode_length: Mapped[int] = mapped_column(Integer, default=4)
        postcode_zero_pad: Mapped[bool] = mapped_column(default=True)
        postcode_uppercase: Mapped[bool] = mapped_column(default=False)

        # Status / audit
        is_active: Mapped[bool] = mapped_column(default=True)
        created_at: Mapped[datetime] = mapped_column(
            DateTime(timezone=True), server_default=func.now()
        )
        updated_at: Mapped[datetime] = mapped_column(
            DateTime(timezone=True),
            server_default=func.now(),
            onupdate=func.now(),
        )


    class IngestRun(Base):
        __tablename__ = "ingest_runs"

        id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
        profile_name: Mapped[str] = mapped_column(String(64))
        vendor_id: Mapped[str] = mapped_column(String(128))
        source_name: Mapped[str] = mapped_column(String(256))
        created_at: Mapped[datetime] = mapped_column(
            DateTime(timezone=True), server_default=func.now()
        )

        rows_total: Mapped[int] = mapped_column(Integer, default=0)
        rows_valid: Mapped[int] = mapped_column(Integer, default=0)
        rows_invalid: Mapped[int] = mapped_column(Integer, default=0)
        duplicates: Mapped[int] = mapped_column(Integer, default=0)
        unique_skus: Mapped[int] = mapped_column(Integer, default=0)
        rows_uploaded: Mapped[int] = mapped_column(Integer, default=0)

        status: Mapped[str] = mapped_column(
            String(32), default="pending"
        )  # pending/success/failed/retrying
        error_message: Mapped[Optional[str]] = mapped_column(
            String(1024), nullable=True
        )

        # Retry metadata
        retry_count: Mapped[int] = mapped_column(Integer, default=0)
        max_retries: Mapped[int] = mapped_column(Integer, default=3)

        rows: Mapped[list["IngestRow"]] = relationship(
            "IngestRow", back_populates="run", cascade="all, delete-orphan"
        )


    class IngestRow(Base):
        __tablename__ = "ingest_rows"

        id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
        vendor_id: Mapped[str] = mapped_column(String(128), index=True)
        sku: Mapped[str] = mapped_column(String(128), index=True)
        postCode: Mapped[str] = mapped_column(String(16), index=True)
        price: Mapped[float] = mapped_column(Float)

        run_id: Mapped[int | None] = mapped_column(
            ForeignKey("ingest_runs.id", ondelete="SET NULL"), nullable=True
        )
        run: Mapped["IngestRun" | None] = relationship(
            "IngestRun", back_populates="rows"
        )

        __table_args__ = (
            UniqueConstraint("vendor_id", "sku", "postCode", name="uq_vendor_sku_postcode"),
        )
    """
    write_file("models.py", models_py)

    # -------------------------------------------------------------------------
    # pipeline.py (CSV + JSON → validate → NDJSON → dmt.exe → Cosmos)
    # -------------------------------------------------------------------------
    pipeline_py = r"""
    from __future__ import annotations

    import csv
    import json
    import os
    import subprocess
    from datetime import datetime, timezone
    from pathlib import Path
    from typing import Iterable, Tuple

    from sqlalchemy import create_engine, select
    from sqlalchemy.orm import sessionmaker

    from models import Base, VendorProfile, IngestRun, IngestRow


    ENV = os.getenv("APP_ENV", "dev").lower()  # dev / prod


    BASE_DIR = Path(__file__).resolve().parent

    DB_DIR = BASE_DIR / "db"
    DB_DIR.mkdir(parents=True, exist_ok=True)
    DB_PATH = DB_DIR / f"ingest_stats.{ENV}.db"

    DMT_EXE = BASE_DIR / "win-x64-package" / "dmt.exe"
    CONFIG_JSON_TO_COSMOS = (
        BASE_DIR / "configs" / f"json_to_cosmos.{ENV}.migrationsettings.json"
    )

    DATA_IN_CSV = BASE_DIR / "data" / "incoming" / "csv"
    DATA_IN_JSON = BASE_DIR / "data" / "incoming" / "json"
    DATA_ARCHIVE_CSV_OK = BASE_DIR / "data" / "archive" / "csv" / "success"
    DATA_ARCHIVE_CSV_BAD = BASE_DIR / "data" / "archive" / "csv" / "failed"
    DATA_ARCHIVE_JSON_OK = BASE_DIR / "data" / "archive" / "json" / "success"
    DATA_ARCHIVE_JSON_BAD = BASE_DIR / "data" / "archive" / "json" / "failed"
    DATA_SANITIZED = BASE_DIR / "data" / "sanitized"

    for p in (
        DATA_IN_CSV,
        DATA_IN_JSON,
        DATA_ARCHIVE_CSV_OK,
        DATA_ARCHIVE_CSV_BAD,
        DATA_ARCHIVE_JSON_OK,
        DATA_ARCHIVE_JSON_BAD,
        DATA_SANITIZED,
    ):
        p.mkdir(parents=True, exist_ok=True)

    engine = create_engine(f"sqlite:///{DB_PATH}", future=True)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    Base.metadata.create_all(engine)


    # --- helpers --------------------------------------------------------------


    def infer_vendor_id_from_filename(path: Path) -> str:
        """
        'VENDOR123_pricing_20250101.csv' -> 'VENDOR123'
        Adjust this logic to your naming convention if needed.
        """
        stem = path.stem
        return stem.split("_")[0]


    def get_or_create_profile(session, vendor_id: str) -> VendorProfile:
        stmt = select(VendorProfile).where(VendorProfile.vendor_id == vendor_id)
        profile = session.execute(stmt).scalar_one_or_none()
        if profile:
            return profile

        profile = VendorProfile(
            vendor_id=vendor_id,
            profile_name=f"default_profile_{vendor_id}",
            cosmos_database_name="soh",
            cosmos_container_name="dropshipPricingTest",
            cosmos_partition_key_path="/id",
            id_pattern="{vendor_id}|{sku}|{postCode}",
            postcode_length=4,
            postcode_zero_pad=True,
            postcode_uppercase=False,
            is_active=True,
        )
        session.add(profile)
        session.commit()
        session.refresh(profile)
        return profile


    def normalize_postcode(raw: str | None, profile: VendorProfile) -> str:
        if raw is None:
            return ""
        s = str(raw).strip()
        if profile.postcode_uppercase:
            s = s.upper()
        if s.isdigit() and profile.postcode_zero_pad and profile.postcode_length > 0:
            if len(s) <= profile.postcode_length:
                s = s.zfill(profile.postcode_length)
        return s


    def build_cosmos_id(
        profile: VendorProfile,
        vendor_id: str,
        sku: str,
        post_code: str,
    ) -> str:
        try:
            return profile.id_pattern.format(
                vendor_id=vendor_id,
                sku=sku,
                postCode=post_code,
                postcode=post_code,
            )
        except Exception:
            return f"{vendor_id}|{sku}|{post_code}"


    def validate_and_prepare_docs(
        vendor_id: str,
        profile: VendorProfile,
        rows: Iterable[dict],
    ) -> Tuple[list[dict], dict]:
        """
        Returns:
          docs: list of valid Cosmos docs
          stats: dict with rows_total, rows_valid, rows_invalid, duplicates, unique_skus
        """
        seen_keys: set[tuple[str, str, str]] = set()
        skus: set[str] = set()
        docs: list[dict] = []

        rows_total = rows_valid = rows_invalid = duplicates = 0

        for row in rows:
            rows_total += 1

            sku = (row.get("sku") or "").strip()
            post_code_raw = (
                row.get("postCode")
                or row.get("postcode")
                or row.get("PostCode")
                or row.get("POSTCODE")
            )
            price_raw = row.get("price")

            if not sku:
                rows_invalid += 1
                continue

            post_code = normalize_postcode(post_code_raw, profile)

            try:
                price = float(price_raw)
            except (TypeError, ValueError):
                rows_invalid += 1
                continue

            key = (vendor_id, sku, post_code)
            if key in seen_keys:
                duplicates += 1
                rows_invalid += 1
                continue
            seen_keys.add(key)
            skus.add(sku)

            cosmos_id = build_cosmos_id(profile, vendor_id, sku, post_code)

            doc = {
                "id": cosmos_id,
                "vendor_id": vendor_id,
                "sku": sku,
                "postCode": post_code,
                "price": price,
            }
            docs.append(doc)
            rows_valid += 1

        stats = {
            "rows_total": rows_total,
            "rows_valid": rows_valid,
            "rows_invalid": rows_invalid,
            "duplicates": duplicates,
            "unique_skus": len(skus),
        }
        return docs, stats


    def read_csv_rows(path: Path) -> Iterable[dict]:
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                yield row


    def read_json_rows(path: Path) -> Iterable[dict]:
        """
        Supports:
        - JSON array: [ {..}, {..} ]
        - NDJSON (JSONL): one JSON object per line
        """
        with path.open("r", encoding="utf-8") as f:
            content = f.read()
        stripped = content.lstrip()
        if stripped.startswith("["):
            data = json.loads(content)
            for obj in data:
                if isinstance(obj, dict):
                    yield obj
        else:
            for line in content.splitlines():
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                if isinstance(obj, dict):
                    yield obj


    def write_ndjson(path: Path, docs: list[dict]) -> None:
        with path.open("w", encoding="utf-8") as f:
            for doc in docs:
                f.write(json.dumps(doc, ensure_ascii=False) + "\n")


    def run_dmt_json_to_cosmos(ndjson_path: Path, profile: VendorProfile) -> int:
        cosmos_conn = os.environ.get("COSMOS_CONN")
        if not cosmos_conn:
            raise RuntimeError("COSMOS_CONN env var is not set.")

        args = [
            str(DMT_EXE),
            "--source",
            "JSON",
            "--destination",
            "Cosmos-nosql",
            "--settings",
            str(CONFIG_JSON_TO_COSMOS),
            "--SourceSettings:FilePath",
            str(ndjson_path),
            "--SinkSettings:ConnectionString",
            cosmos_conn,
            # IMPORTANT:
            # replace these names with the actual ones from `dmt.exe settings`
            # if/when your extension exposes database/container in settings:
            # "--SinkSettings:DatabaseName", profile.cosmos_database_name,
            # "--SinkSettings:ContainerName", profile.cosmos_container_name,
        ]

        proc = subprocess.run(args, capture_output=True, text=True)
        # You can log proc.stdout / proc.stderr to a file here if needed.
        return proc.returncode


    # --- main processing ------------------------------------------------------


    def process_file(path: Path, input_type: str) -> None:
        """
        One file -> one IngestRun -> stats + NDJSON -> dmt.exe -> archive.
        input_type: "csv" or "json"
        """
        session = SessionLocal()
        vendor_id = infer_vendor_id_from_filename(path)
        profile = get_or_create_profile(session, vendor_id)

        run = IngestRun(
            profile_name=profile.profile_name,
            vendor_id=vendor_id,
            source_name=str(path),
            created_at=datetime.now(timezone.utc),
        )
        session.add(run)
        session.flush()  # assign run.id

        try:
            if input_type == "csv":
                rows = list(read_csv_rows(path))
            elif input_type == "json":
                rows = list(read_json_rows(path))
            else:
                raise ValueError(f"Unsupported input_type: {input_type}")

            docs, stats = validate_and_prepare_docs(vendor_id, profile, rows)

            run.rows_total = stats["rows_total"]
            run.rows_valid = stats["rows_valid"]
            run.rows_invalid = stats["rows_invalid"]
            run.duplicates = stats["duplicates"]
            run.unique_skus = stats["unique_skus"]

            # Optional: persist valid rows for audit
            for doc in docs:
                row = IngestRow(
                    vendor_id=doc["vendor_id"],
                    sku=doc["sku"],
                    postCode=doc["postCode"],
                    price=doc["price"],
                    run_id=run.id,
                )
                session.add(row)

            ndjson_path = DATA_SANITIZED / f"{input_type}_run_{run.id}.ndjson"
            write_ndjson(ndjson_path, docs)

            session.commit()

            code = run_dmt_json_to_cosmos(ndjson_path, profile)
            if code == 0:
                run.status = "success"
                run.rows_uploaded = run.rows_valid
                session.commit()
                if input_type == "csv":
                    path.rename(DATA_ARCHIVE_CSV_OK / path.name)
                else:
                    path.rename(DATA_ARCHIVE_JSON_OK / path.name)
            else:
                run.status = "failed"
                run.error_message = f"dmt.exe exit code {code}"
                session.commit()
                if input_type == "csv":
                    path.rename(DATA_ARCHIVE_CSV_BAD / path.name)
                else:
                    path.rename(DATA_ARCHIVE_JSON_BAD / path.name)

        except Exception as exc:
            session.rollback()
            run.status = "failed"
            run.error_message = str(exc)[:1024]
            session.add(run)
            session.commit()
            if input_type == "csv":
                path.rename(DATA_ARCHIVE_CSV_BAD / path.name)
            else:
                path.rename(DATA_ARCHIVE_JSON_BAD / path.name)
        finally:
            session.close()


    def main() -> None:
        # CSV
        for csv_file in sorted(DATA_IN_CSV.glob("*.csv")):
            process_file(csv_file, "csv")

        # JSON
        for json_file in sorted(DATA_IN_JSON.glob("*.json")):
            process_file(json_file, "json")


    if __name__ == "__main__":
        main()
    """
    write_file("pipeline.py", pipeline_py)

    # -------------------------------------------------------------------------
    # ingest_dashboard.py (Tk dashboard + retry queue)
    # -------------------------------------------------------------------------
    ingest_dashboard_py = r"""
    from __future__ import annotations

    import json
    import os
    import queue
    import subprocess
    import threading
    import tkinter as tk
    from datetime import datetime
    from pathlib import Path
    from tkinter import messagebox, ttk
    from typing import List

    from sqlalchemy import create_engine, select
    from sqlalchemy.orm import sessionmaker

    from models import Base, VendorProfile, IngestRun, IngestRow


    ENV = os.getenv("APP_ENV", "dev").lower()

    BASE_DIR = Path(__file__).resolve().parent

    DB_DIR = BASE_DIR / "db"
    DB_DIR.mkdir(parents=True, exist_ok=True)
    DB_PATH = DB_DIR / f"ingest_stats.{ENV}.db"

    DMT_EXE = BASE_DIR / "win-x64-package" / "dmt.exe"
    CONFIG_JSON_TO_COSMOS = (
        BASE_DIR / "configs" / f"json_to_cosmos.{ENV}.migrationsettings.json"
    )

    DATA_SANITIZED = BASE_DIR / "data" / "sanitized"
    DATA_SANITIZED.mkdir(parents=True, exist_ok=True)

    engine = create_engine(f"sqlite:///{DB_PATH}", future=True)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    Base.metadata.create_all(engine)


    # --- shared helpers (mirrors pipeline) ------------------------------------


    def get_or_create_profile(session, vendor_id: str) -> VendorProfile:
        stmt = select(VendorProfile).where(VendorProfile.vendor_id == vendor_id)
        profile = session.execute(stmt).scalar_one_or_none()
        if profile:
            return profile

        profile = VendorProfile(
            vendor_id=vendor_id,
            profile_name=f"default_profile_{vendor_id}",
            cosmos_database_name="soh",
            cosmos_container_name="dropshipPricingTest",
            cosmos_partition_key_path="/id",
            id_pattern="{vendor_id}|{sku}|{postCode}",
            postcode_length=4,
            postcode_zero_pad=True,
            postcode_uppercase=False,
            is_active=True,
        )
        session.add(profile)
        session.commit()
        session.refresh(profile)
        return profile


    def normalize_postcode(raw: str | None, profile: VendorProfile) -> str:
        if raw is None:
            return ""
        s = str(raw).strip()
        if profile.postcode_uppercase:
            s = s.upper()
        if s.isdigit() and profile.postcode_zero_pad and profile.postcode_length > 0:
            if len(s) <= profile.postcode_length:
                s = s.zfill(profile.postcode_length)
        return s


    def build_cosmos_id(
        profile: VendorProfile,
        vendor_id: str,
        sku: str,
        post_code: str,
    ) -> str:
        try:
            return profile.id_pattern.format(
                vendor_id=vendor_id,
                sku=sku,
                postCode=post_code,
                postcode=post_code,
            )
        except Exception:
            return f"{vendor_id}|{sku}|{post_code}"


    def write_ndjson(path: Path, docs: list[dict]) -> None:
        with path.open("w", encoding="utf-8") as f:
            for doc in docs:
                f.write(json.dumps(doc, ensure_ascii=False) + "\n")


    def run_dmt_json_to_cosmos(ndjson_path: Path, profile: VendorProfile) -> int:
        cosmos_conn = os.environ.get("COSMOS_CONN")
        if not cosmos_conn:
            raise RuntimeError("COSMOS_CONN env var is not set.")

        args = [
            str(DMT_EXE),
            "--source",
            "JSON",
            "--destination",
            "Cosmos-nosql",
            "--settings",
            str(CONFIG_JSON_TO_COSMOS),
            "--SourceSettings:FilePath",
            str(ndjson_path),
            "--SinkSettings:ConnectionString",
            cosmos_conn,
            # IMPORTANT:
            # replace these names with actual ones from `dmt.exe settings`
            # "--SinkSettings:DatabaseName", profile.cosmos_database_name,
            # "--SinkSettings:ContainerName", profile.cosmos_container_name,
        ]

        proc = subprocess.run(args, capture_output=True, text=True)
        return proc.returncode


    def retry_run_once(run_id: int) -> None:
        """
        One retry attempt:
          - respects max_retries
          - reuses NDJSON if present or rebuilds from IngestRow
          - calls dmt.exe and updates run status
        """
        session = SessionLocal()
        try:
            run = session.get(IngestRun, run_id)
            if not run:
                return

            if run.retry_count >= run.max_retries:
                return

            profile = get_or_create_profile(session, run.vendor_id)

            # Try existing NDJSON files first
            candidates = [
                DATA_SANITIZED / f"run_{run.id}.ndjson",
                DATA_SANITIZED / f"csv_run_{run.id}.ndjson",
                DATA_SANITIZED / f"json_run_{run.id}.ndjson",
            ]
            ndjson_path = None
            for c in candidates:
                if c.exists():
                    ndjson_path = c
                    break

            # Rebuild from IngestRow if necessary
            if ndjson_path is None:
                ndjson_path = candidates[0]
                docs: list[dict] = []
                for row in run.rows:
                    pc_norm = normalize_postcode(row.postCode, profile)
                    cosmos_id = build_cosmos_id(profile, row.vendor_id, row.sku, pc_norm)
                    docs.append(
                        {
                            "id": cosmos_id,
                            "vendor_id": row.vendor_id,
                            "sku": row.sku,
                            "postCode": pc_norm,
                            "price": row.price,
                        }
                    )
                write_ndjson(ndjson_path, docs)

            run.retry_count += 1
            run.status = "retrying"
            session.commit()

            code = run_dmt_json_to_cosmos(ndjson_path, profile)

            run = session.get(IngestRun, run_id)
            if code == 0:
                run.status = "success"
                run.rows_uploaded = run.rows_valid
            else:
                msg = f"dmt.exe exit code {code}"
                if run.error_message:
                    merged = f"{run.error_message} | {msg}"
                else:
                    merged = msg
                run.status = "failed"
                run.error_message = merged[:1024]

            session.commit()
        finally:
            session.close()


    # --- Tk dashboard ---------------------------------------------------------


    class IngestDashboard(tk.Tk):
        def __init__(self) -> None:
            super().__init__()
            self.title(f"Ingest Monitor & Retry Dashboard ({ENV})")
            self.geometry("1100x600")

            self.retry_queue: queue.Queue[int] = queue.Queue()
            self.worker_thread: threading.Thread | None = None

            self._build_ui()
            self.refresh_runs()

        # UI ------------------------------------------------------------------

        def _build_ui(self) -> None:
            filter_frame = ttk.Frame(self)
            filter_frame.pack(side=tk.TOP, fill=tk.X, padx=8, pady=4)

            ttk.Label(filter_frame, text="Status:").pack(side=tk.LEFT, padx=(0, 4))
            self.status_var = tk.StringVar(value="All")
            status_cb = ttk.Combobox(
                filter_frame,
                textvariable=self.status_var,
                state="readonly",
                values=["All", "pending", "success", "failed", "retrying"],
                width=12,
            )
            status_cb.pack(side=tk.LEFT, padx=(0, 8))

            ttk.Label(filter_frame, text="Vendor:").pack(side=tk.LEFT, padx=(0, 4))
            self.vendor_var = tk.StringVar()
            vendor_entry = ttk.Entry(filter_frame, textvariable=self.vendor_var, width=18)
            vendor_entry.pack(side=tk.LEFT, padx=(0, 8))

            ttk.Button(filter_frame, text="Apply", command=self.refresh_runs).pack(
                side=tk.LEFT, padx=(0, 4)
            )
            ttk.Button(filter_frame, text="Clear", command=self.clear_filters).pack(
                side=tk.LEFT
            )

            ttk.Button(filter_frame, text="Refresh", command=self.refresh_runs).pack(
                side=tk.RIGHT
            )

            tree_frame = ttk.Frame(self)
            tree_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=8, pady=(0, 4))

            columns = [
                "id",
                "created_at",
                "vendor_id",
                "profile_name",
                "source_name",
                "status",
                "rows_total",
                "rows_valid",
                "rows_invalid",
                "duplicates",
                "unique_skus",
                "rows_uploaded",
                "retry_count",
            ]

            self.tree = ttk.Treeview(
                tree_frame, columns=columns, show="headings", selectmode="extended"
            )

            headings = {
                "id": "Run ID",
                "created_at": "Created",
                "vendor_id": "Vendor",
                "profile_name": "Profile",
                "source_name": "Source",
                "status": "Status",
                "rows_total": "Total",
                "rows_valid": "Valid",
                "rows_invalid": "Invalid",
                "duplicates": "Dupes",
                "unique_skus": "Unique SKUs",
                "rows_uploaded": "Uploaded",
                "retry_count": "Retries",
            }

            widths = {
                "id": 60,
                "created_at": 140,
                "vendor_id": 100,
                "profile_name": 150,
                "source_name": 220,
                "status": 80,
                "rows_total": 80,
                "rows_valid": 80,
                "rows_invalid": 80,
                "duplicates": 80,
                "unique_skus": 100,
                "rows_uploaded": 90,
                "retry_count": 80,
            }

            for col in columns:
                self.tree.heading(col, text=headings[col])
                self.tree.column(col, width=widths[col], anchor=tk.W)

            vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview)
            hsb = ttk.Scrollbar(tree_frame, orient="horizontal", command=self.tree.xview)
            self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

            self.tree.grid(row=0, column=0, sticky="nsew")
            vsb.grid(row=0, column=1, sticky="ns")
            hsb.grid(row=1, column=0, sticky="ew")

            tree_frame.rowconfigure(0, weight=1)
            tree_frame.columnconfigure(0, weight=1)

            self.tree.tag_configure("success", background="#e1f7e1")
            self.tree.tag_configure("failed", background="#fbdada")
            self.tree.tag_configure("pending", background="#f7f3d6")
            self.tree.tag_configure("retrying", background="#d6e4f7")

            bottom_frame = ttk.Frame(self)
            bottom_frame.pack(side=tk.BOTTOM, fill=tk.X, padx=8, pady=4)

            ttk.Button(
                bottom_frame, text="View Details", command=self.view_details
            ).pack(side=tk.LEFT, padx=(0, 4))

            ttk.Button(
                bottom_frame, text="Retry Selected", command=self.retry_selected
            ).pack(side=tk.LEFT, padx=(0, 4))

            ttk.Button(
                bottom_frame, text="Retry All Failed", command=self.retry_all_failed
            ).pack(side=tk.LEFT, padx=(0, 4))

            ttk.Button(bottom_frame, text="Exit", command=self.destroy).pack(
                side=tk.RIGHT
            )

            self.status_label = ttk.Label(bottom_frame, text="Idle")
            self.status_label.pack(side=tk.RIGHT, padx=(0, 8))

        # Filters --------------------------------------------------------------

        def clear_filters(self) -> None:
            self.status_var.set("All")
            self.vendor_var.set("")
            self.refresh_runs()

        def load_runs(self) -> List[IngestRun]:
            session = SessionLocal()
            try:
                stmt = select(IngestRun).order_by(IngestRun.id.desc())
                runs = session.execute(stmt).scalars().all()

                status_filter = self.status_var.get()
                vendor_filter = self.vendor_var.get().strip()

                filtered: List[IngestRun] = []
                for r in runs:
                    if status_filter != "All" and r.status != status_filter:
                        continue
                    if vendor_filter and vendor_filter.lower() not in r.vendor_id.lower():
                        continue
                    filtered.append(r)
                return filtered
            finally:
                session.close()

        def refresh_runs(self) -> None:
            self.tree.delete(*self.tree.get_children())
            runs = self.load_runs()
            for run in runs:
                if run.created_at is not None:
                    try:
                        created_local = run.created_at.astimezone()
                        created = created_local.strftime("%Y-%m-%d %H:%M")
                    except Exception:
                        created = str(run.created_at)
                else:
                    created = ""

                values = [
                    run.id,
                    created,
                    run.vendor_id,
                    run.profile_name,
                    run.source_name,
                    run.status,
                    run.rows_total,
                    run.rows_valid,
                    run.rows_invalid,
                    run.duplicates,
                    run.unique_skus,
                    run.rows_uploaded,
                    run.retry_count,
                ]
                tag = run.status if run.status in ("success", "failed", "pending", "retrying") else ""
                self.tree.insert("", tk.END, values=values, tags=(tag,))

        # Details --------------------------------------------------------------

        def view_details(self) -> None:
            selection = self.tree.selection()
            if not selection:
                messagebox.showinfo("Details", "Select a run first.")
                return

            item = self.tree.item(selection[0])
            run_id = int(item["values"][0])

            session = SessionLocal()
            try:
                run = session.get(IngestRun, run_id)
                if not run:
                    messagebox.showerror("Details", f"Run {run_id} not found.")
                    return

                win = tk.Toplevel(self)
                win.title(f"Run {run.id} details")
                win.geometry("700x400")

                info_frame = ttk.Frame(win)
                info_frame.pack(side=tk.TOP, fill=tk.X, padx=8, pady=4)

                def add_row(label, value):
                    row = ttk.Frame(info_frame)
                    row.pack(side=tk.TOP, anchor="w", pady=2, fill=tk.X)
                    ttk.Label(row, text=f"{label}:", width=16, anchor="w").pack(
                        side=tk.LEFT
                    )
                    ttk.Label(row, text=str(value), anchor="w").pack(
                        side=tk.LEFT, fill=tk.X, expand=True
                    )

                add_row("Run ID", run.id)
                add_row("Vendor", run.vendor_id)
                add_row("Profile", run.profile_name)
                add_row("Status", run.status)
                add_row("Source", run.source_name)
                add_row("Created", run.created_at)
                add_row("Rows total", run.rows_total)
                add_row("Valid", run.rows_valid)
                add_row("Invalid", run.rows_invalid)
                add_row("Duplicates", run.duplicates)
                add_row("Unique SKUs", run.unique_skus)
                add_row("Uploaded", run.rows_uploaded)
                add_row("Retries", f"{run.retry_count}/{run.max_retries}")

                err_frame = ttk.LabelFrame(win, text="Error Message")
                err_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=8, pady=4)

                txt = tk.Text(err_frame, wrap="word", height=6)
                txt.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
                if run.error_message:
                    txt.insert("1.0", run.error_message)
                txt.config(state="disabled")

                yscroll = ttk.Scrollbar(err_frame, orient="vertical", command=txt.yview)
                txt.configure(yscrollcommand=yscroll.set)
                yscroll.pack(side=tk.RIGHT, fill=tk.Y)

            finally:
                session.close()

        # Retry handling -------------------------------------------------------

        def get_selected_run_ids(self) -> List[int]:
            ids: List[int] = []
            for item_id in self.tree.selection():
                item = self.tree.item(item_id)
                try:
                    ids.append(int(item["values"][0]))
                except Exception:
                    continue
            return ids

        def retry_selected(self) -> None:
            run_ids = self.get_selected_run_ids()
            if not run_ids:
                messagebox.showinfo("Retry", "Select one or more runs to retry.")
                return
            self.enqueue_retries(run_ids)

        def retry_all_failed(self) -> None:
            ids: List[int] = []
            for item_id in self.tree.get_children():
                item = self.tree.item(item_id)
                run_id = int(item["values"][0])
                status = item["values"][5]  # status column
                if status == "failed":
                    ids.append(run_id)

            if not ids:
                messagebox.showinfo("Retry", "No failed runs in the current view.")
                return

            self.enqueue_retries(ids)

        def enqueue_retries(self, run_ids: List[int]) -> None:
            for run_id in run_ids:
                self.retry_queue.put(run_id)
            self.start_worker()

        def start_worker(self) -> None:
            if self.worker_thread and self.worker_thread.is_alive():
                return
            self.worker_thread = threading.Thread(
                target=self._retry_worker_loop, daemon=True
            )
            self.worker_thread.start()

        def _retry_worker_loop(self) -> None:
            while not self.retry_queue.empty():
                run_id = self.retry_queue.get()
                self._update_status(f"Retrying run {run_id}...")
                try:
                    retry_run_once(run_id)
                except Exception as exc:
                    msg = f"Error retrying run {run_id}: {exc}"
                    self.after(0, lambda m=msg: messagebox.showerror("Retry error", m))
                finally:
                    self.retry_queue.task_done()
                    self.after(0, self.refresh_runs)
            self._update_status("Idle")

        def _update_status(self, text: str) -> None:
            def _set():
                self.status_label.config(text=text)
            self.after(0, _set)


    if __name__ == "__main__":
        app = IngestDashboard()
        app.mainloop()
    """
    write_file("ingest_dashboard.py", ingest_dashboard_py)

    # -------------------------------------------------------------------------
    # configs (dev / prod) – placeholders, user must align with dmt.exe settings
    # -------------------------------------------------------------------------
    json_config_dev = r"""
    {
      "source": {
        "name": "JSON",
        "settings": {
          "file_path": "REPLACED_BY_CLI",
          "mode": "Lines"
        }
      },
      "sink": {
        "name": "Cosmos-nosql",
        "settings": {
          "connection_string": "REPLACED_BY_CLI_OR_ENV",
          "database_name": "soh",
          "container_name": "dropshipPricingTest_dev",
          "partition_key_path": "/id",
          "allow_partial_upload": true,
          "log_level": "WARNING"
        }
      }
    }
    """
    write_file("configs/json_to_cosmos.dev.migrationsettings.json", json_config_dev)

    json_config_prod = r"""
    {
      "source": {
        "name": "JSON",
        "settings": {
          "file_path": "REPLACED_BY_CLI",
          "mode": "Lines"
        }
      },
      "sink": {
        "name": "Cosmos-nosql",
        "settings": {
          "connection_string": "REPLACED_BY_CLI_OR_ENV",
          "database_name": "soh",
          "container_name": "dropshipPricingTest",
          "partition_key_path": "/id",
          "allow_partial_upload": true,
          "log_level": "WARNING"
        }
      }
    }
    """
    write_file("configs/json_to_cosmos.prod.migrationsettings.json", json_config_prod)

    # -------------------------------------------------------------------------
    # win-x64-package placeholder
    # -------------------------------------------------------------------------
    win_pkg_readme = r"""
    Put `dmt.exe` and its `Extensions` folder in this directory:

        cosmos_ingest_pipeline/
          win-x64-package/
            dmt.exe
            Extensions/

    The Python scripts call `win-x64-package/dmt.exe` by default.
    """
    write_file("win-x64-package/README.txt", win_pkg_readme)

    # -------------------------------------------------------------------------
    # .env.example
    # -------------------------------------------------------------------------
    env_example = r"""
    # Environment: dev or prod
    APP_ENV=dev

    # Cosmos connection string – used by pipeline & dashboard
    COSMOS_CONN=AccountEndpoint=https://YOUR-ACCOUNT.documents.azure.com:443/;AccountKey=YOUR-KEY==;
    """
    write_file(".env.example", env_example)

    # -------------------------------------------------------------------------
    # requirements.txt
    # -------------------------------------------------------------------------
    requirements = r"""
    sqlalchemy>=2.0.0
    """
    write_file("requirements.txt", requirements)

    # -------------------------------------------------------------------------
    # README.md
    # -------------------------------------------------------------------------
    readme = r"""
    # Cosmos Ingest Pipeline (CSV/JSON → Cosmos)

    This project is a small but robust skeleton for ingesting CSV/JSON into
    Azure Cosmos DB (NoSQL), using:

    - A **validation + stats pipeline** (`pipeline.py`)
    - A **SQLite metadata DB** (runs + rows + vendor profiles)
    - A **Tk dashboard** with retry queue (`ingest_dashboard.py`)
    - `dmt.exe` as the data mover (JSON → Cosmos, via `win-x64-package/dmt.exe`)

    ## Layout

    - `models.py` – SQLAlchemy models:
      - `VendorProfile` – per-vendor config (ID pattern, postcode rules, cosmos settings)
      - `IngestRun` – per-run stats and status
      - `IngestRow` – per-row data and uniqueness constraint
    - `pipeline.py` – processes:
      - `data/incoming/csv/*.csv`
      - `data/incoming/json/*.json`
      - validates + normalizes data
      - writes NDJSON to `data/sanitized`
      - calls `dmt.exe` (JSON → Cosmos)
      - archives input files under `data/archive/...`
    - `ingest_dashboard.py` – Tk dashboard for:
      - viewing `ingest_runs`
      - inspecting errors
      - retrying failed runs (queue-based, background worker)
    - `configs/` – dev/prod JSON settings for the DMT Cosmos extension
    - `db/` – per-environment SQLite files:
      - `ingest_stats.dev.db`
      - `ingest_stats.prod.db`
    - `win-x64-package/` – expected location of `dmt.exe`

    ## Environments

    The environment is controlled by `APP_ENV`:

    - `APP_ENV=dev` (default)
    - `APP_ENV=prod`

    It affects:

    - SQLite DB path: `db/ingest_stats.<ENV>.db`
    - DMT settings path: `configs/json_to_cosmos.<ENV>.migrationsettings.json`

    `COSMOS_CONN` must be set (e.g. from `.env`, or user / system env):

    ```bash
    set APP_ENV=dev
    set COSMOS_CONN=AccountEndpoint=...;AccountKey=...;
    ```

    ## Running

    1. Install dependencies:

       ```bash
       pip install -r requirements.txt
       ```

    2. Copy `dmt.exe` (and its `Extensions` folder) into:

       ```text
       win-x64-package/
       ```

    3. Drop CSV files into:

       ```text
       data/incoming/csv/
       ```

       or JSON files into:

       ```text
       data/incoming/json/
       ```

    4. Run the pipeline:

       ```bash
       python pipeline.py
       ```

    5. Inspect runs / retry failed:

       ```bash
       python ingest_dashboard.py
       ```

    Adjust the configs under `configs/` to match the actual DMT extension
    settings (use `dmt.exe settings` to see the exact property names).
    """
    write_file("README.md", readme)

    print("\nDone. Next steps:")
    print(f"  1) cd {PROJECT_ROOT}")
    print("  2) pip install -r requirements.txt")
    print("  3) Put dmt.exe + Extensions into win-x64-package/")
    print("  4) Set APP_ENV and COSMOS_CONN, then run:")
    print("       python pipeline.py")
    print("       python ingest_dashboard.py")


if __name__ == "__main__":
    main()
````

---

If you want, next step we can tweak this bootstrapper to:

* integrate `python-dotenv` loading, or
* add a minimal `invoke_dmt.ps1` wrapper in `scripts/` for scheduled runs.
