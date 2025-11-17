from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path.cwd() / "cosmos_ingest_pipeline"


def create_dirs(dirs: list[str]) -> None:
    for rel in dirs:
        path = PROJECT_ROOT / rel
        path.mkdir(parents=True, exist_ok=True)
        print(f"[DIR]  {path}")


def create_files(files: list[str]) -> None:
    for rel in files:
        path = PROJECT_ROOT / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            path.touch()
            print(f"[FILE] {path}")
        else:
            print(f"[SKIP] {path} already exists")


def main() -> None:
    print(f"Project root: {PROJECT_ROOT}")
    PROJECT_ROOT.mkdir(parents=True, exist_ok=True)

    # Directories for dev/prod-ready layout
    dirs = [
        "db",                              # SQLite per env: ingest_stats.dev.db / ingest_stats.prod.db
        "configs",                         # json_to_cosmos.dev/prod.migrationsettings.json
        "data/incoming/csv",
        "data/incoming/json",
        "data/archive/csv/success",
        "data/archive/csv/failed",
        "data/archive/json/success",
        "data/archive/json/failed",
        "data/sanitized",                  # NDJSON / sanitized payloads
        "win-x64-package",                 # dmt.exe + Extensions go here
        "scripts",                         # optional for future schedulers/wrappers
    ]

    create_dirs(dirs)

    # Empty placeholder files you’ll paste real code/config into later
    files = [
        "models.py",
        "pipeline.py",
        "ingest_dashboard.py",

        # Configs for dev/prod
        "configs/json_to_cosmos.dev.migrationsettings.json",
        "configs/json_to_cosmos.prod.migrationsettings.json",

        # Env/config stubs
        ".env.example",
        "requirements.txt",
        "README.md",

        # Optional script placeholders
        "scripts/invoke_dmt.ps1",
        "scripts/run_pipeline_dev.ps1",
        "scripts/run_pipeline_prod.ps1",
    ]

    create_files(files)

    print("\nScaffolding complete.")
    print(f"cd {PROJECT_ROOT} to start filling in code and configs.")


if __name__ == "__main__":
    main()

# bootstrap full

from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path.cwd() / "cosmos_ingest_pipeline"


def create_dirs(dirs: list[str]) -> None:
    for rel in dirs:
        path = PROJECT_ROOT / rel
        path.mkdir(parents=True, exist_ok=True)
        print(f"[DIR]  {path}")


def create_files(files: list[str]) -> None:
    for rel in files:
        path = PROJECT_ROOT / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            path.touch()
            print(f"[FILE] {path}")
        else:
            print(f"[SKIP] {path} already exists")


def main() -> None:
    print(f"Project root: {PROJECT_ROOT}")
    PROJECT_ROOT.mkdir(parents=True, exist_ok=True)

    # Directories for dev/prod-ready layout
    dirs = [
        "db",                              # SQLite per env: ingest_stats.dev.db / ingest_stats.prod.db
        "configs",                         # json_to_cosmos.dev/prod.migrationsettings.json
        "data/incoming/csv",
        "data/incoming/json",
        "data/archive/csv/success",
        "data/archive/csv/failed",
        "data/archive/json/success",
        "data/archive/json/failed",
        "data/sanitized",                  # NDJSON / sanitized payloads
        "win-x64-package",                 # dmt.exe + Extensions go here
        "scripts",                         # optional for future schedulers/wrappers
    ]

    create_dirs(dirs)

    # Empty placeholder files you’ll paste real code/config into later
    files = [
        "models.py",
        "pipeline.py",
        "ingest_dashboard.py",

        # Configs for dev/prod
        "configs/json_to_cosmos.dev.migrationsettings.json",
        "configs/json_to_cosmos.prod.migrationsettings.json",

        # Env/config stubs
        ".env.example",
        "requirements.txt",
        "README.md",

        # Optional script placeholders
        "scripts/invoke_dmt.ps1",
        "scripts/run_pipeline_dev.ps1",
        "scripts/run_pipeline_prod.ps1",
    ]

    create_files(files)

    print("\nScaffolding complete.")
    print(f"cd {PROJECT_ROOT} to start filling in code and configs.")


if __name__ == "__main__":
    main()

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

        """
    # --- helpers --------------------------------------------------------------


    def infer_vendor_id_from_filename(path: Path) -> str:
        """
        'VENDOR123_pricing_20250101.csv' -> 'VENDOR123'
        # Adjust this logic to your naming convention if needed.
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
          "allow_partial_upload": True,
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
          "allow_partial_upload": True,
          "log_level": "WARNING"
        }
      }
    }
    """
    write_file("configs/json_to_cosmos.prod.migrationsettings.json", json_config_prod)
        """
    # -------------------------------------------------------------------------
    # win-x64-package placeholder
    # -------------------------------------------------------------------------
    win_pkg_readme = r"""
    Put `dmt.exe` and its `Extensions` folder in this directory:

        cosmos_ingest_pipeline/
          win-x64-package/
            dmt.exe
            Extensions/
        """
    # The Python scripts call `win-x64-package/dmt.exe` by default.
    """
    write_file("win-x64-package/README.txt", win_pkg_readme)

    # -------------------------------------------------------------------------
    # .env.example
    # -------------------------------------------------------------------------
    env_example = r"""
    # Environment: dev or prod
    # APP_ENV=dev

    # Cosmos connection string – used by pipeline & dashboard
    "COSMOS_CONN=AccountEndpoint=https://YOUR-ACCOUNT.documents.azure.com:443/;AccountKey=YOUR-KEY==;"
    """
    write_file(".env.example", env_example)

    # -------------------------------------------------------------------------
    # requirements.txt
    # -------------------------------------------------------------------------
    requirements = r"""
    # sqlalchemy>=2.0.0
    """
    write_file("requirements.txt", requirements)

    # -------------------------------------------------------------------------
    # README.md
    # -------------------------------------------------------------------------
    readme = r"""
    # Cosmos Ingest Pipeline (CSV/JSON → Cosmos)
    """
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
