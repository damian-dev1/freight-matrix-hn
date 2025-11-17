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
