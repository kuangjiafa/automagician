# Automagician — Project Instructions

Automates the submission and lifecycle management of [VASP](https://www.vasp.at/) geometry optimisation (NEB) calculations on HPC clusters (FRI, Halifax, Stampede2, Frontera, LS6). Tracks every job in a local SQLite database, detects convergence, fixes common VASP errors, spawns follow-on DOS/WAV calculations, and optionally balances load across machines via SSH/SCP.

CLI entry point: `automagician` → `automagician:main.main` (see [pyproject.toml](pyproject.toml)).

## Module layout — `src/automagician/`

- [main.py](src/automagician/main.py) — CLI entry. `set_up_parser` defines all flags; `main_wrapper` runs Phase 1 (init: machine detection, lockfile, DB, squeue/sacct sync) then Phase 2 (execute register/process/reset, then submit queue and write statuses in `finally`).
- [classes.py](src/automagician/classes.py) — domain types. `JobStatus` (CONVERGED=0, INCOMPLETE=1, ERROR=2, RUNNING=-1, NOT_FOUND=-10), `Machine` (FRI, HALIFAX, STAMPEDE2_TACC, FRONTERA_TACC, LS6_TACC, UNKNOWN), `OptJob`/`DosJob`/`WavJob`/`GoneJob` dataclasses, `SSHConfig`/`SshScp` wrappers (fabric optional), `JobLimitError`.
- [constants.py](src/automagician/constants.py) — lockfile paths, DB name, remote dir prefix, subfile paths, TACC queue limits. Several paths are hardcoded to specific users' home dirs (see backlog item #7).
- [database.py](src/automagician/database.py) — `Database` wraps sqlite3. Five tables: `opt_jobs`, `dos_jobs`, `wav_jobs`, `gone_jobs`, `insta_submit`. `dos_jobs`/`wav_jobs` reference `opt_jobs` by rowid (`opt_id`). `write_job_statuses` batches inserts/updates via `executemany` for performance; orphan rows are logged and skipped in `get_dos_jobs`/`get_wav_jobs`.
- [machine.py](src/automagician/machine.py) — `get_machine_number` maps hostname → `Machine`. `ssh_scp_init` opens a fabric connection when `--balance` is set and machine is FRI/Halifax. `write_lockfile` enforces secure lockdir ownership/mode (0700); `automagic_exit` cleans up.
- [register.py](src/automagician/register.py) — `register` walks `cwd`, skipping directories that match `_EXCLUDE_REGEX` (compiled once at module scope) for `run*`, `dos`, `sc`, `ini`, `fin`, `wav`. Detects NEB bundles (band+ini+fin), reads `automagic_note` for `dos`/`wav`/`exclude` markers, then hands off to `process_queue`.
- [process_job.py](src/automagician/process_job.py) — largest module. `process_opt`/`process_dos`/`process_wav` are the state machines. `determine_convergence` runs `vef.pl`, greps `ll_out`, handles box relaxation (ISIF=3) via `determine_box_convergence`. `get_submitted_jobs` syncs state with `squeue`; `_log_sacct_failures` queries `sacct` for jobs that disappeared from the queue. `submit_queue` batches `sbatch` calls, optionally balancing across FRI↔Halifax via SSH or across TACC queues proportionally. `gone_job_check` moves missing opt_jobs into the `gone_jobs` table.
- [create_job.py](src/automagician/create_job.py) — builds `dos/`, `sc/`, `wav/` subdirectories from a converged parent. `copy_inputs` copies KPOINTS/POTCAR/INCAR/CHGCAR/subfile and prefers CONTCAR over POSCAR. `add_to_sub_queue` enqueues and raises `JobLimitError` when `limit` is breached (unless `continue_past_limit`).
- [finish_job.py](src/automagician/finish_job.py) — `wrap_up` runs `vfin.pl` into `run{N}/`, picking the next free index. `give_certificate` creates the `convergence_certificate` file. `sc_is_complete`/`dos_is_complete`/`wav_is_complete` consider a calc done if CHGCAR/DOSCAR/WAVECAR hasn't been written for 2 minutes.
- [update_job.py](src/automagician/update_job.py) — `fix_error` handles ZBRENT (re-wraps and resubmits) and POTCAR-count errors (calls external `sortpos.py`/`sogetsoftpbe.py`). `set_incar_tags` edits INCAR tags in place. `update_job_name` rewrites `#SBATCH -J` to `AM_<cwd-path>`.
- [small_functions.py](src/automagician/small_functions.py) — `classify_job_dir` (→ `"dos"|"sc"|"wav"|"opt"`), `get_opt_dir` (strips trailing `/dos|/sc|/wav`). The SSH-dependent `scp_get_dir` lives here under a `try/except ImportError` fallback. Deprecated `archive_converged`/`reset_converged` still exist for the flat-file era (see backlog #2).
- [vfin.pl](src/automagician/vfin.pl) — Perl helper shipped with the package; path is `constants.V_FIN_PL_PATH`.

## Job lifecycle

An opt job lives in a directory containing `POSCAR POTCAR INCAR KPOINTS <subfile>`. `check_has_opt` gates everything on those five files. Registration adds the dir to `opt_jobs` with status INCOMPLETE. `process_opt` branches:

1. RUNNING — write preliminary results, do nothing.
2. No `ll_out` — resubmit.
3. Error in `ll_out` (`"I REFUSE TO CONTINUE WITH THIS SICK JOB"`) — log, attempt `fix_error`.
4. Otherwise — `determine_convergence` via `vef.pl`; if converged, `give_certificate` and set status CONVERGED; else `wrap_up` (if CONTCAR present), record preliminary result, and resubmit.

DOS/WAV jobs only start after the parent opt is CONVERGED. `process_dos` creates `sc/` first, waits for it to complete, then creates `dos/` from it. `process_wav` creates a `wav/` sibling with INCAR tags `IBRION=-1, LWAVE=.TRUE., NSW=0`.

Special directory names reserved (case-insensitive where noted): `run*`, `dos`, `sc`, `ini`/`Ini`, `fin`/`Fin`, `wav`, `band`/`Band` — do not nest these.

## Build, test, lint

Everything goes through [build.sh](build.sh):

- `./build.sh create` — `uv venv .venv`
- `./build.sh install_dev` — `uv pip install -e ".[dev,remote]"`
- `./build.sh test` — `pytest --cov=automagician test/`
- `./build.sh lint` — `ruff format`, `isort --profile=black`, `mypy --strict --disallow-untyped-defs`, `ruff check`
- `./build.sh release` — lint + test + build

`pyproject.toml` requires Python >=3.10. Pytest pythonpath is `src`; `--import-mode=importlib`. CI runs `pylint --errors-only` on push ([.github/workflows/pylint.yml](.github/workflows/pylint.yml)).

## Testing conventions

See [DEVELOPMENT.md](DEVELOPMENT.md). Tests live in `test/`, fixtures in `test/test_files/` (treat read-only — copy into `tmp_path` before mutating). Avoid mocking; the only permitted mock is `sbatch`. Use `tmp_path` for per-test SQLite DBs. Prefer assertion coverage over line coverage.

## Conventions

- New constants go in [constants.py](src/automagician/constants.py) when adding to the codebase.
- Type hints are required — mypy runs in `--strict` mode.
- `fabric` is an optional dependency; code paths that use SSH/SCP must degrade to no-ops under `ImportError` (see `classes.py`, `small_functions.py`, `machine.py` for the pattern).
- Database writes: prefer the batching `write_job_statuses` over per-job `add_*_to_db` when updating many rows. When doing many single-row writes in a loop, pass `commit=False` and commit once at the end.
- Lockfile and DB live in `$HOME` on FRI/Halifax, in `$WORK/..` on TACC — the path logic is in `main.main_wrapper`.
- Directory paths flowing through the database are absolute; `get_opt_dir` / `classify_job_dir` both use `os.path.normpath` — pass normalized paths in.

## Known issues / backlog

See the project memory file `project_backlog.md` (in the user's auto-memory) for seven ready-to-execute fix plans: `hit_limit` propagation, deprecated-flag cleanup, dead `opt_dir` class attrs, `get_residueSFE` stub, `combine_XDAT_FE` decision, hardcoded user paths in constants.
