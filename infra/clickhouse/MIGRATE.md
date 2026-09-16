# ClickHouse 25.10 → 26.8 (prod)

On the host as `databarn`. Needs ~120GB free on `/mnt/data` (25.10 is kept until soak). AVX2 required.

Nothing is deleted. Data dirs are only moved aside.

## 1. Look

```bash
cd ~/beefy-databarn
git pull
cd infra/prod
make clickhouse preflight
```

You want: version `25.10.*`, `avx2: yes`, backups listed, enough disk.

## 2. Cut over

```bash
make clickhouse migrate CONFIRM=1
```

Stops dlt/dbt, takes a last incremental, restores it onto empty 26.8, then points `/mnt/data/clickhouse` at 26.8. Old data stays at `/mnt/data/clickhouse.bak`. Pins `CLICKHOUSE_VERSION=26.8.4` in `.env` only after the live path is 26.8.

Downtime is mostly the restore (~100GB).

If it dies, run the **same command** again. It resumes.

Optional: `BACKUP=inc-YYYY-MM-DD-HH` to restore a specific prefix instead of the incremental it just took.

Do not `rm -rf` anything under `/mnt/data`. Do not `make infra start` while migrate is running.

## 3. If 26.8 is wrong

```bash
make clickhouse rollback CONFIRM=1
```

Safe mid-restore (25.10 is still live until the final rename) and after cutover (puts `clickhouse.bak` back, pins `CLICKHOUSE_VERSION=25.10`).

## 4. After soak

Keep `clickhouse.bak` until you trust 26.8. Then move it aside yourself if you need the disk.

`make infra start` after this uses 26.8 because migrate pinned `.env`.
