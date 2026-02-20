#!/usr/bin/env python3
"""
Sync all Superset datasources (refresh dataset metadata from databases).
Runs on every Superset restart when SUPERSET_ENV=production.
"""
import sys

sys.path.insert(0, "/app")


def main():
    try:
        from superset.app import create_app
    except ImportError as e:
        print(f"Sync datasources: Superset not ready ({e})")
        sys.exit(0)

    app = create_app()
    with app.app_context():
        from superset.extensions import db

        # Refresh all datasets (table/column metadata) from their databases
        try:
            from superset.connectors.sqla.models import SqlaTable
        except ImportError:
            try:
                from superset.datasets.models import Dataset as SqlaTable
            except ImportError:
                print("Sync datasources: could not import Dataset model, skipping refresh")
                sys.exit(0)

        datasets = db.session.query(SqlaTable).all()
        total = len(datasets)
        synced = 0
        errors = 0
        for ds in datasets:
            try:
                if hasattr(ds, "fetch_metadata"):
                    ds.fetch_metadata()
                    db.session.commit()
                    synced += 1
                elif hasattr(ds, "fetch_columns"):
                    ds.fetch_columns()
                    db.session.commit()
                    synced += 1
            except Exception as e:
                errors += 1
                db.session.rollback()
                print(f"  Warning: sync failed for dataset {getattr(ds, 'table_name', ds.id)}: {e}")

        print(f"Sync datasources: {total} dataset(s), refreshed {synced}, {errors} error(s)")


if __name__ == "__main__":
    main()
