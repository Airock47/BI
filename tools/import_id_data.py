#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Import user accounts from Excel into database/id_database.db (table: id_data).
- Adds missing columns: department (TEXT), is_supervisor (TEXT)
- Upserts by username (update if exists, otherwise insert)
- Creates a timestamped backup before writing
"""

import os
import sys
import shutil
import sqlite3
from datetime import datetime
from typing import List

import pandas as pd


def ensure_dirs(path: str) -> None:
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


def backup_db(db_path: str) -> str:
    ts = datetime.now().strftime('%Y%m%d-%H%M%S')
    backup_path = os.path.join(os.path.dirname(db_path), f"id_database.backup-{ts}.db")
    shutil.copy2(db_path, backup_path)
    return backup_path


def get_table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    cur = conn.execute(f"PRAGMA table_info({table});")
    return [row[1] for row in cur.fetchall()]


def add_column_if_missing(conn: sqlite3.Connection, table: str, column: str, col_type: str) -> None:
    cols = set(get_table_columns(conn, table))
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type};")


def upsert_user(conn: sqlite3.Connection, row: dict) -> None:
    # Upsert by username: try update, if 0 rows affected then insert
    cur = conn.execute(
        """
        UPDATE id_data
        SET password = COALESCE(?, password),
            name = COALESCE(?, name),
            department = COALESCE(?, department),
            is_supervisor = COALESCE(?, is_supervisor),
            is_resigned = COALESCE(?, is_resigned)
        WHERE username = ?
        """,
        (
            row.get('password'),
            row.get('name'),
            row.get('department'),
            row.get('is_supervisor'),
            row.get('is_resigned'),
            row['username'],
        ),
    )
    if cur.rowcount == 0:
        conn.execute(
            """
            INSERT INTO id_data (username, password, name, department, is_supervisor, is_resigned)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                row['username'],
                row.get('password', ''),
                row.get('name', ''),
                row.get('department', ''),
                row.get('is_supervisor', ''),
                row.get('is_resigned', ''),
            ),
        )


def normalize_is_supervisor(val):
    if pd.isna(val):
        return ''
    s = str(val).strip()
    if not s:
        return ''
    # Accept common truthy values
    truthy = {'y', 'yes', 'true', '1', 't', '是', '主管'}
    falsy = {'n', 'no', 'false', '0', 'f', '否'}
    ls = s.lower()
    if ls in truthy:
        return 'Y'
    if ls in falsy:
        return 'N'
    # pass-through original (e.g., already 'Y'/'N')
    return s


def normalize_is_resigned(val):
    if pd.isna(val):
        return ''
    s = str(val).strip()
    if not s:
        return ''
    truthy = {'y', 'yes', 'true', '1', 't', '是', '離職', '已離職'}
    falsy = {'n', 'no', 'false', '0', 'f', '否', '在職'}
    ls = s.lower()
    if ls in truthy:
        return 'Y'
    if ls in falsy:
        return 'N'
    return s


def main():
    root = os.path.dirname(os.path.abspath(__file__))
    bi_root = os.path.dirname(root)

    # Paths
    db_path = os.path.join(bi_root, 'database', 'id_database.db')
    excel_default = os.path.join(bi_root, '資料來源', '員工帳號.xlsx')
    excel_path = sys.argv[1] if len(sys.argv) > 1 else excel_default

    if not os.path.exists(db_path):
        print(f"ERROR: DB not found: {db_path}")
        sys.exit(1)
    if not os.path.exists(excel_path):
        print(f"ERROR: Excel not found: {excel_path}")
        sys.exit(1)

    # Load Excel (first sheet)
    df = pd.read_excel(excel_path)
    # Normalize expected columns
    required_cols = ['username', 'password', 'name']
    optional_cols = ['department', 'is_supervisor', 'is_resigned']
    missing_required = [c for c in required_cols if c not in df.columns]
    if missing_required:
        print(f"ERROR: Missing required columns in Excel: {missing_required}")
        sys.exit(1)

    for c in optional_cols:
        if c not in df.columns:
            df[c] = ''

    # Clean values
    df['username'] = df['username'].astype(str).str.strip()
    df['password'] = df['password'].astype(str).str.strip()
    df['name'] = df['name'].astype(str).str.strip()
    df['department'] = df['department'].astype(str).fillna('').str.strip()
    df['is_supervisor'] = df['is_supervisor'].apply(normalize_is_supervisor)
    df['is_resigned'] = df['is_resigned'].apply(normalize_is_resigned)

    # Drop empty usernames
    df = df[df['username'] != '']

    # Show preview
    print('Excel preview:')
    print(df.head().to_string(index=False))
    print(f"Total rows to process: {len(df)}")

    # Backup
    backup_path = backup_db(db_path)
    print(f"Backup created: {backup_path}")

    # Apply to DB
    with sqlite3.connect(db_path) as conn:
        # Ensure columns exist
        add_column_if_missing(conn, 'id_data', 'department', 'TEXT')
        add_column_if_missing(conn, 'id_data', 'is_supervisor', 'TEXT')
        add_column_if_missing(conn, 'id_data', 'is_resigned', 'TEXT')

        # Process rows
        processed = 0
        for _, r in df.iterrows():
            row = {
                'username': r['username'],
                'password': r.get('password', ''),
                'name': r.get('name', ''),
                'department': r.get('department', ''),
                'is_supervisor': r.get('is_supervisor', ''),
                'is_resigned': r.get('is_resigned', ''),
            }
            upsert_user(conn, row)
            processed += 1

        conn.commit()

    print(f"Completed. Rows processed: {processed}")

    # Show final row count
    with sqlite3.connect(db_path) as conn:
        cur = conn.execute('SELECT COUNT(*) FROM id_data;')
        total = cur.fetchone()[0]
        print(f"id_data row count now: {total}")


if __name__ == '__main__':
    main()
