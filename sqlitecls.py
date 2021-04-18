#!/usr/bin/env python3
# encoding: utf-8
#
# Copyright © 2021, Matjaž Guštin <dev@matjaz.it> <https://matjaz.it>.
# Released under the BSD 3-Clause License

import sqlite3
from typing import Optional, List, Any


class SqliteDb:
    """Class wrapper of an SQLite connection that can be reopened, loading
    a DB-initialisations script when creating the file the first time.
    """
    Q_COUNT_TABLES = "SELECT count(*) FROM sqlite_master WHERE type='table'"
    Q_TABLES = "SELECT name FROM sqlite_master WHERE type='table'"
    Q_START = 'START TRANSACTION'
    Q_ROLLBACK = 'ROLLBACK'
    Q_COMMIT = 'COMMIT'
    Q_VACUUM = 'VACUUM'
    PRAGMAS = " " \
              "" \
              "PRAGMA journal_mode = WAL; " \
              "PRAGMA cache_size = -100000; " \
              "PRAGMA synchronous = EXTRA; " \
              "PRAGMA temp_store = memory; "
    PRAGMAS_PER_CONNECTION = "PRAGMA encoding = 'UTF-8'; " \
                             "PRAGMA foreign_keys = ON; "
    PRAGMAS_PER_DB = ""
    PRAGMAS_HARD_CONSISTENCY = ""

    def __init__(self, db_file_name: str,
                 init_script_file_name: Optional[str] = None):
        """Prepares a closed connection, to be opened when entering the
        context (__enter__) using the `with` operator.

        Upon entering, the init script is loaded if the DB has not tables.

        Args:
            db_file_name: path to the SQLite database file to open or to
                create.
            init_script_file_name: path to the SQL script that fills the
                database only when it's created for the first time.
                When None, no init operation is performed.
                Must be UTF-8 encoded and relatively small, as it's loaded
                wholly into memory.
        """
        self.db_file_name = db_file_name
        self.init_script_file_name = init_script_file_name
        self.connection: Optional[sqlite3.Connection] = None
        self.cursor: Optional[sqlite3.Cursor] = None

    def __enter__(self) -> 'SqliteDb':
        """Enter the DB context: connect to it and load the init script
        if the DB file was newly created."""
        self.connection = sqlite3.connect(self.db_file_name)
        self.cursor = self.connection.cursor()
        if self.init_script_file_name and self.is_empty_db():
            self.execute_file(self.init_script_file_name)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Close the connection gracefully."""
        if self.cursor:
            self.cursor.close()
            self.cursor = None
        if self.connection:
            self.connection.close()
            self.connection = None

    def execute_file(self, file_name: str,
                     encoding: str = 'UTF-8') -> sqlite3.Cursor:
        with open(file_name, encoding=encoding) as script:
            return self.cursor.executescript(script.read())

    def is_empty_db(self) -> bool:
        """Returns true if the DB contains no tables, thus it's empty."""
        return self.cursor.execute(self.Q_COUNT_TABLES).fetchone()[0] == 0

    def __getitem__(self, item: str) -> Any:
        return self.cursor.execute('PRAGMA ?', (item,)).fetchone()[0]

    def __setitem__(self, key, value) -> None:
        self.cursor.execute('PRAGMA ? = ?', (key, value))

    def start_transaction(self) -> None:
        """Starts a transaction on the cursor."""
        self.cursor.execute(self.Q_START)

    def rollback(self) -> None:
        """Rolls back the transaction on the cursor."""
        self.cursor.execute(self.Q_ROLLBACK)

    def commit(self) -> None:
        """Commits the transaction on the cursor."""
        self.cursor.execute(self.Q_COMMIT)

    def vacuum(self) -> None:
        """Vacuums the database."""
        self.cursor.execute(self.Q_VACUUM)

    def tables_names(self) -> List[str]:
        """Fetches the names of all tables in the DB."""
        return [row[0] for row in self.cursor.execute(self.Q_TABLES)]

    def backup(self):
        pass

    def import(self):
        pass

    def export(self):
        pass

    def check(self):
        self['foreign_key_check']  # TODO check output
        self['integrity_check']  # Faster version: quick_check
        pass  # .lint + pragma integrity check

    def isolation_level(self):
        pass

    def journal_mode(self):
        pass

    def syncrhonisation(self):
        pass

    def db_size(self):
        pass  # SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size();
        # Also PRAGMA page_count; * PRAGMA page_size;
