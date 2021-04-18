#!/usr/bin/env python3
# encoding: utf-8
#
# Copyright © 2021, Matjaž Guštin <dev@matjaz.it> <https://matjaz.it>.
# Released under the BSD 3-Clause License

"""SQLiteDb class wrapping the operations from the `sqlite3` module,
providing better support for the `with` operator and automatic
initialisation of an empty DB upon first creation of the DB.

Additional wrappers are available as a utility, including
extraction of a list of tables, extraction of the list of columns
of a `SELECT` query, commit, rollback, start of a transaction,
check if the DB is in memory and if it's empty. Executions of SQL script
files is also made easy."""

import sqlite3
from typing import Optional, List

__VERSION__ = '0.1.0'


def cursor_column_names(cursor: sqlite3.Cursor) -> List[str]:
    """Extracts the column names out of a cursor.

    Very useful to get the list of column names for a just-performed
    SELECT query. Does not alter the cursor."""
    return [col[0] for col in cursor.description]


class SqliteDb:
    """Class wrapper of an SQLite connection that can be reopened, loading
    a DB-initialisations script when creating the file the first time."""

    def __init__(self, db_file_name: str = ':memory:',
                 init_script_file_name: Optional[str] = None):
        """Prepares a closed connection, to be opened when entering the
        context (__enter__) using the `with` operator.

        Upon entering, the init script is loaded if the DB has not tables.

        Args:
            db_file_name: path to the SQLite database file to open or to
                create. Default to an in-memory database.
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

    def open(self) -> 'SqliteDb':
        """Connect to the DB and load the init script if the DB file was newly
        created."""
        self.connection = sqlite3.connect(self.db_file_name)
        self.cursor = self.connection.cursor()
        if self.init_script_file_name and self.is_empty_db():
            self.execute_sql_file(self.init_script_file_name)
        return self

    def close(self, commit: bool = False) -> None:
        """Close the connection gracefully."""
        if self.cursor:
            self.cursor.close()
            self.cursor = None
        if self.connection:
            if commit:
                self.connection.commit()
            self.connection.close()
            self.connection = None

    def __enter__(self) -> 'SqliteDb':
        """Enter the DB context: connect to it and load the init script
        if the DB file was newly created."""
        return self.open()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Close the connection gracefully."""
        self.close()

    def __del__(self) -> None:
        """Close the connection gracefully on destruction."""
        self.close()

    def execute_sql_file(self, file_name: str,
                         encoding: str = 'UTF-8') -> sqlite3.Cursor:
        """Executes all the SQL queries loaded from a file."""
        with open(file_name, encoding=encoding) as script:
            return self.cursor.executescript(script.read())

    def is_empty_db(self) -> bool:
        """Returns true if the DB contains no objects, thus it's empty."""
        cursor = self.cursor.execute(
            "SELECT count(*) = 0 FROM sqlite_master")
        return cursor.fetchone()[0]

    def execute(self, query: str, args: tuple = ()) -> sqlite3.Cursor:
        """Executes a query using the pre-loaded cursor.
        Refer to sqlite3.Cursor.execute() for more details."""
        return self.cursor.execute(query, args)

    def start_transaction(self) -> None:
        """Starts a transaction on the cursor."""
        self.cursor.execute("BEGIN TRANSACTION")

    def rollback(self) -> None:
        """Rolls back the transaction on the cursor."""
        self.cursor.execute("ROLLBACK")

    def commit(self) -> None:
        """Commits the transaction on the cursor."""
        self.cursor.execute("COMMIT")

    def vacuum(self) -> None:
        """Vacuums the database."""
        self.cursor.execute("VACUUM")

    def tables_names(self) -> List[str]:
        """Fetches the names of all tables in the DB."""
        cursor = self.cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")
        return [row[0] for row in cursor]

    def columns_names(self, table_name: str) -> List[str]:
        """Fetches the names of the columns in a given table.
        If the table does not exists, an empty list is returned.

        The table name IS NOT SANITISED, so please be careful with the
        input.
        """
        cursor = self.cursor.execute(f'PRAGMA table_info({table_name});')
        return [row[1] for row in cursor]  # Extract just the names

    def is_in_memory(self) -> bool:
        """Returns true if the database in in-memory."""
        return self.db_file_name == ':memory:'
