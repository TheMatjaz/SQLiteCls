SQLite Class wrapper with auto-initialisation of empty DB
===============================================================================

SQLiteDb class wrapping the operations from the `sqlite3` module,
providing better support for the `with` operator and automatic
initialisation of an empty DB upon first creation of the DB.

Additional wrappers are available as a utility, including
extraction of a list of tables, extraction of the list of columns
of a `SELECT` query, commit, rollback, start of a transaction,
check if the DB is in memory and if it's empty. Executions of SQL script
files is also made easy.


Example usage
----------------------------------------

```python
import sqlitecls

with sqlitecls.SqliteDb('mydata.db', 'myinitscript.sql') as db:
    # Now the DB connection is established and the DB is guaranteed
    # to be initialised, as if it the DB file was not existing before,
    # the init script is run, preparing it for whatever your
    # application has to do.
    #
    # You can easily check which tables are available
    tables = db.tables_names()
    # and what columns they have
    columns = db.columns_names('some_table')
    # Otherwise use as any other DB API from now on
    cursor = db.execute('SELECT * FROM mytable')
    # Simplified extraction of the column names from the SELECT query
    columns = sqlitecls.cursor_column_names(cursor)
    # Use as any other DB API from now on
    for row in cursor:
        pass  # Do something with each row
    db.connection, db.cursor  # Available for custom operations
    # More wrappers!
    db.start_transaction()
    db.commit()
    db.rollback()
    db.vacuum()
    # Have existing SQL script files? Just run them as they are!
    db.execute_sql_file('myotherfile.sql')  # Load WHOLE file in memory
# Connection automatically closed now
```
