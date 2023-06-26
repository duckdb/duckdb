# ODBC 101: A Guide to ODBC for DuckDB Developers and Users

## What is ODBC?
[Open Database Connectivity (ODBC)](https://learn.microsoft.com/en-us/sql/odbc/microsoft-open-database-connectivity-odbc?view=sql-server-ver16) is a standard API for accessing databases. It is a C API, and is supported by most programming languages as well as most databases, including, of course, **DuckDB**🦆.  Essentially ODBC acts as a middleman between the database and the application. The application uses the ODBC API to communicate with the database, and the ODBC driver translates the ODBC API calls into database specific calls.  This allows the application to be database agnostic, and the database to be application agnostic.  

*There will be links throughout this README to the official [Microsoft ODBC documentation](https://learn.microsoft.com/en-us/sql/odbc/reference/odbc-programmer-s-reference?view=sql-server-ver16), which is a great resource for learning more about ODBC.*

## Important Concepts
### Data Source Name (DSN)
A DSN is a string that identifies a database.  It is used to connect to the database.  It can be a file path, a URL, or a database name.  For example, the following are all valid DSNs:

[//]: # (change to example relevent to duckdb)
- `C:\Users\me\mydatabase.db`
- `https://mydatabase.com`
- `mydatabase`

### Handle
A [handle](https://learn.microsoft.com/en-us/sql/odbc/reference/develop-app/handles?view=sql-server-ver16) is a pointer to a specific ODBC object which is used to interact with the database.  There are several different types of handles, each with a different purpose, these are the environment handle, the connection handle, the statement handle, and the descriptor handle. Handles are allocated using the [`SQLAllocHandle`](https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlallochandle-function?view=sql-server-ver16) which takes as input the type of handle to allocate, and a pointer to the handle, the driver then creates a new handle of the specified type which it returns to the application. 

#### Handle Types

| Handle                                                                                                                    | Type              | Description                                                                                                                                                            | Use Case                                                                         | Additional Information                                                                                                                                                                                                                                                                                                                                                                                            |
|---------------------------------------------------------------------------------------------------------------------------|-------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [Environment](https://learn.microsoft.com/en-us/sql/odbc/reference/develop-app/environment-handles?view=sql-server-ver16) | `SQL_HANDLE_ENV`  | Manages the environment settings for ODBC operations, and provides a global context in which to access data.                                                           | Initializing ODBC, managing driver behavior, resource allocation                 | Must be [allocated](https://learn.microsoft.com/en-us/sql/odbc/reference/develop-app/allocating-the-environment-handle?view=sql-server-ver16) once per application upon starting, and freed at the end.                                                                                                                                                                                                           |
| [Connection](https://learn.microsoft.com/en-us/sql/odbc/reference/develop-app/connection-handles?view=sql-server-ver16)   | `SQL_HANDLE_DBC`  | Represents a connection to a data source. Used to establish, manage, and terminate connections.  Defines both the driver and the data source to use within the driver. | Establishing a connection to a database, managing the connection state           | Multiple connection handles can be [created](https://learn.microsoft.com/en-us/sql/odbc/reference/develop-app/allocating-a-connection-handle-odbc?view=sql-server-ver16) as needed, allowing simultaneous connections to multiple data sources. <br/>*Note: allocating a connection handle does not establish a connection, but must be allocated first, and then used once the connection has been established.* |
| [Statement](https://learn.microsoft.com/en-us/sql/odbc/reference/develop-app/statement-handles?view=sql-server-ver16)     | `SQL_HANDLE_STMT` | 	Handles the execution of SQL statements, as well as handles the returned result sets.                                                                                 | Executing SQL queries, fetching result sets, managing statement options          | To facilitate the execution of concurrent queries, multiple handles can be allocated per connection.                                                                                                                                                                                                                                                                                                              |
| Descriptor                                                                                                                | `SQL_HANDLE_DESC` | 	Describes the attributes of a data structure or parameter, and allows the application to specify the structure of data to be bound/retrieved.                         | Describing table structures, result sets, binding columns to application buffers | Used in situations where data structures need to be explicitly defined, for example during parameter binding or result set fetching.                                                                                                                                                                                                                                                                              |





## Running the ODBC Client and Tests

#### Build the ODBC client (from within the main DuckDB repository)

```bash
BUILD_ODBC=1 DISABLE_SANITIZER=1 make debug -j
```

#### Run the ODBC client tests

```bash
# run all tests
python3 tools/odbc/test/run_psqlodbc_test.py --build_psqlodbc --overwrite --no-trace

# run an individual test
python3 tools/odbc/test/run_psqlodbc_test.py --build_psqlodbc --overwrite --no-trace --test result-conversions-test
```

#### Fix the ODBC Client Tests
First verify that the new output is correct. Then run the test using the `run_psqlodbc_test.py` script as usual, but use the `--fix` parameter.

```bash
python3 tools/odbc/test/run_psqlodbc_test.py --build_psqlodbc --overwrite --no-trace --test result-conversions-test --fix
```

This overwrites the stored output. You can run the test without the fix flag, and it should now pass.

Afterwards, we need to commit the changes to the psqlodbc repository:

```bash
cd psqlodbc
git add -p
git commit -m "Fix output"
git push
```

Then we need to update the git hash in our PR that broke the psqlodbc tests. Use `git log` to get the hash of the previous commit and the current commit.

```
git log
# old: d4d9097b87c46ddee5c8ae015c3ea27a6bd8938e
# new: 4acd0c615b690d526fd3cf7f2e666eee4ff9cbdb
```

Then search in the CI folder for the old hash, and replace it within the new hash:

```bash
grep -R .github 4acd0c615b690d526fd3cf7f2e666eee4ff9cbdb
.github/workflows/Windows.yml:        (cd psqlodbc && git checkout d4d9097b87c46ddee5c8ae015c3ea27a6bd8938e && make release)
.github/workflows/Main.yml:        (cd psqlodbc && git checkout d4d9097b87c46ddee5c8ae015c3ea27a6bd8938e && make debug)
```
