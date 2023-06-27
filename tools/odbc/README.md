# ODBC 101: A Guide to ODBC for DuckDB Developers and Users

## What is ODBC?
[Open Database Connectivity (ODBC)](https://learn.microsoft.com/en-us/sql/odbc/microsoft-open-database-connectivity-odbc?view=sql-server-ver16) is a standard API for accessing databases. It is a C API, and is supported by most programming languages as well as most databases, including, of course, **DuckDB**🦆.  Essentially ODBC acts as a middleman between the database and the application. The application uses the ODBC API to communicate with the database, and the ODBC driver translates the ODBC API calls into database specific calls.  This allows the application to be database agnostic, and the database to be application agnostic.  

*There will be links throughout this README to the official [Microsoft ODBC documentation](https://learn.microsoft.com/en-us/sql/odbc/reference/odbc-programmer-s-reference?view=sql-server-ver16), which is a great resource for learning more about ODBC.*

## Important Concepts

### Handle
A [handle](https://learn.microsoft.com/en-us/sql/odbc/reference/develop-app/handles?view=sql-server-ver16) is a pointer to a specific ODBC object which is used to interact with the database.  There are several different types of handles, each with a different purpose, these are the environment handle, the connection handle, the statement handle, and the descriptor handle. Handles are allocated using the [`SQLAllocHandle`](https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlallochandle-function?view=sql-server-ver16) which takes as input the type of handle to allocate, and a pointer to the handle, the driver then creates a new handle of the specified type which it returns to the application.

#### Handle Types

| Handle                                                                                                                    | Type              | Description                                                                                                                                                            | Use Case                                                                         | Additional Information                                                                                                                                                                                                                                                                                                                                                                                                                             |
|---------------------------------------------------------------------------------------------------------------------------|-------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [Environment](https://learn.microsoft.com/en-us/sql/odbc/reference/develop-app/environment-handles?view=sql-server-ver16) | `SQL_HANDLE_ENV`  | Manages the environment settings for ODBC operations, and provides a global context in which to access data.                                                           | Initializing ODBC, managing driver behavior, resource allocation                 | Must be [allocated](https://learn.microsoft.com/en-us/sql/odbc/reference/develop-app/allocating-the-environment-handle?view=sql-server-ver16) once per application upon starting, and freed at the end.                                                                                                                                                                                                                                            |
| [Connection](https://learn.microsoft.com/en-us/sql/odbc/reference/develop-app/connection-handles?view=sql-server-ver16)   | `SQL_HANDLE_DBC`  | Represents a connection to a data source. Used to establish, manage, and terminate connections.  Defines both the driver and the data source to use within the driver. | Establishing a connection to a database, managing the connection state           | Multiple connection handles can be [created](https://learn.microsoft.com/en-us/sql/odbc/reference/develop-app/allocating-a-connection-handle-odbc?view=sql-server-ver16) as needed, allowing simultaneous connections to multiple data sources. <details><summary>*Note*</summary>_Allocating a connection handle does not establish a connection, but must be allocated first, and then used once the connection has been established._</details> |
| [Statement](https://learn.microsoft.com/en-us/sql/odbc/reference/develop-app/statement-handles?view=sql-server-ver16)     | `SQL_HANDLE_STMT` | 	Handles the execution of SQL statements, as well as the returned result sets.                                                                                         | Executing SQL queries, fetching result sets, managing statement options          | To facilitate the execution of concurrent queries, multiple handles can be [allocated](https://learn.microsoft.com/en-us/sql/odbc/reference/develop-app/allocating-a-statement-handle-odbc?view=sql-server-ver16) per connection.                                                                                                                                                                                                                  |
| [Descriptor](https://learn.microsoft.com/en-us/sql/odbc/reference/develop-app/descriptor-handles?view=sql-server-ver16)   | `SQL_HANDLE_DESC` | 	Describes the attributes of a data structure or parameter, and allows the application to specify the structure of data to be bound/retrieved.                         | Describing table structures, result sets, binding columns to application buffers | Used in situations where data structures need to be explicitly defined, for example during parameter binding or result set fetching.  They are automatically allocated when a statement is allocated, but can also be allocated explicitly.                                                                                                                                                                                                        |

### Connecting
The first step in using ODBC is to connect to the database.  This is done by allocating a [connection handle](#handle-types) using the [`SQLAllocHandle`](https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlallochandle-function?view=sql-server-ver16) function, and then using the [`SQLDriverConnect`](https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqldriverconnect-function?view=sql-server-ver16) or the [`SQLConnect`](https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlconnect-function?view=sql-server-ver16) function to connect to the database. 


#### Data Source Name (DSN)
A DSN is a string that identifies a database.  It is used to connect to the database.  It can be a file path, a URL, or a database name.  For example, the following are all valid DSNs:

[//]: # (TODO: change to example relevent to duckdb)
- `C:\Users\me\mydatabase.db`
- `https://mydatabase.com`
- `mydatabase`



### Error Handling
All functions in ODBC return a code which represents the success or failure of the function.  This allows for easy error handling, as the application can simply check the return code of each function call to determine if it was successful.  If the function was unsuccessful, the application can then use the [`SQLGetDiagRec`](https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlgetdiagrec-function?view=sql-server-ver16) function to retrieve the error information. The following table defines the [return codes](https://learn.microsoft.com/en-us/sql/odbc/reference/develop-app/return-codes-odbc?view=sql-server-ver16):

| Return Code             | Description                                                                                                                                  |
|-------------------------|----------------------------------------------------------------------------------------------------------------------------------------------|
| `SQL_SUCCESS`           | The function completed successfully.                                                                                                         |
| `SQL_SUCCESS_WITH_INFO` | The function completed successfully, but additional information is available, including a warning                                            |
| `SQL_ERROR`             | The function failed.                                                                                                                         |
| `SQL_INVALID_HANDLE`    | The handle provided was invalid, indicating a programming error, i.e. when a handle is not allocated before it is used, or is the wrong type |
| `SQL_NO_DATA`           | The function completed successfully, but no more data is available                                                                           |
| `SQL_NEED_DATA`         | More data is needed, such as when a parameter data is sent at execution time, or additional connection information is required.              |
| `SQL_STILL_EXECUTING`   | A function that was asynchronously executed is still executing.                                                                              |

### Buffers and Binding
A buffer is a block of memory used to store data.  Buffers are used to store data retrieved from the database, or to send data to the database.  Buffers are allocated by the application, and then bound to a column in a result set, or a parameter in a query, using the [`SQLBindCol`](https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlbindcol-function?view=sql-server-ver16) and [`SQLBindParameter`](https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlbindparameter-function?view=sql-server-ver16) functions.  When the application fetches a row from the result set, or executes a query, the data is stored in the buffer.  When the application sends a query to the database, the data in the buffer is sent to the database.

[//]: # (<details>)

[//]: # (    <summary>Click to expand for more information on handles</summary>)

[//]: # ()
[//]: # (</details>)



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
