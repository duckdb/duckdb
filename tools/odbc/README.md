# ODBC 101: A Guide to ODBC for DuckDB Developers and Users

## What is ODBC?
[Open Database Connectivity (ODBC)](https://learn.microsoft.com/en-us/sql/odbc/microsoft-open-database-connectivity-odbc?view=sql-server-ver16) is a standard API for accessing databases. It is a C API, and is supported by most programming languages as well as most databases, including, of course, **DuckDB**🦆.  Essentially ODBC acts as a middleman between the database and the application. The application uses the ODBC API to communicate with the database, and the ODBC driver translates the ODBC API calls into database specific calls.  This allows the application to be database agnostic, and the database to be application agnostic.  

*There will be links throughout this README to the official [Microsoft ODBC documentation](https://learn.microsoft.com/en-us/sql/odbc/reference/odbc-programmer-s-reference?view=sql-server-ver16), which is a great resource for learning more about ODBC.*

## General Concepts:

* [Handles](#handles)
* [Connecting](#connecting)
* [Error Handling and Diagnostics](#error-handling-and-diagnostics)
* [Buffers and Binding](#buffers-and-binding)

### Handles
A [handle](https://learn.microsoft.com/en-us/sql/odbc/reference/develop-app/handles?view=sql-server-ver16) is a pointer to a specific ODBC object which is used to interact with the database.  There are several different types of handles, each with a different purpose, these are the environment handle, the connection handle, the statement handle, and the descriptor handle. Handles are allocated using the [`SQLAllocHandle`](https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlallochandle-function?view=sql-server-ver16) which takes as input the type of handle to allocate, and a pointer to the handle, the driver then creates a new handle of the specified type which it returns to the application.

#### Handle Types

| Handle                                                                                                                    | Type              | Description                                                                                                                                                            | Use Case                                                                         | Additional Information                                                                                                                                                                                                                                                                                                                                                                                                                             |
|---------------------------------------------------------------------------------------------------------------------------|-------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [Environment](https://learn.microsoft.com/en-us/sql/odbc/reference/develop-app/environment-handles?view=sql-server-ver16) | `SQL_HANDLE_ENV`  | Manages the environment settings for ODBC operations, and provides a global context in which to access data.                                                           | Initializing ODBC, managing driver behavior, resource allocation                 | Must be [allocated](https://learn.microsoft.com/en-us/sql/odbc/reference/develop-app/allocating-the-environment-handle?view=sql-server-ver16) once per application upon starting, and freed at the end.                                                                                                                                                                                                                                            |
| [Connection](https://learn.microsoft.com/en-us/sql/odbc/reference/develop-app/connection-handles?view=sql-server-ver16)   | `SQL_HANDLE_DBC`  | Represents a connection to a data source. Used to establish, manage, and terminate connections.  Defines both the driver and the data source to use within the driver. | Establishing a connection to a database, managing the connection state           | Multiple connection handles can be [created](https://learn.microsoft.com/en-us/sql/odbc/reference/develop-app/allocating-a-connection-handle-odbc?view=sql-server-ver16) as needed, allowing simultaneous connections to multiple data sources. <details><summary>*Note*</summary>_Allocating a connection handle does not establish a connection, but must be allocated first, and then used once the connection has been established._</details> |
| [Statement](https://learn.microsoft.com/en-us/sql/odbc/reference/develop-app/statement-handles?view=sql-server-ver16)     | `SQL_HANDLE_STMT` | 	Handles the execution of SQL statements, as well as the returned result sets.                                                                                         | Executing SQL queries, fetching result sets, managing statement options          | To facilitate the execution of concurrent queries, multiple handles can be [allocated](https://learn.microsoft.com/en-us/sql/odbc/reference/develop-app/allocating-a-statement-handle-odbc?view=sql-server-ver16) per connection.                                                                                                                                                                                                                  |
| [Descriptor](https://learn.microsoft.com/en-us/sql/odbc/reference/develop-app/descriptor-handles?view=sql-server-ver16)   | `SQL_HANDLE_DESC` | 	Describes the attributes of a data structure or parameter, and allows the application to specify the structure of data to be bound/retrieved.                         | Describing table structures, result sets, binding columns to application buffers | Used in situations where data structures need to be explicitly defined, for example during parameter binding or result set fetching.  They are automatically allocated when a statement is allocated, but can also be allocated explicitly.                                                                                                                                                                                                        |

### Connecting
The first step is to connect to the data source so that the application can perform database operations.  First the application must allocate an environment handle, and then a connection handle.  The connection handle is then used to connect to the data source.  There are two functions which can be used to connect to a data source, [`SQLDriverConnect`](https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqldriverconnect-function?view=sql-server-ver16) and [`SQLConnect`](https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlconnect-function?view=sql-server-ver16).  The former is used to connect to a data source using a connection string, while the latter is used to connect to a data source using a DSN. 

#### Connection String
A [connection string](https://learn.microsoft.com/en-us/sql/odbc/reference/develop-app/connection-strings?view=sql-server-ver16) is a string which contains the information needed to connect to a data source.  It is formatted as a semicolon separated list of key-value pairs, however DuckDB currently  only utilizes the DSN and ignores the rest of the parameters.  

#### DSN
A DSN (_Data Source Name_) is a string that identifies a database.  It can be a file path, URL, or a database name.  For example:  `C:\Users\me\duckdb.db`, and `DuckDB` are both valid DSN's. More information on DSN's can be found [here](https://learn.microsoft.com/en-us/sql/odbc/reference/develop-app/choosing-a-data-source-or-driver?view=sql-server-ver16).

[//]: # (| Key        | Value                                                                                                                                                                     |)

[//]: # (|------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------|)

[//]: # (| `DSN`      | A DSN is a string that identifies a database. It can be a file path, a URL, or a database name.  For example:  `C:\Users\me\duckdb.db`, and `DuckDB` are both valid DSN's |                                                     )

[//]: # (| `FILEDSN`  | The path to a file containing a DSN.                                                                                                                                      |)

[//]: # (| `DRIVER`   | The name of the ODBC driver to use.  This is the name of the driver as it appears in the ODBC Data Source Administrator.                                                  |)

[//]: # (| `UID`      | The username to use when connecting to the database.                                                                                                                      |)

[//]: # (| `PWD`      | The password to use when connecting to the database.                                                                                                                      |)

[//]: # (| `SAVEFILE` | The path to a file to which the connection string should be saved.                                                                                                        |)

### Error Handling and Diagnostics
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

## Setting up an Application:
1. [Include the SQL Header Files](#1-include-the-sql-header-files)
2. [Define the ODBC Handles and Connect to the Database ](#2-define-the-odbc-handles-and-connect-to-the-database)
3. [Adding a Query](#3-adding-a-query)
4. [Fetching Results](#4-fetching-results)
5. [Go Wild](#5-go-wild)
6. [Free the Handles and Disconnecting](#6-free-the-handles-and-disconnecting)

### 1. Include the SQL Header Files
The first step is to include the SQL header files:

```c
#include <sql.h>
#include <sqlext.h>
```

These files contain the definitions of the ODBC functions, as well as the data types used by ODBC.  In order to be able to use these header files you have to have the `unixodbc` package installed.
Either with homebrew or with apt:
```bash
brew install unixodbc
#or
sudo apt-get install unixodbc-dev
```

Remember to include the header file location in your `CFLAGS`

For `MAKEFILE`:
```MAKE
CFLAGS=-I/usr/local/include
#or 
CFLAGS=-/opt/homebrew/Cellar/unixodbc/2.3.11/include
```

For `CMAKE`:
```CMAKE
include_directories(/usr/local/include)
#or
include_directories(/opt/homebrew/Cellar/unixodbc/2.3.11/include)
```

You also have to link the library in your `CMAKE` or `MAKEFILE`:
For `CMAKE`:
```CMAKE
target_link_libraries(ODBC_application /path/to/duckdb_odbc/libduckdb_odbc.dylib)
```

For `MAKEFILE`:
```MAKE
LDLIBS=-L/path/to/duckdb_odbc/libduckdb_odbc.dylib
```

### 2. Define the ODBC Handles and Connect to the Database
Then set up the ODBC handles, allocate them, and connect to the database.  First the environment handle is allocated, then the environment is set to ODBC version 3, then the connection handle is allocated, and finally the connection is made to the database.  The following code snippet shows how to do this:

```c
SQLHANDLE env;
SQLHANDLE dbc;

SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);

SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);

SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

std::string dsn = "DSN=duckdbmemory";
SQLConnect(dbc, (SQLCHAR*)dsn.c_str(), SQL_NTS, NULL, 0, NULL, 0);

std::cout << "Connected!" << std::endl;
```

### 3. Adding a Query
Now that the application is set up, we can add a query to it.  First, we need to allocate a statement handle:

```c
SQLHANDLE stmt;
SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
```

Then we can execute a query:

```c
SQLExecDirect(stmt, (SQLCHAR*)"SELECT * FROM integers", SQL_NTS);
```

### 4. Fetching Results
Now that we have executed a query, we can fetch the results.  First, we need to bind the columns in the result set to buffers:

```c
SQLLEN int_val;
SQLLEN null_val;
SQLBindCol(stmt, 1, SQL_C_SLONG, &int_val, 0, &null_val);
```

Then we can fetch the results:

```c
SQLFetch(stmt);
```

### 5. Go Wild
Now that we have the results, we can do whatever we want with them.  For example, we can print them:

```c
std::cout << "Value: " << int_val << std::endl;
```

or do any other processing we want.  As well as executing more queries and doing any thing else we want to do with the database such as inserting, updating, or deleting data.

### 6. Free the Handles and Disconnecting
Finally, we need to free the handles and disconnect from the database.  First, we need to free the statement handle:

```c
SQLFreeHandle(SQL_HANDLE_STMT, stmt);
```

Then we need to disconnect from the database:

```c
SQLDisconnect(dbc);
```

And finally, we need to free the connection handle and the environment handle:

```c
SQLFreeHandle(SQL_HANDLE_DBC, dbc);
SQLFreeHandle(SQL_HANDLE_ENV, env);
```

Freeing the connection and environment handles can only be done after the connection to the database has been closed.  Trying to free them before disconnecting from the database will result in an error.

## Sample Application
The following is a sample application that includes a `cpp` file that connects to the database, executes a query, fetches the results, and prints them.  It also disconnects from the database and frees the handles, and includes a function to check the return value of ODBC functions.  It also includes a `CMakeLists.txt` file that can be used to build the application.

#### Sample `.cpp` file:
```c
#include <iostream>
#include <sql.h>
#include <sqlext.h>

void check_ret(SQLRETURN ret, std::string msg) {
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        std::cout << ret << ": " << msg << " failed" << std::endl;
        exit(1);
    }
    if (ret == SQL_SUCCESS_WITH_INFO) {
        std::cout << ret << ": " << msg << " succeeded with info" << std::endl;
    }
}

int main() {
    SQLHANDLE env;
    SQLHANDLE dbc;
    SQLRETURN ret;
	
    ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    check_ret(ret, "SQLAllocHandle(env)");
    
    ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);
    check_ret(ret, "SQLSetEnvAttr");
    
    ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
    check_ret(ret, "SQLAllocHandle(dbc)");
    
    std::string dsn = "DSN=duckdbmemory";
    ret = SQLConnect(dbc, (SQLCHAR*)dsn.c_str(), SQL_NTS, NULL, 0, NULL, 0);
    check_ret(ret, "SQLConnect");
    
    std::cout << "Connected!" << std::endl;
    
    SQLHANDLE stmt;
    ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
	check_ret(ret, "SQLAllocHandle(stmt)");
    
    ret = SQLExecDirect(stmt, (SQLCHAR*)"SELECT * FROM integers", SQL_NTS);
    check_ret(ret, "SQLExecDirect(SELECT * FROM integers)");
    
    SQLLEN int_val;
    SQLLEN null_val;
    ret = SQLBindCol(stmt, 1, SQL_C_SLONG, &int_val, 0, &null_val);
    check_ret(ret, "SQLBindCol");
    
    ret = SQLFetch(stmt);
    check_ret(ret, "SQLFetch");
    
    std::cout << "Value: " << int_val << std::endl;
    
    ret = SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    check_ret(ret, "SQLFreeHandle(stmt)");
    
    ret = SQLDisconnect(dbc);
    check_ret(ret, "SQLDisconnect");
    
    ret = SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    check_ret(ret, "SQLFreeHandle(dbc)");
    
    ret = SQLFreeHandle(SQL_HANDLE_ENV, env);
    check_ret(ret, "SQLFreeHandle(env)");
}
```

#### Sample `CMakelists.txt` file
```CMAKE
cmake_minimum_required(VERSION 3.25)
project(ODBC_Tester_App)

set(CMAKE_CXX_STANDARD 17)
include_directories(/opt/homebrew/Cellar/unixodbc/2.3.11/include)

add_executable(ODBC_Tester_App main.cpp)
target_link_libraries(ODBC_Tester_App /duckdb_odbc/libduckdb_odbc.dylib)
```

## Running the ODBC Client and Tests

#### Build the ODBC client (from within the main DuckDB repository)

```bash
BUILD_ODBC=1 DISABLE_SANITIZER=1 make debug -j
```

#### Run the ODBC Unit Tests

```bash
build/debug/tools/odbc/test/test_odbc
```