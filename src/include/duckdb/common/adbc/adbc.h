// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

/// \file adbc.h ADBC: Arrow Database connectivity
///
/// An Arrow-based interface between applications and database
/// drivers.  ADBC aims to provide a vendor-independent API for SQL
/// and Substrait-based database access that is targeted at
/// analytics/OLAP use cases.
///
/// This API is intended to be implemented directly by drivers and
/// used directly by client applications.  To assist portability
/// between different vendors, a "driver manager" library is also
/// provided, which implements this same API, but dynamically loads
/// drivers internally and forwards calls appropriately.
///
/// ADBC uses structs with free functions that operate on those
/// structs to model objects.
///
/// In general, objects allow serialized access from multiple threads,
/// but not concurrent access.  Specific implementations may permit
/// multiple threads.
///
/// \version 1.0.0

#pragma once

#include <stddef.h>
#include <stdint.h>
#include "duckdb/common/arrow/arrow.hpp"

/// \defgroup Arrow C Data Interface
/// Definitions for the C Data Interface/C Stream Interface.
///
/// See https://arrow.apache.org/docs/format/CDataInterface.html
///
/// @{

//! @cond Doxygen_Suppress
namespace duckdb_adbc {

#ifdef __cplusplus
extern "C" {
#endif

#ifndef ARROW_ADBC
#define ARROW_ADBC

#ifndef ADBC_EXPORTING
#define ADBC_EXPORTING
#endif

// Storage class macros for Windows
// Allow overriding/aliasing with application-defined macros
#if !defined(ADBC_EXPORT)
#if defined(_WIN32)
#if defined(ADBC_EXPORTING)
#define ADBC_EXPORT __declspec(dllexport)
#else
#define ADBC_EXPORT __declspec(dllimport)
#endif // defined(ADBC_EXPORTING)
#else
#define ADBC_EXPORT
#endif // defined(_WIN32)
#endif // !defined(ADBC_EXPORT)

/// \defgroup adbc-error-handling Error Handling
/// ADBC uses integer error codes to signal errors. To provide more
/// detail about errors, functions may also return an AdbcError via an
/// optional out parameter, which can be inspected. If provided, it is
/// the responsibility of the caller to zero-initialize the AdbcError
/// value.
///
/// @{

/// \brief Error codes for operations that may fail.
typedef uint8_t AdbcStatusCode;

/// \brief No error.
#define ADBC_STATUS_OK 0
/// \brief An unknown error occurred.
///
/// May indicate a driver-side or database-side error.
#define ADBC_STATUS_UNKNOWN 1
/// \brief The operation is not implemented or supported.
///
/// May indicate a driver-side or database-side error.
#define ADBC_STATUS_NOT_IMPLEMENTED 2
/// \brief A requested resource was not found.
///
/// May indicate a driver-side or database-side error.
#define ADBC_STATUS_NOT_FOUND 3
/// \brief A requested resource already exists.
///
/// May indicate a driver-side or database-side error.
#define ADBC_STATUS_ALREADY_EXISTS 4
/// \brief The arguments are invalid, likely a programming error.
///
/// For instance, they may be of the wrong format, or out of range.
///
/// May indicate a driver-side or database-side error.
#define ADBC_STATUS_INVALID_ARGUMENT 5
/// \brief The preconditions for the operation are not met, likely a
///   programming error.
///
/// For instance, the object may be uninitialized, or may have not
/// been fully configured.
///
/// May indicate a driver-side or database-side error.
#define ADBC_STATUS_INVALID_STATE 6
/// \brief Invalid data was processed (not a programming error).
///
/// For instance, a division by zero may have occurred during query
/// execution.
///
/// May indicate a database-side error only.
#define ADBC_STATUS_INVALID_DATA 7
/// \brief The database's integrity was affected.
///
/// For instance, a foreign key check may have failed, or a uniqueness
/// constraint may have been violated.
///
/// May indicate a database-side error only.
#define ADBC_STATUS_INTEGRITY 8
/// \brief An error internal to the driver or database occurred.
///
/// May indicate a driver-side or database-side error.
#define ADBC_STATUS_INTERNAL 9
/// \brief An I/O error occurred.
///
/// For instance, a remote service may be unavailable.
///
/// May indicate a driver-side or database-side error.
#define ADBC_STATUS_IO 10
/// \brief The operation was cancelled, not due to a timeout.
///
/// May indicate a driver-side or database-side error.
#define ADBC_STATUS_CANCELLED 11
/// \brief The operation was cancelled due to a timeout.
///
/// May indicate a driver-side or database-side error.
#define ADBC_STATUS_TIMEOUT 12
/// \brief Authentication failed.
///
/// May indicate a database-side error only.
#define ADBC_STATUS_UNAUTHENTICATED 13
/// \brief The client is not authorized to perform the given operation.
///
/// May indicate a database-side error only.
#define ADBC_STATUS_UNAUTHORIZED 14

/// \brief A detailed error message for an operation.
struct ADBC_EXPORT AdbcError {
	/// \brief The error message.
	char *message;

	/// \brief A vendor-specific error code, if applicable.
	int32_t vendor_code;

	/// \brief A SQLSTATE error code, if provided, as defined by the
	///   SQL:2003 standard.  If not set, it should be set to
	///   "\0\0\0\0\0".
	char sqlstate[5];

	/// \brief Release the contained error.
	///
	/// Unlike other structures, this is an embedded callback to make it
	/// easier for the driver manager and driver to cooperate.
	void (*release)(struct AdbcError *error);
};

/// @}

/// \defgroup adbc-constants Constants
/// @{

/// \brief ADBC revision 1.0.0.
///
/// When passed to an AdbcDriverInitFunc(), the driver parameter must
/// point to an AdbcDriver.
#define ADBC_VERSION_1_0_0 1000000

/// \brief Canonical option value for enabling an option.
///
/// For use as the value in SetOption calls.
#define ADBC_OPTION_VALUE_ENABLED "true"
/// \brief Canonical option value for disabling an option.
///
/// For use as the value in SetOption calls.
#define ADBC_OPTION_VALUE_DISABLED "false"

/// \brief The database vendor/product name (e.g. the server name).
///   (type: utf8).
///
/// \see AdbcConnectionGetInfo
#define ADBC_INFO_VENDOR_NAME 0
/// \brief The database vendor/product version (type: utf8).
///
/// \see AdbcConnectionGetInfo
#define ADBC_INFO_VENDOR_VERSION 1
/// \brief The database vendor/product Arrow library version (type:
///   utf8).
///
/// \see AdbcConnectionGetInfo
#define ADBC_INFO_VENDOR_ARROW_VERSION 2

/// \brief The driver name (type: utf8).
///
/// \see AdbcConnectionGetInfo
#define ADBC_INFO_DRIVER_NAME 100
/// \brief The driver version (type: utf8).
///
/// \see AdbcConnectionGetInfo
#define ADBC_INFO_DRIVER_VERSION 101
/// \brief The driver Arrow library version (type: utf8).
///
/// \see AdbcConnectionGetInfo
#define ADBC_INFO_DRIVER_ARROW_VERSION 102

/// \brief Return metadata on catalogs, schemas, tables, and columns.
///
/// \see AdbcConnectionGetObjects
#define ADBC_OBJECT_DEPTH_ALL 0
/// \brief Return metadata on catalogs only.
///
/// \see AdbcConnectionGetObjects
#define ADBC_OBJECT_DEPTH_CATALOGS 1
/// \brief Return metadata on catalogs and schemas.
///
/// \see AdbcConnectionGetObjects
#define ADBC_OBJECT_DEPTH_DB_SCHEMAS 2
/// \brief Return metadata on catalogs, schemas, and tables.
///
/// \see AdbcConnectionGetObjects
#define ADBC_OBJECT_DEPTH_TABLES 3
/// \brief Return metadata on catalogs, schemas, tables, and columns.
///
/// \see AdbcConnectionGetObjects
#define ADBC_OBJECT_DEPTH_COLUMNS ADBC_OBJECT_DEPTH_ALL

/// \brief The name of the canonical option for whether autocommit is
///   enabled.
///
/// \see AdbcConnectionSetOption
#define ADBC_CONNECTION_OPTION_AUTOCOMMIT "adbc.connection.autocommit"

/// \brief The name of the canonical option for whether the current
///   connection should be restricted to being read-only.
///
/// \see AdbcConnectionSetOption
#define ADBC_CONNECTION_OPTION_READ_ONLY "adbc.connection.readonly"

/// \brief The name of the canonical option for setting the isolation
///   level of a transaction.
///
/// Should only be used in conjunction with autocommit disabled and
/// AdbcConnectionCommit / AdbcConnectionRollback. If the desired
/// isolation level is not supported by a driver, it should return an
/// appropriate error.
///
/// \see AdbcConnectionSetOption
#define ADBC_CONNECTION_OPTION_ISOLATION_LEVEL "adbc.connection.transaction.isolation_level"

/// \brief Use database or driver default isolation level
///
/// \see AdbcConnectionSetOption
#define ADBC_OPTION_ISOLATION_LEVEL_DEFAULT "adbc.connection.transaction.isolation.default"

/// \brief The lowest isolation level. Dirty reads are allowed, so one
///   transaction may see not-yet-committed changes made by others.
///
/// \see AdbcConnectionSetOption
#define ADBC_OPTION_ISOLATION_LEVEL_READ_UNCOMMITTED "adbc.connection.transaction.isolation.read_uncommitted"

/// \brief Lock-based concurrency control keeps write locks until the
///   end of the transaction, but read locks are released as soon as a
///   SELECT is performed. Non-repeatable reads can occur in this
///   isolation level.
///
/// More simply put, Read Committed is an isolation level that guarantees
/// that any data read is committed at the moment it is read. It simply
/// restricts the reader from seeing any intermediate, uncommitted,
/// 'dirty' reads. It makes no promise whatsoever that if the transaction
/// re-issues the read, it will find the same data; data is free to change
/// after it is read.
///
/// \see AdbcConnectionSetOption
#define ADBC_OPTION_ISOLATION_LEVEL_READ_COMMITTED "adbc.connection.transaction.isolation.read_committed"

/// \brief Lock-based concurrency control keeps read AND write locks
///   (acquired on selection data) until the end of the transaction.
///
/// However, range-locks are not managed, so phantom reads can occur.
/// Write skew is possible at this isolation level in some systems.
///
/// \see AdbcConnectionSetOption
#define ADBC_OPTION_ISOLATION_LEVEL_REPEATABLE_READ "adbc.connection.transaction.isolation.repeatable_read"

/// \brief This isolation guarantees that all reads in the transaction
///   will see a consistent snapshot of the database and the transaction
///   should only successfully commit if no updates conflict with any
///   concurrent updates made since that snapshot.
///
/// \see AdbcConnectionSetOption
#define ADBC_OPTION_ISOLATION_LEVEL_SNAPSHOT "adbc.connection.transaction.isolation.snapshot"

/// \brief Serializability requires read and write locks to be released
///   only at the end of the transaction. This includes acquiring range-
///   locks when a select query uses a ranged WHERE clause to avoid
///   phantom reads.
///
/// \see AdbcConnectionSetOption
#define ADBC_OPTION_ISOLATION_LEVEL_SERIALIZABLE "adbc.connection.transaction.isolation.serializable"

/// \brief The central distinction between serializability and linearizability
///   is that serializability is a global property; a property of an entire
///   history of operations and transactions. Linearizability is a local
///   property; a property of a single operation/transaction.
///
/// Linearizability can be viewed as a special case of strict serializability
/// where transactions are restricted to consist of a single operation applied
/// to a single object.
///
/// \see AdbcConnectionSetOption
#define ADBC_OPTION_ISOLATION_LEVEL_LINEARIZABLE "adbc.connection.transaction.isolation.linearizable"

/// \defgroup adbc-statement-ingestion Bulk Data Ingestion
/// While it is possible to insert data via prepared statements, it can
/// be more efficient to explicitly perform a bulk insert.  For
/// compatible drivers, this can be accomplished by setting up and
/// executing a statement.  Instead of setting a SQL query or Substrait
/// plan, bind the source data via AdbcStatementBind, and set the name
/// of the table to be created via AdbcStatementSetOption and the
/// options below.  Then, call AdbcStatementExecute with a NULL for
/// the out parameter (to indicate you do not expect a result set).
///
/// @{

/// \brief The name of the target table for a bulk insert.
///
/// The driver should attempt to create the table if it does not
/// exist.  If the table exists but has a different schema,
/// ADBC_STATUS_ALREADY_EXISTS should be raised.  Else, data should be
/// appended to the target table.
#define ADBC_INGEST_OPTION_TARGET_TABLE "adbc.ingest.target_table"
/// \brief Whether to create (the default) or append.
#define ADBC_INGEST_OPTION_MODE "adbc.ingest.mode"
/// \brief Create the table and insert data; error if the table exists.
#define ADBC_INGEST_OPTION_MODE_CREATE "adbc.ingest.mode.create"
/// \brief Do not create the table, and insert data; error if the
///   table does not exist (ADBC_STATUS_NOT_FOUND) or does not match
///   the schema of the data to append (ADBC_STATUS_ALREADY_EXISTS).
#define ADBC_INGEST_OPTION_MODE_APPEND "adbc.ingest.mode.append"

/// @}

/// @}

/// \defgroup adbc-database Database Initialization
/// Clients first initialize a database, then create a connection
/// (below).  This gives the implementation a place to initialize and
/// own any common connection state.  For example, in-memory databases
/// can place ownership of the actual database in this object.
/// @{

/// \brief An instance of a database.
///
/// Must be kept alive as long as any connections exist.
struct ADBC_EXPORT AdbcDatabase {
	/// \brief Opaque implementation-defined state.
	/// This field is NULLPTR iff the connection is unintialized/freed.
	void *private_data;
	/// \brief The associated driver (used by the driver manager to help
	///   track state).
	struct AdbcDriver *private_driver;
};

/// @}

/// \defgroup adbc-connection Connection Establishment
/// Functions for creating, using, and releasing database connections.
/// @{

/// \brief An active database connection.
///
/// Provides methods for query execution, managing prepared
/// statements, using transactions, and so on.
///
/// Connections are not required to be thread-safe, but they can be
/// used from multiple threads so long as clients take care to
/// serialize accesses to a connection.
struct ADBC_EXPORT AdbcConnection {
	/// \brief Opaque implementation-defined state.
	/// This field is NULLPTR iff the connection is unintialized/freed.
	void *private_data;
	/// \brief The associated driver (used by the driver manager to help
	///   track state).
	struct AdbcDriver *private_driver;
};

/// @}

/// \defgroup adbc-statement Managing Statements
/// Applications should first initialize a statement with
/// AdbcStatementNew. Then, the statement should be configured with
/// functions like AdbcStatementSetSqlQuery and
/// AdbcStatementSetOption. Finally, the statement can be executed
/// with AdbcStatementExecuteQuery (or call AdbcStatementPrepare first
/// to turn it into a prepared statement instead).
/// @{

/// \brief A container for all state needed to execute a database
/// query, such as the query itself, parameters for prepared
/// statements, driver parameters, etc.
///
/// Statements may represent queries or prepared statements.
///
/// Statements may be used multiple times and can be reconfigured
/// (e.g. they can be reused to execute multiple different queries).
/// However, executing a statement (and changing certain other state)
/// will invalidate result sets obtained prior to that execution.
///
/// Multiple statements may be created from a single connection.
/// However, the driver may block or error if they are used
/// concurrently (whether from a single thread or multiple threads).
///
/// Statements are not required to be thread-safe, but they can be
/// used from multiple threads so long as clients take care to
/// serialize accesses to a statement.
struct ADBC_EXPORT AdbcStatement {
	/// \brief Opaque implementation-defined state.
	/// This field is NULLPTR iff the connection is unintialized/freed.
	void *private_data;

	/// \brief The associated driver (used by the driver manager to help
	///   track state).
	struct AdbcDriver *private_driver;
};

/// \defgroup adbc-statement-partition Partitioned Results
/// Some backends may internally partition the results. These
/// partitions are exposed to clients who may wish to integrate them
/// with a threaded or distributed execution model, where partitions
/// can be divided among threads or machines and fetched in parallel.
///
/// To use partitioning, execute the statement with
/// AdbcStatementExecutePartitions to get the partition descriptors.
/// Call AdbcConnectionReadPartition to turn the individual
/// descriptors into ArrowArrayStream instances.  This may be done on
/// a different connection than the one the partition was created
/// with, or even in a different process on another machine.
///
/// Drivers are not required to support partitioning.
///
/// @{

/// \brief The partitions of a distributed/partitioned result set.
struct AdbcPartitions {
	/// \brief The number of partitions.
	size_t num_partitions;

	/// \brief The partitions of the result set, where each entry (up to
	///   num_partitions entries) is an opaque identifier that can be
	///   passed to AdbcConnectionReadPartition.
	const uint8_t **partitions;

	/// \brief The length of each corresponding entry in partitions.
	const size_t *partition_lengths;

	/// \brief Opaque implementation-defined state.
	/// This field is NULLPTR iff the connection is unintialized/freed.
	void *private_data;

	/// \brief Release the contained partitions.
	///
	/// Unlike other structures, this is an embedded callback to make it
	/// easier for the driver manager and driver to cooperate.
	void (*release)(struct AdbcPartitions *partitions);
};

/// @}

/// @}

/// \defgroup adbc-driver Driver Initialization
///
/// These functions are intended to help support integration between a
/// driver and the driver manager.
/// @{

/// \brief An instance of an initialized database driver.
///
/// This provides a common interface for vendor-specific driver
/// initialization routines. Drivers should populate this struct, and
/// applications can call ADBC functions through this struct, without
/// worrying about multiple definitions of the same symbol.
struct ADBC_EXPORT AdbcDriver {
	/// \brief Opaque driver-defined state.
	/// This field is NULL if the driver is unintialized/freed (but
	/// it need not have a value even if the driver is initialized).
	void *private_data;
	/// \brief Opaque driver manager-defined state.
	/// This field is NULL if the driver is unintialized/freed (but
	/// it need not have a value even if the driver is initialized).
	void *private_manager;

	/// \brief Release the driver and perform any cleanup.
	///
	/// This is an embedded callback to make it easier for the driver
	/// manager and driver to cooperate.
	AdbcStatusCode (*release)(struct AdbcDriver *driver, struct AdbcError *error);

	AdbcStatusCode (*DatabaseInit)(struct AdbcDatabase *, struct AdbcError *);
	AdbcStatusCode (*DatabaseNew)(struct AdbcDatabase *, struct AdbcError *);
	AdbcStatusCode (*DatabaseSetOption)(struct AdbcDatabase *, const char *, const char *, struct AdbcError *);
	AdbcStatusCode (*DatabaseRelease)(struct AdbcDatabase *, struct AdbcError *);

	AdbcStatusCode (*ConnectionCommit)(struct AdbcConnection *, struct AdbcError *);
	AdbcStatusCode (*ConnectionGetInfo)(struct AdbcConnection *, uint32_t *, size_t, struct ArrowArrayStream *,
	                                    struct AdbcError *);
	AdbcStatusCode (*ConnectionGetObjects)(struct AdbcConnection *, int, const char *, const char *, const char *,
	                                       const char **, const char *, struct ArrowArrayStream *, struct AdbcError *);
	AdbcStatusCode (*ConnectionGetTableSchema)(struct AdbcConnection *, const char *, const char *, const char *,
	                                           struct ArrowSchema *, struct AdbcError *);
	AdbcStatusCode (*ConnectionGetTableTypes)(struct AdbcConnection *, struct ArrowArrayStream *, struct AdbcError *);
	AdbcStatusCode (*ConnectionInit)(struct AdbcConnection *, struct AdbcDatabase *, struct AdbcError *);
	AdbcStatusCode (*ConnectionNew)(struct AdbcConnection *, struct AdbcError *);
	AdbcStatusCode (*ConnectionSetOption)(struct AdbcConnection *, const char *, const char *, struct AdbcError *);
	AdbcStatusCode (*ConnectionReadPartition)(struct AdbcConnection *, const uint8_t *, size_t,
	                                          struct ArrowArrayStream *, struct AdbcError *);
	AdbcStatusCode (*ConnectionRelease)(struct AdbcConnection *, struct AdbcError *);
	AdbcStatusCode (*ConnectionRollback)(struct AdbcConnection *, struct AdbcError *);

	AdbcStatusCode (*StatementBind)(struct AdbcStatement *, struct ArrowArray *, struct ArrowSchema *,
	                                struct AdbcError *);
	AdbcStatusCode (*StatementBindStream)(struct AdbcStatement *, struct ArrowArrayStream *, struct AdbcError *);
	AdbcStatusCode (*StatementExecuteQuery)(struct AdbcStatement *, struct ArrowArrayStream *, int64_t *,
	                                        struct AdbcError *);
	AdbcStatusCode (*StatementExecutePartitions)(struct AdbcStatement *, struct ArrowSchema *, struct AdbcPartitions *,
	                                             int64_t *, struct AdbcError *);
	AdbcStatusCode (*StatementGetParameterSchema)(struct AdbcStatement *, struct ArrowSchema *, struct AdbcError *);
	AdbcStatusCode (*StatementNew)(struct AdbcConnection *, struct AdbcStatement *, struct AdbcError *);
	AdbcStatusCode (*StatementPrepare)(struct AdbcStatement *, struct AdbcError *);
	AdbcStatusCode (*StatementRelease)(struct AdbcStatement *, struct AdbcError *);
	AdbcStatusCode (*StatementSetOption)(struct AdbcStatement *, const char *, const char *, struct AdbcError *);
	AdbcStatusCode (*StatementSetSqlQuery)(struct AdbcStatement *, const char *, struct AdbcError *);
	AdbcStatusCode (*StatementSetSubstraitPlan)(struct AdbcStatement *, const uint8_t *, size_t, struct AdbcError *);
};

/// @}

/// \addtogroup adbc-database
/// @{

/// \brief Allocate a new (but uninitialized) database.
///
/// Callers pass in a zero-initialized AdbcDatabase.
///
/// Drivers should allocate their internal data structure and set the private_data
/// field to point to the newly allocated struct. This struct should be released
/// when AdbcDatabaseRelease is called.
ADBC_EXPORT
AdbcStatusCode AdbcDatabaseNew(struct AdbcDatabase *database, struct AdbcError *error);

/// \brief Set a char* option.
///
/// Options may be set before AdbcDatabaseInit.  Some drivers may
/// support setting options after initialization as well.
///
/// \return ADBC_STATUS_NOT_IMPLEMENTED if the option is not recognized
ADBC_EXPORT
AdbcStatusCode AdbcDatabaseSetOption(struct AdbcDatabase *database, const char *key, const char *value,
                                     struct AdbcError *error);

/// \brief Finish setting options and initialize the database.
///
/// Some drivers may support setting options after initialization
/// as well.
ADBC_EXPORT
AdbcStatusCode AdbcDatabaseInit(struct AdbcDatabase *database, struct AdbcError *error);

/// \brief Destroy this database. No connections may exist.
/// \param[in] database The database to release.
/// \param[out] error An optional location to return an error
///   message if necessary.
ADBC_EXPORT
AdbcStatusCode AdbcDatabaseRelease(struct AdbcDatabase *database, struct AdbcError *error);

/// @}

/// \addtogroup adbc-connection
/// @{

/// \brief Allocate a new (but uninitialized) connection.
///
/// Callers pass in a zero-initialized AdbcConnection.
///
/// Drivers should allocate their internal data structure and set the private_data
/// field to point to the newly allocated struct. This struct should be released
/// when AdbcConnectionRelease is called.
ADBC_EXPORT
AdbcStatusCode AdbcConnectionNew(struct AdbcConnection *connection, struct AdbcError *error);

/// \brief Set a char* option.
///
/// Options may be set before AdbcConnectionInit.  Some drivers may
/// support setting options after initialization as well.
///
/// \return ADBC_STATUS_NOT_IMPLEMENTED if the option is not recognized
ADBC_EXPORT
AdbcStatusCode AdbcConnectionSetOption(struct AdbcConnection *connection, const char *key, const char *value,
                                       struct AdbcError *error);

/// \brief Finish setting options and initialize the connection.
///
/// Some drivers may support setting options after initialization
/// as well.
ADBC_EXPORT
AdbcStatusCode AdbcConnectionInit(struct AdbcConnection *connection, struct AdbcDatabase *database,
                                  struct AdbcError *error);

/// \brief Destroy this connection.
///
/// \param[in] connection The connection to release.
/// \param[out] error An optional location to return an error
///   message if necessary.
ADBC_EXPORT
AdbcStatusCode AdbcConnectionRelease(struct AdbcConnection *connection, struct AdbcError *error);

/// \defgroup adbc-connection-metadata Metadata
/// Functions for retrieving metadata about the database.
///
/// Generally, these functions return an ArrowArrayStream that can be
/// consumed to get the metadata as Arrow data.  The returned metadata
/// has an expected schema given in the function docstring. Schema
/// fields are nullable unless otherwise marked.  While no
/// AdbcStatement is used in these functions, the result set may count
/// as an active statement to the driver for the purposes of
/// concurrency management (e.g. if the driver has a limit on
/// concurrent active statements and it must execute a SQL query
/// internally in order to implement the metadata function).
///
/// Some functions accept "search pattern" arguments, which are
/// strings that can contain the special character "%" to match zero
/// or more characters, or "_" to match exactly one character.  (See
/// the documentation of DatabaseMetaData in JDBC or "Pattern Value
/// Arguments" in the ODBC documentation.)  Escaping is not currently
/// supported.
///
/// @{

/// \brief Get metadata about the database/driver.
///
/// The result is an Arrow dataset with the following schema:
///
/// Field Name                  | Field Type
/// ----------------------------|------------------------
/// info_name                   | uint32 not null
/// info_value                  | INFO_SCHEMA
///
/// INFO_SCHEMA is a dense union with members:
///
/// Field Name (Type Code)      | Field Type
/// ----------------------------|------------------------
/// string_value (0)            | utf8
/// bool_value (1)              | bool
/// int64_value (2)             | int64
/// int32_bitmask (3)           | int32
/// string_list (4)             | list<utf8>
/// int32_to_int32_list_map (5) | map<int32, list<int32>>
///
/// Each metadatum is identified by an integer code.  The recognized
/// codes are defined as constants.  Codes [0, 10_000) are reserved
/// for ADBC usage.  Drivers/vendors will ignore requests for
/// unrecognized codes (the row will be omitted from the result).
///
/// \param[in] connection The connection to query.
/// \param[in] info_codes A list of metadata codes to fetch, or NULL
///   to fetch all.
/// \param[in] info_codes_length The length of the info_codes
///   parameter.  Ignored if info_codes is NULL.
/// \param[out] out The result set.
/// \param[out] error Error details, if an error occurs.
ADBC_EXPORT
AdbcStatusCode AdbcConnectionGetInfo(struct AdbcConnection *connection, uint32_t *info_codes, size_t info_codes_length,
                                     struct ArrowArrayStream *out, struct AdbcError *error);

/// \brief Get a hierarchical view of all catalogs, database schemas,
///   tables, and columns.
///
/// The result is an Arrow dataset with the following schema:
///
/// | Field Name               | Field Type              |
/// |--------------------------|-------------------------|
/// | catalog_name             | utf8                    |
/// | catalog_db_schemas       | list<DB_SCHEMA_SCHEMA>  |
///
/// DB_SCHEMA_SCHEMA is a Struct with fields:
///
/// | Field Name               | Field Type              |
/// |--------------------------|-------------------------|
/// | db_schema_name           | utf8                    |
/// | db_schema_tables         | list<TABLE_SCHEMA>      |
///
/// TABLE_SCHEMA is a Struct with fields:
///
/// | Field Name               | Field Type              |
/// |--------------------------|-------------------------|
/// | table_name               | utf8 not null           |
/// | table_type               | utf8 not null           |
/// | table_columns            | list<COLUMN_SCHEMA>     |
/// | table_constraints        | list<CONSTRAINT_SCHEMA> |
///
/// COLUMN_SCHEMA is a Struct with fields:
///
/// | Field Name               | Field Type              | Comments |
/// |--------------------------|-------------------------|----------|
/// | column_name              | utf8 not null           |          |
/// | ordinal_position         | int32                   | (1)      |
/// | remarks                  | utf8                    | (2)      |
/// | xdbc_data_type           | int16                   | (3)      |
/// | xdbc_type_name           | utf8                    | (3)      |
/// | xdbc_column_size         | int32                   | (3)      |
/// | xdbc_decimal_digits      | int16                   | (3)      |
/// | xdbc_num_prec_radix      | int16                   | (3)      |
/// | xdbc_nullable            | int16                   | (3)      |
/// | xdbc_column_def          | utf8                    | (3)      |
/// | xdbc_sql_data_type       | int16                   | (3)      |
/// | xdbc_datetime_sub        | int16                   | (3)      |
/// | xdbc_char_octet_length   | int32                   | (3)      |
/// | xdbc_is_nullable         | utf8                    | (3)      |
/// | xdbc_scope_catalog       | utf8                    | (3)      |
/// | xdbc_scope_schema        | utf8                    | (3)      |
/// | xdbc_scope_table         | utf8                    | (3)      |
/// | xdbc_is_autoincrement    | bool                    | (3)      |
/// | xdbc_is_generatedcolumn  | bool                    | (3)      |
///
/// 1. The column's ordinal position in the table (starting from 1).
/// 2. Database-specific description of the column.
/// 3. Optional value.  Should be null if not supported by the driver.
///    xdbc_ values are meant to provide JDBC/ODBC-compatible metadata
///    in an agnostic manner.
///
/// CONSTRAINT_SCHEMA is a Struct with fields:
///
/// | Field Name               | Field Type              | Comments |
/// |--------------------------|-------------------------|----------|
/// | constraint_name          | utf8                    |          |
/// | constraint_type          | utf8 not null           | (1)      |
/// | constraint_column_names  | list<utf8> not null     | (2)      |
/// | constraint_column_usage  | list<USAGE_SCHEMA>      | (3)      |
///
/// 1. One of 'CHECK', 'FOREIGN KEY', 'PRIMARY KEY', or 'UNIQUE'.
/// 2. The columns on the current table that are constrained, in
///    order.
/// 3. For FOREIGN KEY only, the referenced table and columns.
///
/// USAGE_SCHEMA is a Struct with fields:
///
/// | Field Name               | Field Type              |
/// |--------------------------|-------------------------|
/// | fk_catalog               | utf8                    |
/// | fk_db_schema             | utf8                    |
/// | fk_table                 | utf8 not null           |
/// | fk_column_name           | utf8 not null           |
///
/// \param[in] connection The database connection.
/// \param[in] depth The level of nesting to display. If 0, display
///   all levels. If 1, display only catalogs (i.e.  catalog_schemas
///   will be null). If 2, display only catalogs and schemas
///   (i.e. db_schema_tables will be null), and so on.
/// \param[in] catalog Only show tables in the given catalog. If NULL,
///   do not filter by catalog. If an empty string, only show tables
///   without a catalog.  May be a search pattern (see section
///   documentation).
/// \param[in] db_schema Only show tables in the given database schema. If
///   NULL, do not filter by database schema. If an empty string, only show
///   tables without a database schema. May be a search pattern (see section
///   documentation).
/// \param[in] table_name Only show tables with the given name. If NULL, do not
///   filter by name. May be a search pattern (see section documentation).
/// \param[in] table_type Only show tables matching one of the given table
///   types. If NULL, show tables of any type. Valid table types can be fetched
///   from GetTableTypes.  Terminate the list with a NULL entry.
/// \param[in] column_name Only show columns with the given name. If
///   NULL, do not filter by name.  May be a search pattern (see
///   section documentation).
/// \param[out] out The result set.
/// \param[out] error Error details, if an error occurs.
ADBC_EXPORT
AdbcStatusCode AdbcConnectionGetObjects(struct AdbcConnection *connection, int depth, const char *catalog,
                                        const char *db_schema, const char *table_name, const char **table_type,
                                        const char *column_name, struct ArrowArrayStream *out, struct AdbcError *error);

/// \brief Get the Arrow schema of a table.
///
/// \param[in] connection The database connection.
/// \param[in] catalog The catalog (or nullptr if not applicable).
/// \param[in] db_schema The database schema (or nullptr if not applicable).
/// \param[in] table_name The table name.
/// \param[out] schema The table schema.
/// \param[out] error Error details, if an error occurs.
ADBC_EXPORT
AdbcStatusCode AdbcConnectionGetTableSchema(struct AdbcConnection *connection, const char *catalog,
                                            const char *db_schema, const char *table_name, struct ArrowSchema *schema,
                                            struct AdbcError *error);

/// \brief Get a list of table types in the database.
///
/// The result is an Arrow dataset with the following schema:
///
/// Field Name     | Field Type
/// ---------------|--------------
/// table_type     | utf8 not null
///
/// \param[in] connection The database connection.
/// \param[out] out The result set.
/// \param[out] error Error details, if an error occurs.
ADBC_EXPORT
AdbcStatusCode AdbcConnectionGetTableTypes(struct AdbcConnection *connection, struct ArrowArrayStream *out,
                                           struct AdbcError *error);

/// @}

/// \defgroup adbc-connection-partition Partitioned Results
/// Some databases may internally partition the results. These
/// partitions are exposed to clients who may wish to integrate them
/// with a threaded or distributed execution model, where partitions
/// can be divided among threads or machines for processing.
///
/// Drivers are not required to support partitioning.
///
/// Partitions are not ordered. If the result set is sorted,
/// implementations should return a single partition.
///
/// @{

/// \brief Construct a statement for a partition of a query. The
///   results can then be read independently.
///
/// A partition can be retrieved from AdbcPartitions.
///
/// \param[in] connection The connection to use.  This does not have
///   to be the same connection that the partition was created on.
/// \param[in] serialized_partition The partition descriptor.
/// \param[in] serialized_length The partition descriptor length.
/// \param[out] out The result set.
/// \param[out] error Error details, if an error occurs.
ADBC_EXPORT
AdbcStatusCode AdbcConnectionReadPartition(struct AdbcConnection *connection, const uint8_t *serialized_partition,
                                           size_t serialized_length, struct ArrowArrayStream *out,
                                           struct AdbcError *error);

/// @}

/// \defgroup adbc-connection-transaction Transaction Semantics
///
/// Connections start out in auto-commit mode by default (if
/// applicable for the given vendor). Use AdbcConnectionSetOption and
/// ADBC_CONNECTION_OPTION_AUTO_COMMIT to change this.
///
/// @{

/// \brief Commit any pending transactions. Only used if autocommit is
///   disabled.
///
/// Behavior is undefined if this is mixed with SQL transaction
/// statements.
ADBC_EXPORT
AdbcStatusCode AdbcConnectionCommit(struct AdbcConnection *connection, struct AdbcError *error);

/// \brief Roll back any pending transactions. Only used if autocommit
///   is disabled.
///
/// Behavior is undefined if this is mixed with SQL transaction
/// statements.
ADBC_EXPORT
AdbcStatusCode AdbcConnectionRollback(struct AdbcConnection *connection, struct AdbcError *error);

/// @}

/// @}

/// \addtogroup adbc-statement
/// @{

/// \brief Create a new statement for a given connection.
///
/// Callers pass in a zero-initialized AdbcStatement.
///
/// Drivers should allocate their internal data structure and set the private_data
/// field to point to the newly allocated struct. This struct should be released
/// when AdbcStatementRelease is called.
ADBC_EXPORT
AdbcStatusCode AdbcStatementNew(struct AdbcConnection *connection, struct AdbcStatement *statement,
                                struct AdbcError *error);

/// \brief Destroy a statement.
/// \param[in] statement The statement to release.
/// \param[out] error An optional location to return an error
///   message if necessary.
ADBC_EXPORT
AdbcStatusCode AdbcStatementRelease(struct AdbcStatement *statement, struct AdbcError *error);

/// \brief Execute a statement and get the results.
///
/// This invalidates any prior result sets.
///
/// \param[in] statement The statement to execute.
/// \param[out] out The results. Pass NULL if the client does not
///   expect a result set.
/// \param[out] rows_affected The number of rows affected if known,
///   else -1. Pass NULL if the client does not want this information.
/// \param[out] error An optional location to return an error
///   message if necessary.
ADBC_EXPORT
AdbcStatusCode AdbcStatementExecuteQuery(struct AdbcStatement *statement, struct ArrowArrayStream *out,
                                         int64_t *rows_affected, struct AdbcError *error);

/// \brief Turn this statement into a prepared statement to be
///   executed multiple times.
///
/// This invalidates any prior result sets.
ADBC_EXPORT
AdbcStatusCode AdbcStatementPrepare(struct AdbcStatement *statement, struct AdbcError *error);

/// \defgroup adbc-statement-sql SQL Semantics
/// Functions for executing SQL queries, or querying SQL-related
/// metadata. Drivers are not required to support both SQL and
/// Substrait semantics. If they do, it may be via converting
/// between representations internally.
/// @{

/// \brief Set the SQL query to execute.
///
/// The query can then be executed with AdbcStatementExecute.  For
/// queries expected to be executed repeatedly, AdbcStatementPrepare
/// the statement first.
///
/// \param[in] statement The statement.
/// \param[in] query The query to execute.
/// \param[out] error Error details, if an error occurs.
ADBC_EXPORT
AdbcStatusCode AdbcStatementSetSqlQuery(struct AdbcStatement *statement, const char *query, struct AdbcError *error);

/// @}

/// \defgroup adbc-statement-substrait Substrait Semantics
/// Functions for executing Substrait plans, or querying
/// Substrait-related metadata.  Drivers are not required to support
/// both SQL and Substrait semantics.  If they do, it may be via
/// converting between representations internally.
/// @{

/// \brief Set the Substrait plan to execute.
///
/// The query can then be executed with AdbcStatementExecute.  For
/// queries expected to be executed repeatedly, AdbcStatementPrepare
/// the statement first.
///
/// \param[in] statement The statement.
/// \param[in] plan The serialized substrait.Plan to execute.
/// \param[in] length The length of the serialized plan.
/// \param[out] error Error details, if an error occurs.
ADBC_EXPORT
AdbcStatusCode AdbcStatementSetSubstraitPlan(struct AdbcStatement *statement, const uint8_t *plan, size_t length,
                                             struct AdbcError *error);

/// @}

/// \brief Bind Arrow data. This can be used for bulk inserts or
///   prepared statements.
///
/// \param[in] statement The statement to bind to.
/// \param[in] values The values to bind. The driver will call the
///   release callback itself, although it may not do this until the
///   statement is released.
/// \param[in] schema The schema of the values to bind.
/// \param[out] error An optional location to return an error message
///   if necessary.
ADBC_EXPORT
AdbcStatusCode AdbcStatementBind(struct AdbcStatement *statement, struct ArrowArray *values, struct ArrowSchema *schema,
                                 struct AdbcError *error);

/// \brief Bind Arrow data. This can be used for bulk inserts or
///   prepared statements.
/// \param[in] statement The statement to bind to.
/// \param[in] stream The values to bind. The driver will call the
///   release callback itself, although it may not do this until the
///   statement is released.
/// \param[out] error An optional location to return an error message
///   if necessary.
ADBC_EXPORT
AdbcStatusCode AdbcStatementBindStream(struct AdbcStatement *statement, struct ArrowArrayStream *stream,
                                       struct AdbcError *error);

/// \brief Get the schema for bound parameters.
///
/// This retrieves an Arrow schema describing the number, names, and
/// types of the parameters in a parameterized statement.  The fields
/// of the schema should be in order of the ordinal position of the
/// parameters; named parameters should appear only once.
///
/// If the parameter does not have a name, or the name cannot be
/// determined, the name of the corresponding field in the schema will
/// be an empty string.  If the type cannot be determined, the type of
/// the corresponding field will be NA (NullType).
///
/// This should be called after AdbcStatementPrepare.
///
/// \return ADBC_STATUS_NOT_IMPLEMENTED if the schema cannot be determined.
ADBC_EXPORT
AdbcStatusCode AdbcStatementGetParameterSchema(struct AdbcStatement *statement, struct ArrowSchema *schema,
                                               struct AdbcError *error);

/// \brief Set a string option on a statement.
ADBC_EXPORT
AdbcStatusCode AdbcStatementSetOption(struct AdbcStatement *statement, const char *key, const char *value,
                                      struct AdbcError *error);

/// \addtogroup adbc-statement-partition
/// @{

/// \brief Execute a statement and get the results as a partitioned
///   result set.
///
/// \param[in] statement The statement to execute.
/// \param[out] schema The schema of the result set.
/// \param[out] partitions The result partitions.
/// \param[out] rows_affected The number of rows affected if known,
///   else -1. Pass NULL if the client does not want this information.
/// \param[out] error An optional location to return an error
///   message if necessary.
/// \return ADBC_STATUS_NOT_IMPLEMENTED if the driver does not support
///   partitioned results
ADBC_EXPORT
AdbcStatusCode AdbcStatementExecutePartitions(struct AdbcStatement *statement, struct ArrowSchema *schema,
                                              struct AdbcPartitions *partitions, int64_t *rows_affected,
                                              struct AdbcError *error);

/// @}

/// @}

/// \addtogroup adbc-driver
/// @{

/// \brief Common entry point for drivers via the driver manager
///   (which uses dlopen(3)/LoadLibrary). The driver manager is told
///   to load a library and call a function of this type to load the
///   driver.
///
/// Although drivers may choose any name for this function, the
/// recommended name is "AdbcDriverInit".
///
/// \param[in] version The ADBC revision to attempt to initialize (see
///   ADBC_VERSION_1_0_0).
/// \param[out] driver The table of function pointers to
///   initialize. Should be a pointer to the appropriate struct for
///   the given version (see the documentation for the version).
/// \param[out] error An optional location to return an error message
///   if necessary.
/// \return ADBC_STATUS_OK if the driver was initialized, or
///   ADBC_STATUS_NOT_IMPLEMENTED if the version is not supported.  In
///   that case, clients may retry with a different version.
typedef AdbcStatusCode (*AdbcDriverInitFunc)(int version, void *driver, struct AdbcError *error);

/// @}

#endif // ADBC

#ifdef __cplusplus
}
#endif
} // namespace duckdb_adbc
