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

#include "duckdb/common/adbc/driver_manager.h"
#include "duckdb/common/adbc/adbc.h"
#include "duckdb/common/adbc/adbc.hpp"

#include <algorithm>
#include <cstring>
#include <string>
#include <unordered_map>
#include <utility>

#if defined(_WIN32)
#include <windows.h> // Must come first

#include <libloaderapi.h>
#include <strsafe.h>
#else
#include <dlfcn.h>
#endif // defined(_WIN32)

namespace duckdb_adbc {

// Platform-specific helpers

#if defined(_WIN32)
/// Append a description of the Windows error to the buffer.
void GetWinError(std::string *buffer) {
	DWORD rc = GetLastError();
	LPVOID message;

	FormatMessage(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
	              /*lpSource=*/nullptr, rc, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
	              reinterpret_cast<LPSTR>(&message), /*nSize=*/0, /*Arguments=*/nullptr);

	(*buffer) += '(';
	(*buffer) += std::to_string(rc);
	(*buffer) += ") ";
	(*buffer) += reinterpret_cast<char *>(message);
	LocalFree(message);
}

#endif // defined(_WIN32)

// Temporary state while the database is being configured.
struct TempDatabase {
	std::unordered_map<std::string, std::string> options;
	std::string driver;
	// Default name (see adbc.h)
	std::string entrypoint = "AdbcDriverInit";
	AdbcDriverInitFunc init_func = nullptr;
};

// Error handling

void ReleaseError(struct AdbcError *error) {
	if (error) {
		if (error->message) {
			delete[] error->message;
		}
		error->message = nullptr;
		error->release = nullptr;
	}
}

void SetError(struct AdbcError *error, const std::string &message) {
	if (!error) {
		return;
	}
	if (error->message) {
		// Append
		std::string buffer = error->message;
		buffer.reserve(buffer.size() + message.size() + 1);
		buffer += '\n';
		buffer += message;
		error->release(error);

		error->message = new char[buffer.size() + 1];
		buffer.copy(error->message, buffer.size());
		error->message[buffer.size()] = '\0';
	} else {
		error->message = new char[message.size() + 1];
		message.copy(error->message, message.size());
		error->message[message.size()] = '\0';
	}
	error->release = ReleaseError;
}

void SetError(struct AdbcError *error, const char *message_p) {
	if (!message_p) {
		message_p = "";
	}
	std::string message(message_p);
	SetError(error, message);
}

// Driver state

/// Hold the driver DLL and the driver release callback in the driver struct.
struct ManagerDriverState {
	// The original release callback
	AdbcStatusCode (*driver_release)(struct AdbcDriver *driver, struct AdbcError *error);

#if defined(_WIN32)
	// The loaded DLL
	HMODULE handle;
#endif // defined(_WIN32)
};

/// Unload the driver DLL.
static AdbcStatusCode ReleaseDriver(struct AdbcDriver *driver, struct AdbcError *error) {
	AdbcStatusCode status = ADBC_STATUS_OK;

	if (!driver->private_manager) {
		return status;
	}
	ManagerDriverState *state = reinterpret_cast<ManagerDriverState *>(driver->private_manager);

	if (state->driver_release) {
		status = state->driver_release(driver, error);
	}

#if defined(_WIN32)
	// TODO(apache/arrow-adbc#204): causes tests to segfault
	// if (!FreeLibrary(state->handle)) {
	//   std::string message = "FreeLibrary() failed: ";
	//   GetWinError(&message);
	//   SetError(error, message);
	// }
#endif // defined(_WIN32)

	driver->private_manager = nullptr;
	delete state;
	return status;
}

/// Temporary state while the database is being configured.
struct TempConnection {
	std::unordered_map<std::string, std::string> options;
};

// Direct implementations of API methods

AdbcStatusCode AdbcDatabaseNew(struct AdbcDatabase *database, struct AdbcError *error) {
	// Allocate a temporary structure to store options pre-Init
	database->private_data = new TempDatabase();
	database->private_driver = nullptr;
	return ADBC_STATUS_OK;
}

AdbcStatusCode AdbcDatabaseSetOption(struct AdbcDatabase *database, const char *key, const char *value,
                                     struct AdbcError *error) {
	if (!database) {
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	if (database->private_driver) {
		return database->private_driver->DatabaseSetOption(database, key, value, error);
	}

	TempDatabase *args = reinterpret_cast<TempDatabase *>(database->private_data);
	if (std::strcmp(key, "driver") == 0) {
		args->driver = value;
	} else if (std::strcmp(key, "entrypoint") == 0) {
		args->entrypoint = value;
	} else {
		args->options[key] = value;
	}
	return ADBC_STATUS_OK;
}

AdbcStatusCode AdbcDriverManagerDatabaseSetInitFunc(struct AdbcDatabase *database, AdbcDriverInitFunc init_func,
                                                    struct AdbcError *error) {
	if (!database) {
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	if (database->private_driver) {
		return ADBC_STATUS_INVALID_STATE;
	}

	TempDatabase *args = reinterpret_cast<TempDatabase *>(database->private_data);
	args->init_func = init_func;
	return ADBC_STATUS_OK;
}

AdbcStatusCode AdbcDatabaseInit(struct AdbcDatabase *database, struct AdbcError *error) {
	if (!database->private_data) {
		SetError(error, "Must call AdbcDatabaseNew first");
		return ADBC_STATUS_INVALID_STATE;
	}
	TempDatabase *args = reinterpret_cast<TempDatabase *>(database->private_data);
	if (args->init_func) {
		// Do nothing
	} else if (args->driver.empty()) {
		SetError(error, "Must provide 'driver' parameter");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}

	database->private_driver = new AdbcDriver;
	std::memset(database->private_driver, 0, sizeof(AdbcDriver));
	AdbcStatusCode status;
	// So we don't confuse a driver into thinking it's initialized already
	database->private_data = nullptr;
	if (args->init_func) {
		status = AdbcLoadDriverFromInitFunc(args->init_func, ADBC_VERSION_1_0_0, database->private_driver, error);
	} else {
		status = AdbcLoadDriver(args->driver.c_str(), args->entrypoint.c_str(), ADBC_VERSION_1_0_0,
		                        database->private_driver, error);
	}
	if (status != ADBC_STATUS_OK) {
		// Restore private_data so it will be released by AdbcDatabaseRelease
		database->private_data = args;
		if (database->private_driver->release) {
			database->private_driver->release(database->private_driver, error);
		}
		delete database->private_driver;
		database->private_driver = nullptr;
		return status;
	}
	status = database->private_driver->DatabaseNew(database, error);
	if (status != ADBC_STATUS_OK) {
		if (database->private_driver->release) {
			database->private_driver->release(database->private_driver, error);
		}
		delete database->private_driver;
		database->private_driver = nullptr;
		return status;
	}
	for (const auto &option : args->options) {
		status =
		    database->private_driver->DatabaseSetOption(database, option.first.c_str(), option.second.c_str(), error);
		if (status != ADBC_STATUS_OK) {
			delete args;
			// Release the database
			std::ignore = database->private_driver->DatabaseRelease(database, error);
			if (database->private_driver->release) {
				database->private_driver->release(database->private_driver, error);
			}
			delete database->private_driver;
			database->private_driver = nullptr;
			// Should be redundant, but ensure that AdbcDatabaseRelease
			// below doesn't think that it contains a TempDatabase
			database->private_data = nullptr;
			return status;
		}
	}
	delete args;
	return database->private_driver->DatabaseInit(database, error);
}

AdbcStatusCode AdbcDatabaseRelease(struct AdbcDatabase *database, struct AdbcError *error) {
	if (!database->private_driver) {
		if (database->private_data) {
			TempDatabase *args = reinterpret_cast<TempDatabase *>(database->private_data);
			delete args;
			database->private_data = nullptr;
			return ADBC_STATUS_OK;
		}
		return ADBC_STATUS_INVALID_STATE;
	}
	auto status = database->private_driver->DatabaseRelease(database, error);
	if (database->private_driver->release) {
		database->private_driver->release(database->private_driver, error);
	}
	delete database->private_driver;
	database->private_data = nullptr;
	database->private_driver = nullptr;
	return status;
}

AdbcStatusCode AdbcConnectionCommit(struct AdbcConnection *connection, struct AdbcError *error) {
	if (!connection) {
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	if (!connection->private_driver) {
		return ADBC_STATUS_INVALID_STATE;
	}
	return connection->private_driver->ConnectionCommit(connection, error);
}

AdbcStatusCode AdbcConnectionGetInfo(struct AdbcConnection *connection, uint32_t *info_codes, size_t info_codes_length,
                                     struct ArrowArrayStream *out, struct AdbcError *error) {
	if (!connection) {
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	if (!connection->private_driver) {
		return ADBC_STATUS_INVALID_STATE;
	}
	return connection->private_driver->ConnectionGetInfo(connection, info_codes, info_codes_length, out, error);
}

AdbcStatusCode AdbcConnectionGetObjects(struct AdbcConnection *connection, int depth, const char *catalog,
                                        const char *db_schema, const char *table_name, const char **table_types,
                                        const char *column_name, struct ArrowArrayStream *stream,
                                        struct AdbcError *error) {
	if (!connection) {
		SetError(error, "connection can't be null");
		return ADBC_STATUS_INVALID_STATE;
	}
	if (!connection->private_data) {
		SetError(error, "connection must be initialized");
		return ADBC_STATUS_INVALID_STATE;
	}
	return connection->private_driver->ConnectionGetObjects(connection, depth, catalog, db_schema, table_name,
	                                                        table_types, column_name, stream, error);
}

AdbcStatusCode AdbcConnectionGetTableSchema(struct AdbcConnection *connection, const char *catalog,
                                            const char *db_schema, const char *table_name, struct ArrowSchema *schema,
                                            struct AdbcError *error) {
	if (!connection) {
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	if (!connection->private_driver) {
		return ADBC_STATUS_INVALID_STATE;
	}
	return connection->private_driver->ConnectionGetTableSchema(connection, catalog, db_schema, table_name, schema,
	                                                            error);
}

AdbcStatusCode AdbcConnectionGetTableTypes(struct AdbcConnection *connection, struct ArrowArrayStream *stream,
                                           struct AdbcError *error) {
	if (!connection) {
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	if (!connection->private_driver) {
		return ADBC_STATUS_INVALID_STATE;
	}
	return connection->private_driver->ConnectionGetTableTypes(connection, stream, error);
}

AdbcStatusCode AdbcConnectionInit(struct AdbcConnection *connection, struct AdbcDatabase *database,
                                  struct AdbcError *error) {
	if (!connection) {
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	if (!connection->private_data) {
		SetError(error, "Must call AdbcConnectionNew first");
		return ADBC_STATUS_INVALID_STATE;
	} else if (!database->private_driver) {
		SetError(error, "Database is not initialized");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	TempConnection *args = reinterpret_cast<TempConnection *>(connection->private_data);
	connection->private_data = nullptr;
	std::unordered_map<std::string, std::string> options = std::move(args->options);
	delete args;

	auto status = database->private_driver->ConnectionNew(connection, error);
	if (status != ADBC_STATUS_OK) {
		return status;
	}
	connection->private_driver = database->private_driver;

	for (const auto &option : options) {
		status = database->private_driver->ConnectionSetOption(connection, option.first.c_str(), option.second.c_str(),
		                                                       error);
		if (status != ADBC_STATUS_OK) {
			return status;
		}
	}
	return connection->private_driver->ConnectionInit(connection, database, error);
}

AdbcStatusCode AdbcConnectionNew(struct AdbcConnection *connection, struct AdbcError *error) {
	// Allocate a temporary structure to store options pre-Init, because
	// we don't get access to the database (and hence the driver
	// function table) until then
	connection->private_data = new TempConnection;
	connection->private_driver = nullptr;
	return ADBC_STATUS_OK;
}

AdbcStatusCode AdbcConnectionReadPartition(struct AdbcConnection *connection, const uint8_t *serialized_partition,
                                           size_t serialized_length, struct ArrowArrayStream *out,
                                           struct AdbcError *error) {
	if (!connection) {
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	if (!connection->private_driver) {
		return ADBC_STATUS_INVALID_STATE;
	}
	return connection->private_driver->ConnectionReadPartition(connection, serialized_partition, serialized_length, out,
	                                                           error);
}

AdbcStatusCode AdbcConnectionRelease(struct AdbcConnection *connection, struct AdbcError *error) {
	if (!connection) {
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	if (!connection->private_driver) {
		if (connection->private_data) {
			TempConnection *args = reinterpret_cast<TempConnection *>(connection->private_data);
			delete args;
			connection->private_data = nullptr;
			return ADBC_STATUS_OK;
		}
		return ADBC_STATUS_INVALID_STATE;
	}
	auto status = connection->private_driver->ConnectionRelease(connection, error);
	connection->private_driver = nullptr;
	return status;
}

AdbcStatusCode AdbcConnectionRollback(struct AdbcConnection *connection, struct AdbcError *error) {
	if (!connection) {
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	if (!connection->private_driver) {
		return ADBC_STATUS_INVALID_STATE;
	}
	return connection->private_driver->ConnectionRollback(connection, error);
}

AdbcStatusCode AdbcConnectionSetOption(struct AdbcConnection *connection, const char *key, const char *value,
                                       struct AdbcError *error) {
	if (!connection) {
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	if (!connection->private_data) {
		SetError(error, "AdbcConnectionSetOption: must AdbcConnectionNew first");
		return ADBC_STATUS_INVALID_STATE;
	}
	if (!connection->private_driver) {
		// Init not yet called, save the option
		TempConnection *args = reinterpret_cast<TempConnection *>(connection->private_data);
		args->options[key] = value;
		return ADBC_STATUS_OK;
	}
	return connection->private_driver->ConnectionSetOption(connection, key, value, error);
}

AdbcStatusCode AdbcStatementBind(struct AdbcStatement *statement, struct ArrowArray *values, struct ArrowSchema *schema,
                                 struct AdbcError *error) {
	if (!statement) {
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	if (!statement->private_driver) {
		return ADBC_STATUS_INVALID_STATE;
	}
	return statement->private_driver->StatementBind(statement, values, schema, error);
}

AdbcStatusCode AdbcStatementBindStream(struct AdbcStatement *statement, struct ArrowArrayStream *stream,
                                       struct AdbcError *error) {
	if (!statement) {
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	if (!statement->private_driver) {
		return ADBC_STATUS_INVALID_STATE;
	}
	return statement->private_driver->StatementBindStream(statement, stream, error);
}

// XXX: cpplint gets confused here if declared as 'struct ArrowSchema* schema'
AdbcStatusCode AdbcStatementExecutePartitions(struct AdbcStatement *statement, ArrowSchema *schema,
                                              struct AdbcPartitions *partitions, int64_t *rows_affected,
                                              struct AdbcError *error) {
	if (!statement) {
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	if (!statement->private_driver) {
		return ADBC_STATUS_INVALID_STATE;
	}
	return statement->private_driver->StatementExecutePartitions(statement, schema, partitions, rows_affected, error);
}

AdbcStatusCode AdbcStatementExecuteQuery(struct AdbcStatement *statement, struct ArrowArrayStream *out,
                                         int64_t *rows_affected, struct AdbcError *error) {
	if (!statement) {
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	if (!statement->private_driver) {
		return ADBC_STATUS_INVALID_STATE;
	}
	return statement->private_driver->StatementExecuteQuery(statement, out, rows_affected, error);
}

AdbcStatusCode AdbcStatementGetParameterSchema(struct AdbcStatement *statement, struct ArrowSchema *schema,
                                               struct AdbcError *error) {
	if (!statement) {
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	if (!statement->private_driver) {
		return ADBC_STATUS_INVALID_STATE;
	}
	return statement->private_driver->StatementGetParameterSchema(statement, schema, error);
}

AdbcStatusCode AdbcStatementNew(struct AdbcConnection *connection, struct AdbcStatement *statement,
                                struct AdbcError *error) {
	if (!connection) {
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	if (!connection->private_driver) {
		return ADBC_STATUS_INVALID_STATE;
	}
	auto status = connection->private_driver->StatementNew(connection, statement, error);
	statement->private_driver = connection->private_driver;
	return status;
}

AdbcStatusCode AdbcStatementPrepare(struct AdbcStatement *statement, struct AdbcError *error) {
	if (!statement) {
		SetError(error, "Missing statement object");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	if (!statement->private_data) {
		SetError(error, "Invalid statement object");
		return ADBC_STATUS_INVALID_STATE;
	}
	return statement->private_driver->StatementPrepare(statement, error);
}

AdbcStatusCode AdbcStatementRelease(struct AdbcStatement *statement, struct AdbcError *error) {
	if (!statement) {
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	if (!statement->private_driver) {
		return ADBC_STATUS_INVALID_STATE;
	}
	auto status = statement->private_driver->StatementRelease(statement, error);
	statement->private_driver = nullptr;
	return status;
}

AdbcStatusCode AdbcStatementSetOption(struct AdbcStatement *statement, const char *key, const char *value,
                                      struct AdbcError *error) {
	if (!statement) {
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	if (!statement->private_driver) {
		return ADBC_STATUS_INVALID_STATE;
	}
	return statement->private_driver->StatementSetOption(statement, key, value, error);
}

AdbcStatusCode AdbcStatementSetSqlQuery(struct AdbcStatement *statement, const char *query, struct AdbcError *error) {
	if (!statement) {
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	if (!statement->private_driver) {
		return ADBC_STATUS_INVALID_STATE;
	}
	return statement->private_driver->StatementSetSqlQuery(statement, query, error);
}

AdbcStatusCode AdbcStatementSetSubstraitPlan(struct AdbcStatement *statement, const uint8_t *plan, size_t length,
                                             struct AdbcError *error) {
	if (!statement) {
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	if (!statement->private_driver) {
		return ADBC_STATUS_INVALID_STATE;
	}
	return statement->private_driver->StatementSetSubstraitPlan(statement, plan, length, error);
}

const char *AdbcStatusCodeMessage(AdbcStatusCode code) {
#define STRINGIFY(s)       #s
#define STRINGIFY_VALUE(s) STRINGIFY(s)
#define CASE(CONSTANT)                                                                                                 \
	case CONSTANT:                                                                                                     \
		return #CONSTANT " (" STRINGIFY_VALUE(CONSTANT) ")";

	switch (code) {
		CASE(ADBC_STATUS_OK);
		CASE(ADBC_STATUS_UNKNOWN);
		CASE(ADBC_STATUS_NOT_IMPLEMENTED);
		CASE(ADBC_STATUS_NOT_FOUND);
		CASE(ADBC_STATUS_ALREADY_EXISTS);
		CASE(ADBC_STATUS_INVALID_ARGUMENT);
		CASE(ADBC_STATUS_INVALID_STATE);
		CASE(ADBC_STATUS_INVALID_DATA);
		CASE(ADBC_STATUS_INTEGRITY);
		CASE(ADBC_STATUS_INTERNAL);
		CASE(ADBC_STATUS_IO);
		CASE(ADBC_STATUS_CANCELLED);
		CASE(ADBC_STATUS_TIMEOUT);
		CASE(ADBC_STATUS_UNAUTHENTICATED);
		CASE(ADBC_STATUS_UNAUTHORIZED);
	default:
		return "(invalid code)";
	}
#undef CASE
#undef STRINGIFY_VALUE
#undef STRINGIFY
}

AdbcStatusCode AdbcLoadDriver(const char *driver_name, const char *entrypoint, int version, void *raw_driver,
                              struct AdbcError *error) {
	AdbcDriverInitFunc init_func;
	std::string error_message;

	if (version != ADBC_VERSION_1_0_0) {
		SetError(error, "Only ADBC 1.0.0 is supported");
		return ADBC_STATUS_NOT_IMPLEMENTED;
	}

	auto *driver = reinterpret_cast<struct AdbcDriver *>(raw_driver);

	if (!entrypoint) {
		// Default entrypoint (see adbc.h)
		entrypoint = "AdbcDriverInit";
	}

#if defined(_WIN32)

	HMODULE handle = LoadLibraryExA(driver_name, NULL, 0);
	if (!handle) {
		error_message += driver_name;
		error_message += ": LoadLibraryExA() failed: ";
		GetWinError(&error_message);

		std::string full_driver_name = driver_name;
		full_driver_name += ".lib";
		handle = LoadLibraryExA(full_driver_name.c_str(), NULL, 0);
		if (!handle) {
			error_message += '\n';
			error_message += full_driver_name;
			error_message += ": LoadLibraryExA() failed: ";
			GetWinError(&error_message);
		}
	}
	if (!handle) {
		SetError(error, error_message);
		return ADBC_STATUS_INTERNAL;
	}

	void *load_handle = reinterpret_cast<void *>(GetProcAddress(handle, entrypoint));
	init_func = reinterpret_cast<AdbcDriverInitFunc>(load_handle);
	if (!init_func) {
		std::string message = "GetProcAddress(";
		message += entrypoint;
		message += ") failed: ";
		GetWinError(&message);
		if (!FreeLibrary(handle)) {
			message += "\nFreeLibrary() failed: ";
			GetWinError(&message);
		}
		SetError(error, message);
		return ADBC_STATUS_INTERNAL;
	}

#else

#if defined(__APPLE__)
	const std::string kPlatformLibraryPrefix = "lib";
	const std::string kPlatformLibrarySuffix = ".dylib";
#else
	const std::string kPlatformLibraryPrefix = "lib";
	const std::string kPlatformLibrarySuffix = ".so";
#endif // defined(__APPLE__)

	void *handle = dlopen(driver_name, RTLD_NOW | RTLD_LOCAL);
	if (!handle) {
		error_message = "dlopen() failed: ";
		error_message += dlerror();

		// If applicable, append the shared library prefix/extension and
		// try again (this way you don't have to hardcode driver names by
		// platform in the application)
		const std::string driver_str = driver_name;

		std::string full_driver_name;
		if (driver_str.size() < kPlatformLibraryPrefix.size() ||
		    driver_str.compare(0, kPlatformLibraryPrefix.size(), kPlatformLibraryPrefix) != 0) {
			full_driver_name += kPlatformLibraryPrefix;
		}
		full_driver_name += driver_name;
		if (driver_str.size() < kPlatformLibrarySuffix.size() ||
		    driver_str.compare(full_driver_name.size() - kPlatformLibrarySuffix.size(), kPlatformLibrarySuffix.size(),
		                       kPlatformLibrarySuffix) != 0) {
			full_driver_name += kPlatformLibrarySuffix;
		}
		handle = dlopen(full_driver_name.c_str(), RTLD_NOW | RTLD_LOCAL);
		if (!handle) {
			error_message += "\ndlopen() failed: ";
			error_message += dlerror();
		}
	}
	if (!handle) {
		SetError(error, error_message);
		// AdbcDatabaseInit tries to call this if set
		driver->release = nullptr;
		return ADBC_STATUS_INTERNAL;
	}

	void *load_handle = dlsym(handle, entrypoint);
	if (!load_handle) {
		std::string message = "dlsym(";
		message += entrypoint;
		message += ") failed: ";
		message += dlerror();
		SetError(error, message);
		return ADBC_STATUS_INTERNAL;
	}
	init_func = reinterpret_cast<AdbcDriverInitFunc>(load_handle);

#endif // defined(_WIN32)

	AdbcStatusCode status = AdbcLoadDriverFromInitFunc(init_func, version, driver, error);
	if (status == ADBC_STATUS_OK) {
		ManagerDriverState *state = new ManagerDriverState;
		state->driver_release = driver->release;
#if defined(_WIN32)
		state->handle = handle;
#endif // defined(_WIN32)
		driver->release = &ReleaseDriver;
		driver->private_manager = state;
	} else {
#if defined(_WIN32)
		if (!FreeLibrary(handle)) {
			std::string message = "FreeLibrary() failed: ";
			GetWinError(&message);
			SetError(error, message);
		}
#endif // defined(_WIN32)
	}
	return status;
}

AdbcStatusCode AdbcLoadDriverFromInitFunc(AdbcDriverInitFunc init_func, int version, void *raw_driver,
                                          struct AdbcError *error) {
#define FILL_DEFAULT(DRIVER, STUB)                                                                                     \
	if (!DRIVER->STUB) {                                                                                               \
		DRIVER->STUB = &STUB;                                                                                          \
	}
#define CHECK_REQUIRED(DRIVER, STUB)                                                                                   \
	if (!DRIVER->STUB) {                                                                                               \
		SetError(error, "Driver does not implement required function Adbc" #STUB);                                     \
		return ADBC_STATUS_INTERNAL;                                                                                   \
	}

	auto result = init_func(version, raw_driver, error);
	if (result != ADBC_STATUS_OK) {
		return result;
	}

	if (version == ADBC_VERSION_1_0_0) {
		auto *driver = reinterpret_cast<struct AdbcDriver *>(raw_driver);
		CHECK_REQUIRED(driver, DatabaseNew);
		CHECK_REQUIRED(driver, DatabaseInit);
		CHECK_REQUIRED(driver, DatabaseRelease);
		FILL_DEFAULT(driver, DatabaseSetOption);

		CHECK_REQUIRED(driver, ConnectionNew);
		CHECK_REQUIRED(driver, ConnectionInit);
		CHECK_REQUIRED(driver, ConnectionRelease);
		FILL_DEFAULT(driver, ConnectionCommit);
		FILL_DEFAULT(driver, ConnectionGetInfo);
		FILL_DEFAULT(driver, ConnectionGetObjects);
		FILL_DEFAULT(driver, ConnectionGetTableSchema);
		FILL_DEFAULT(driver, ConnectionGetTableTypes);
		FILL_DEFAULT(driver, ConnectionReadPartition);
		FILL_DEFAULT(driver, ConnectionRollback);
		FILL_DEFAULT(driver, ConnectionSetOption);

		FILL_DEFAULT(driver, StatementExecutePartitions);
		CHECK_REQUIRED(driver, StatementExecuteQuery);
		CHECK_REQUIRED(driver, StatementNew);
		CHECK_REQUIRED(driver, StatementRelease);
		FILL_DEFAULT(driver, StatementBind);
		FILL_DEFAULT(driver, StatementGetParameterSchema);
		FILL_DEFAULT(driver, StatementPrepare);
		FILL_DEFAULT(driver, StatementSetOption);
		FILL_DEFAULT(driver, StatementSetSqlQuery);
		FILL_DEFAULT(driver, StatementSetSubstraitPlan);
	}

	return ADBC_STATUS_OK;

#undef FILL_DEFAULT
#undef CHECK_REQUIRED
}
} // namespace duckdb_adbc
