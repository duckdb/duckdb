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

#include <dlfcn.h>
#include <algorithm>
#include <cstring>
#include <string>
#include <unordered_map>

namespace {
void ReleaseError(struct AdbcError *error) {
	if (error) {
		delete[] error->message;
		error->message = nullptr;
	}
}

void SetError(struct AdbcError *error, const std::string &message) {
	error->message = new char[message.size() + 1];
	message.copy(error->message, message.size());
	error->message[message.size()] = '\0';
	error->release = ReleaseError;
}

// Default stubs

AdbcStatusCode StatementBind(struct AdbcStatement *, struct ArrowArray *, struct ArrowSchema *,
                             struct AdbcError *error) {
	return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode StatementExecute(struct AdbcStatement *, struct AdbcError *error) {
	return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode StatementPrepare(struct AdbcStatement *, struct AdbcError *error) {
	return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode StatementSetOption(struct AdbcStatement *, const char *, const char *, struct AdbcError *error) {
	return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode StatementSetSqlQuery(struct AdbcStatement *, const char *, struct AdbcError *error) {
	return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode StatementSetSubstraitPlan(struct AdbcStatement *, const uint8_t *, size_t, struct AdbcError *error) {
	return ADBC_STATUS_NOT_IMPLEMENTED;
}

/// Temporary state while the database is being configured.
struct TempDatabase {
	std::unordered_map<std::string, std::string> options;
	std::string driver;
	std::string entrypoint;
};
} // namespace

// Direct implementations of API methods

AdbcStatusCode AdbcDatabaseNew(struct AdbcDatabase *database, struct AdbcError *error) {
	// Allocate a temporary structure to store options pre-Init
	database->private_data = new TempDatabase;
	database->private_driver = nullptr;
	return ADBC_STATUS_OK;
}

AdbcStatusCode AdbcDatabaseSetOption(struct AdbcDatabase *database, const char *key, const char *value,
                                     struct AdbcError *error) {
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

AdbcStatusCode AdbcDatabaseInit(struct AdbcDatabase *database, struct AdbcError *error) {
	if (!database->private_data) {
		SetError(error, "Must call AdbcDatabaseNew first");
		return ADBC_STATUS_UNINITIALIZED;
	}
	TempDatabase *args = reinterpret_cast<TempDatabase *>(database->private_data);
	if (args->driver.empty()) {
		delete args;
		SetError(error, "Must provide 'driver' parameter");
		return ADBC_STATUS_INVALID_ARGUMENT;
	} else if (args->entrypoint.empty()) {
		delete args;
		SetError(error, "Must provide 'entrypoint' parameter");
		return ADBC_STATUS_INVALID_ARGUMENT;
	}

	database->private_driver = new AdbcDriver;
	size_t initialized = 0;
	AdbcStatusCode status = AdbcLoadDriver(args->driver.c_str(), args->entrypoint.c_str(), ADBC_VERSION_0_0_1,
	                                       database->private_driver, &initialized, error);
	if (status != ADBC_STATUS_OK) {
		delete args;
		delete database->private_driver;
		return status;
	} else if (initialized < ADBC_VERSION_0_0_1) {
		delete args;
		delete database->private_driver;
		SetError(error, "Database version is too old"); // TODO: clearer error
		return status;
	}
	status = database->private_driver->DatabaseNew(database, error);
	if (status != ADBC_STATUS_OK) {
		delete args;
		delete database->private_driver;
		return status;
	}
	for (const auto &option : args->options) {
		status =
		    database->private_driver->DatabaseSetOption(database, option.first.c_str(), option.second.c_str(), error);
		if (status != ADBC_STATUS_OK) {
			delete args;
			delete database->private_driver;
			return status;
		}
	}
	delete args;
	return database->private_driver->DatabaseInit(database, error);
}

AdbcStatusCode AdbcDatabaseRelease(struct AdbcDatabase *database, struct AdbcError *error) {
	if (!database->private_driver) {
		return ADBC_STATUS_UNINITIALIZED;
	}
	auto status = database->private_driver->DatabaseRelease(database, error);
	delete database->private_driver;
	return status;
}

AdbcStatusCode AdbcConnectionNew(struct AdbcDatabase *database, struct AdbcConnection *connection,
                                 struct AdbcError *error) {
	if (!database->private_driver) {
		return ADBC_STATUS_UNINITIALIZED;
	}
	auto status = database->private_driver->ConnectionNew(database, connection, error);
	connection->private_driver = database->private_driver;
	return status;
}

AdbcStatusCode AdbcConnectionInit(struct AdbcConnection *connection, struct AdbcError *error) {
	if (!connection->private_driver) {
		// TODO: set error
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	return connection->private_driver->ConnectionInit(connection, error);
}

AdbcStatusCode AdbcConnectionRelease(struct AdbcConnection *connection, struct AdbcError *error) {
	if (!connection->private_driver) {
		return ADBC_STATUS_UNINITIALIZED;
	}
	auto status = connection->private_driver->ConnectionRelease(connection, error);
	connection->private_driver = nullptr;
	return status;
}

AdbcStatusCode AdbcStatementBind(struct AdbcStatement *statement, struct ArrowArray *values, struct ArrowSchema *schema,
                                 struct AdbcError *error) {
	if (!statement->private_driver) {
		return ADBC_STATUS_UNINITIALIZED;
	}
	return statement->private_driver->StatementBind(statement, values, schema, error);
}

AdbcStatusCode AdbcStatementBindStream(struct AdbcStatement *statement, struct ArrowArrayStream *stream,
                                       struct AdbcError *error) {
	if (!statement->private_driver) {
		return ADBC_STATUS_UNINITIALIZED;
	}
	return statement->private_driver->StatementBindStream(statement, stream, error);
}

AdbcStatusCode AdbcStatementExecute(struct AdbcStatement *statement, struct AdbcError *error) {
	if (!statement->private_driver) {
		return ADBC_STATUS_UNINITIALIZED;
	}
	return statement->private_driver->StatementExecute(statement, error);
}

AdbcStatusCode AdbcStatementGetStream(struct AdbcStatement *statement, struct ArrowArrayStream *out,
                                      struct AdbcError *error) {
	if (!statement->private_driver) {
		return ADBC_STATUS_UNINITIALIZED;
	}
	return statement->private_driver->StatementGetStream(statement, out, error);
}

AdbcStatusCode AdbcStatementNew(struct AdbcConnection *connection, struct AdbcStatement *statement,
                                struct AdbcError *error) {
	if (!connection->private_driver) {
		return ADBC_STATUS_INVALID_ARGUMENT;
	}
	auto status = connection->private_driver->StatementNew(connection, statement, error);
	statement->private_driver = connection->private_driver;
	return status;
}

AdbcStatusCode AdbcStatementPrepare(struct AdbcStatement *statement, struct AdbcError *error) {
	if (!statement->private_driver) {
		return ADBC_STATUS_UNINITIALIZED;
	}
	return statement->private_driver->StatementPrepare(statement, error);
}

AdbcStatusCode AdbcStatementRelease(struct AdbcStatement *statement, struct AdbcError *error) {
	if (!statement->private_driver) {
		return ADBC_STATUS_UNINITIALIZED;
	}
	auto status = statement->private_driver->StatementRelease(statement, error);
	statement->private_driver = nullptr;
	return status;
}

AdbcStatusCode AdbcStatementSetOption(struct AdbcStatement *statement, const char *key, const char *value,
                                      struct AdbcError *error) {
	if (!statement->private_driver) {
		return ADBC_STATUS_UNINITIALIZED;
	}
	return statement->private_driver->StatementSetOption(statement, key, value, error);
}

AdbcStatusCode AdbcStatementSetSqlQuery(struct AdbcStatement *statement, const char *query, struct AdbcError *error) {
	if (!statement->private_driver) {
		return ADBC_STATUS_UNINITIALIZED;
	}
	return statement->private_driver->StatementSetSqlQuery(statement, query, error);
}

AdbcStatusCode AdbcStatementSetSubstraitPlan(struct AdbcStatement *statement, const uint8_t *plan, size_t length,
                                             struct AdbcError *error) {
	if (!statement->private_driver) {
		return ADBC_STATUS_UNINITIALIZED;
	}
	return statement->private_driver->StatementSetSubstraitPlan(statement, plan, length, error);
}

const char *AdbcStatusCodeMessage(AdbcStatusCode code) {
#define STRINGIFY(s)       #s
#define STRINGIFY_VALUE(s) STRINGIFY(s)
#define CASE(CONSTANT)                                                                                                 \
	case CONSTANT:                                                                                                     \
		return STRINGIFY(CONSTANT) " (" STRINGIFY_VALUE(CONSTANT) ")";

	switch (code) {
		CASE(ADBC_STATUS_OK)
		CASE(ADBC_STATUS_UNKNOWN)
		CASE(ADBC_STATUS_NOT_IMPLEMENTED)
		CASE(ADBC_STATUS_UNINITIALIZED)
		CASE(ADBC_STATUS_INVALID_ARGUMENT)
		CASE(ADBC_STATUS_INTERNAL)
		CASE(ADBC_STATUS_IO)
	default:
		return "(invalid code)";
	}
#undef CASE
#undef STRINGIFY_VALUE
#undef STRINGIFY
}

AdbcStatusCode AdbcLoadDriver(const char *driver_name, const char *entrypoint, size_t count, struct AdbcDriver *driver,
                              size_t *initialized, struct AdbcError *error) {
#define FILL_DEFAULT(DRIVER, STUB)                                                                                     \
	if (!DRIVER->STUB) {                                                                                               \
		DRIVER->STUB = &STUB;                                                                                          \
	}
#define CHECK_REQUIRED(DRIVER, STUB)                                                                                   \
	if (!DRIVER->STUB) {                                                                                               \
		SetError(error, "Driver does not implement required function Adbc" #STUB);                                     \
		return ADBC_STATUS_INTERNAL;                                                                                   \
	}

	void *handle = dlopen(driver_name, RTLD_NOW | RTLD_LOCAL);
	if (!handle) {
		std::string message = "dlopen() failed: ";
		message += dlerror();
		SetError(error, message);
		return ADBC_STATUS_UNKNOWN;
	}

	void *load_handle = dlsym(handle, entrypoint);
	auto *load = reinterpret_cast<AdbcDriverInitFunc>(load_handle);
	if (!load) {
		std::string message = "dlsym() failed: ";
		message += dlerror();
		SetError(error, message);
		return ADBC_STATUS_INTERNAL;
	}

	auto result = load(count, driver, initialized, error);
	if (result != ADBC_STATUS_OK) {
		return result;
	}

	CHECK_REQUIRED(driver, DatabaseNew);
	CHECK_REQUIRED(driver, DatabaseInit);
	CHECK_REQUIRED(driver, DatabaseRelease);

	FILL_DEFAULT(driver, StatementBind);
	FILL_DEFAULT(driver, StatementExecute);
	FILL_DEFAULT(driver, StatementPrepare);
	FILL_DEFAULT(driver, StatementSetSqlQuery);
	FILL_DEFAULT(driver, StatementSetSubstraitPlan);

	return ADBC_STATUS_OK;

#undef FILL_DEFAULT
#undef CHECK_REQUIRED
}