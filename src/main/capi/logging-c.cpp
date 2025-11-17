#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/logging/log_storage.hpp"

namespace duckdb {

class CallbackLogStorage : public LogStorage {
public:
	CallbackLogStorage(const string &name, write_log_entry_t write_log_entry_fun)
	    : name(name), write_log_entry_fun(write_log_entry_fun) {
	}

	void WriteLogEntry(timestamp_t timestamp, LogLevel level, const string &log_type, const string &log_message,
	                   const RegisteredLoggingContext &context) override {
		if (write_log_entry_fun == nullptr) {
			return;
		}
		write_log_entry_fun(timestamp, level, log_type, log_message);
	};

	void WriteLogEntries(DataChunk &chunk, const RegisteredLoggingContext &context) override {};

	void Flush(LoggingTargetTable table) override {};

	void FlushAll() override {};

	bool IsEnabled(LoggingTargetTable table) override {
		return true;
	}

	const string GetStorageName() override {
		return name;
	}

private:
	const string name;
	write_log_entry_t write_log_entry_fun;
};

//===--------------------------------------------------------------------===//
// Default Callbacks
//===--------------------------------------------------------------------===//

struct CLogStorage {
	duckdb_logger_write_log_entry_t write_log_entry;
};

} // namespace duckdb

using duckdb::CLogStorage;
using duckdb::DatabaseWrapper;

duckdb_log_storage duckdb_create_log_storage() {
	auto clog_storage = new CLogStorage();
	return reinterpret_cast<duckdb_log_storage>(clog_storage);
}

void duckdb_destroy_log_storage(duckdb_log_storage storage) {
	if (!storage) {
		return;
	}
	auto clog_storage = reinterpret_cast<CLogStorage *>(storage);
	delete clog_storage;
}

void duckdb_log_storage_set_write_log_entry(duckdb_log_storage storage, duckdb_logger_write_log_entry_t function) {
	if (!storage || !function) {
		return;
	}

	auto clog_storage = reinterpret_cast<CLogStorage *>(storage);
	clog_storage->write_log_entry = function;
}

void duckdb_register_log_storage(duckdb_database database, const char *name, duckdb_log_storage storage) {
	if (!database || !name || !storage) {
		return;
	}

	const auto db_wrapper = reinterpret_cast<DatabaseWrapper *>(database);
	auto converted_storage = reinterpret_cast<CLogStorage *>(storage);

	const auto &db = *db_wrapper->database;

	auto converted_write_log_entry_fun =
	    *reinterpret_cast<duckdb::write_log_entry_t *>(converted_storage->write_log_entry);

	auto shared_storage_ptr = duckdb::make_shared_ptr<duckdb::CallbackLogStorage>(name, converted_write_log_entry_fun);
	duckdb::shared_ptr<duckdb::LogStorage> storage_ptr = shared_storage_ptr;
	db.instance->GetLogManager().RegisterLogStorage(name, storage_ptr);
}
