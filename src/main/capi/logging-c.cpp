#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/logging/log_storage.hpp"

namespace duckdb {

class CallbackLogStorage : public LogStorage {
public:
	CallbackLogStorage(const string &name, duckdb_logger_write_log_entry_t write_log_entry_fun)
	    : name(name), write_log_entry_fun(write_log_entry_fun) {
	}

	void WriteLogEntry(timestamp_t timestamp, LogLevel level, const string &log_type, const string &log_message,
	                   const RegisteredLoggingContext &context) override {
		if (write_log_entry_fun == nullptr) {
			return;
		}

		auto c_timestamp = reinterpret_cast<duckdb_timestamp *>(&timestamp);
		write_log_entry_fun(*c_timestamp, EnumUtil::ToChars(level), log_type.c_str(), log_message.c_str());
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
	duckdb_logger_write_log_entry_t write_log_entry_fun;
};

struct LogStorageWrapper {
	duckdb_logger_write_log_entry_t write_log_entry;
};

} // namespace duckdb

using duckdb::DatabaseWrapper;
using duckdb::LogStorageWrapper;

duckdb_log_storage duckdb_create_log_storage() {
	auto clog_storage = new LogStorageWrapper();
	return reinterpret_cast<duckdb_log_storage>(clog_storage);
}

void duckdb_destroy_log_storage(duckdb_log_storage storage) {
	if (!storage) {
		return;
	}
	auto clog_storage = reinterpret_cast<LogStorageWrapper *>(storage);
	delete clog_storage;
}

void duckdb_log_storage_set_write_log_entry(duckdb_log_storage storage, duckdb_logger_write_log_entry_t function) {
	if (!storage || !function) {
		return;
	}

	auto clog_storage = reinterpret_cast<LogStorageWrapper *>(storage);
	clog_storage->write_log_entry = function;
}

void duckdb_register_log_storage(duckdb_database database, const char *name, duckdb_log_storage storage) {
	if (!database || !name || !storage) {
		return;
	}

	const auto db_wrapper = reinterpret_cast<DatabaseWrapper *>(database);
	auto cast_storage = reinterpret_cast<LogStorageWrapper *>(storage);

	const auto &db = *db_wrapper->database;

	auto shared_storage_ptr = duckdb::make_shared_ptr<duckdb::CallbackLogStorage>(name, cast_storage->write_log_entry);
	duckdb::shared_ptr<duckdb::LogStorage> storage_ptr = shared_storage_ptr;
	db.instance->GetLogManager().RegisterLogStorage(name, storage_ptr);
}
