//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/logging/log_storage.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unordered_set.hpp"

#include "duckdb/common/types/column/column_data_scan_states.hpp"
#include "duckdb/parallel/thread_context.hpp"

namespace duckdb {
struct RegisteredLoggingContext;
class ColumnDataCollection;
struct ColumnDataScanState;
class MemoryStream;
struct LogStorageConfig;

class LogStorageScanState {
public:
	virtual ~LogStorageScanState() = default;

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

// Interface for writing log entries
class LogStorage {
public:
	DUCKDB_API explicit LogStorage() {
	}
	DUCKDB_API virtual ~LogStorage() = default;

	//! WRITING
	DUCKDB_API virtual void WriteLogEntry(timestamp_t timestamp, LogLevel level, const string &log_type,
	                                      const string &log_message, const RegisteredLoggingContext &context) = 0;
	DUCKDB_API virtual void WriteLogEntries(DataChunk &chunk, const RegisteredLoggingContext &context) = 0;
	DUCKDB_API virtual void Flush() = 0;

	//! READING (OPTIONAL)
	DUCKDB_API virtual bool CanScan() {
		return false;
	}
	DUCKDB_API virtual unique_ptr<LogStorageScanState> CreateScanEntriesState() const;
	DUCKDB_API virtual bool ScanEntries(LogStorageScanState &state, DataChunk &result) const;
	DUCKDB_API virtual void InitializeScanEntries(LogStorageScanState &state) const;
	DUCKDB_API virtual unique_ptr<LogStorageScanState> CreateScanContextsState() const;
	DUCKDB_API virtual bool ScanContexts(LogStorageScanState &state, DataChunk &result) const;
	DUCKDB_API virtual void InitializeScanContexts(LogStorageScanState &state) const;

	DUCKDB_API virtual void Truncate();

	DUCKDB_API virtual void UpdateConfig(DatabaseInstance &db, case_insensitive_map_t<Value> &config);
};

struct LogStorageCsvConfig {
	LogStorageCsvConfig() {
		requires_quotes = make_unsafe_uniq_array<bool>(256);
		memset(requires_quotes.get(), 0, sizeof(bool) * 256);
		requires_quotes['\n'] = true;
		requires_quotes['\r'] = true;
		requires_quotes[NumericCast<idx_t>('\n')] = true;
		requires_quotes[NumericCast<idx_t>(',')] = true;
	}

	string delim = "\t";
	string newline = "\r\n";
	char quote = '\"';
	char escape = '\"';
	vector<string> null_strings = {""};

	unsafe_unique_array<bool> requires_quotes;
};

// Base class for loggers that write out log entries as CSV-parsable strings
class CSVLogStorage : public LogStorage {
public:
	explicit CSVLogStorage(DatabaseInstance &db);
	~CSVLogStorage() override;

	//! LogStorage API: WRITING
	void WriteLogEntry(timestamp_t timestamp, LogLevel level, const string &log_type, const string &log_message,
	                   const RegisteredLoggingContext &context) override;
	void WriteLogEntries(DataChunk &chunk, const RegisteredLoggingContext &context) override;

	void UpdateConfig(DatabaseInstance &db, case_insensitive_map_t<Value> &config) override;
	void Flush() override;

protected:
	virtual void UpdateConfigInternal(DatabaseInstance &db, case_insensitive_map_t<Value> &config);
	virtual void FlushInternal() = 0;

	mutable mutex lock;

	// Configuration for csv logger
	idx_t buffer_limit = 0;
	bool normalize_contexts = false;

	LogStorageCsvConfig csv_config;
	unique_ptr<MemoryStream> log_entries_stream;
	unique_ptr<MemoryStream> log_contexts_stream;

	// Used when normalizing the log into a log.csv and log_contexts.csv
	unordered_set<idx_t> registered_contexts;
};

class StdOutLogStorage : public CSVLogStorage {
public:
	explicit StdOutLogStorage(DatabaseInstance &db);
	~StdOutLogStorage() override;

protected:
	void Truncate() override;
	void FlushInternal() override;
};

class FileLogStorage : public CSVLogStorage {
public:
	explicit FileLogStorage(DatabaseInstance &db);
	~FileLogStorage() override;

	void Truncate() override;

protected:
	static string GetDefaultLogEntriesFilePath(DatabaseInstance &db);
	static string GetDefaultLogContextsFilePath(DatabaseInstance &db);

	void InitializeLogEntriesFile(DatabaseInstance &db, const string &path = "");
	void InitializeLogContextsFile(DatabaseInstance &db, const string &path = "");
	void InitializeFile(DatabaseInstance &db, const string &path, unique_ptr<FileHandle> &handle,
	                    bool &should_write_header);

	void UpdateConfigInternal(DatabaseInstance &db, case_insensitive_map_t<Value> &config) override;
	void FlushInternal() override;

	void WriteLogEntriesHeader();
	void WriteLogContextsHeader();

	DatabaseInstance &db;

	unique_ptr<FileHandle> log_entries_file_handle;
	unique_ptr<FileHandle> log_contexts_file_handle;

	bool log_entries_should_write_header = false;
	bool log_contexts_should_write_header = false;

	bool initialized = false;
};

class InMemoryLogStorageScanState : public LogStorageScanState {
public:
	InMemoryLogStorageScanState();
	~InMemoryLogStorageScanState() override;

	ColumnDataScanState scan_state;
};

class InMemoryLogStorage : public LogStorage {
public:
	explicit InMemoryLogStorage(DatabaseInstance &db);
	~InMemoryLogStorage() override;

	//! LogStorage API: WRITING
	void WriteLogEntry(timestamp_t timestamp, LogLevel level, const string &log_type, const string &log_message,
	                   const RegisteredLoggingContext &context) override;
	void WriteLogEntries(DataChunk &chunk, const RegisteredLoggingContext &context) override;
	void Flush() override;

	void Truncate() override;

	//! LogStorage API: READING
	bool CanScan() override;

	unique_ptr<LogStorageScanState> CreateScanEntriesState() const override;
	bool ScanEntries(LogStorageScanState &state, DataChunk &result) const override;
	void InitializeScanEntries(LogStorageScanState &state) const override;
	unique_ptr<LogStorageScanState> CreateScanContextsState() const override;
	bool ScanContexts(LogStorageScanState &state, DataChunk &result) const override;
	void InitializeScanContexts(LogStorageScanState &state) const override;

protected:
	void WriteLoggingContext(const RegisteredLoggingContext &context);
	void ResetBuffers();

protected:
	mutable mutex lock;

	void FlushInternal();

	//! Internal log entry storage
	unique_ptr<ColumnDataCollection> log_entries;
	unique_ptr<ColumnDataCollection> log_contexts;

	unordered_set<idx_t> registered_contexts;

	// Cache for direct logging
	unique_ptr<DataChunk> entry_buffer;
	unique_ptr<DataChunk> log_context_buffer;
	idx_t max_buffer_size;
};

} // namespace duckdb
