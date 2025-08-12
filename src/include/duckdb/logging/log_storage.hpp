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
class CSVWriter;
struct CSVWriterState;
class BufferedFileWriter;

//! Logging storage can store entries normalized or denormalized. This enum describes what a single table/file/etc contains
enum class LoggingTargetTable {
	ALL_LOGS,			// Denormalized: log entries consisting of both the full log entry and the context
	LOG_ENTRIES,		// Normalized: contains only the log entries and a context_id
	LOG_CONTEXTS,    // Normalized: contains only the log contexts
};

class LogStorageScanState {
public:
	LogStorageScanState(LoggingTargetTable table_p) : table(table_p) {

	}
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

	LoggingTargetTable table;
};

// Interface for Log Storage
class LogStorage {
public:
	DUCKDB_API explicit LogStorage() {
	}
	DUCKDB_API virtual ~LogStorage() = default;

	static vector<LogicalType> GetSchema(LoggingTargetTable table);
	static vector<string> GetColumnNames(LoggingTargetTable table);


	//! WRITING
	DUCKDB_API virtual void WriteLogEntry(timestamp_t timestamp, LogLevel level, const string &log_type,
	                                      const string &log_message, const RegisteredLoggingContext &context) = 0;
	DUCKDB_API virtual void WriteLogEntries(DataChunk &chunk, const RegisteredLoggingContext &context) = 0;
	DUCKDB_API virtual void FlushAll() = 0;
	DUCKDB_API virtual void Flush(LoggingTargetTable table) = 0;

	DUCKDB_API virtual void Truncate();

	DUCKDB_API virtual bool IsEnabled(LoggingTargetTable table) = 0;

	//! READING (OPTIONAL)
	DUCKDB_API virtual bool CanScan(LoggingTargetTable table) {
		return false;
	}
	// Reading interface 1: basic single-threaded scan
	DUCKDB_API virtual unique_ptr<LogStorageScanState> CreateScanState(LoggingTargetTable table) const;
	DUCKDB_API virtual bool Scan(LogStorageScanState &state, DataChunk &result) const;
	DUCKDB_API virtual void InitializeScan(LogStorageScanState &state) const;

	// Reading interface 2: using bind_replace
	DUCKDB_API virtual unique_ptr<TableRef> BindReplace(ClientContext &context, TableFunctionBindInput &input, LoggingTargetTable table);

	//! CONFIGURATION
	DUCKDB_API virtual void UpdateConfig(DatabaseInstance &db, case_insensitive_map_t<Value> &config);
};

//! The buffering Log storage implements a buffering mechanism around the Base LogStorage class. It simplifies the
//! implementation of LogStorage
class BufferingLogStorage : public LogStorage {
public:
	explicit BufferingLogStorage(DatabaseInstance &db_p, idx_t buffer_size, bool normalize);
	~BufferingLogStorage() override;

	//! The BufferingLogStorage implements the core Log Writing functions: all log entries will be ingested through this
	void WriteLogEntry(timestamp_t timestamp, LogLevel level, const string &log_type, const string &log_message,
	                   const RegisteredLoggingContext &context) final;
	void WriteLogEntries(DataChunk &chunk, const RegisteredLoggingContext &context) final;
	void FlushAll() final;
	void Flush(LoggingTargetTable table) final;

	void Truncate() override;

	void UpdateConfig(DatabaseInstance &db, case_insensitive_map_t<Value> &config) override;

	bool IsEnabled(LoggingTargetTable table) override;

protected:
	//! To be implemented by inheriting classes: the BufferedLogStorage will call this method once a buffer is full
	virtual void FlushChunk(LoggingTargetTable table, DataChunk &chunk) = 0;

	//! TODO: rename to make this more clear?
	virtual void UpdateConfigInternal(DatabaseInstance &db, case_insensitive_map_t<Value> &config);

	virtual bool IsEnabledInternal(LoggingTargetTable table);

protected:
	void FlushAllInternal();
	void FlushInternal(LoggingTargetTable table);
	void WriteLoggingContext(const RegisteredLoggingContext &context);

	//! ResetAllBuffers will clear all unflushed data
	virtual void ResetAllBuffers();

protected:
	mutable mutex lock;

	// TODO: move to private
	bool normalize_contexts = true;

private:
	//! Resets the log buffers
	void ResetLogBuffers();

	unordered_set<idx_t> registered_contexts;

	// Configuration for buffering
	idx_t buffer_limit = 0;

	// Debug option for testing buffering behaviour
	bool only_flush_on_full_buffer = false;

	unordered_map<LoggingTargetTable, unique_ptr<DataChunk>> buffers;

	// This flag is set whenever a new context_is written to the entry buffer. It means that the next flush of
	// LoggingTargetTable::LOG_ENTRIES also requires a flush of LoggingTargetTable::LOG_CONTEXTS
	bool flush_contexts_on_next_entry_flush = false;

};
// Abstract base class for loggers that write out log entries as CSV-parsable strings
//
class CSVLogStorage : public BufferingLogStorage {
public:
	explicit CSVLogStorage(DatabaseInstance &db, bool normalize);
	~CSVLogStorage() override;


protected:
	//! Implement the BufferingLogStorage interface
	void FlushChunk(LoggingTargetTable table, DataChunk &chunk) final;

protected:
	//! Hooks to be implemented on flushing
	virtual void BeforeFlush(LoggingTargetTable table, DataChunk &chunk) {
	};
	virtual void OnFlush(LoggingTargetTable table, DataChunk &chunk) {
	};


	//! Resets all buffers and state
	void ResetAllBuffers() override;

	static void SetWriterConfigs(CSVWriter &Writer, vector<string> column_names);

	unordered_map<LoggingTargetTable, unique_ptr<DataChunk>> cast_buffers;
	//! Todo make private and access through methods?
	unordered_map<LoggingTargetTable, unique_ptr<CSVWriter>> writers;

private:
	void ExecuteCast(LoggingTargetTable table, DataChunk &chunk);
	// Reset the Cast chunks
	void ResetCastChunk();

	void InitializeCastChunk(LoggingTargetTable table);

	unordered_set<idx_t> registered_contexts;
};

class StdOutLogStorage : public CSVLogStorage {
public:
	explicit StdOutLogStorage(DatabaseInstance &db);
	~StdOutLogStorage() override;

protected:
	void OnFlush(LoggingTargetTable table, DataChunk &chunk) override;
	//! TODO: implement StdoutStream instead
	unique_ptr<MemoryStream> stdout_stream;
};

class FileLogStorage : public CSVLogStorage {
public:
	explicit FileLogStorage(DatabaseInstance &db);
	~FileLogStorage() override;

	void Truncate() override;

	unique_ptr<TableRef> BindReplace(ClientContext &context, TableFunctionBindInput &input, LoggingTargetTable table) override;

protected:
	void InitializeFile(DatabaseInstance &db, LoggingTargetTable table);
	static unique_ptr<BufferedFileWriter> InitializeFileWriter(DatabaseInstance &db, const string &path);

	void UpdateConfigInternal(DatabaseInstance &db, case_insensitive_map_t<Value> &config) override;

	void BeforeFlush(LoggingTargetTable table, DataChunk &chunk) override;
	void OnFlush(LoggingTargetTable table, DataChunk &chunk) override;

	void Initialize(LoggingTargetTable table);

	unique_ptr<TableRef> BindReplaceInternal(ClientContext &context, TableFunctionBindInput &input, const string &path,
	                                         const string &select_clause, const string &csv_columns);

	DatabaseInstance &db;

	//! Passed as WriteStreams to the CSVWriter in the base class
	unordered_map<LoggingTargetTable, unique_ptr<BufferedFileWriter>> file_writers;
	unordered_map<LoggingTargetTable, bool> initialized_writers;
	unordered_map<LoggingTargetTable, string> file_paths;

	//!
	string base_path;

private:
	void SetPaths(const string &base_path);
};

class InMemoryLogStorageScanState : public LogStorageScanState {
public:
	InMemoryLogStorageScanState(LoggingTargetTable table);
	~InMemoryLogStorageScanState() override;

	ColumnDataScanState scan_state;
};

class InMemoryLogStorage : public BufferingLogStorage {
public:
	explicit InMemoryLogStorage(DatabaseInstance &db);
	~InMemoryLogStorage() override;

	//! LogStorage API: READING
	bool CanScan(LoggingTargetTable table) override;

	unique_ptr<LogStorageScanState> CreateScanState(LoggingTargetTable table) const override;
	bool Scan(LogStorageScanState &state, DataChunk &result) const override;
	void InitializeScan(LogStorageScanState &state) const override;

protected:
	void FlushChunk(LoggingTargetTable table, DataChunk &chunk) override;
	void ResetAllBuffers() override;

private:
	ColumnDataCollection &GetBuffer(LoggingTargetTable table) const {
		auto res = log_storage_buffers.find(table);
		if (res == log_storage_buffers.end()) {
			throw InternalException("Failed to find table");
		}
		return *res->second;
	}

	unordered_map<LoggingTargetTable, unique_ptr<ColumnDataCollection>> log_storage_buffers;
};

} // namespace duckdb
