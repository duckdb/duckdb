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
	DUCKDB_API virtual void Truncate();

	//! READING (OPTIONAL)
	DUCKDB_API virtual bool CanScan() {
		return false;
	}
	// Reading interface 1: basic single-threaded scan
	DUCKDB_API virtual unique_ptr<LogStorageScanState> CreateScanEntriesState() const;
	DUCKDB_API virtual bool ScanEntries(LogStorageScanState &state, DataChunk &result) const;
	DUCKDB_API virtual void InitializeScanEntries(LogStorageScanState &state) const;
	DUCKDB_API virtual unique_ptr<LogStorageScanState> CreateScanContextsState() const;
	DUCKDB_API virtual bool ScanContexts(LogStorageScanState &state, DataChunk &result) const;
	DUCKDB_API virtual void InitializeScanContexts(LogStorageScanState &state) const;

	// Reading interface 2: using bind_replace
	DUCKDB_API virtual unique_ptr<TableRef> BindReplaceEntries(ClientContext &context, TableFunctionBindInput &input);
	DUCKDB_API virtual unique_ptr<TableRef> BindReplaceContexts(ClientContext &context, TableFunctionBindInput &input);

	//! CONFIGURATION
	DUCKDB_API virtual void UpdateConfig(DatabaseInstance &db, case_insensitive_map_t<Value> &config);
};

class BufferingLogStorage : public LogStorage {
public:
	explicit BufferingLogStorage(DatabaseInstance &db);
	~BufferingLogStorage() override;

	//! Log message buffer schemas
	static vector<LogicalType> GetContextsSchema();
	static vector<LogicalType> GetEntriesSchema(bool normalize);
	static vector<string> GetContextsColumnNames();
	static vector<string> GetEntriesColumnNames(bool normalize);

	//! LogStorage API: WRITING
	void WriteLogEntry(timestamp_t timestamp, LogLevel level, const string &log_type, const string &log_message,
	                   const RegisteredLoggingContext &context) override;
	void WriteLogEntries(DataChunk &chunk, const RegisteredLoggingContext &context) override;
	void Flush() override;
	void Truncate() override;

protected:
	void WriteLoggingContext(const RegisteredLoggingContext &context);

	//! ResetAllBuffers will clear all unflushed data
	virtual void ResetAllBuffers();

private:
	//! Resets the log buffers
	void ResetLogBuffers();

protected:
	mutable mutex lock;

	//! To be implemented by base classes: Flush the buffers to storage
	virtual void FlushInternal() = 0;

	unordered_set<idx_t> registered_contexts;

	// Configuration for csv logger
	idx_t buffer_limit = 0;
	bool normalize_contexts = true;

	// Cache for direct logging
	unique_ptr<DataChunk> log_entries_buffer;
	unique_ptr<DataChunk> log_contexts_buffer;
	idx_t max_buffer_size;
};

// TODO: remove
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

// Abstract base class for loggers that write out log entries as CSV-parsable strings
// subclasses should:
class CSVLogStorage : public BufferingLogStorage {
public:
	explicit CSVLogStorage(DatabaseInstance &db);
	~CSVLogStorage() override;

	void UpdateConfig(DatabaseInstance &db, case_insensitive_map_t<Value> &config) override;

protected:
	virtual void UpdateConfigInternal(DatabaseInstance &db, case_insensitive_map_t<Value> &config);
	void FlushInternal() override;
	void ExecuteCast();

	//! Resets all buffers and state
	void ResetAllBuffers() override;
	// Reset the Cast chunks
	void ResetCastChunk();

	static void SetWriterConfigs(CSVWriter &Writer, vector<string> column_names);

	mutable mutex lock;

	// Subclasses use these to write out the log entries to CSV
	unique_ptr<CSVWriter> log_entries_writer;
	unique_ptr<CSVWriter> log_contexts_writer;

	// Cast chunks for casting to string
	unique_ptr<DataChunk> log_entries_cast_buffer;
	unique_ptr<DataChunk> log_contexts_cast_buffer;

	// Used when normalizing the log into a log.csv and log_contexts.csv
	unordered_set<idx_t> registered_contexts;
};

class StdOutLogStorage : public CSVLogStorage {
public:
	explicit StdOutLogStorage(DatabaseInstance &db);
	~StdOutLogStorage() override;

protected:
	void FlushInternal() override;

	unique_ptr<MemoryStream> log_entries_stream;
	unique_ptr<MemoryStream> log_contexts_stream;
};

class FileLogStorage : public CSVLogStorage {
public:
	explicit FileLogStorage(DatabaseInstance &db);
	~FileLogStorage() override;

	void Truncate() override;

	unique_ptr<TableRef> BindReplaceEntries(ClientContext &context, TableFunctionBindInput &input) override;
	unique_ptr<TableRef> BindReplaceContexts(ClientContext &context, TableFunctionBindInput &input) override;

protected:
	static string GetDefaultLogEntriesFilePath(DatabaseInstance &db);
	static string GetDefaultLogContextsFilePath(DatabaseInstance &db);

	void InitializeLogEntriesFile(DatabaseInstance &db);
	void InitializeLogContextsFile(DatabaseInstance &db);
	static unique_ptr<BufferedFileWriter> InitializeFileWriter(DatabaseInstance &db, const string &path);

	void UpdateConfigInternal(DatabaseInstance &db, case_insensitive_map_t<Value> &config) override;
	void FlushInternal() override;

	void Initialize();

	static void InitializeFile(DatabaseInstance &db, const string &path,
	                           unique_ptr<BufferedFileWriter> &log_contexts_file_writer,
	                           unique_ptr<CSVWriter> &log_contexts_writer, vector<string> column_names);

	unique_ptr<TableRef> BindReplaceInternal(ClientContext &context, TableFunctionBindInput &input, const string &path,
	                                         const string &select_clause, const string &csv_columns);

	DatabaseInstance &db;

	//! Passed as WriteStreams to the CSVWriter in the base class
	unique_ptr<BufferedFileWriter> log_entries_file_writer;
	unique_ptr<BufferedFileWriter> log_contexts_file_writer;

	//! Used for lazily opening the `log_entries_file_handle` and `log_contexts_file_handle` on first Flush
	bool initialized = false;

	string log_contexts_path;
	string log_entries_path;

	//! Keep track whether the writers are initialized (have header written)
	bool log_entries_should_initialize = false;
	bool log_contexts_should_initialize = false;
};

class InMemoryLogStorageScanState : public LogStorageScanState {
public:
	InMemoryLogStorageScanState();
	~InMemoryLogStorageScanState() override;

	ColumnDataScanState scan_state;
};

class InMemoryLogStorage : public BufferingLogStorage {
public:
	explicit InMemoryLogStorage(DatabaseInstance &db);
	~InMemoryLogStorage() override;

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
	mutable mutex lock;
	void ResetInMemoryBuffers();
	void FlushInternal() override;

	void ResetAllBuffers() override;

	//! Passed as WriteStreams to the base class CSVWriter
	unique_ptr<ColumnDataCollection> log_entries;
	unique_ptr<ColumnDataCollection> log_contexts;
};

} // namespace duckdb
