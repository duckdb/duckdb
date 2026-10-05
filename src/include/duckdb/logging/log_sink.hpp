//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/logging/log_sink.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/logging/logging.hpp"
#include "duckdb/logging/log_format_writer.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/serializer/write_stream.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"

#include "duckdb/common/types/column/column_data_scan_states.hpp"
#include "duckdb/parallel/thread_context.hpp"

namespace duckdb {
struct TableFunctionBindInput;
struct RegisteredLoggingContext;
class ColumnDataCollection;
struct ColumnDataScanState;
class MemoryStream;
struct LogSinkConfig;
class CSVWriter;
struct CSVWriterState;
class BufferedFileWriter;
struct CSVWriterOptions;
struct CSVReaderOptions;

//! Logging sink can store entries normalized or denormalized. This enum describes what a single table/file/etc
//! contains
enum class LoggingTargetTable : uint8_t {
	ALL_LOGS,     // Denormalized: log entries consisting of both the full log entry and the context
	LOG_ENTRIES,  // Normalized: contains only the log entries and a context_id
	LOG_CONTEXTS, // Normalized: contains only the log contexts
};

class LogSinkScanState {
public:
	explicit LogSinkScanState(LoggingTargetTable table_p) : table(table_p) {
	}
	virtual ~LogSinkScanState() = default;

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

// Interface for Log Sink
class LogSink {
public:
	DUCKDB_API explicit LogSink() {
	}
	DUCKDB_API virtual ~LogSink() = default;

	virtual const string GetSinkName() = 0;

	static vector<LogicalType> GetSchema(LoggingTargetTable table);
	static vector<Identifier> GetColumnNames(LoggingTargetTable table);

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
	DUCKDB_API virtual unique_ptr<LogSinkScanState> CreateScanState(LoggingTargetTable table) const;
	DUCKDB_API virtual bool Scan(LogSinkScanState &state, DataChunk &result) const;
	DUCKDB_API virtual void InitializeScan(LogSinkScanState &state) const;

	// Reading interface 2: using bind_replace
	DUCKDB_API virtual unique_ptr<TableRef> BindReplace(ClientContext &context, TableFunctionBindInput &input,
	                                                    LoggingTargetTable table);

	//! CONFIGURATION
	DUCKDB_API virtual void UpdateConfig(DatabaseInstance &db, case_insensitive_map_t<Value> &config);
};

//! The buffering Log sink implements a buffering mechanism around the Base LogSink class. It implements some
//! general features that most log sinks will need.
class BufferingLogSink : public LogSink {
public:
	explicit BufferingLogSink(DatabaseInstance &db_p, idx_t buffer_size, bool normalize);
	~BufferingLogSink() override;

	/// (Partially) Implements  LogSink API

	//! Write out the entry to the buffers
	void WriteLogEntry(timestamp_t timestamp, LogLevel level, const string &log_type, const string &log_message,
	                   const RegisteredLoggingContext &context) final;
	//! Write out the chunk to the buffers
	void WriteLogEntries(DataChunk &chunk, const RegisteredLoggingContext &context) final;
	//! Flushes buffers for all tables
	void FlushAll() final;
	//! Flushes buffer for a specific table
	void Flush(LoggingTargetTable table) final;
	//! Truncates log sink: both buffers and persistent storage (if applicable)
	void Truncate() override;
	//! Apply a new log sink configuration
	void UpdateConfig(DatabaseInstance &db, case_insensitive_map_t<Value> &config) override;
	//! Returns whether the table is enabled for this sink
	bool IsEnabled(LoggingTargetTable table) override;

protected:
	/// Interface to child classes

	//! Invoked whenever buffers are full to flush to storage
	virtual void FlushChunk(LoggingTargetTable table, DataChunk &chunk) = 0;
	//! This method is called in a chained way down the class hierarchy. This allows each class to interpret its own
	//! part of the config. Unhandled config values that are left over will result in an error
	virtual void UpdateConfigInternal(DatabaseInstance &db, case_insensitive_map_t<Value> &config);
	//! ResetAllBuffers will clear all buffered data. To be overridden by child classes to ensure their buffers are
	//! flushed too
	virtual void ResetAllBuffers();

	/// Helper methods

	//! Flushes all tables
	void FlushAllInternal();
	//! Flushes one of the tables
	void FlushInternal(LoggingTargetTable table);
	//! Whether a specific table is available in the log sink
	bool IsEnabledInternal(LoggingTargetTable table);

	idx_t GetBufferLimit() const;

	//! lock to be used by this class and child classes to ensure thread safety TODO: maybe remove and delegate
	//! thread-safety to LogManager?
	mutable mutex lock;
	//! Switches between using false = use LoggingTargetTable::ALL_LOGS, true = use LoggingTargetTable::LOG_ENTRIES +
	//! LoggingTargetTable::CONTEXTS
	bool normalize_contexts = true;

private:
	//! Resets the log buffers
	void ResetLogBuffers();
	//! Write out a logging context
	void WriteLoggingContext(const RegisteredLoggingContext &context);

	//! The currently registered RegisteredLoggingContext's
	unordered_set<idx_t> registered_contexts;
	//! Configuration for buffering
	idx_t buffer_limit = 0;
	//! Debug option for testing buffering behaviour
	bool only_flush_on_full_buffer = false;
	//! The buffers used for each table
	map<LoggingTargetTable, unique_ptr<DataChunk>> buffers;
	//! This flag is set whenever a new context_is written to the entry buffer. It means that the next flush of
	//! LoggingTargetTable::LOG_ENTRIES also requires a flush of LoggingTargetTable::LOG_CONTEXTS
	bool flush_contexts_on_next_entry_flush = false;
};

//! stdout sink using log lines in CSV. Denormalized only (single stream).
class StdOutLogSink : public BufferingLogSink {
public:
	explicit StdOutLogSink(DatabaseInstance &db);
	~StdOutLogSink() override;
 
	const string GetSinkName() override {
		return "StdOutLogSink";
	}
 
protected:
	void FlushChunk(LoggingTargetTable table, DataChunk &chunk) override;
	void UpdateConfigInternal(DatabaseInstance &db, case_insensitive_map_t<Value> &config) override;
 
private:
	class StdOutWriteStream : public WriteStream {
		void WriteData(const_data_ptr_t buffer, idx_t write_size) override;
	};
 
	StdOutWriteStream stdout_stream;
	unique_ptr<CSVFormatWriter> writer; // single writer, ALL_LOGS table only
};

class FileLogSink : public BufferingLogSink {
public:
	explicit FileLogSink(DatabaseInstance &db);
	~FileLogSink() override;

	const string GetSinkName() override {
		return "FileLogSink";
	}

	void Truncate() override;
	unique_ptr<TableRef> BindReplace(ClientContext &context, TableFunctionBindInput &input,
	                                 LoggingTargetTable table) override;

protected:
	void FlushChunk(LoggingTargetTable table, DataChunk &chunk) override;
	void UpdateConfigInternal(DatabaseInstance &db, case_insensitive_map_t<Value> &config) override;
	// ResetAllBuffers: inherited from BufferingLogSink unchanged — no CSV-specific
	// cast-buffer reset here anymore, that's CSVFormatWriter's own concern now.

private:
	// Lazily creates file_writer + LogFormatWriter for `table`, if not already done.
	void Initialize(LoggingTargetTable table);
	void InitializeFile(DatabaseInstance &db, LoggingTargetTable table);
	static unique_ptr<BufferedFileWriter> InitializeFileWriter(DatabaseInstance &db, const string &path);
	unique_ptr<TableRef> BindReplaceInternal(ClientContext &context, TableFunctionBindInput &input,
	                                         const string &path, const string &select_clause,
	                                         const string &csv_columns);
	void SetPaths(const string &base_path);

	DatabaseInstance &db;

	struct TableWriter {
		unique_ptr<CSVFormatWriter> writer;
		unique_ptr<BufferedFileWriter> file_writer;
		string path;
		bool initialized = false;
	};
	map<LoggingTargetTable, TableWriter> tables;

	string base_path;
};

//! State for scanning the in memory buffers
class InMemoryLogSinkScanState : public LogSinkScanState {
public:
	explicit InMemoryLogSinkScanState(LoggingTargetTable table);
	~InMemoryLogSinkScanState() override;

	ColumnDataScanState scan_state;
};

//! The InMemoryLogSink implements a log sink that is backed by ColumnDataCollection's. It only support normalized
//! mode and support a basic single-threaded scan. TODO: improve?
class InMemoryLogSink : public BufferingLogSink {
public:
	explicit InMemoryLogSink(DatabaseInstance &db);
	~InMemoryLogSink() override;

	const string GetSinkName() override {
		return "InMemoryLogSink";
	}

	//! Implement LogSink Single-threaded scan interface
	bool CanScan(LoggingTargetTable table) override;
	unique_ptr<LogSinkScanState> CreateScanState(LoggingTargetTable table) const override;
	bool Scan(LogSinkScanState &state, DataChunk &result) const override;
	void InitializeScan(LogSinkScanState &state) const override;

protected:
	/// Implement BufferingLogSink interface

	//! Flushes a chunk to the corresponding ColumnDataCollection
	void FlushChunk(LoggingTargetTable table, DataChunk &chunk) override;
	//! Resets the all ColumnDataCollection's
	void ResetAllBuffers() override;

private:
	//! Helper function to get the buffer
	ColumnDataCollection &GetBuffer(LoggingTargetTable table) const;

	map<LoggingTargetTable, unique_ptr<ColumnDataCollection>> log_sink_buffers;
};

} // namespace duckdb
