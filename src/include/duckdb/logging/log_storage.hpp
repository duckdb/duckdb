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
};

class StdOutLogStorage : public LogStorage {
public:
	explicit StdOutLogStorage();
	~StdOutLogStorage() override;

	//! LogStorage API: WRITING
	void WriteLogEntry(timestamp_t timestamp, LogLevel level, const string &log_type, const string &log_message,
	                   const RegisteredLoggingContext &context) override;
	void WriteLogEntries(DataChunk &chunk, const RegisteredLoggingContext &context) override;
	void Flush() override;
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
