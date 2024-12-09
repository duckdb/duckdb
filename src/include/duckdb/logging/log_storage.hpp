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

#include <duckdb/parallel/thread_context.hpp>

namespace duckdb {
struct RegisteredLoggingContext;
class ColumnDataCollection;

// Interface for writing log entries
class LogStorage {
public:
	explicit LogStorage() {
	}
	virtual ~LogStorage() = default;

	//! WRITING
	virtual void WriteLogEntry(timestamp_t timestamp, LogLevel level, const string &log_type, const string &log_message,
	                           const RegisteredLoggingContext &context) = 0;
	virtual void WriteLogEntries(DataChunk &chunk, const RegisteredLoggingContext &context) = 0;
	virtual void Flush() = 0;
	//! Registers a logging context. TOOD: remove?
	virtual void WriteLoggingContext(RegisteredLoggingContext &context) {

	};

	//! READING (OPTIONAL)
	virtual bool IsInternal() {
		return false;
	}
	virtual ColumnDataCollection &GetEntries() {
		throw NotImplementedException("Can not call GetEntries on LogStorage of this type");
	}
	virtual ColumnDataCollection &GetContexts() {
		throw NotImplementedException("Can not call GetContexts on LogStorage of this type");
	}
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
	void WriteLoggingContext(RegisteredLoggingContext &context) override;
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
	void WriteLoggingContext(RegisteredLoggingContext &context) override;

	//! LogStorage API: READING
	bool IsInternal() override;
	ColumnDataCollection &GetEntries() override;
	ColumnDataCollection &GetContexts() override;

protected:
	//! Internal log entry storage
	unique_ptr<ColumnDataCollection> log_entries;
	unique_ptr<ColumnDataCollection> log_contexts;

	// Cache for direct logging
	unique_ptr<DataChunk> entry_buffer;
	unique_ptr<DataChunk> log_context_buffer;
	idx_t max_buffer_size;
};

} // namespace duckdb
