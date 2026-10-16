//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/logging/log_format_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/identifier.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_reader_options.hpp"
#include "duckdb/common/csv_writer.hpp"

namespace duckdb {

enum class LoggingTargetTable : uint8_t;

class DataChunk;
class WriteStream;

//! Serializes log chunks into a stream that is owned by the log sink
class LogFormatWriter {
public:
	DUCKDB_API virtual ~LogFormatWriter() = default;

	//! Binds the writer to a stream, which must outlive the writer or the next Initialize. write_header is set by the
	//! sink (e.g. if the file is empty)
	DUCKDB_API virtual void Initialize(WriteStream &stream, bool write_header) = 0;
	//! Casts chunk, writes it, and flushes
	DUCKDB_API virtual void WriteChunk(DataChunk &chunk) = 0;
	//! Called after the sink truncated the stream: discards the formatting state and writes the header again
	DUCKDB_API virtual void Reset() = 0;
	//! Applies the format options in config and removes them from it. Returns whether any option was applied
	DUCKDB_API virtual bool UpdateConfig(case_insensitive_map_t<Value> &config) = 0;
	DUCKDB_API virtual const string GetFormatName() const = 0;
};

class CSVFormatWriter : public LogFormatWriter {
public:
	DUCKDB_API CSVFormatWriter(LoggingTargetTable table, const string &delimiter, CSVNewLineMode newline_mode);

	DUCKDB_API void Initialize(WriteStream &stream, bool write_header) override;
	DUCKDB_API void WriteChunk(DataChunk &chunk) override;
	DUCKDB_API void Reset() override;
	DUCKDB_API bool UpdateConfig(case_insensitive_map_t<Value> &config) override;
	DUCKDB_API const string GetFormatName() const override {
		return "csv";
	}

private:
	void ResetCastChunk(idx_t capacity);
	void ExecuteCast(DataChunk &chunk);
	void SetDelimiter(const string &delimiter);
	void ApplyOptions();

	LoggingTargetTable table;
	vector<Identifier> column_names;

	//! Created by Initialize, writes to the stream bound there
	unique_ptr<CSVWriter> writer;
	unique_ptr<DataChunk> cast_chunk;
	idx_t cast_chunk_capacity = 0;

	CSVReaderOptions reader_options;
	//! Derived from reader_options, rebuilt whenever the delimiter changes
	unique_ptr<CSVWriterOptions> writer_options;
};

} // namespace duckdb
