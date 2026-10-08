//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/logging/log_format_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/logging/logging.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/serializer/write_stream.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/common/identifier.hpp"
#include "duckdb/common/types/column/column_data_scan_states.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_reader_options.hpp"
#include "duckdb/common/csv_writer.hpp"

namespace duckdb {

enum class LoggingTargetTable : uint8_t;

class DataChunk;
class WriteStream;
class CSVWriter;
struct CSVReaderOptions;
struct CSVWriterOptions;

class LogFormatWriter {
public:
	DUCKDB_API virtual ~LogFormatWriter() = default;

	//! Casts chunk, writes it, and flushes. Stream is already bound at construction.
	DUCKDB_API virtual void WriteChunk(DataChunk &chunk) = 0;

	//! Called once the stream is ready, write_header is set by the sink (e.g. if the file is empty)
	DUCKDB_API virtual void Initialize(bool write_header) = 0;
	DUCKDB_API virtual const string GetFormatName() const = 0;
	DUCKDB_API virtual void Truncate() = 0;
	DUCKDB_API virtual void UpdateConfig(const case_insensitive_map_t<Value> &config) = 0;
};

class CSVFormatWriter : public LogFormatWriter {
public:
	//! The stream must outlive this writer
	DUCKDB_API CSVFormatWriter(WriteStream &stream, LoggingTargetTable table, vector<Identifier> column_names,
	                           const string &delimiter, CSVNewLineMode newline_mode);

	DUCKDB_API void WriteChunk(DataChunk &chunk) override;
	DUCKDB_API void Initialize(bool write_header) override;
	DUCKDB_API void Truncate() override;
	DUCKDB_API void UpdateConfig(const case_insensitive_map_t<Value> &config) override;
	DUCKDB_API const string GetFormatName() const override {
		return "csv";
	}

private:
	void ResetCastChunk(idx_t capacity);
	void ExecuteCast(DataChunk &chunk);
	void ApplyOptions(const CSVReaderOptions &reader_options, const CSVWriterOptions &writer_options);

	LoggingTargetTable table;
	vector<Identifier> column_names;

	unique_ptr<CSVWriter> writer;
	unique_ptr<DataChunk> cast_chunk;
	idx_t cast_chunk_capacity = 0;

	CSVReaderOptions reader_options;
	unique_ptr<CSVWriterOptions> writer_options;
};

} // namespace duckdb
