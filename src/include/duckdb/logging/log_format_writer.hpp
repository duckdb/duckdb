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

	//! Called once by the owning sink, lazily, once the stream is ready.
	//! write_header: sink decides this (e.g. FileLogSink checks GetFileSize() == 0);
	//! the writer has no visibility into whether its stream already has content.
	DUCKDB_API virtual void Initialize(bool write_header) = 0;
	DUCKDB_API virtual const string GetFormatName() const = 0;
	DUCKDB_API virtual void Truncate() = 0;
	DUCKDB_API virtual void UpdateConfig(const case_insensitive_map_t<Value> &config) = 0;
};

class CSVFormatWriter : public LogFormatWriter {
public:
	//! stream must outlive this writer — same lifetime contract CSVWriter has today.
	DUCKDB_API CSVFormatWriter(WriteStream &stream, LoggingTargetTable table, vector<Identifier> column_names);

	DUCKDB_API void WriteChunk(DataChunk &chunk) override;
	DUCKDB_API void Initialize(bool write_header) override;
	DUCKDB_API void Truncate() override;
	DUCKDB_API void UpdateConfig(const case_insensitive_map_t<Value> &config) override;
	DUCKDB_API const string GetFormatName() const override {
		return "csv";
	}

private:
	void ResetCastChunk();
	void ExecuteCast(DataChunk &chunk);
	void ApplyOptions(const CSVReaderOptions &reader_options, const CSVWriterOptions &writer_options);


	LoggingTargetTable table;
	vector<Identifier> column_names;

	unique_ptr<CSVWriter> writer;
	unique_ptr<DataChunk> cast_chunk;

	CSVReaderOptions reader_options;
	unique_ptr<CSVWriterOptions> writer_options;
};

} // namespace duckdb