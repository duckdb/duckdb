#pragma once

#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "arrow/ipc/reader.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/c/bridge.h"

#include <string>
#include <unordered_map>
#include <iostream>

/// File copied from
/// https://github.com/duckdb/duckdb-wasm/blob/0ad10e7db4ef4025f5f4120be37addc4ebe29618/lib/include/duckdb/web/arrow_stream_buffer.h

namespace duckdb {

struct ArrowIPCStreamBuffer : public arrow::ipc::Listener {
protected:
	/// The schema
	std::shared_ptr<arrow::Schema> schema_;
	/// The batches
	std::vector<std::shared_ptr<arrow::RecordBatch>> batches_;
	/// Is eos?
	bool is_eos_;

	/// Decoded a record batch
	arrow::Status OnSchemaDecoded(std::shared_ptr<arrow::Schema> schema);
	/// Decoded a record batch
	arrow::Status OnRecordBatchDecoded(std::shared_ptr<arrow::RecordBatch> record_batch);
	/// Reached end of stream
	arrow::Status OnEOS();

public:
	/// Constructor
	ArrowIPCStreamBuffer();

	/// Is end of stream?
	bool is_eos() const {
		return is_eos_;
	}
	/// Return the schema
	std::shared_ptr<arrow::Schema> &schema() {
		return schema_;
	}
	/// Return the batches
	std::vector<std::shared_ptr<arrow::RecordBatch>> &batches() {
		return batches_;
	}
};

struct ArrowIPCStreamBufferReader : public arrow::RecordBatchReader {
protected:
	/// The buffer
	std::shared_ptr<ArrowIPCStreamBuffer> buffer_;
	/// The batch index
	size_t next_batch_id_;

public:
	/// Constructor
	ArrowIPCStreamBufferReader(std::shared_ptr<ArrowIPCStreamBuffer> buffer);
	/// Destructor
	~ArrowIPCStreamBufferReader() = default;

	/// Get the schema
	std::shared_ptr<arrow::Schema> schema() const override;
	/// Read the next record batch in the stream. Return null for batch when reaching end of stream
	arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch> *batch) override;

	/// Create arrow array stream wrapper
	static std::unique_ptr<ArrowArrayStreamWrapper> CreateStream(uintptr_t buffer_ptr,
	                                                             ArrowStreamParameters &parameters);
	/// Create arrow array stream wrapper
	static void GetSchema(uintptr_t buffer_ptr, ArrowSchemaWrapper &schema);
};

struct BufferingArrowIPCStreamDecoder : public arrow::ipc::StreamDecoder {
protected:
	/// The buffer
	std::shared_ptr<ArrowIPCStreamBuffer> buffer_;

public:
	/// Constructor
	BufferingArrowIPCStreamDecoder(
	    std::shared_ptr<ArrowIPCStreamBuffer> buffer = std::make_shared<ArrowIPCStreamBuffer>());
	/// Get the buffer
	std::shared_ptr<ArrowIPCStreamBuffer> &buffer() {
		return buffer_;
	}
};

} // namespace duckdb
