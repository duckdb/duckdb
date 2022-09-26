#include "arrow_stream_buffer.hpp"

#include <iostream>

/// File copied from
/// https://github.com/duckdb/duckdb-wasm/blob/0ad10e7db4ef4025f5f4120be37addc4ebe29618/lib/src/arrow_stream_buffer.cc
namespace duckdb {

/// Constructor
ArrowIPCStreamBuffer::ArrowIPCStreamBuffer() : schema_(nullptr), batches_(), is_eos_(false) {}
/// Decoded a schema
arrow::Status ArrowIPCStreamBuffer::OnSchemaDecoded(std::shared_ptr<arrow::Schema> s) {
    schema_ = s;
    return arrow::Status::OK();
}
/// Decoded a record batch
arrow::Status ArrowIPCStreamBuffer::OnRecordBatchDecoded(std::shared_ptr<arrow::RecordBatch> batch) {
    batches_.push_back(batch);
    return arrow::Status::OK();
}
/// Reached end of stream
arrow::Status ArrowIPCStreamBuffer::OnEOS() {
    is_eos_ = true;
    return arrow::Status::OK();
}

/// Constructor
ArrowIPCStreamBufferReader::ArrowIPCStreamBufferReader(std::shared_ptr<ArrowIPCStreamBuffer> buffer)
    : buffer_(buffer), next_batch_id_(0) {}

/// Get the schema
std::shared_ptr<arrow::Schema> ArrowIPCStreamBufferReader::schema() const { return buffer_->schema(); }
/// Read the next record batch in the stream. Return null for batch when reaching end of stream
arrow::Status ArrowIPCStreamBufferReader::ReadNext(std::shared_ptr<arrow::RecordBatch>* batch) {
    if (next_batch_id_ >= buffer_->batches().size()) {
        *batch = nullptr;
        return arrow::Status::OK();
    }
    *batch = buffer_->batches()[next_batch_id_++];
    return arrow::Status::OK();
}

/// Arrow array stream factory function
std::unique_ptr<duckdb::ArrowArrayStreamWrapper> ArrowIPCStreamBufferReader::CreateStream(uintptr_t buffer_ptr, ArrowStreamParameters &parameters) {
    assert(buffer_ptr != 0);
    auto buffer = reinterpret_cast<std::shared_ptr<ArrowIPCStreamBuffer>*>(buffer_ptr);
    auto reader = std::make_shared<ArrowIPCStreamBufferReader>(*buffer);

	// TODO we should handle pushdown here i think?
	// - Filter pushdown on top of IPC makes not a lot of sense as there are no statistics?
	// - Projection pushdown does make sense, we should be able to use the projected columns from the ArrowStreamParameters
	// 		- However im not sure where to pass this, the StreamDecoder does have a option for it, also the RecordBatchStreamReader,
	//        seems to, but im not really sure yet how this would work. We could re-decode the stream with the projection pushdown
	//        but it seems like there should be a better way?

    // Create arrow stream
    auto stream_wrapper = duckdb::make_unique<duckdb::ArrowArrayStreamWrapper>();
    stream_wrapper->arrow_array_stream.release = nullptr;
    auto maybe_ok = arrow::ExportRecordBatchReader(reader, &stream_wrapper->arrow_array_stream);
    if (!maybe_ok.ok()) {
        if (stream_wrapper->arrow_array_stream.release) {
            stream_wrapper->arrow_array_stream.release(&stream_wrapper->arrow_array_stream);
        }
        return nullptr;
    }

    // Release the stream
    return stream_wrapper;
}

void ArrowIPCStreamBufferReader::GetSchema(uintptr_t buffer_ptr, duckdb::ArrowSchemaWrapper& schema) {
    assert(buffer_ptr != 0);
    auto buffer = reinterpret_cast<std::shared_ptr<ArrowIPCStreamBuffer>*>(buffer_ptr);
    auto reader = std::make_shared<ArrowIPCStreamBufferReader>(*buffer);

    // Create arrow stream
    auto stream_wrapper = duckdb::make_unique<duckdb::ArrowArrayStreamWrapper>();
    stream_wrapper->arrow_array_stream.release = nullptr;
    auto maybe_ok = arrow::ExportRecordBatchReader(reader, &stream_wrapper->arrow_array_stream);
    if (!maybe_ok.ok()) {
        if (stream_wrapper->arrow_array_stream.release) {
            stream_wrapper->arrow_array_stream.release(&stream_wrapper->arrow_array_stream);
        }
        return;
    }

    // Pass ownership to caller
    stream_wrapper->arrow_array_stream.get_schema(&stream_wrapper->arrow_array_stream, &schema.arrow_schema);
}

/// Constructor
BufferingArrowIPCStreamDecoder::BufferingArrowIPCStreamDecoder(std::shared_ptr<ArrowIPCStreamBuffer> buffer) : buffer_(buffer), arrow::ipc::StreamDecoder(buffer) {}

}  // namespace duckdb
