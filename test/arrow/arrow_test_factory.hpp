#pragma once
#include "arrow/record_batch.h"
#include "duckdb/common/arrow_wrapper.hpp"
#include "arrow/array.h"
#include "catch.hpp"

constexpr size_t ARRAY_SIZE = 1024;

#define REQUIRE_RESULT(OUT, IN)                                                                                        \
	REQUIRE(IN.ok());                                                                                                  \
	OUT = IN.ValueUnsafe();

struct SimpleFactory {
	/// All materialized batches
	arrow::RecordBatchVector batches;
	/// The schema
	std::shared_ptr<arrow::Schema> schema;

	SimpleFactory(arrow::RecordBatchVector batches, std::shared_ptr<arrow::Schema> schema)
	    : batches(std::move(batches)), schema(std::move(schema)) {
	}

	static std::unique_ptr<duckdb::ArrowArrayStreamWrapper> CreateStream(uintptr_t this_ptr) {
		//! Create a new batch reader
		auto &factory = *reinterpret_cast<SimpleFactory *>(this_ptr); //! NOLINT
		REQUIRE_RESULT(auto reader, arrow::RecordBatchReader::Make(factory.batches, factory.schema));

		//! Export C arrow stream stream
		auto stream_wrapper = duckdb::make_unique<duckdb::ArrowArrayStreamWrapper>();
		stream_wrapper->arrow_array_stream.release = nullptr;
		auto maybe_ok = arrow::ExportRecordBatchReader(reader, &stream_wrapper->arrow_array_stream);
		if (!maybe_ok.ok()) {
			if (stream_wrapper->arrow_array_stream.release) {
				stream_wrapper->arrow_array_stream.release(&stream_wrapper->arrow_array_stream);
			}
			return nullptr;
		}

		//! Pass ownership to caller
		return stream_wrapper;
	}
};