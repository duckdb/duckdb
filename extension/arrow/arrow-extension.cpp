#define DUCKDB_EXTENSION_MAIN

// TODO clean this
#include "arrow/array/array_dict.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/buffer.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/options.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/type_fwd.h"
#include "arrow/ipc/writer.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type_fwd.h"
#include "arrow/c/bridge.h"
#include "arrow-extension.hpp"
#include "arrow_stream_buffer.hpp"
#include <iostream>

#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/arrow/result_arrow_wrapper.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/function/table/arrow.hpp"
#endif

namespace duckdb {

class ArrowStringVectorBuffer : public VectorBuffer {
public:
	explicit ArrowStringVectorBuffer(std::shared_ptr<arrow::Buffer> buffer_p)
	    : VectorBuffer(VectorBufferType::OPAQUE_BUFFER), buffer(move(buffer_p)) {
	}
private:
	std::shared_ptr<arrow::Buffer> buffer;
};

struct ArrowIPCScanFunctionData : public ArrowScanFunctionData {
public:
	using ArrowScanFunctionData::ArrowScanFunctionData;
	unique_ptr<BufferingArrowIPCStreamDecoder> stream_decoder = nullptr; // TODO do we even need this?
};

// IPC Table scan is identical to regular arrow scan except we need to produce the stream from the ipc pointers beforehand
struct ArrowIPCTableFunction : public ArrowTableFunction {
public:
	static TableFunction GetFunction() {
		child_list_t<LogicalType> make_buffer_struct_children {
		    {"ptr", LogicalType::UBIGINT}, {"size", LogicalType::UBIGINT}};

		TableFunction scan_arrow_ipc_func("scan_arrow_ipc",
		                                  {LogicalType::LIST(LogicalType::STRUCT(make_buffer_struct_children))},
		                                  ArrowTableFunction::ArrowScanFunction, ArrowIPCTableFunction::ArrowScanBind,
		                                  ArrowTableFunction::ArrowScanInitGlobal, ArrowTableFunction::ArrowScanInitLocal);

		scan_arrow_ipc_func.cardinality = ArrowTableFunction::ArrowScanCardinality;
		scan_arrow_ipc_func.projection_pushdown = true;
		scan_arrow_ipc_func.filter_pushdown = false;
		scan_arrow_ipc_func.table_scan_progress = ArrowTableFunction::ArrowProgress;

		return scan_arrow_ipc_func;
	}

	static void RegisterFunction(BuiltinFunctions &set) {
		set.AddFunction(GetFunction());
	}

private:
	// need this to parse different args and produce pointer to streamreader myself
	static unique_ptr<FunctionData> ArrowScanBind(ClientContext &context, TableFunctionBindInput &input,
	                                              vector<LogicalType> &return_types, vector<string> &names) {
		auto stream_decoder = make_unique<BufferingArrowIPCStreamDecoder>();

		auto buffer_ptr_list = ListValue::GetChildren(input.inputs[0]);
		for (auto& buffer_ptr_struct : buffer_ptr_list) {
			auto unpacked = StructValue::GetChildren(buffer_ptr_struct);
			uint64_t ptr = unpacked[0].GetValue<uint64_t>();
			uint64_t size = unpacked[1].GetValue<uint64_t>();

			// Feed stream into decoder
			auto res = stream_decoder->Consume((const uint8_t *)ptr, size);

			if (!res.ok()) {
				throw IOException("Invalid IPC stream");
			}
		}

		if (!stream_decoder->buffer()->is_eos()) {
			throw IOException("IPC buffers passed to arrow scan should contain entire stream");
		}

		// These are the params I need to produce from the ipc buffers using the WebDB.cc code
		auto stream_factory_ptr = (uintptr_t)&stream_decoder->buffer();
		auto stream_factory_produce = (stream_factory_produce_t)&ArrowIPCStreamBufferReader::CreateStream;
		auto stream_factory_get_schema = (stream_factory_get_schema_t)&ArrowIPCStreamBufferReader::GetSchema;
		auto rows_per_thread = 1000000;

		auto res = make_unique<ArrowIPCScanFunctionData>(rows_per_thread, stream_factory_produce, stream_factory_ptr);

		// TODO Everything below this is identical to the bind in duckdb/src/function/table/arrow.cpp

		// Store decoder
		res->stream_decoder = std::move(stream_decoder);

		auto &data = *res;
		stream_factory_get_schema(stream_factory_ptr, data.schema_root);
		for (idx_t col_idx = 0; col_idx < (idx_t)data.schema_root.arrow_schema.n_children; col_idx++) {
			auto &schema = *data.schema_root.arrow_schema.children[col_idx];
			if (!schema.release) {
				throw InvalidInputException("arrow_scan: released schema passed");
			}
			if (schema.dictionary) {
				res->arrow_convert_data[col_idx] =
				    make_unique<ArrowConvertData>(GetArrowLogicalType(schema, res->arrow_convert_data, col_idx));
				return_types.emplace_back(GetArrowLogicalType(*schema.dictionary, res->arrow_convert_data, col_idx));
			} else {
				return_types.emplace_back(GetArrowLogicalType(schema, res->arrow_convert_data, col_idx));
			}
			auto format = string(schema.format);
			auto name = string(schema.name);
			if (name.empty()) {
				name = string("v") + to_string(col_idx);
			}
			names.push_back(name);
		}
		RenameArrowColumns(names);
		return move(res);
	}
};


//! note: this is the number of vectors per chunk
static constexpr idx_t DEFAULT_CHUNK_SIZE = 120;

struct ToArrowIpcFunctionData : public TableFunctionData {
	ToArrowIpcFunctionData() {}
	string query;
	idx_t chunk_size;
	bool finished = false;
	std::shared_ptr<arrow::RecordBatchReader> record_batch_reader = nullptr;
	shared_ptr<arrow::Schema> schema;
};

// TODO allow omitting argument to use default
static unique_ptr<FunctionData>
ToArrowIpcBind(ClientContext &context, TableFunctionBindInput &input,
                vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_unique<ToArrowIpcFunctionData>();
	result->query = input.inputs[0].ToString();

	if (input.inputs.size() > 1) {
		result->chunk_size = input.inputs[1].GetValue<int>() * STANDARD_VECTOR_SIZE;
	} else {
		result->chunk_size = DEFAULT_CHUNK_SIZE * STANDARD_VECTOR_SIZE;
	}

	return_types.emplace_back(LogicalType::BLOB);
	names.emplace_back("IPC stream blob");
	return move(result);
}

static void ToArrowIpcFunction(ClientContext &context, TableFunctionInput &data_p,
                          DataChunk &output) {
	std::shared_ptr<arrow::Buffer> arrow_serialized_ipc_buffer;
	auto &data = (ToArrowIpcFunctionData &)*data_p.bind_data;
	if (data.finished) {
		output.SetCardinality(0);
		return;
	}
	if (!data.record_batch_reader) {
		// First get the duckdb query result
		auto new_conn = Connection(*context.db);
		auto query_result = new_conn.SendQuery(data.query);

		// Then convert query result into a RecordBatchReader
		ResultArrowArrayStreamWrapper *result_stream = new ResultArrowArrayStreamWrapper(move(query_result), data.chunk_size);
		data.record_batch_reader = arrow::ImportRecordBatchReader(&result_stream->stream).ValueOrDie();

		// Serialize the schema
		auto schema = data.record_batch_reader->schema();
		auto result = arrow::ipc::SerializeSchema(*schema);
		arrow_serialized_ipc_buffer = result.ValueOrDie();
	} else {
		auto batch = data.record_batch_reader->Next().ValueOrDie();

		if (!batch) {
			output.SetCardinality(0);
			data.finished = true;
			return;
		}

		// Serialize recordbatch
		auto options = arrow::ipc::IpcWriteOptions::Defaults();
		auto result = arrow::ipc::SerializeRecordBatch(*batch, options);
		arrow_serialized_ipc_buffer = result.ValueOrDie();
	}

	output.SetCardinality(1);

	// TODO: benchmark difference here
	if (true) {
		auto wrapped_buffer = make_buffer<ArrowStringVectorBuffer>(arrow_serialized_ipc_buffer);

		// Instead of calling setvalue which copies the blob, we need to move it into there
		auto& vector = output.data[0];
		StringVector::AddBuffer(vector, wrapped_buffer);
		auto data_ptr = (string_t*)vector.GetData();
		*data_ptr = string_t((const char*)arrow_serialized_ipc_buffer->data(), arrow_serialized_ipc_buffer->size());
	} else {
		output.SetValue(0, 0, Value::BLOB((duckdb::const_data_ptr_t)arrow_serialized_ipc_buffer->data(), arrow_serialized_ipc_buffer->size()));
	}
}

static void LoadInternal(DatabaseInstance &instance) {
	Connection con(instance);
	con.BeginTransaction();
	auto &catalog = Catalog::GetCatalog(*con.context);

	// TODO refactor to take a Query as a parameter instead of a String. There's a way to do this, see:
	// test/sql/function/generic/test_table_param.test
	TableFunction get_arrow_ipc_func("get_arrow_ipc", {LogicalType::VARCHAR, LogicalType::INTEGER},
	                                 ToArrowIpcFunction, ToArrowIpcBind);
	CreateTableFunctionInfo get_arrow_ipc_info(get_arrow_ipc_func);
	catalog.CreateTableFunction(*con.context, &get_arrow_ipc_info);

	TableFunction scan_arrow_ipc = ArrowIPCTableFunction::GetFunction();
	CreateTableFunctionInfo scan_arrow_ipc_info(scan_arrow_ipc);
	catalog.CreateTableFunction(*con.context, &scan_arrow_ipc_info);

	con.Commit();
}

void ArrowExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string ArrowExtension::Name() {
	return "arrow";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void arrow_init(duckdb::DatabaseInstance &db) {
	LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *arrow_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
