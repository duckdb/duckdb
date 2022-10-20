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
#include "duckdb/common/arrow/arrow_appender.hpp"
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
		                                  ArrowIPCTableFunction::ArrowScanFunction, ArrowIPCTableFunction::ArrowScanBind,
		                                  ArrowTableFunction::ArrowScanInitGlobal, ArrowTableFunction::ArrowScanInitLocal);

		scan_arrow_ipc_func.cardinality = ArrowTableFunction::ArrowScanCardinality;
		scan_arrow_ipc_func.projection_pushdown = true;
		scan_arrow_ipc_func.filter_pushdown = false;

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
//		auto rows_per_thread = 1000000;

		// TODO: Can no longer set this limit?
		auto res = make_unique<ArrowIPCScanFunctionData>(stream_factory_produce, stream_factory_ptr);

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

	// TODO: cleanup: only difference is the ArrowToDuckDB call
	static void ArrowScanFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
		if (!data_p.local_state) {
			return;
		}
		auto &data = (ArrowScanFunctionData &)*data_p.bind_data;
		auto &state = (ArrowScanLocalState &)*data_p.local_state;
		auto &global_state = (ArrowScanGlobalState &)*data_p.global_state;

		//! Out of tuples in this chunk
		if (state.chunk_offset >= (idx_t)state.chunk->arrow_array.length) {
			if (!ArrowTableFunction::ArrowScanParallelStateNext(context, data_p.bind_data, state, global_state)) {
				return;
			}
		}
		int64_t output_size = MinValue<int64_t>(STANDARD_VECTOR_SIZE, state.chunk->arrow_array.length - state.chunk_offset);
		data.lines_read += output_size;
		output.SetCardinality(output_size);
		ArrowToDuckDB(state, data.arrow_convert_data, output, data.lines_read - output_size, false);
		output.Verify();
		state.chunk_offset += output.size();
	}
};


//! note: this is the number of vectors per chunk
static constexpr idx_t DEFAULT_CHUNK_SIZE = 120;

struct ToArrowIpcFunctionData : public TableFunctionData {
	ToArrowIpcFunctionData() {}
	idx_t chunk_size;
	idx_t current_count = 0;
	bool finished = false;
	bool sent_schema = false;
	shared_ptr<arrow::Schema> schema;
	unique_ptr<ArrowAppender> appender;
	vector<string> input_table_names;
};

// TODO allow omitting argument to use default
static unique_ptr<FunctionData>
ToArrowIpcBind(ClientContext &context, TableFunctionBindInput &input,
                vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_unique<ToArrowIpcFunctionData>();

//	if (input.inputs.size() > 1) {
//		result->chunk_size = input.inputs[1].GetValue<int>() * STANDARD_VECTOR_SIZE;
//	} else {
		result->chunk_size = DEFAULT_CHUNK_SIZE * STANDARD_VECTOR_SIZE;
//	}

	result->input_table_names = input.input_table_names;
	return_types.emplace_back(LogicalType::BLOB);
	names.emplace_back("ipc");
	return move(result);
}

static OperatorResultType ToArrowIpcFunction(ExecutionContext &context, TableFunctionInput &data_p, DataChunk &input,
                          DataChunk &output) {
	std::shared_ptr<arrow::Buffer> arrow_serialized_ipc_buffer;
	auto &data = (ToArrowIpcFunctionData &)*data_p.bind_data;

	if (data.finished) {
		output.SetCardinality(0);
		return OperatorResultType::FINISHED;
	}

	if (!data.sent_schema) {
		// First time we send the serialized schema only
		auto tz = context.client.GetClientProperties().timezone;
		auto types = input.GetTypes();

		ArrowSchema schema;
		ArrowConverter::ToArrowSchema(&schema, types, data.input_table_names, tz);

		// Serialize the schema
		data.schema = arrow::ImportSchema(&schema).ValueOrDie();
		auto result = arrow::ipc::SerializeSchema(*data.schema);
		arrow_serialized_ipc_buffer = result.ValueOrDie();

	} else {
		if (!data.appender) {
			data.appender = make_unique<ArrowAppender>(input.GetTypes(), data.chunk_size);
		}

		// Append input chunk
		data.appender->Append(input);
		data.current_count += input.size();

		// If chunk size is reached, we can flush to IPC blob
		if (data.current_count >= data.chunk_size) {
			// Construct record batch from DataChunk
			ArrowArray arr = data.appender->Finalize();
			auto record_batch = arrow::ImportRecordBatch(&arr, data.schema).ValueOrDie();

			// Serialize recordbatch
			auto options = arrow::ipc::IpcWriteOptions::Defaults();
			auto result = arrow::ipc::SerializeRecordBatch(*record_batch, options);
			arrow_serialized_ipc_buffer = result.ValueOrDie();

			// Reset appender
			data.appender.reset();
			data.current_count = 0;
		} else {
			return OperatorResultType::NEED_MORE_INPUT;
		}
	}

	// TODO clean up
	auto wrapped_buffer = make_buffer<ArrowStringVectorBuffer>(arrow_serialized_ipc_buffer);
	auto& vector = output.data[0];
	StringVector::AddBuffer(vector, wrapped_buffer);
	auto data_ptr = (string_t*)vector.GetData();
	*data_ptr = string_t((const char*)arrow_serialized_ipc_buffer->data(), arrow_serialized_ipc_buffer->size());
	output.SetCardinality(1);

	if (!data.sent_schema) {
		data.sent_schema = true;
		return OperatorResultType::HAVE_MORE_OUTPUT;
	} else {
		return OperatorResultType::NEED_MORE_INPUT;
	}
}

static OperatorFinalizeResultType ToArrowIpcFunctionFinal(ExecutionContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = (ToArrowIpcFunctionData &)*data_p.bind_data;
	std::shared_ptr<arrow::Buffer> arrow_serialized_ipc_buffer;

	// TODO clean up
	if (data.appender) {
		ArrowArray arr = data.appender->Finalize();
		auto record_batch = arrow::ImportRecordBatch(&arr, data.schema).ValueOrDie();

		// Serialize recordbatch
		auto options = arrow::ipc::IpcWriteOptions::Defaults();
		auto result = arrow::ipc::SerializeRecordBatch(*record_batch, options);
		arrow_serialized_ipc_buffer = result.ValueOrDie();

		auto wrapped_buffer = make_buffer<ArrowStringVectorBuffer>(arrow_serialized_ipc_buffer);
		auto& vector = output.data[0];
		StringVector::AddBuffer(vector, wrapped_buffer);
		auto data_ptr = (string_t*)vector.GetData();
		*data_ptr = string_t((const char*)arrow_serialized_ipc_buffer->data(), arrow_serialized_ipc_buffer->size());
		output.SetCardinality(1);
		data.appender.reset();
	}

	return OperatorFinalizeResultType::FINISHED;
}

static void LoadInternal(DatabaseInstance &instance) {
	Connection con(instance);
	con.BeginTransaction();
	auto &catalog = Catalog::GetCatalog(*con.context);

	// test/sql/function/generic/test_table_param.test
	TableFunction get_arrow_ipc_func("get_arrow_ipc", {LogicalType::TABLE},
	                                 nullptr, ToArrowIpcBind);
	get_arrow_ipc_func.in_out_function = ToArrowIpcFunction;
	get_arrow_ipc_func.in_out_function_final = ToArrowIpcFunctionFinal;

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
