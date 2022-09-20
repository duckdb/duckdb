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

#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/arrow/result_arrow_wrapper.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#endif

namespace duckdb {

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

	// TODO this copy
	output.SetValue(0, 0, Value::BLOB((duckdb::const_data_ptr_t)arrow_serialized_ipc_buffer->data(), arrow_serialized_ipc_buffer->size()));
}

static void LoadInternal(DatabaseInstance &instance) {
	Connection con(instance);
	con.BeginTransaction();
	auto &catalog = Catalog::GetCatalog(*con.context);

	TableFunction get_arrow_ipc_func("get_arrow_ipc", {LogicalType::VARCHAR, LogicalType::INTEGER},
	                                 ToArrowIpcFunction, ToArrowIpcBind);
	CreateTableFunctionInfo get_arrow_ipc_info(get_arrow_ipc_func);
	catalog.CreateTableFunction(*con.context, &get_arrow_ipc_info);

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
