//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/copy_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/parser/statement/copy_statement.hpp"

namespace duckdb {

class Binder;
struct BoundStatement;
class ColumnDataCollection;
class ExecutionContext;

struct LocalFunctionData {
	virtual ~LocalFunctionData() = default;

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

struct GlobalFunctionData {
	virtual ~GlobalFunctionData() = default;

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

struct PreparedBatchData {
	virtual ~PreparedBatchData() = default;

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

struct CopyFunctionBindInput {
	explicit CopyFunctionBindInput(const CopyInfo &info_p) : info(info_p) {
	}

	const CopyInfo &info;

	string file_extension;
};

struct CopyToSelectInput {
	ClientContext &context;
	case_insensitive_map_t<vector<Value>> &options;
	vector<unique_ptr<Expression>> select_list;
	CopyToType copy_to_type;
};

enum class CopyFunctionExecutionMode { REGULAR_COPY_TO_FILE, PARALLEL_COPY_TO_FILE, BATCH_COPY_TO_FILE };

typedef BoundStatement (*copy_to_plan_t)(Binder &binder, CopyStatement &stmt);
typedef unique_ptr<FunctionData> (*copy_to_bind_t)(ClientContext &context, CopyFunctionBindInput &input,
                                                   const vector<string> &names, const vector<LogicalType> &sql_types);
typedef unique_ptr<LocalFunctionData> (*copy_to_initialize_local_t)(ExecutionContext &context, FunctionData &bind_data);
typedef unique_ptr<GlobalFunctionData> (*copy_to_initialize_global_t)(ClientContext &context, FunctionData &bind_data,
                                                                      const string &file_path);
typedef void (*copy_to_sink_t)(ExecutionContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                               LocalFunctionData &lstate, DataChunk &input);
typedef void (*copy_to_combine_t)(ExecutionContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                                  LocalFunctionData &lstate);
typedef void (*copy_to_finalize_t)(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate);

typedef void (*copy_to_serialize_t)(Serializer &serializer, const FunctionData &bind_data,
                                    const CopyFunction &function);

typedef unique_ptr<FunctionData> (*copy_to_deserialize_t)(Deserializer &deserializer, CopyFunction &function);

typedef unique_ptr<FunctionData> (*copy_from_bind_t)(ClientContext &context, CopyInfo &info,
                                                     vector<string> &expected_names,
                                                     vector<LogicalType> &expected_types);
typedef CopyFunctionExecutionMode (*copy_to_execution_mode_t)(bool preserve_insertion_order, bool supports_batch_index);

typedef unique_ptr<PreparedBatchData> (*copy_prepare_batch_t)(ClientContext &context, FunctionData &bind_data,
                                                              GlobalFunctionData &gstate,
                                                              unique_ptr<ColumnDataCollection> collection);
typedef void (*copy_flush_batch_t)(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                                   PreparedBatchData &batch);
typedef idx_t (*copy_desired_batch_size_t)(ClientContext &context, FunctionData &bind_data);

typedef bool (*copy_rotate_files_t)(FunctionData &bind_data, const optional_idx &file_size_bytes);

typedef bool (*copy_rotate_next_file_t)(GlobalFunctionData &gstate, FunctionData &bind_data,
                                        const optional_idx &file_size_bytes);

typedef vector<unique_ptr<Expression>> (*copy_to_select_t)(CopyToSelectInput &input);

enum class CopyFunctionReturnType : uint8_t { CHANGED_ROWS = 0, CHANGED_ROWS_AND_FILE_LIST = 1 };
vector<string> GetCopyFunctionReturnNames(CopyFunctionReturnType return_type);
vector<LogicalType> GetCopyFunctionReturnLogicalTypes(CopyFunctionReturnType return_type);

class CopyFunction : public Function { // NOLINT: work-around bug in clang-tidy
public:
	explicit CopyFunction(const string &name)
	    : Function(name), plan(nullptr), copy_to_select(nullptr), copy_to_bind(nullptr),
	      copy_to_initialize_local(nullptr), copy_to_initialize_global(nullptr), copy_to_sink(nullptr),
	      copy_to_combine(nullptr), copy_to_finalize(nullptr), execution_mode(nullptr), prepare_batch(nullptr),
	      flush_batch(nullptr), desired_batch_size(nullptr), rotate_files(nullptr), rotate_next_file(nullptr),
	      serialize(nullptr), deserialize(nullptr), copy_from_bind(nullptr) {
	}

	//! Plan rewrite copy function
	copy_to_plan_t plan;

	copy_to_select_t copy_to_select;
	copy_to_bind_t copy_to_bind;
	copy_to_initialize_local_t copy_to_initialize_local;
	copy_to_initialize_global_t copy_to_initialize_global;
	copy_to_sink_t copy_to_sink;
	copy_to_combine_t copy_to_combine;
	copy_to_finalize_t copy_to_finalize;
	copy_to_execution_mode_t execution_mode;

	copy_prepare_batch_t prepare_batch;
	copy_flush_batch_t flush_batch;
	copy_desired_batch_size_t desired_batch_size;

	copy_rotate_files_t rotate_files;
	copy_rotate_next_file_t rotate_next_file;

	copy_to_serialize_t serialize;
	copy_to_deserialize_t deserialize;

	copy_from_bind_t copy_from_bind;
	TableFunction copy_from_function;

	string extension;
};

} // namespace duckdb
