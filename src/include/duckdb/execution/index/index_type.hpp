//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/index_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"

namespace duckdb {

class BoundIndex;
class PhysicalOperator;
class LogicalCreateIndex;
enum class IndexConstraintType : uint8_t;
class Expression;
class TableIOManager;
class AttachedDatabase;
struct IndexStorageInfo;
class ClientContext;

struct CreateIndexInput {
	ClientContext &context;
	TableIOManager &table_io_manager;
	AttachedDatabase &db;
	IndexConstraintType constraint_type;
	const string &name;
	const vector<column_t> &column_ids;
	const vector<unique_ptr<Expression>> &unbound_expressions;
	const IndexStorageInfo &storage_info;
	const case_insensitive_map_t<Value> &options;

	CreateIndexInput(ClientContext &context, TableIOManager &table_io_manager, AttachedDatabase &db,
	                 IndexConstraintType constraint_type, const string &name, const vector<column_t> &column_ids,
	                 const vector<unique_ptr<Expression>> &unbound_expressions, const IndexStorageInfo &storage_info,
	                 const case_insensitive_map_t<Value> &options)
	    : context(context), table_io_manager(table_io_manager), db(db), constraint_type(constraint_type), name(name),
	      column_ids(column_ids), unbound_expressions(unbound_expressions), storage_info(storage_info),
	      options(options) {};
};

struct IndexTypeInfo {
	DUCKDB_API virtual ~IndexTypeInfo() = default;

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

struct IndexBuildBindData {
	DUCKDB_API virtual ~IndexBuildBindData() = default;

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

struct IndexBuildGlobalState {
	DUCKDB_API virtual ~IndexBuildGlobalState() = default;

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

struct IndexBuildLocalState {
	DUCKDB_API virtual ~IndexBuildLocalState() = default;

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

class DuckTableEntry;

struct IndexBuildInitLocalStateInput {
	optional_ptr<IndexBuildBindData> bind_data;
	ClientContext &context;
	DuckTableEntry &table;
	CreateIndexInfo &info;
	const vector<unique_ptr<Expression>> &expressions;
	const vector<column_t> storage_ids;
};

struct IndexBuildInitGlobalStateInput {
	optional_ptr<IndexBuildBindData> bind_data;
	ClientContext &context;
	DuckTableEntry &table;
	CreateIndexInfo &info;
	const vector<unique_ptr<Expression>> &expressions;
	const vector<column_t> storage_ids;
};

struct IndexBuildBindInput {
	ClientContext &context;
	DuckTableEntry &table;
	CreateIndexInfo &info;
	const vector<unique_ptr<Expression>> &expressions;
};

struct IndexBuildSortInput {
	optional_ptr<IndexBuildBindData> bind_data;
};

struct IndexBuildCombineInput {
	optional_ptr<IndexBuildBindData> bind_data;
	IndexBuildGlobalState &global_state;
	IndexBuildLocalState &local_state;
	DuckTableEntry &table;
	CreateIndexInfo &info;
};

struct IndexBuildSinkInput {
	optional_ptr<IndexBuildBindData> bind_data;
	IndexBuildGlobalState &global_state;
	IndexBuildLocalState &local_state;
	DuckTableEntry &table;
	CreateIndexInfo &info;
};

struct IndexBuildFinalizeInput {
	IndexBuildGlobalState &global_state;
};

typedef unique_ptr<IndexBuildBindData> (*index_build_bind_t)(IndexBuildBindInput &input);
typedef bool (*index_build_sort_t)(IndexBuildSortInput &input);
typedef unique_ptr<IndexBuildGlobalState> (*index_build_global_init_t)(IndexBuildInitGlobalStateInput &input);
typedef unique_ptr<IndexBuildLocalState> (*index_build_local_init_t)(IndexBuildInitLocalStateInput &input);
typedef void (*index_build_sink_t)(IndexBuildSinkInput &input, DataChunk &key_chunk, DataChunk &row_chunk);
typedef void (*index_build_combine_t)(IndexBuildCombineInput &input);
typedef unique_ptr<BoundIndex> (*index_build_finalize_t)(IndexBuildFinalizeInput &input);

struct PlanIndexInput {
	ClientContext &context;
	LogicalCreateIndex &op;
	PhysicalPlanGenerator &planner;
	PhysicalOperator &table_scan;
	shared_ptr<IndexTypeInfo> index_info;

	PlanIndexInput(ClientContext &context_p, LogicalCreateIndex &op_p, PhysicalPlanGenerator &planner,
	               PhysicalOperator &table_scan_p, shared_ptr<IndexTypeInfo> index_info_p = nullptr)
	    : context(context_p), op(op_p), planner(planner), table_scan(table_scan_p),
	      index_info(std::move(index_info_p)) {
	}
};

typedef unique_ptr<BoundIndex> (*index_create_function_t)(CreateIndexInput &input);
typedef PhysicalOperator &(*index_build_plan_t)(PlanIndexInput &input);

//! A index "type"
class IndexType {
public:
	// The name of the index type
	string name;

	// Callbacks
	index_build_bind_t build_bind = nullptr;
	index_build_sort_t build_sort = nullptr;
	index_build_global_init_t build_global_init = nullptr;
	index_build_local_init_t build_local_init = nullptr;
	index_build_sink_t build_sink = nullptr;
	index_build_combine_t build_combine = nullptr;
	index_build_finalize_t build_finalize = nullptr;

	//! Extra information for the index type
	shared_ptr<IndexTypeInfo> index_info = nullptr;

	index_build_plan_t create_plan = nullptr;          // escape hatch for creating the physical plan
	index_create_function_t create_instance = nullptr; // function to create an instance of the index
};

} // namespace duckdb
