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

struct CreateIndexInput {
	TableIOManager &table_io_manager;
	AttachedDatabase &db;
	IndexConstraintType constraint_type;
	const string &name;
	const vector<column_t> &column_ids;
	const vector<unique_ptr<Expression>> &unbound_expressions;
	const IndexStorageInfo &storage_info;
	const case_insensitive_map_t<Value> &options;

	CreateIndexInput(TableIOManager &table_io_manager, AttachedDatabase &db, IndexConstraintType constraint_type,
	                 const string &name, const vector<column_t> &column_ids,
	                 const vector<unique_ptr<Expression>> &unbound_expressions, const IndexStorageInfo &storage_info,
	                 const case_insensitive_map_t<Value> &options)
	    : table_io_manager(table_io_manager), db(db), constraint_type(constraint_type), name(name),
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

//! Global shared state for the whole index build pipeline
struct IndexBuildState {
	DUCKDB_API virtual ~IndexBuildState() = default;

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

//! Local sink state (optional)
struct IndexBuildSinkState {
	DUCKDB_API virtual ~IndexBuildSinkState() = default;

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

//! Local work state (optional)
struct IndexBuildWorkState {
	DUCKDB_API virtual ~IndexBuildWorkState() = default;

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

struct IndexBuildBindInput {
	ClientContext &context;
	DuckTableEntry &table;
	CreateIndexInfo &info;
	const vector<unique_ptr<Expression>> &expressions;
	const vector<column_t> storage_ids;
};

struct IndexBuildSortInput {
	optional_ptr<IndexBuildBindData> bind_data;
};

struct IndexBuildPrepareInput {
	ClientContext &context;
	optional_ptr<IndexBuildState> global_state;
	optional_ptr<IndexBuildBindData> bind_data;
};

// global state input
struct IndexBuildInitStateInput {
	optional_ptr<IndexBuildBindData> bind_data;
	ClientContext &context;
	DuckTableEntry &table;
	CreateIndexInfo &info;
	const vector<unique_ptr<Expression>> &expressions;
	const vector<column_t> storage_ids;

	const idx_t estimated_cardinality;
};

struct IndexBuildInitSinkInput {
	optional_ptr<IndexBuildBindData> bind_data;
	ClientContext &context;
	DuckTableEntry &table;
	// todo move to bind
	CreateIndexInfo &info;
	const vector<LogicalType> &data_types;
	const vector<unique_ptr<Expression>> &expressions;
	const vector<column_t> storage_ids;
};

struct IndexBuildSinkInput {
	// bind data
	optional_ptr<IndexBuildBindData> bind_data;
	// global State
	optional_ptr<IndexBuildState> global_state;
	// local sink state
	optional_ptr<IndexBuildSinkState> local_state;
	DuckTableEntry &table;
	CreateIndexInfo &info;
};

struct IndexBuildSinkCombineInput {
	// for combines we need local and global states
	optional_ptr<IndexBuildBindData> bind_data;
	optional_ptr<IndexBuildState> global_state;
	optional_ptr<IndexBuildSinkState> local_state;
	DuckTableEntry &table;
	CreateIndexInfo &info;
};

struct IndexBuildInitWorkInput {
	optional_ptr<IndexBuildBindData> bind_data;
	optional_ptr<IndexBuildState> global_state;
};

struct IndexBuildWorkInput {
	optional_ptr<IndexBuildState> global_state;
	// local work state
	optional_ptr<IndexBuildWorkState> local_state;

	const idx_t thread_id;
};

struct IndexBuildWorkCombineInput {
	optional_ptr<IndexBuildBindData> bind_data;
	// Or should this be; IndexBuildSinkState?
	optional_ptr<IndexBuildState> global_state;
	optional_ptr<IndexBuildWorkState> local_state;
};

struct IndexBuildFinalizeInput {
	// Explicit constructor to bind the reference
	IndexBuildFinalizeInput(IndexBuildState &gstate) : global_state(gstate) {
	}

	IndexBuildState &global_state;
	optional_ptr<ColumnDataCollection> collection;

	bool has_count = false;
	bool is_sorted = false;
	idx_t exact_count = 0;
};

typedef unique_ptr<IndexBuildBindData> (*index_build_bind_t)(IndexBuildBindInput &input);
typedef bool (*index_build_sort_t)(IndexBuildSortInput &input);

typedef unique_ptr<IndexBuildState> (*index_build_init_t)(IndexBuildInitStateInput &input);
typedef unique_ptr<IndexBuildSinkState> (*index_build_sink_init_t)(IndexBuildInitSinkInput &input);
typedef void (*index_build_sink_t)(IndexBuildSinkInput &state, DataChunk &key_chunk, DataChunk &row_chunk);
typedef void (*index_build_sink_combine_t)(IndexBuildSinkCombineInput &input);

typedef void (*index_build_prepare_t)(IndexBuildPrepareInput &input);

typedef unique_ptr<IndexBuildWorkState> (*index_build_work_init_t)(IndexBuildInitWorkInput &input);
typedef bool (*index_build_work_t)(IndexBuildWorkInput &input); // TODO: Figure out what makes sense to return here
typedef void (*index_build_work_combine_t)(IndexBuildWorkCombineInput &input);

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

//! An index "type"
class IndexType {
public:
	// The name of the index type
	string name;

	// Callbacks
	index_build_bind_t build_bind = nullptr;
	index_build_sort_t build_sort = nullptr;
	// former global init
	index_build_init_t build_init = nullptr;

	//! Sink phase; former local init
	index_build_sink_init_t build_sink_init = nullptr;
	index_build_sink_t build_sink = nullptr;

	//! Optional Callbacks
	// -----------------------------------------------------
	index_build_sink_combine_t build_sink_combine = nullptr;

	//! Midpoint
	index_build_prepare_t build_prepare = nullptr;

	//! Work phase
	index_build_work_init_t build_work_init = nullptr;
	index_build_work_t build_work = nullptr;
	index_build_work_combine_t build_work_combine = nullptr;

	// -----------------------------------------------------

	// Finalize
	index_build_finalize_t build_finalize = nullptr;

	//! Extra information for the index type
	shared_ptr<IndexTypeInfo> index_info = nullptr;

	index_build_plan_t create_plan = nullptr; // escape hatch for creating the physical plan
	index_create_function_t create_instance = nullptr;

	// function to create an instance of the index
};

} // namespace duckdb
