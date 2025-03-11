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

struct PlanIndexInput {
	ClientContext &context;
	LogicalCreateIndex &op;
	unique_ptr<PhysicalOperator> &table_scan;

	PlanIndexInput(ClientContext &context_p, LogicalCreateIndex &op_p, unique_ptr<PhysicalOperator> &table_scan_p)
	    : context(context_p), op(op_p), table_scan(table_scan_p) {
	}
};

typedef unique_ptr<BoundIndex> (*index_create_function_t)(CreateIndexInput &input);
typedef unique_ptr<PhysicalOperator> (*index_plan_function_t)(PlanIndexInput &input);

//! A index "type"
class IndexType {
public:
	// The name of the index type
	string name;

	// Callbacks
	index_plan_function_t create_plan = nullptr;
	index_create_function_t create_instance = nullptr;
};

} // namespace duckdb
