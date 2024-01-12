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

namespace duckdb {

class Index;
enum class IndexConstraintType : uint8_t;
class Expression;
class TableIOManager;
class AttachedDatabase;
struct IndexStorageInfo;

typedef unique_ptr<Index> (*index_create_function_t)(const string &name,
                                                     const IndexConstraintType index_constraint_type,
                                                     const vector<column_t> &column_ids,
                                                     const vector<unique_ptr<Expression>> &unbound_expressions,
                                                     TableIOManager &table_io_manager, AttachedDatabase &db,
                                                     const IndexStorageInfo &storage_info);
//! A index "type"
class IndexType {
public:
	// The name of the index type
	string name;

	// Callbacks
	index_create_function_t create_instance;
};

} // namespace duckdb
